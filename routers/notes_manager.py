import sqlite3
import os
from datetime import datetime
from fastapi import APIRouter, HTTPException

# ADMIN_PIN aus der Portainer Umgebungsvariable
ADMIN_PIN = os.getenv("ADMIN_PIN") 

class NotesManager:
    def __init__(self, db_path='data/notes.db'):
        # Erstellt den Ordner, falls er im Volume noch nicht existiert
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = db_path
        self._init_db()

    def _get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        """Erstellt Tabellen und fügt fehlende Spalten automatisch hinzu."""
        with self._get_connection() as conn:
            # Basistabellen
            conn.execute('CREATE TABLE IF NOT EXISTS categories (name TEXT PRIMARY KEY, password TEXT)')
            conn.execute('''CREATE TABLE IF NOT EXISTS notes 
                            (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, content TEXT, 
                             category TEXT, color TEXT DEFAULT "default")''')
            
            # Sicherheits-Check: Spalten einzeln nachrüsten, falls sie fehlen
            cursor = conn.execute("PRAGMA table_info(notes)")
            cols = [c[1] for c in cursor.fetchall()]
            
            upgrades = [
                ("pinned", "INTEGER DEFAULT 0"),
                ("isTodo", "INTEGER DEFAULT 0"),
                ("completed", "INTEGER DEFAULT 0"),
                ("author", "TEXT DEFAULT ''"),
                ("created_at", "TEXT DEFAULT ''")
            ]
            
            for col_name, col_type in upgrades:
                if col_name not in cols:
                    try:
                        conn.execute(f"ALTER TABLE notes ADD COLUMN {col_name} {col_type}")
                    except:
                        pass # Spalte existiert evtl. schon
            
            conn.execute("INSERT OR IGNORE INTO categories (name) VALUES ('Allgemein')")
            conn.commit()

db = NotesManager()
router = APIRouter()

@router.get("/api/notes")
async def get_notes():
    try:
        with db._get_connection() as conn:
            res = conn.execute("SELECT * FROM notes ORDER BY pinned DESC, id DESC").fetchall()
            return [dict(r) for r in res]
    except Exception as e:
        return []

@router.post("/api/notes")
async def add_note(n: dict):
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    with db._get_connection() as conn:
        cursor = conn.execute(
            "INSERT INTO notes (title, content, category, color, pinned, isTodo, completed, author, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (n.get('title',''), n.get('content',''), n.get('category','Allgemein'), 
             n.get('color','default'), n.get('pinned',0), n.get('isTodo',0), 0, n.get('author',''), now)
        )
        return {"id": cursor.lastrowid}

@router.put("/api/notes/{nid}")
async def update_note(nid: int, n: dict):
    with db._get_connection() as conn:
        conn.execute(
            "UPDATE notes SET title=?, content=?, category=?, color=?, pinned=?, isTodo=?, completed=?, author=? WHERE id=?",
            (n.get('title',''), n.get('content',''), n.get('category','Allgemein'), 
             n.get('color','default'), n.get('pinned',0), n.get('isTodo',0), n.get('completed',0), n.get('author',''), nid)
        )
    return {"ok": True}

@router.delete("/api/notes/{nid}")
async def del_note(nid: int):
    with db._get_connection() as conn:
        conn.execute("DELETE FROM notes WHERE id=?", (nid,))
    return {"ok": True}

@router.get("/api/categories")
async def get_cats():
    with db._get_connection() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM categories").fetchall()]

@router.post("/api/categories")
async def add_cat(c: dict):
    with db._get_connection() as conn:
        conn.execute("INSERT OR REPLACE INTO categories (name, password) VALUES (?,?)", (c['name'], c.get('password', '')))
    return {"ok": True}

@router.delete("/api/categories/{name}")
async def del_cat(name: str):
    with db._get_connection() as conn:
        conn.execute("DELETE FROM notes WHERE category=?", (name,))
        conn.execute("DELETE FROM categories WHERE name=?", (name,))
        conn.commit()
    return {"ok": True}

@router.post("/api/admin/login")
async def admin_login(req: dict):
    if str(req.get("pin")) == ADMIN_PIN:
        return {"status": "success", "role": "admin"}
    raise HTTPException(status_code=401)

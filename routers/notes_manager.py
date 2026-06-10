from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
import mysql.connector
from typing import Optional
import os

router = APIRouter(prefix="/api/notes", tags=["Notes"])

DB_PASSWORD = os.getenv("DB_PASSWORD")

def get_db_connection():
    return mysql.connector.connect(
        host="db", user="app_user", password=DB_PASSWORD, database="attendance_system"
    )

def init_notes_db():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS notes (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(255) NOT NULL,
            title VARCHAR(255) NOT NULL,
            content TEXT NOT NULL,
            visibility VARCHAR(50) DEFAULT 'private',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB;
    """)
    conn.commit()
    cur.close()
    conn.close()

init_notes_db()

class NoteCreate(BaseModel):
    title: str
    content: str
    visibility: str # 'private', 'public', 'admin', 'geratewart'

def get_user_from_request(request: Request):
    from main import get_current_user
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht angemeldet")
    return user

@router.get("")
def list_notes(request: Request):
    user = get_user_from_request(request)
    username = user["username"]
    role = user["role"]
    
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    
    query = """
        SELECT id, username, title, content, visibility, 
               DATE_FORMAT(created_at, '%d.%m.%Y %H:%i') as date_formatted 
        FROM notes 
        WHERE username = %s
           OR visibility = 'public'
           OR (visibility = 'admin' AND %s = 'admin')
           OR (visibility = 'geratewart' AND %s IN ('geratewart', 'admin'))
        ORDER BY created_at DESC
    """
    cur.execute(query, (username, role, role))
    notes = cur.fetchall()
    cur.close()
    conn.close()
    return notes

@router.post("")
def create_note(data: NoteCreate, request: Request):
    user = get_user_from_request(request)
    
    if data.visibility == "admin" and user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Nur Admins dürfen Admin-Notizen erstellen!")
        
    conn = get_db_connection()
    cur = conn.cursor()
    query = "INSERT INTO notes (username, title, content, visibility) VALUES (%s, %s, %s, %s)"
    cur.execute(query, (user["username"], data.title.strip(), data.content.strip(), data.visibility))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "success"}

# --- NEU: ROUTE ZUM ÄNDERN VORHANDENER NOTIZEN ---
@router.put("/{note_id}")
def update_note(note_id: int, data: NoteCreate, request: Request):
    user = get_user_from_request(request)
    username = user["username"]
    
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    
    # Prüfen, ob die Notiz existiert
    cur.execute("SELECT username FROM notes WHERE id = %s", (note_id,))
    note = cur.fetchone()
    
    if not note:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Notiz nicht gefunden")
        
    # Datensicherheit: Nur der echte Ersteller darf den Inhalt modifizieren
    if note["username"] != username:
        cur.close()
        conn.close()
        raise HTTPException(status_code=403, detail="Nur der Ersteller darf diese Notiz bearbeiten!")
        
    if data.visibility == "admin" and user["role"] != "admin":
        cur.close()
        conn.close()
        raise HTTPException(status_code=403, detail="Nur Admins dürfen Sichtbarkeit auf Admin setzen!")
        
    query = "UPDATE notes SET title = %s, content = %s, visibility = %s WHERE id = %s"
    cur.execute(query, (data.title.strip(), data.content.strip(), data.visibility, note_id))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "success"}

@router.delete("/{note_id}")
def delete_note(note_id: int, request: Request):
    user = get_user_from_request(request)
    
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT username, visibility FROM notes WHERE id = %s", (note_id,))
    note = cur.fetchone()
    
    if not note:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Notiz nicht gefunden")
        
    can_delete = (
        user["username"] == note["username"] or 
        user["role"] == "admin" or 
        (user["role"] == "geratewart" and note["visibility"] == "geratewart")
    )
    
    if not can_delete:
        cur.close()
        conn.close()
        raise HTTPException(status_code=403, detail="Keine Berechtigung zum Löschen")
        
    cur.execute("DELETE FROM notes WHERE id = %s", (note_id,))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "success"}

刻from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import mysql.connector
import os

router = APIRouter(prefix="/api/personnel", tags=["personnel"])
DB_PASSWORD = os.getenv("DB_PASSWORD")

def get_db_connection():
    return mysql.connector.connect(
        host="db", user="app_user", password=DB_PASSWORD, database="attendance_system"
    )

# --- DATENMODELLE ---
class PersonnelMember(BaseModel):
    name: str
    is_truppmann: bool = False
    is_funk: bool = False
    is_agt: bool = False
    is_maschinist: bool = False
    is_tf: bool = False
    is_gf: bool = False
    g26_3_date: Optional[str] = None
    belastungslauf_date: Optional[str] = None
    unterweisung_date: Optional[str] = None

class GlobalSettings(BaseModel):
    int_g26: int
    int_belastung: int
    int_unterweisung: int

# --- API ROUTEN ---

@router.get("/list")
def get_all_personnel():
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM personnel ORDER BY name ASC")
    res = cur.fetchall()
    conn.close()
    return res

@router.post("/add")
def add_member(m: PersonnelMember):
    conn = get_db_connection()
    cur = conn.cursor()
    # Nutze INSERT IGNORE, falls der Name schon existiert
    cur.execute("INSERT IGNORE INTO personnel (name) VALUES (%s)", (m.name,))
    conn.commit()
    conn.close()
    return {"status": "success"}

@router.post("/update/{member_id}")
def update_member(member_id: int, m: PersonnelMember):
    conn = get_db_connection()
    cur = conn.cursor()
    
    # 1. Vorherigen Namen abrufen, um ihn in der persons-Tabelle abgleichen zu können
    cur.execute("SELECT name FROM personnel WHERE id=%s", (member_id,))
    old_name_row = cur.fetchone()
    old_name = old_name_row[0] if old_name_row else None
    
    # Konvertierung von leeren Strings zu None für die Datenbank (DATE Felder)
    g26 = m.g26_3_date if m.g26_3_date else None
    bel = m.belastungslauf_date if m.belastungslauf_date else None
    unt = m.unterweisung_date if m.unterweisung_date else None

    # Update der Personal-Stammdaten
    sql = """UPDATE personnel SET 
             name=%s,
             is_truppmann=%s, is_funk=%s, is_agt=%s, is_maschinist=%s, is_tf=%s, is_gf=%s, 
             g26_3_date=%s, belastungslauf_date=%s, unterweisung_date=%s 
             WHERE id=%s"""
    
    vals = (m.name, m.is_truppmann, m.is_funk, m.is_agt, m.is_maschinist, m.is_tf, m.is_gf,
            g26, bel, unt, member_id)
    
    cur.execute(sql, vals)

    # 2. AUTOMATIK: Wenn der Name geändert wurde, auch in der Gruppenliste (persons) aktualisieren
    if old_name and old_name != m.name:
        cur.execute("UPDATE persons SET name=%s WHERE name=%s", (m.name, old_name))
    
    conn.commit()
    conn.close()
    return {"status": "updated"}

@router.delete("/delete/{member_id}")
def delete_member(member_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM personnel WHERE id = %s", (member_id,))
    conn.commit()
    conn.close()
    return {"status": "deleted"}

# --- SETTINGS ROUTEN ---
@router.get("/settings")
def get_settings():
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT setting_key, setting_value FROM settings")
    rows = cur.fetchall()
    conn.close()
    res = {row['setting_key']: row['setting_value'] for row in rows}
    if not res:
        return {"int_g26": 36, "int_belastung": 12, "int_unterweisung": 12}
    return res

@router.post("/settings")
def save_settings(s: GlobalSettings):
    conn = get_db_connection()
    cur = conn.cursor()
    settings = [
        ('int_g26', s.int_g26),
        ('int_belastung', s.int_belastung),
        ('int_unterweisung', s.int_unterweisung)
    ]
    for key, val in settings:
        cur.execute("INSERT INTO settings (setting_key, setting_value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE setting_value=%s", (key, val, val))
    conn.commit()
    conn.close()
    return {"status": "settings updated"}

# --- INITIALISIERUNG & MIGRATION ---

def init_personnel_db():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS personnel (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL UNIQUE,
                rank VARCHAR(100) DEFAULT '',
                is_truppmann BOOLEAN DEFAULT FALSE,
                is_funk BOOLEAN DEFAULT FALSE,
                is_agt BOOLEAN DEFAULT FALSE,
                is_maschinist BOOLEAN DEFAULT FALSE,
                is_tf BOOLEAN DEFAULT FALSE,
                is_gf BOOLEAN DEFAULT FALSE,
                g26_3_date DATE NULL,
                belastungslauf_date DATE NULL,
                unterweisung_date DATE NULL
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                setting_key VARCHAR(50) PRIMARY KEY,
                setting_value INT
            ) ENGINE=InnoDB;
        """)

        cur.execute("INSERT IGNORE INTO personnel (name) SELECT DISTINCT name FROM persons")
        cur.execute("INSERT IGNORE INTO settings (setting_key, setting_value) VALUES ('int_g26', 36), ('int_belastung', 12), ('int_unterweisung', 12)")

        conn.commit()
        cur.close()
        conn.close()
        print("--- PERSONAL-POOL & MIGRATION ERFOLGREICH ---")
    except Exception as e:
        print(f"Fehler bei init_personnel_db: {e}")

init_personnel_db()

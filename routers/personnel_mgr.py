from fastapi import APIRouter, HTTPException
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
    cur.execute("INSERT INTO personnel (name) VALUES (%s)", (m.name,))
    conn.commit()
    conn.close()
    return {"status": "success"}

@router.post("/update/{member_id}")
def update_member(member_id: int, m: PersonnelMember):
    conn = get_db_connection()
    cur = conn.cursor()
    sql = """UPDATE personnel SET 
             is_truppmann=%s, is_funk=%s, is_agt=%s, is_maschinist=%s, is_tf=%s, is_gf=%s, 
             g26_3_date=%s, belastungslauf_date=%s, unterweisung_date=%s 
             WHERE id=%s"""
    vals = (m.is_truppmann, m.is_funk, m.is_agt, m.is_maschinist, m.is_tf, m.is_gf,
            m.g26_3_date, m.belastungslauf_date, m.unterweisung_date, member_id)
    cur.execute(sql, vals)
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

# --- SETTINGS ROUTEN (Fristen) ---
@router.get("/settings")
def get_settings():
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT setting_key, setting_value FROM settings")
    rows = cur.fetchall()
    conn.close()
    # Umwandlung in ein flaches JSON Objekt
    res = {row['setting_key']: row['setting_value'] for row in rows}
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
        cur.execute("UPDATE settings SET setting_value=%s WHERE setting_key=%s", (val, key))
    conn.commit()
    conn.close()
    return {"status": "settings updated"}

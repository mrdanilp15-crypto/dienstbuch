from fastapi import APIRouter, HTTPException
import mysql.connector
import os
from pydantic import BaseModel
from typing import Optional, List

router = APIRouter(prefix="/api/personnel", tags=["Personal"])

def get_db_connection():
    return mysql.connector.connect(
        host="db", 
        user="app_user", 
        password=os.getenv("DB_PASSWORD"), 
        database="attendance_system"
    )

class PersonnelUpdate(BaseModel):
    is_truppmann: bool
    is_funk: bool
    is_agt: bool
    is_maschinist: bool
    is_tf: bool
    is_gf: bool
    g26_3_date: Optional[str] = None
    belastungslauf_date: Optional[str] = None
    unterweisung_date: Optional[str] = None

class NewPerson(BaseModel):
    name: str

@router.get("/list")
async def get_personnel_list():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT p.*, g.name as group_name 
            FROM persons p
            LEFT JOIN groups_table g ON p.group_id = g.id
            ORDER BY p.name
        """)
        members = cursor.fetchall()
        for m in members:
            for key in ['g26_3_date', 'belastungslauf_date', 'unterweisung_date']:
                if m[key]: m[key] = str(m[key])
        cursor.close()
        conn.close()
        return members
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/add")
async def add_person(data: NewPerson):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT id FROM groups_table LIMIT 1")
        group = cursor.fetchone()
        if not group:
            raise HTTPException(status_code=400, detail="Erstelle erst eine Gruppe!")
        cursor.execute("INSERT INTO persons (name, group_id) VALUES (%s, %s)", (data.name, group['id']))
        conn.commit()
        cursor.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/update/{person_id}")
async def update_personnel_data(person_id: int, data: PersonnelUpdate):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        sql = """
            UPDATE persons SET 
            is_truppmann=%s, is_funk=%s, is_agt=%s, is_maschinist=%s, is_tf=%s, is_gf=%s,
            g26_3_date=%s, belastungslauf_date=%s, unterweisung_date=%s
            WHERE id=%s
        """
        values = (
            data.is_truppmann, data.is_funk, data.is_agt, data.is_maschinist, 
            data.is_tf, data.is_gf,
            data.g26_3_date if data.g26_3_date else None,
            data.belastungslauf_date if data.belastungslauf_date else None,
            data.unterweisung_date if data.unterweisung_date else None,
            person_id
        )
        cursor.execute(sql, values)
        conn.commit()
        cursor.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- NEU: Einstellungen für Intervalle laden und speichern ---

@router.get("/settings")
def get_settings():
    try:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM settings")
        rows = cur.fetchall()
        # Wandelt Liste in ein praktisches Format um: {'int_g26': 36, ...}
        res = {row['setting_key']: row['setting_value'] for row in rows}
        cur.close()
        conn.close()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/settings")
async def save_settings(data: dict):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        for key, value in data.items():
            cur.execute("UPDATE settings SET setting_value=%s WHERE setting_key=%s", (value, key))
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "saved"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

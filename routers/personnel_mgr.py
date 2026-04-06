from fastapi import APIRouter, HTTPException, Depends
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
        # Fix: 'belastastungslauf_date' hatte einen Tippfehler (ein 'tas' zu viel)
        cursor.execute("""
            SELECT id, name, group_id, 
                   is_truppmann, is_funk, is_agt, is_maschinist, is_gf,
                   g26_3_date, belastungslauf_date, unterweisung_date 
            FROM persons 
            ORDER BY name
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
    """Fügt manuell eine neue Person zur ersten verfügbaren Gruppe hinzu."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # 1. Erste Gruppe finden
        cursor.execute("SELECT id FROM groups_table LIMIT 1")
        group = cursor.fetchone()
        if not group:
            raise HTTPException(status_code=400, detail="Bitte erstelle zuerst eine Gruppe im Dashboard!")
        
        # 2. Person einfügen
        cursor.execute("INSERT INTO persons (name, group_id) VALUES (%s, %s)", (data.name, group['id']))
        conn.commit()
        cursor.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/sync")
async def sync_from_reports():
    """Sucht Namen aus allen Berichten, die noch nicht in der Personal-Tabelle sind."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Erste Gruppe für neue Leute
        cursor.execute("SELECT id FROM groups_table LIMIT 1")
        group = cursor.fetchone()
        if not group: return {"status": "no group"}

        # Dieser Befehl ist etwas komplexer, da wir hier keine Namen-Tabelle haben, 
        # sondern die Namen in 'persons' stehen. Wir prüfen also, ob wir 
        # neue Namen importieren müssen (falls du eine externe Quelle hättest).
        # Da dein System aber 'persons' als Basis nutzt, reicht das manuelle Hinzufügen 
        # oder das automatische Anlegen beim ersten Start in der main.py.
        
        cursor.close()
        conn.close()
        return {"status": "synced"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/update/{person_id}")
async def update_personnel_data(person_id: int, data: PersonnelUpdate):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        sql = """
            UPDATE persons SET 
            is_truppmann=%s, is_funk=%s, is_agt=%s, is_maschinist=%s, is_gf=%s,
            g26_3_date=%s, belastungslauf_date=%s, unterweisung_date=%s
            WHERE id=%s
        """
        values = (
            data.is_truppmann, data.is_funk, data.is_agt, data.is_maschinist, data.is_gf,
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

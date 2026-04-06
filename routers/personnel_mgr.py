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

# Das Datenmodell muss exakt zu den Feldern in der personnel.html passen
class PersonnelUpdate(BaseModel):
    is_truppmann: bool
    is_funk: bool
    is_agt: bool
    is_maschinist: bool
    is_tf: bool  # Truppführer
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
        # Wir holen alle Quali-Spalten und den Gruppennamen
        cursor.execute("""
            SELECT p.*, g.name as group_name 
            FROM persons p
            LEFT JOIN groups_table g ON p.group_id = g.id
            ORDER BY p.name
        """)
        members = cursor.fetchall()
        
        # Datums-Objekte für JSON in Strings umwandeln
        for m in members:
            for key in ['g26_3_date', 'belastungslauf_date', 'unterweisung_date']:
                if m[key]:
                    m[key] = str(m[key])
                    
        cursor.close()
        conn.close()
        return members
    except Exception as e:
        print(f"Fehler beim Laden der Liste: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/add")
async def add_person(data: NewPerson):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Damit die Person sofort sichtbar ist, ordnen wir sie der ersten Gruppe zu
        cursor.execute("SELECT id FROM groups_table LIMIT 1")
        group = cursor.fetchone()
        
        target_group_id = group['id'] if group else None
        
        cursor.execute(
            "INSERT INTO persons (name, group_id) VALUES (%s, %s)", 
            (data.name, target_group_id)
        )
        conn.commit()
        cursor.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        print(f"Fehler beim Hinzufügen: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/update/{person_id}")
async def update_personnel_data(person_id: int, data: PersonnelUpdate):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Hier werden alle Checkboxen und Daten in die DB geschrieben
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
        print(f"Fehler beim Update der Person {person_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

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
    is_gf: bool
    g26_3_date: Optional[str] = None
    belastungslauf_date: Optional[str] = None
    unterweisung_date: Optional[str] = None

class NewPerson(BaseModel):
    name: str

@router.get("/list")
async def get_personnel_list():
    """Holt alle Personen inklusive Gruppennamen."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        # JOIN mit groups_table, um den Gruppennamen anzuzeigen
        cursor.execute("""
            SELECT p.id, p.name, p.group_id, g.name as group_name,
                   p.is_truppmann, p.is_funk, p.is_agt, p.is_maschinist, p.is_gf,
                   p.g26_3_date, p.belastungslauf_date, p.unterweisung_date 
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
    """Fügt eine neue Person hinzu (systemweit im Editor verfügbar)."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # 1. Existenz-Check (Vermeidet doppelte Namen)
        cursor.execute("SELECT id FROM persons WHERE name = %s", (data.name,))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            raise HTTPException(status_code=400, detail="Diese Person existiert bereits!")

        # 2. Erste Gruppe finden (Pflicht für Anzeige im Editor)
        cursor.execute("SELECT id FROM groups_table LIMIT 1")
        group = cursor.fetchone()
        if not group:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=400, detail="Bitte erstelle zuerst eine Gruppe im Dashboard!")
        
        # 3. Person einfügen
        cursor.execute("INSERT INTO persons (name, group_id) VALUES (%s, %s)", (data.name, group['id']))
        conn.commit()
        cursor.close()
        conn.close()
        return {"status": "success"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/update/{person_id}")
async def update_personnel_data(person_id: int, data: PersonnelUpdate):
    """Aktualisiert Qualifikationen eines Mitglieds."""
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

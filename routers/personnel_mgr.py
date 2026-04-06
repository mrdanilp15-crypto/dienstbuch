from fastapi import APIRouter, HTTPException, Depends
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

# Datenbank beim Start prüfen
def init_personnel_db():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS personnel (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            rank VARCHAR(100),
            is_agt BOOLEAN DEFAULT FALSE,
            is_maschinist BOOLEAN DEFAULT FALSE,
            is_funk BOOLEAN DEFAULT FALSE,
            is_truppmann BOOLEAN DEFAULT FALSE,
            is_tf BOOLEAN DEFAULT FALSE,
            is_gf BOOLEAN DEFAULT FALSE,
            g26_3_date DATE,
            belastungslauf_date DATE,
            unterweisung_date DATE
        ) ENGINE=InnoDB;
    """)
    conn.commit()
    cur.close()
    conn.close()

init_personnel_db()

class PersonnelMember(BaseModel):
    name: str
    rank: Optional[str] = ""
    is_agt: bool = False
    is_maschinist: bool = False
    is_funk: bool = False
    is_truppmann: bool = False
    is_tf: bool = False
    is_gf: bool = False
    g26_3_date: Optional[str] = None
    belastungslauf_date: Optional[str] = None
    unterweisung_date: Optional[str] = None

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
    sql = """INSERT INTO personnel (name, rank, is_agt, is_maschinist, is_funk, is_truppmann, is_tf, is_gf, 
             g26_3_date, belastungslauf_date, unterweisung_date) 
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    vals = (m.name, m.rank, m.is_agt, m.is_maschinist, m.is_funk, m.is_truppmann, m.is_tf, m.is_gf,
            m.g26_3_date, m.belastungslauf_date, m.unterweisung_date)
    cur.execute(sql, vals)
    conn.commit()
    conn.close()
    return {"status": "success"}

@router.delete("/{member_id}")
def delete_member(member_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM personnel WHERE id = %s", (member_id,))
    conn.commit()
    conn.close()
    return {"status": "deleted"}

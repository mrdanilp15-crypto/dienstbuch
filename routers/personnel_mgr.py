from fastapi import APIRouter, HTTPException, Response
from pydantic import BaseModel
from typing import List, Optional
import mysql.connector
import os
import base64
from datetime import date

router = APIRouter(prefix="/api/personnel", tags=["personnel"])
DB_PASSWORD = os.getenv("DB_PASSWORD")

def get_db_connection():
    return mysql.connector.connect(
        host="db", user="app_user", password=DB_PASSWORD, database="attendance_system"
    )

class PersonnelMember(BaseModel):
    name: str
    rank: Optional[str] = ""
    membership_status: Optional[str] = "Aktiv"
    phone: Optional[str] = ""
    email: Optional[str] = ""
    address: Optional[str] = ""
    badge_number: Optional[str] = ""
    birth_date: Optional[str] = None
    entry_date: Optional[str] = None
    honors: Optional[str] = ""
    profile_picture: Optional[str] = None
    is_truppmann: int = 0
    is_funk: int = 0
    is_agt: int = 0
    is_maschinist: int = 0
    is_tf: int = 0
    is_gf: int = 0
    lic_b: int = 0
    lic_be: int = 0
    lic_c: int = 0
    lic_ce: int = 0
    g26_3_date: Optional[str] = None
    belastungslauf_date: Optional[str] = None
    unterweisung_date: Optional[str] = None

class GlobalSettings(BaseModel):
    int_g26: int
    int_belastung: int
    int_unterweisung: int

# --- SCHNELLE ÜBERSICHTSLISTE (OHNE BILDER UND NOTIZEN) ---
@router.get("/list")
def get_all_personnel():
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    # profile_picture und honors werden hier bewusst ausgelassen! Wir prüfen nur, ob ein Bild existiert.
    sql = """SELECT id, name, rank, membership_status, phone, email, address, 
                    badge_number, birth_date, entry_date, is_truppmann, is_funk, 
                    is_agt, is_maschinist, is_tf, is_gf, lic_b, lic_be, lic_c, lic_ce, 
                    g26_3_date, belastungslauf_date, unterweisung_date,
                    CASE WHEN profile_picture IS NOT NULL AND LENGTH(profile_picture) > 0 THEN 1 ELSE 0 END AS has_picture
             FROM personnel ORDER BY name ASC"""
    cur.execute(sql)
    res = cur.fetchall()
    conn.close()
    
    for row in res:
        for key, value in row.items():
            if isinstance(value, date):
                row[key] = str(value)
            if key.startswith("is_") or key.startswith("lic_"):
                row[key] = bool(value)
        row["has_picture"] = bool(row["has_picture"])
    return res

# --- EINZELNES MITGLIED VOLLSTÄNDIG LADEN (FÜR MODAL) ---
@router.get("/get/{member_id}")
def get_single_member(member_id: int):
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM personnel WHERE id = %s", (member_id,))
    row = cur.fetchone()
    conn.close()
    
    if not row:
        raise HTTPException(status_code=404, detail="Mitglied nicht gefunden")
        
    for key, value in row.items():
        if isinstance(value, date):
            row[key] = str(value)
        if key.startswith("is_") or key.startswith("lic_"):
            row[key] = bool(value)
    return row

# --- BILDER DIREKT ALS BINÄRDATEI STREAMEN (KANN VOM BROWSER GECACHED WERDEN) ---
@router.get("/avatar/{member_id}")
def get_avatar(member_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT profile_picture FROM personnel WHERE id = %s", (member_id,))
    row = cur.fetchone()
    conn.close()
    
    if not row or not row[0]:
        raise HTTPException(status_code=404, detail="Kein Bild vorhanden")
    
    try:
        data_str = row[0]
        if "," in data_str:
            header, encoded = data_str.split(",", 1)
            mime = header.split(";")[0].split(":")[1]
            image_bytes = base64.b64decode(encoded)
            return Response(content=image_bytes, media_type=mime)
    except Exception:
        pass
    raise HTTPException(status_code=400, detail="Ungültige Bilddaten")

@router.post("/add")
def add_member(m: PersonnelMember):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("INSERT IGNORE INTO personnel (name, membership_status) VALUES (%s, %s)", (m.name, m.membership_status))
    conn.commit()
    conn.close()
    return {"status": "success"}

@router.post("/update/{member_id}")
def update_member(member_id: int, m: PersonnelMember):
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT name FROM personnel WHERE id=%s", (member_id,))
    old_name_row = cur.fetchone()
    old_name = old_name_row[0] if old_name_row else None
    
    b_date = m.birth_date if m.birth_date else None
    e_date = m.entry_date if m.entry_date else None
    g26 = m.g26_3_date if m.g26_3_date else None
    bel = m.belastungslauf_date if m.belastungslauf_date else None
    unt = m.unterweisung_date if m.unterweisung_date else None

    sql = """UPDATE personnel SET 
             name=%s, rank=%s, membership_status=%s, phone=%s, email=%s, address=%s,
             badge_number=%s, birth_date=%s, entry_date=%s, honors=%s, profile_picture=%s,
             is_truppmann=%s, is_funk=%s, is_agt=%s, is_maschinist=%s, is_tf=%s, is_gf=%s, 
             lic_b=%s, lic_be=%s, lic_c=%s, lic_ce=%s,
             g26_3_date=%s, belastungslauf_date=%s, unterweisung_date=%s 
             WHERE id=%s"""
    
    vals = (m.name, m.rank, m.membership_status, m.phone, m.email, m.address,
            m.badge_number, b_date, e_date, m.honors, m.profile_picture,
            int(m.is_truppmann), int(m.is_funk), int(m.is_agt), int(m.is_maschinist), int(m.is_tf), int(m.is_gf),
            int(m.lic_b), int(m.lic_be), int(m.lic_c), int(m.lic_ce),
            g26, bel, unt, member_id)
    
    cur.execute(sql, vals)

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

def init_personnel_db():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS personnel (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL UNIQUE
            ) ENGINE=InnoDB;
        """)

        extended_columns = [
            ("rank", "VARCHAR(100) DEFAULT ''"),
            ("membership_status", "VARCHAR(50) DEFAULT 'Aktiv'"),
            ("phone", "VARCHAR(100) DEFAULT ''"),
            ("email", "VARCHAR(255) DEFAULT ''"),
            ("address", "TEXT NULL"),
            ("badge_number", "VARCHAR(100) DEFAULT ''"),
            ("birth_date", "DATE NULL"),
            ("entry_date", "DATE NULL"),
            ("honors", "TEXT NULL"),
            ("profile_picture", "LONGTEXT NULL"),
            ("is_truppmann", "BOOLEAN DEFAULT FALSE"),
            ("is_funk", "BOOLEAN DEFAULT FALSE"),
            ("is_agt", "BOOLEAN DEFAULT FALSE"),
            ("is_maschinist", "BOOLEAN DEFAULT FALSE"),
            ("is_tf", "BOOLEAN DEFAULT FALSE"),
            ("is_gf", "BOOLEAN DEFAULT FALSE"),
            ("lic_b", "BOOLEAN DEFAULT FALSE"),
            ("lic_be", "BOOLEAN DEFAULT FALSE"),
            ("lic_c", "BOOLEAN DEFAULT FALSE"),
            ("lic_ce", "BOOLEAN DEFAULT FALSE"),
            ("g26_3_date", "DATE NULL"),
            ("belastungslauf_date", "DATE NULL"),
            ("unterweisung_date", "DATE NULL")
        ]

        for col_name, col_type in extended_columns:
            try:
                cur.execute(f"ALTER TABLE personnel ADD COLUMN {col_name} {col_type}")
            except mysql.connector.Error as err:
                if err.errno == 1060: pass

        conn.commit()
        cur.close()
        conn.close()
        print("--- INTERSCHUTZ-DB-MIGRATION ERFOLGREICH ---")
    except Exception as e:
        print(f"Fehler bei init_personnel_db: {e}")

init_personnel_db()

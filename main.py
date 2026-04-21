import os
import mysql.connector
import urllib.request
import time
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

# --- Gehirne ---
from routers import reports
from routers import notes_manager
from routers import personnel_mgr

# --- KONFIGURATION ---
CURRENT_VERSION = "1.70"
ADMIN_PIN = os.getenv("ADMIN_PIN")
USER_PIN = os.getenv("USER_PIN")
DB_PASSWORD = os.getenv("DB_PASSWORD")
TOWN_NAME = os.getenv("TOWN_NAME", "Deine Feuerwehr")
UPDATE_BASE_URL = os.getenv("UPDATE_BASE_URL", "https://raw.githubusercontent.com/mrdanilp15-crypto/dienstbuch/main/")

app = FastAPI()

# Statische Dateien mounten
if not os.path.exists("static"):
    os.makedirs("static")
app.mount("/static", StaticFiles(directory="static"), name="static")

# --- ZWEITES GEHIRN EINBINDEN ---
app.include_router(notes_manager.router)
app.include_router(personnel_mgr.router)

# --- DATENBANK INITIALISIERUNG (MYSQL) ---
def get_db_connection():
    return mysql.connector.connect(
        host="db", 
        user="app_user", 
        password=DB_PASSWORD, 
        database="attendance_system"
    )

def init_db_extensions():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        required_columns = [
            ("is_truppmann", "BOOLEAN DEFAULT FALSE"),
            ("is_funk", "BOOLEAN DEFAULT FALSE"),
            ("is_agt", "BOOLEAN DEFAULT FALSE"),
            ("is_maschinist", "BOOLEAN DEFAULT FALSE"),
            ("is_tf", "BOOLEAN DEFAULT FALSE"),
            ("is_gf", "BOOLEAN DEFAULT FALSE"),
            ("g26_3_date", "DATE NULL"),
            ("belastungslauf_date", "DATE NULL"),
            ("unterweisung_date", "DATE NULL")
        ]

        for col_name, col_type in required_columns:
            try:
                cur.execute(f"ALTER TABLE persons ADD COLUMN {col_name} {col_type}")
            except mysql.connector.Error as err:
                if err.errno == 1060: pass 
                else: print(f"Fehler bei Spalte {col_name}: {err}")
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                setting_key VARCHAR(50) PRIMARY KEY,
                setting_value INT
            ) ENGINE=InnoDB;
        """)
        
        default_settings = [('int_g26', 36), ('int_belastung', 12), ('int_unterweisung', 12)]
        for key, val in default_settings:
            cur.execute("INSERT IGNORE INTO settings (setting_key, setting_value) VALUES (%s, %s)", (key, val))

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Fehler bei DB-Erweiterung: {e}")

def init_db():
    max_retries = 10
    for i in range(max_retries):
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS groups_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL) ENGINE=InnoDB;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS persons (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    group_id INT,
                    name VARCHAR(255) NOT NULL,
                    FOREIGN KEY (group_id) REFERENCES groups_table(id) ON DELETE CASCADE
                ) ENGINE=InnoDB;
            """)
            cur.execute("CREATE TABLE IF NOT EXISTS vehicles (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL) ENGINE=InnoDB;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    group_id INT,
                    date DATE,
                    category VARCHAR(50),
                    duration DECIMAL(5,2),
                    description TEXT,
                    instructors TEXT,
                    leader_signature LONGTEXT,
                    FOREIGN KEY (group_id) REFERENCES groups_table(id) ON DELETE CASCADE
                ) ENGINE=InnoDB;
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS attendance (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    session_id INT,
                    person_id INT,
                    is_present BOOLEAN,
                    note TEXT,
                    vehicle VARCHAR(50),
                    signature LONGTEXT,
                    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE,
                    FOREIGN KEY (person_id) REFERENCES persons(id) ON DELETE CASCADE
                ) ENGINE=InnoDB;
            """)
            conn.commit()
            cur.close()
            conn.close()
            init_db_extensions()
            break
        except Exception as e:
            time.sleep(5)

init_db()

def safe_decode(value):
    if isinstance(value, bytes): return value.decode('utf-8')
    return value

# --- DATENMODELLE ---
class PersonData(BaseModel): name: str
class VehicleData(BaseModel): name: str
class EntryDto(BaseModel): 
    person_id: int; is_present: bool; note: Optional[str] = ""; 
    vehicle: Optional[str] = ""; signature: Optional[str] = None
class AttendanceUpload(BaseModel): 
    session_id: Optional[int] = None; date: str; group_id: int; category: str = "Übung"; 
    duration: float = 0.0; description: str; instructors: Optional[str] = ""; 
    leader_signature: Optional[str] = None; entries: List[EntryDto]
class GroupData(BaseModel): name: str

# --- ROUTEN ---
@app.get("/", response_class=FileResponse)
def get_login(): return FileResponse("static/login.html")
@app.get("/dashboard", response_class=FileResponse)
def get_dash(): return FileResponse("static/dashboard.html")
@app.get("/editor", response_class=FileResponse)
def get_edit(): return FileResponse("static/editor.html")
@app.get("/notizen", response_class=FileResponse)
def get_notes_page(): return FileResponse("static/notizen.html")
@app.get("/personal", response_class=FileResponse)
def get_personnel_page(): return FileResponse("static/personnel.html") 
@app.get("/favicon.ico", include_in_schema=False)
async def favicon(): return FileResponse("static/favicon.svg") if os.path.exists("static/favicon.svg") else Response(status_code=204)

@app.get("/api/info")
async def get_info():
    remote_version = CURRENT_VERSION
    try:
        v_url = UPDATE_BASE_URL + "VERSION.txt"
        with urllib.request.urlopen(v_url, timeout=2) as response:
            remote_version = response.read().decode('utf-8').strip()
    except: pass
    return {"version": CURRENT_VERSION, "remote_version": remote_version, "update_available": remote_version != CURRENT_VERSION, "town": TOWN_NAME}

@app.post("/api/login")
async def login(data: dict, response: Response):
    p = data.get("pin", "").strip()
    role = "admin" if (ADMIN_PIN and p == ADMIN_PIN) else "user" if (USER_PIN and p == USER_PIN) else None
    if role:
        response.set_cookie(key="session_token", value="valid", max_age=31536000)
        return {"status": "success", "role": role, "redirect": "/dashboard"}
    raise HTTPException(401, detail="PIN falsch!")

@app.post("/api/verify_admin")
async def verify_admin(data: dict):
    p = data.get("pin", "").strip()
    if ADMIN_PIN and p == ADMIN_PIN: return {"success": True}
    raise HTTPException(status_code=401, detail="PIN falsch!")

@app.get("/groups")
def get_groups():
    c=get_db_connection(); cur=c.cursor(dictionary=True)
    cur.execute("SELECT * FROM groups_table ORDER BY name")
    r=cur.fetchall(); c.close(); return r

@app.post("/groups")
def create_group(g: GroupData):
    c=get_db_connection(); cur=c.cursor()
    cur.execute("INSERT INTO groups_table (name) VALUES (%s)", (g.name,))
    c.commit(); c.close(); return {"status": "created"}

@app.get("/api/personnel/list")
def list_personnel_pool():
    try:
        c = get_db_connection(); cur = c.cursor(dictionary=True)
        # Hier muss die Tabelle 'persons' genutzt werden, da sie die zentrale Namensliste ist
        cur.execute("SELECT id, name FROM persons ORDER BY name ASC")
        r = cur.fetchall(); c.close(); return r
    except: return []

@app.post("/groups/{group_id}/persons")
def add_person(group_id: int, p: PersonData):
    c = get_db_connection(); cur = c.cursor()
    cur.execute("INSERT INTO persons (group_id, name) VALUES (%s, %s)", (group_id, p.name))
    c.commit(); c.close(); return {"status": "person added"}

@app.put("/persons/{id}")
def update_person(id: int, p: PersonData):
    c = get_db_connection(); cur = c.cursor()
    cur.execute("UPDATE persons SET name=%s WHERE id=%s", (p.name, id))
    c.commit(); c.close(); return {"status": "updated"}

@app.delete("/groups/{id}")
def delete_group(id: int):
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM groups_table WHERE id=%s", (id,))
    c.commit(); c.close(); return {"status": "deleted"}

@app.delete("/persons/{id}")
def delete_person(id: int):
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM persons WHERE id=%s", (id,))
    c.commit(); c.close(); return {"status": "deleted"}

@app.get("/api/vehicles")
def get_vehicles():
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT * FROM vehicles ORDER BY name")
    r = cur.fetchall(); c.close(); return r

@app.post("/api/vehicles")
def create_vehicle(v: VehicleData):
    c = get_db_connection(); cur = c.cursor()
    cur.execute("INSERT INTO vehicles (name) VALUES (%s)", (v.name,))
    c.commit(); c.close(); return {"status": "created"}

@app.delete("/api/vehicles/{id}")
def delete_vehicle(id: int):
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM vehicles WHERE id=%s", (id,))
    c.commit(); c.close(); return {"status": "deleted"}

@app.delete("/sessions/{id}")
def delete_session(id: int):
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM sessions WHERE id=%s", (id,))
    c.commit(); c.close(); return {"status": "deleted"}

@app.get("/groups/{id}/sessions")
def get_sessions(id: int):
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, date, category, description, duration, leader_signature FROM sessions WHERE group_id=%s ORDER BY date DESC, id DESC", (id,))
    r = cur.fetchall(); c.close()
    for x in r: 
        x['date'] = str(x['date'])
        x['is_signed'] = bool(x['leader_signature'] and len(str(x['leader_signature'])) > 100)
        if 'leader_signature' in x: del x['leader_signature']
    return r

@app.get("/groups/{id}/stats")
def get_stats(id: int, year: int):
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT COUNT(*) as total FROM sessions WHERE group_id=%s AND YEAR(date)=%s", (id, year))
    max_s = cur.fetchone()['total'] or 0
    sql = """SELECT p.name, 
             SUM(CASE WHEN a.is_present=1 AND s.id IS NOT NULL THEN 1 ELSE 0 END) as present_count, 
             SUM(CASE WHEN a.is_present=1 AND s.id IS NOT NULL THEN s.duration ELSE 0 END) as total_hours 
             FROM persons p LEFT JOIN attendance a ON p.id=a.person_id 
             LEFT JOIN sessions s ON a.session_id=s.id AND YEAR(s.date)=%s AND s.group_id=%s 
             WHERE p.group_id=%s GROUP BY p.id, p.name ORDER BY total_hours DESC"""
    cur.execute(sql, (year, id, id)); p = cur.fetchall(); c.close()
    return {"persons": p, "total_sessions": max_s}

@app.get("/groups/{group_id}/attendance")
async def get_attendance(group_id: int, session_id: Optional[int] = None):
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        session_data = {
            "session_id": session_id, 
            "description": "", 
            "duration": 2.0, 
            "category": "Übung",
            "date": datetime.now().strftime("%Y-%m-%d"),
            "leader_signature": None,
            "instructors": ""
        }
        
        if session_id:
            # FIX: category und leader_signature zum SELECT hinzugefügt
            cur.execute("SELECT id as session_id, description, duration, date, category, leader_signature, instructors FROM sessions WHERE id = %s", (session_id,))
            row = cur.fetchone()
            if row:
                session_data = row
                session_data['date'] = str(session_data['date'])
                # FIX: Unterschrift für das Frontend decodieren
                if session_data.get('leader_signature'):
                    session_data['leader_signature'] = safe_decode(session_data['leader_signature'])

        query = """
            SELECT p.id, p.name, COALESCE(a.is_present, 0) as is_present, 
                   COALESCE(a.note, '') as note, COALESCE(a.vehicle, '') as vehicle, a.signature 
            FROM persons p
            LEFT JOIN attendance a ON p.id = a.person_id AND a.session_id = %s
            WHERE p.group_id = %s ORDER BY p.name
        """
        cur.execute(query, (session_id, group_id))
        persons = cur.fetchall()
        for p in persons:
            p['signature'] = safe_decode(p['signature'])
            p['is_present'] = bool(p['is_present'])

        return {**session_data, "persons": persons}
    finally:
        cur.close()
        conn.close()

@app.post("/attendance")
async def save_attendance(payload: AttendanceUpload):
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        # 1. Session speichern/updaten
        if payload.session_id:
            cur.execute(
                "UPDATE sessions SET date=%s, description=%s, duration=%s, category=%s, instructors=%s WHERE id=%s",
                (payload.date, payload.description, payload.duration, payload.category, payload.instructors, payload.session_id)
            )
            session_id = payload.session_id
        else:
            cur.execute(
                "INSERT INTO sessions (group_id, date, description, duration, category, instructors) VALUES (%s, %s, %s, %s, %s, %s)",
                (payload.group_id, payload.date, payload.description, payload.duration, payload.category, payload.instructors)
            )
            session_id = cur.lastrowid

        # 2. Alte Anwesenheit löschen (für diese spezifische Session)
        cur.execute("DELETE FROM attendance WHERE session_id = %s", (session_id,))

        # --- NEU: SYNCHRONISATION DER GRUPPENLISTE (Das Lösch-Gegenstück) ---
        # Wir holen uns alle IDs, die vom Editor geschickt wurden
        received_ids = [entry.person_id for entry in payload.entries]
        
        if received_ids:
            # Wir löschen alle Personen aus der Tabelle 'persons', 
            # die zu dieser Gruppe gehören, aber NICHT in der Liste vom Editor standen.
            format_strings = ','.join(['%s'] * len(received_ids))
            sql = f"DELETE FROM persons WHERE group_id = %s AND id NOT IN ({format_strings})"
            cur.execute(sql, (payload.group_id, *received_ids))
        # -------------------------------------------------------------------

        for entry in payload.entries:
            # Existiert die Person in der Gruppe?
            cur.execute("SELECT id FROM persons WHERE id = %s", (entry.person_id,))
            exists = cur.fetchone()

            actual_id = entry.person_id

            if not exists:
                # Falls neu aus dem Pool hinzugefügt:
                cur.execute("SELECT name FROM personnel WHERE id = %s", (entry.person_id,))
                pool_person = cur.fetchone()
                
                if pool_person:
                    cur.execute("INSERT INTO persons (name, group_id) VALUES (%s, %s)", 
                                (pool_person['name'], payload.group_id))
                    actual_id = cur.lastrowid
                else:
                    continue

            # 3. Anwesenheit speichern
            cur.execute(
                """INSERT INTO attendance (session_id, person_id, is_present, note, vehicle, signature) 
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (session_id, actual_id, 1 if entry.is_present else 0, entry.note or "", entry.vehicle or "", entry.signature)
            )
        
        conn.commit()
        return {"status": "success", "session_id": session_id}
    except Exception as e:
        conn.rollback()
        print(f"DEBUG FEHLER: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()

@app.post("/sessions/{session_id}/leader_signature")
async def save_leader_sig(session_id: int, data: dict):
    sig = data.get("signature")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("UPDATE sessions SET leader_signature=%s WHERE id=%s", (sig, session_id))
    c.commit(); c.close(); return {"status": "success"}

@app.get("/groups/{group_id}/topics")
def get_topics(group_id: int):
    c = get_db_connection(); cur = c.cursor()
    cur.execute("SELECT DISTINCT description FROM sessions WHERE group_id=%s AND description IS NOT NULL LIMIT 50", (group_id,))
    r = [row[0] for row in cur.fetchall()]; c.close(); return r

@app.get("/groups/{group_id}/instructors")
def get_instructors(group_id: int):
    c = get_db_connection(); cur = c.cursor()
    cur.execute("SELECT DISTINCT instructors FROM sessions WHERE group_id=%s AND instructors IS NOT NULL LIMIT 50", (group_id,))
    r = [row[0] for row in cur.fetchall()]; c.close(); return r

@app.get("/sessions/{session_id}/report", response_class=HTMLResponse)
def single_report(session_id: int):
    c=get_db_connection(); cur=c.cursor(dictionary=True)
    cur.execute("SELECT s.*, g.name as gname FROM sessions s JOIN groups_table g ON s.group_id = g.id WHERE s.id=%s", (session_id,))
    s = cur.fetchone()
    if s and s['leader_signature']: s['leader_signature'] = safe_decode(s['leader_signature'])
    cur.execute("SELECT p.name, a.is_present, a.note, a.vehicle, a.signature FROM attendance a JOIN persons p ON a.person_id = p.id WHERE a.session_id=%s ORDER BY p.name", (session_id,))
    persons = cur.fetchall(); c.close()
    for p in persons: p['signature'] = safe_decode(p['signature'])
    return f"<html><head><meta charset='UTF-8'><style>{reports.get_report_styles()}</style></head><body>{reports.generate_single_report(s, persons, TOWN_NAME)}</body></html>"

@app.get("/groups/{group_id}/print_view", response_class=HTMLResponse)
def year_report(group_id: int, year: int):
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT name FROM groups_table WHERE id=%s", (group_id,))
    gname_res = cur.fetchone(); gname = gname_res['name'] if gname_res else "Unbekannt"
    cur.execute("SELECT COUNT(*) as total FROM sessions WHERE group_id=%s AND YEAR(date)=%s", (group_id, year))
    max_s = cur.fetchone()['total'] or 0
    cur.execute("SELECT s.*, g.name as gname FROM sessions s JOIN groups_table g ON s.group_id = g.id WHERE s.group_id=%s AND YEAR(s.date)=%s ORDER BY s.date ASC, s.id ASC", (group_id, year))
    sessions_list = cur.fetchall()
    html_body = ""; p_stats = {}; cat_sums = {"Übung": 0.0, "Einsatz": 0.0, "Sonstiges": 0.0}
    for s in sessions_list:
        if s['leader_signature']: s['leader_signature'] = safe_decode(s['leader_signature'])
        cur.execute("SELECT p.name, a.is_present, a.note, a.vehicle, a.signature FROM attendance a JOIN persons p ON a.person_id = p.id WHERE a.session_id=%s ORDER BY p.name", (s['id'],))
        persons = cur.fetchall()
        for p in persons: p['signature'] = safe_decode(p['signature'])
        html_body += reports.generate_single_report(s, persons, TOWN_NAME)
        cat = s['category'] if s['category'] in cat_sums else "Sonstiges"
        cat_sums[cat] += float(s['duration'])
        for p in persons:
            if p['name'] not in p_stats: p_stats[p['name']] = {"Übung": 0.0, "Einsatz": 0.0, "Sonstiges": 0.0, "total_h": 0.0, "p": 0}
            if p['is_present']: p_stats[p['name']]["p"] += 1; p_stats[p['name']][cat] += float(s['duration']); p_stats[p['name']]["total_h"] += float(s['duration'])
    for n in p_stats: p_stats[n]['q'] = round((p_stats[n]['p'] / max_s) * 100) if max_s > 0 else 0
    html_body += reports.generate_year_report(gname, year, p_stats, cat_sums, TOWN_NAME)
    c.close(); return f"<html><head><meta charset='UTF-8'><style>{reports.get_report_styles()}</style></head><body>{html_body}</body></html>"

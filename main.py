import os
import mysql.connector
import urllib.request
import time
import hashlib
import secrets
import hmac
import base64
import json
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

# --- Externe Routen-Module laden ---
from routers import reports
from routers import notes_manager
from routers import personnel_mgr

# --- SYSTEM-KONFIGURATION ---
CURRENT_VERSION = "1.80"
DB_PASSWORD = os.getenv("DB_PASSWORD")
TOWN_NAME = os.getenv("TOWN_NAME", "Deine Feuerwehr")
UPDATE_BASE_URL = os.getenv("UPDATE_BASE_URL", "https://raw.githubusercontent.com/mrdanilp15-crypto/dienstbuch/main/")
SECRET_KEY = os.getenv("SECRET_KEY", "feuerwehr-dienstbuch-geheimschluessel-112")

app = FastAPI()

# Verzeichnis für statische Webseiten-Dateien sicherstellen
if not os.path.exists("static"):
    os.makedirs("static")
app.mount("/static", StaticFiles(directory="static"), name="static")

# Externe Module in FastAPI einbinden
app.include_router(notes_manager.router)
app.include_router(personnel_mgr.router)

# --- DATENBANK VERBINDUNGSUNDERBAU (MYSQL) ---
def get_db_connection():
    return mysql.connector.connect(
        host="db", 
        user="app_user", 
        password=DB_PASSWORD, 
        database="attendance_system"
    )

# --- KRYPTOGRAPHIE, HASCHUNG & SESSION-COOKIES ---
def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    hash_value = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
    return f"{salt}:{hash_value.hex()}"

def verify_password(stored_password: str, provided_password: str) -> bool:
    try:
        salt, stored_hash = stored_password.split(":")
        hash_value = hashlib.pbkdf2_hmac('sha256', provided_password.encode(), salt.encode(), 100000)
        return hash_value.hex() == stored_hash
    except Exception:
        return False

def create_session_token(username: str, role: str) -> str:
    payload = {"username": username, "role": role, "ts": time.time()}
    payload_b64 = base64.b64encode(json.dumps(payload).encode()).decode()
    signature = hmac.new(SECRET_KEY.encode(), payload_b64.encode(), hashlib.sha256).hexdigest()
    return f"{payload_b64}.{signature}"

def get_current_user(request: Request) -> Optional[dict]:
    token = request.cookies.get("session_token")
    if not token:
        return None
    try:
        payload_b64, signature = token.split(".")
        expected_sig = hmac.new(SECRET_KEY.encode(), payload_b64.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected_sig):
            return None
        data = json.loads(base64.b64decode(payload_b64.encode()).decode())
        if time.time() - data.get("ts", 0) > 86400 * 30: # 30 Tage Session-Gültigkeit
            return None
        return data
    except Exception:
        return None

def init_db_extensions():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Spalten für Qualifikationen und medizinische Fristen in der Personaltabelle
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

        # Benutzertabelle für Authentifizierung erstellen
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(255) NOT NULL UNIQUE,
                password_hash VARCHAR(255) NOT NULL,
                role VARCHAR(50) NOT NULL
            ) ENGINE=InnoDB;
        """)

        # Erweiterte Spalten für lückenlose Fahrzeugverwaltung nachrüsten
        required_veh_columns = [
            ("radio_name", "VARCHAR(255) DEFAULT ''"),
            ("status", "INT DEFAULT 2"),
            ("tuv_date", "DATE NULL"),
            ("sp_date", "DATE NULL"),
            ("milage", "INT DEFAULT 0"),
            ("next_service", "DATE NULL")
        ]
        for col_name, col_type in required_veh_columns:
            try:
                cur.execute(f"ALTER TABLE vehicles ADD COLUMN {col_name} {col_type}")
            except mysql.connector.Error as err:
                if err.errno == 1060: pass
                else: print(f"Fahrzeug-Erweiterung Spalte {col_name} fehlgeschlagen: {err}")
        
        # PLUG & PLAY INITIALISIERUNG: Prüfen ob ein Administrator existiert, ansonsten automatisch anlegen
        cur.execute("SELECT COUNT(*) FROM users WHERE username = 'admin'")
        if cur.fetchone()[0] == 0:
            default_admin_hash = hash_password("admin123")
            cur.execute(
                "INSERT INTO users (username, password_hash, role) VALUES (%s, %s, %s)",
                ("admin", default_admin_hash, "admin")
            )
            print("\n==================================================")
            print("  PLUG & PLAY START: Default Admin wurde angelegt! ")
            print("  Benutzername: admin  |  Passwort: admin123      ")
            print("==================================================\n")

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

# --- API DATENMODELLE (PYDANTIC) ---
class PersonData(BaseModel): name: str
class VehicleData(BaseModel): 
    name: str
    radio_name: Optional[str] = ""
    status: Optional[int] = 2
    tuv_date: Optional[str] = None
    sp_date: Optional[str] = None
    milage: Optional[int] = 0
    next_service: Optional[str] = None

class EntryDto(BaseModel): 
    person_id: int; is_present: bool; note: Optional[str] = ""; 
    vehicle: Optional[str] = ""; signature: Optional[str] = None
class AttendanceUpload(BaseModel): 
    session_id: Optional[int] = None; date: str; group_id: int; category: str = "Übung"; 
    duration: float = 0.0; description: str; instructors: Optional[str] = ""; 
    leader_signature: Optional[str] = None; entries: List[EntryDto]
class GroupData(BaseModel): name: str

class LoginRequest(BaseModel):
    username: str
    password: str

class UserCreateRequest(BaseModel):
    username: str
    password: str
    role: str

# --- ROUTEN SEITENSTEUERUNG (HTML-AUSGABE MIT COOKIE-AUTH) ---
@app.get("/", response_class=FileResponse)
def get_login(request: Request):
    user = get_current_user(request)
    if user: return FileResponse("static/dashboard.html")
    return FileResponse("static/login.html")

@app.get("/dashboard", response_class=FileResponse)
def get_dash(request: Request):
    if not get_current_user(request): return FileResponse("static/login.html")
    return FileResponse("static/dashboard.html")

@app.get("/editor", response_class=FileResponse)
def get_edit(request: Request):
    if not get_current_user(request): return FileResponse("static/login.html")
    return FileResponse("static/editor.html")

@app.get("/notizen", response_class=FileResponse)
def get_notes_page(request: Request):
    if not get_current_user(request): return FileResponse("static/login.html")
    return FileResponse("static/notizen.html")

@app.get("/personal", response_class=FileResponse)
def get_personnel_page(request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": return FileResponse("static/dashboard.html")
    return FileResponse("static/personnel.html") 

@app.get("/favicon.ico", include_in_schema=False)
async def favicon(): return FileResponse("static/favicon.svg") if os.path.exists("static/favicon.svg") else Response(status_code=204)

# --- APIS FÜR CORE-AUTHENTIFIZIERUNG ---
@app.post("/api/login")
def api_login(data: LoginRequest, response: Response):
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE username = %s", (data.username,))
    user = cur.fetchone()
    cur.close()
    conn.close()
    
    if not user or not verify_password(user['password_hash'], data.password):
        raise HTTPException(status_code=401, detail="Benutzername oder Passwort falsch!")
        
    token = create_session_token(user['username'], user['role'])
    response.set_cookie(
        key="session_token",
        value=token,
        httponly=True,
        max_age=30 * 24 * 60 * 60, 
        samesite="lax"
    )
    return {"status": "success", "username": user['username'], "role": user['role'], "redirect": "/dashboard"}

@app.get("/api/auth/me")
def api_auth_me(request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    return {"username": user["username"], "role": user["role"]}

@app.post("/api/logout")
def api_logout(response: Response):
    response.delete_cookie("session_token")
    return {"status": "success"}

# --- NEU: SELBSTSTÄNDIGE PASSWORTÄNDERUNG FÜR JEDEN USER ---
@app.put("/api/auth/change-password")
def user_change_self_password(data: dict, request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht angemeldet")
        
    old_pw = data.get("old_password")
    new_pw = data.get("new_password")
    
    if not old_pw or not new_pw or len(new_pw.strip()) < 4:
        raise HTTPException(status_code=400, detail="Eingaben ungültig oder Passwort zu kurz (min. 4 Zeichen)!")
        
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    
    # Aktuellen Passwort-Hash aus der DB holen
    cur.execute("SELECT password_hash FROM users WHERE username = %s", (user["username"],))
    db_user = cur.fetchone()
    
    if not db_user or not verify_password(db_user['password_hash'], old_pw):
        cur.close()
        conn.close()
        raise HTTPException(status_code=400, detail="Das aktuelle Passwort ist nicht korrekt!")
        
    # Neues Passwort hashen und wegschreiben
    new_hash = hash_password(new_pw.strip())
    cur.execute("UPDATE users SET password_hash = %s WHERE username = %s", (new_hash, user["username"]))
    conn.commit()
    
    cur.close()
    conn.close()
    return {"status": "success", "detail": "Passwort erfolgreich aktualisiert"}

# --- BENUTZERKONTENVERWALTUNG (NUR FÜR ADMINS) ---

@app.get("/api/users/list")
def list_users(request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, username, role FROM users ORDER BY username ASC")
    users = cur.fetchall(); cur.close(); conn.close()
    return users

@app.post("/api/users/add")
def add_user(data: UserCreateRequest, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    try:
        p_hash = hash_password(data.password)
        cur.execute("INSERT INTO users (username, password_hash, role) VALUES (%s, %s, %s)", (data.username.strip(), p_hash, data.role))
        conn.commit()
    except mysql.connector.Error as err:
        if err.errno == 1062: raise HTTPException(status_code=400, detail="Benutzername existiert bereits!")
        raise HTTPException(status_code=500, detail=str(err))
    finally: cur.close(); conn.close()
    return {"status": "success"}

@app.put("/api/users/{user_id}/role")
def update_user_role(user_id: int, data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": raise HTTPException(status_code=403, detail="Keine Berechtigung")
    new_role = data.get("role")
    if new_role not in ["admin", "user", "mannschaft", "geratewart"]: raise HTTPException(status_code=400, detail="Ungültige Rolle")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("UPDATE users SET role = %s WHERE id = %s AND username != 'admin'", (new_role, user_id))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@app.put("/api/users/{user_id}/password")
def change_user_password(user_id: int, data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": raise HTTPException(status_code=403, detail="Keine Berechtigung")
    new_pw = data.get("password")
    if not new_pw or len(new_pw.strip()) < 4: raise HTTPException(status_code=400, detail="Passwort zu kurz!")
    p_hash = hash_password(new_pw.strip())
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("UPDATE users SET password_hash = %s WHERE id = %s", (p_hash, user_id))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@app.delete("/api/users/{user_id}")
def delete_user(user_id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM users WHERE id = %s AND username != 'admin'", (user_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

# --- SCHREIBGESCHÜTZTE DIENSTBUCH-APIS (MANNSCHAFTS-LOGINS WERDEN BLOCKIERT) ---
@app.put("/groups/{id}")
def update_group(id: int, g: GroupData, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt für Mannschaft")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("UPDATE groups_table SET name=%s WHERE id=%s", (g.name, id))
    c.commit(); cur.close(); c.close()
    return {"status": "updated"}

@app.post("/groups")
def create_group(g: GroupData, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt für Mannschaft")
    c=get_db_connection(); cur=c.cursor()
    cur.execute("INSERT INTO groups_table (name) VALUES (%s)", (g.name,))
    c.commit(); c.close(); return {"status": "created"}

@app.post("/groups/{group_id}/persons")
def add_person(group_id: int, p: PersonData, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt für Mannschaft")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("INSERT INTO persons (group_id, name) VALUES (%s, %s)", (group_id, p.name))
    c.commit(); c.close(); return {"status": "person added"}

@app.put("/persons/{id}")
def update_person(id: int, p: PersonData, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt für Mannschaft")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("UPDATE persons SET name=%s WHERE id=%s", (p.name, id))
    c.commit(); c.close(); return {"status": "updated"}

@app.delete("/groups/{id}")
def delete_group(id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt für Mannschaft")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM groups_table WHERE id=%s", (id,))
    c.commit(); c.close(); return {"status": "deleted"}

@app.delete("/persons/{id}")
def delete_person(id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt für Mannschaft")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM persons WHERE id=%s", (id,))
    c.commit(); c.close(); return {"status": "deleted"}

# --- ERWEITERTE RETTUNGSFAHRZEUG-APIS ---
@app.get("/api/vehicles")
def get_vehicles():
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, name, radio_name, status, tuv_date, sp_date, milage, next_service FROM vehicles ORDER BY name")
    r = cur.fetchall(); c.close()
    for v in r:
        if v['tuv_date']: v['tuv_date'] = str(v['tuv_date'])
        if v['sp_date']: v['sp_date'] = str(v['sp_date'])
        if v['next_service']: v['next_service'] = str(v['next_service'])
    return r

@app.post("/api/vehicles")
def create_vehicle(v: VehicleData, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt für Mannschaft")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("""
        INSERT INTO vehicles (name, radio_name, status, tuv_date, sp_date, milage, next_service) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (v.name, v.radio_name, v.status or 2, v.tuv_date or None, v.sp_date or None, v.milage or 0, v.next_service or None))
    c.commit(); c.close()
    return {"status": "created"}

@app.put("/api/vehicles/{id}")
def update_vehicle(id: int, v: VehicleData, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt für Mannschaft")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("""
        UPDATE vehicles 
        SET name=%s, radio_name=%s, status=%s, tuv_date=%s, sp_date=%s, milage=%s, next_service=%s 
        WHERE id=%s
    """, (v.name, v.radio_name, v.status or 2, v.tuv_date or None, v.sp_date or None, v.milage or 0, v.next_service or None, id))
    c.commit(); c.close()
    return {"status": "updated"}

@app.put("/api/vehicles/{id}/status")
def update_vehicle_status(id: int, data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("UPDATE vehicles SET status=%s WHERE id=%s", (data.get("status", 2), id))
    c.commit(); c.close()
    return {"status": "status updated"}

@app.delete("/api/vehicles/{id}")
def delete_vehicle(id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM vehicles WHERE id=%s", (id,))
    c.commit(); c.close()
    return {"status": "deleted"}

# --- PROTOKOLLIERUNGS- & STATISTIK-APIS ---
@app.delete("/sessions/{id}")
def delete_session(id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM sessions WHERE id=%s", (id,))
    c.commit(); c.close(); return {"status": "deleted"}

@app.get("/groups")
def get_groups():
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT * FROM groups_table ORDER BY name")
    r = cur.fetchall()
    cur.close()
    c.close()
    return r

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
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    try:
        session_data = {"session_id": session_id, "description": "", "duration": 2.0, "category": "Übung", "date": datetime.now().strftime("%Y-%m-%d"), "leader_signature": None, "instructors": ""}
        if session_id:
            cur.execute("SELECT id as session_id, description, duration, date, category, leader_signature, instructors FROM sessions WHERE id = %s", (session_id,))
            row = cur.fetchone()
            if row:
                session_data = row
                session_data['date'] = str(session_data['date'])
                if session_data.get('leader_signature'): session_data['leader_signature'] = safe_decode(session_data['leader_signature'])
        query = """SELECT p.id, p.name, COALESCE(a.is_present, 0) as is_present, COALESCE(a.note, '') as note, COALESCE(a.vehicle, '') as vehicle, a.signature, pl.id AS personnel_id, CASE WHEN pl.profile_picture IS NOT NULL AND LENGTH(pl.profile_picture) > 0 THEN 1 ELSE 0 END AS has_picture FROM persons p LEFT JOIN attendance a ON p.id = a.person_id AND a.session_id = %s LEFT JOIN personnel pl ON p.name = pl.name WHERE p.group_id = %s ORDER BY p.name"""
        cur.execute(query, (session_id, group_id))
        persons = cur.fetchall()
        for p in persons:
            p['signature'] = safe_decode(p['signature'])
            p['is_present'] = bool(p['is_present'])
            p['has_picture'] = bool(p.get('has_picture', 0))
        return {**session_data, "persons": persons}
    finally: cur.close(); conn.close()

@app.post("/attendance")
async def save_attendance(payload: AttendanceUpload, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    try:
        if payload.session_id:
            cur.execute("""UPDATE sessions SET date=%s, description=%s, duration=%s, category=%s, instructors=%s, leader_signature=%s WHERE id=%s""",(payload.date, payload.description, payload.duration, payload.category, payload.instructors, payload.leader_signature, payload.session_id))
            session_id = payload.session_id
        else:
            cur.execute("""INSERT INTO sessions (group_id, date, description, duration, category, instructors, leader_signature) VALUES (%s, %s, %s, %s, %s, %s, %s)""",(payload.group_id, payload.date, payload.description, payload.duration, payload.category, payload.instructors, payload.leader_signature))
            session_id = cur.lastrowid
        cur.execute("DELETE FROM attendance WHERE session_id = %s", (session_id,))
        for entry in payload.entries:
            cur.execute("INSERT INTO attendance (session_id, person_id, is_present, note, vehicle, signature) VALUES (%s, %s, %s, %s, %s, %s)",(session_id, entry.person_id, 1 if entry.is_present else 0, entry.note or "", entry.vehicle or "", entry.signature))
        conn.commit(); return {"status": "success", "session_id": session_id}
    except Exception as e: conn.rollback(); raise HTTPException(status_code=500, detail=str(e))
    finally: cur.close(); conn.close()

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

# --- DRUCKBARE JAHRES- UND EINZELBERICHTE (REALSCHALTUNG) ---
@app.get("/sessions/{session_id}/report", response_class=HTMLResponse)
def single_report(session_id: int):
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT s.*, g.name as gname FROM sessions s JOIN groups_table g ON s.group_id = g.id WHERE s.id=%s", (session_id,))
    s = cur.fetchone()
    if s and s['leader_signature']: 
        s['leader_signature'] = safe_decode(s['leader_signature'])
    cur.execute("SELECT p.name, a.is_present, a.note, a.vehicle, a.signature FROM attendance a JOIN persons p ON a.person_id = p.id WHERE a.session_id=%s ORDER BY p.name", (session_id,))
    persons = cur.fetchall()
    c.close()
    for p in persons: 
        p['signature'] = safe_decode(p['signature'])
    return f"<html><head><meta charset='UTF-8'><style>{reports.get_report_styles()}</style></head><body>{reports.generate_single_report(s, persons, TOWN_NAME)}</body></html>"

@app.get("/groups/{group_id}/print_view", response_class=HTMLResponse)
def year_report(group_id: int, year: int):
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT name FROM groups_table WHERE id=%s", (group_id,))
    gname_res = cur.fetchone()
    gname = gname_res['name'] if gname_res else "Unbekannt"
    cur.execute("SELECT COUNT(*) as total FROM sessions WHERE group_id=%s AND YEAR(date)=%s", (group_id, year))
    max_s = cur.fetchone()['total'] or 0
    cur.execute("SELECT s.*, g.name as gname FROM sessions s JOIN groups_table g ON s.group_id = g.id WHERE s.group_id=%s AND YEAR(s.date)=%s ORDER BY s.date ASC, s.id ASC", (group_id, year))
    sessions_list = cur.fetchall()
    html_body = ""
    p_stats = {}
    cat_sums = {"Übung": 0.0, "Einsatz": 0.0, "Sonstiges": 0.0}
    for s in sessions_list:
        if s['leader_signature']: 
            s['leader_signature'] = safe_decode(s['leader_signature'])
        cur.execute("SELECT p.name, a.is_present, a.note, a.vehicle, a.signature FROM attendance a JOIN persons p ON a.person_id = p.id WHERE a.session_id=%s ORDER BY p.name", (s['id'],))
        persons = cur.fetchall()
        for p in persons: 
            p['signature'] = safe_decode(p['signature'])
        html_body += reports.generate_single_report(s, persons, TOWN_NAME)
        cat = s['category'] if s['category'] in cat_sums else "Sonstiges"
        cat_sums[cat] += float(s['duration'])
        for p in persons:
            if p['name'] not in p_stats: 
                p_stats[p['name']] = {"Übung": 0.0, "Einsatz": 0.0, "Sonstiges": 0.0, "total_h": 0.0, "p": 0}
            if p['is_present']: 
                p_stats[p['name']]["p"] += 1
                p_stats[p['name']][cat] += float(s['duration'])
                p_stats[p['name']]["total_h"] += float(s['duration'])
    for n in p_stats: 
        p_stats[n]['q'] = round((p_stats[n]['p'] / max_s) * 100) if max_s > 0 else 0
    html_body += reports.generate_year_report(gname, year, p_stats, cat_sums, TOWN_NAME)
    c.close()
    return f"<html><head><meta charset='UTF-8'><style>{reports.get_report_styles()}</style></head><body>{html_body}</body></html>"

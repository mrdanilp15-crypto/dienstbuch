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
from datetime import datetime, timedelta

# --- SYSTEM-KONFIGURATION ---
CURRENT_VERSION = "4.5-ULTIMATE"
DB_PASSWORD = os.getenv("DB_PASSWORD")
TOWN_NAME = os.getenv("TOWN_NAME", "Feuerwehr Buxheim")
SECRET_KEY = os.getenv("SECRET_KEY", "feuerwehr-dienstbuch-geheimschluessel-112")

app = FastAPI(title="FeuerwehrHub Ultimate Engine")

if not os.path.exists("static"):
    os.makedirs("static")
app.mount("/static", StaticFiles(directory="static"), name="static")

def get_db_connection():
    return mysql.connector.connect(
        host="db", user="app_user", password=DB_PASSWORD, database="attendance_system"
    )

def log_audit_action(username: str, action: str, details: str):
    try:
        conn = get_db_connection(); cur = conn.cursor()
        cur.execute("INSERT INTO audit_log (username, action, details) VALUES (%s, %s, %s)", (username, action, details))
        conn.commit(); cur.close(); conn.close()
    except Exception as e: print(f"Logbuch-Fehler: {e}")

# --- ALAMOS APAGER PRO LIVE GATEWAY ---
def trigger_apager_push(title: str, message: str, priority: str = "ALARM"):
    try:
        conn = get_db_connection(); cur = conn.cursor(dictionary=True)
        cur.execute("SELECT setting_value FROM settings WHERE setting_key = 'apager_api_key'")
        row = cur.fetchone()
        api_key = row['setting_value'] if row else None
        cur.close(); conn.close()
        
        if api_key and api_key != "0" and len(api_key) > 5:
            payload = {
                "apiKey": api_key,
                "title": f"🚨 {title}",
                "message": message,
                "priority": priority,
                "sound": "alarm_fire"
            }
            req = urllib.request.Request(
                "https://api.alamos-gmbh.com/v1/push",
                data=json.dumps(payload).encode('utf-8'),
                headers={'Content-Type': 'application/json'},
                method='POST'
            )
            with urllib.request.urlopen(req, timeout=5) as response:
                return response.getcode() == 200
    except Exception as e: print(f"aPager PRO Schnittstellenfehler: {e}")
    return False

# --- SESSION-MANAGEMENT & KRYPTOGRAPHIE ---
def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    hash_value = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
    return f"{salt}:{hash_value.hex()}"

def verify_password(stored_password: str, provided_password: str) -> bool:
    try:
        salt, stored_hash = stored_password.split(":")
        hash_value = hashlib.pbkdf2_hmac('sha256', provided_password.encode(), salt.encode(), 100000)
        return hash_value.hex() == stored_hash
    except Exception: return False

def create_session_token(username: str, role: str) -> str:
    payload = {"username": username, "role": role, "ts": time.time()}
    payload_b64 = base64.b64encode(json.dumps(payload).encode()).decode()
    signature = hmac.new(SECRET_KEY.encode(), payload_b64.encode(), hashlib.sha256).hexdigest()
    return f"{payload_b64}.{signature}"

def get_current_user(request: Request) -> Optional[dict]:
    token = request.cookies.get("session_token")
    if not token: return None
    try:
        payload_b64, signature = token.split(".")
        expected_sig = hmac.new(SECRET_KEY.encode(), payload_b64.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected_sig): return None
        return json.loads(base64.b64decode(payload_b64.encode()).decode())
    except Exception: return None

# --- AUTOMATISCHE TABELLEN-INITIALISIERUNG ---
def init_ultimate_database():
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            setting_key VARCHAR(100) PRIMARY KEY,
            setting_value VARCHAR(255) NOT NULL
        ) ENGINE=InnoDB;
    """)
    default_settings = [('int_g26', '36'), ('int_belastung', '12'), ('int_unterweisung', '12'), ('apager_api_key', '0'), ('lager_warnstufe', '5')]
    for k, v in default_settings:
        cur.execute("INSERT IGNORE INTO settings (setting_key, setting_value) VALUES (%s, %s)", (k, v))
        
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(255) NOT NULL UNIQUE,
            password_hash VARCHAR(255) NOT NULL,
            role VARCHAR(50) NOT NULL,
            is_first_login BOOLEAN DEFAULT TRUE,
            personnel_id INT NULL,
            two_factor_enabled BOOLEAN DEFAULT FALSE,
            two_factor_secret VARCHAR(100) NULL
        ) ENGINE=InnoDB;
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS personnel (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE,
            rank VARCHAR(100) DEFAULT '',
            membership_status VARCHAR(50) DEFAULT 'Aktiv',
            phone VARCHAR(100) DEFAULT '',
            email VARCHAR(255) DEFAULT '',
            address TEXT NULL,
            badge_number VARCHAR(100) DEFAULT '',
            birth_date DATE NULL,
            entry_date DATE NULL,
            honors TEXT NULL,
            profile_picture LONGTEXT NULL,
            is_truppmann BOOLEAN DEFAULT FALSE,
            is_funk BOOLEAN DEFAULT FALSE,
            is_agt BOOLEAN DEFAULT FALSE,
            is_maschinist BOOLEAN DEFAULT FALSE,
            is_tf BOOLEAN DEFAULT FALSE,
            is_gf BOOLEAN DEFAULT FALSE,
            lic_b BOOLEAN DEFAULT FALSE,
            lic_be BOOLEAN DEFAULT FALSE,
            lic_c BOOLEAN DEFAULT FALSE,
            lic_ce BOOLEAN DEFAULT FALSE,
            g26_3_date DATE NULL,
            belastungslauf_date DATE NULL,
            unterweisung_date DATE NULL
        ) ENGINE=InnoDB;
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS inventory (
            id INT AUTO_INCREMENT PRIMARY KEY,
            item_name VARCHAR(255) NOT NULL,
            amount INT NOT NULL DEFAULT 0,
            min_amount INT NOT NULL DEFAULT 5,
            unit VARCHAR(50) DEFAULT 'Stück',
            location VARCHAR(100) DEFAULT 'Zentrallager',
            status VARCHAR(50) DEFAULT 'Vollständig',
            requester VARCHAR(255) NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB;
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS notes (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(255) NOT NULL,
            title VARCHAR(255) NOT NULL,
            content TEXT NOT NULL,
            visibility VARCHAR(50) DEFAULT 'public',
            kanban_status VARCHAR(50) DEFAULT 'neu',
            priority VARCHAR(50) DEFAULT 'normal',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB;
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS groups_table (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE
        ) ENGINE=InnoDB;
    """)
    cur.execute("INSERT IGNORE INTO groups_table (id, name) VALUES (1, 'Feuerwehr Buxheim - Aktive')")
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vehicles (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            radio_name VARCHAR(255) DEFAULT '',
            status INT DEFAULT 2,
            tuv_date DATE NULL,
            sp_date DATE NULL,
            milage INT DEFAULT 0,
            next_service DATE NULL
        ) ENGINE=InnoDB;
    """)
    
    cur.execute("SELECT COUNT(*) FROM vehicles")
    if cur.fetchone()[0] == 0:
        cur.execute("INSERT INTO vehicles (name, radio_name, status) VALUES ('HLF 20', 'Florian Buxheim 40/1', 2), ('LF 10', 'Florian Buxheim 43/1', 2)")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            group_id INT NOT NULL,
            description VARCHAR(255) NOT NULL,
            duration FLOAT NOT NULL,
            date DATE NOT NULL,
            category VARCHAR(50) NOT NULL,
            instructors VARCHAR(255) DEFAULT '',
            leader_signature LONGTEXT NULL
        ) ENGINE=InnoDB;
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS attendance (
            id INT AUTO_INCREMENT PRIMARY KEY,
            session_id INT NOT NULL,
            person_id INT NOT NULL,
            is_present BOOLEAN DEFAULT FALSE,
            vehicle VARCHAR(100) DEFAULT '',
            signature LONGTEXT NULL,
            note VARCHAR(255) DEFAULT ''
        ) ENGINE=InnoDB;
    """)
    
    cur.execute("SELECT COUNT(*) FROM users WHERE username = 'admin'")
    if cur.fetchone()[0] == 0:
        cur.execute("INSERT INTO users (username, password_hash, role, is_first_login) VALUES (%s, %s, %s, 0)", ("admin", hash_password("admin123"), "admin"))
    conn.commit(); cur.close(); conn.close()

init_ultimate_database()

# --- PYDANTIC CORES ---
class LoginRequest(BaseModel): username: str; password: str
class KanbanUpdateRequest(BaseModel): status: str
class InventoryItemDto(BaseModel): item_name: str; amount: int; min_amount: int; unit: str; location: str; status: str; requester: Optional[str] = None
class NoteCreateDto(BaseModel): title: str; content: str; visibility: str; priority: str
class VehicleStatusDto(BaseModel): status: int
class LegacyAttendanceEntry(BaseModel): person_id: int; is_present: bool; vehicle: str; signature: Optional[str] = None; note: Optional[str] = None
class LegacySessionPayload(BaseModel): session_id: Optional[int] = None; date: str; group_id: int; category: str; duration: float; description: str; instructors: str; entries: List[LegacyAttendanceEntry]

# --- WEB SEITEN INTERFACES ---
@app.get("/", response_class=FileResponse)
def view_index(request: Request):
    if get_current_user(request): return FileResponse("static/dashboard.html")
    return FileResponse("static/login.html")

@app.get("/login", response_class=FileResponse)
def view_explicit_login():
    return FileResponse("static/login.html")

@app.get("/editor", response_class=FileResponse)
def view_editor(request: Request):
    if not get_current_user(request): return FileResponse("static/login.html")
    return FileResponse("static/editor.html")

# --- AUTH SEKTION (LOOP-FREI) ---
@app.post("/api/login")
def api_login(data: LoginRequest, response: Response):
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE username = %s", (data.username.strip(),))
    user = cur.fetchone(); cur.close(); conn.close()
    if user and verify_password(user['password_hash'], data.password):
        token = create_session_token(user['username'], user['role'])
        response.set_cookie(key="session_token", value=token, httponly=True, max_age=2592000, samesite="lax")
        return {"status": "success", "username": user['username'], "role": user['role'], "redirect": "/"}
    raise HTTPException(status_code=401, detail="Zugangsdaten ungültig")

@app.get("/api/auth/me")
def api_auth_me(request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht authentifiziert")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE username = %s", (user["username"],))
    db_user = cur.fetchone(); cur.close(); conn.close()
    if not db_user: raise HTTPException(status_code=401, detail="Konto existiert nicht mehr")
    return {"username": db_user["username"], "role": db_user["role"], "is_first_login": False}

@app.post("/api/logout")
def api_logout(response: Response):
    response.delete_cookie("session_token")
    return {"status": "success"}

# --- KANBAN & NOTIZEN MIGRATION ---
@app.get("/api/notes")
def get_kanban_notes(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, username, title, content, visibility, kanban_status, priority, DATE_FORMAT(created_at, '%d.%m.%Y %H:%i') as date_formatted FROM notes ORDER BY id DESC")
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@app.post("/api/notes")
def create_kanban_note(data: NoteCreateDto, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401)
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO notes (username, title, content, visibility, priority, kanban_status) VALUES (%s, %s, %s, %s, %s, 'neu')", (user["username"], data.title.strip(), data.content.strip(), data.visibility, data.priority))
    conn.commit(); cur.close(); conn.close()
    if data.priority == "kritisch":
        trigger_apager_push(title=f"Kritischer Defekt: {data.title}", message=data.content)
    return {"status": "created"}

@app.put("/api/notes/{note_id}/status")
def update_kanban_status(note_id: int, data: KanbanUpdateRequest, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("UPDATE notes SET kanban_status = %s WHERE id = %s", (data.status, note_id))
    conn.commit(); cur.close(); conn.close()
    return {"status": "updated"}

@app.delete("/api/notes/{note_id}")
def delete_kanban_note(note_id: int, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM notes WHERE id = %s", (note_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "deleted"}

# --- INVENTAR / LAGER PLATFORM ---
@app.get("/api/inventory")
def get_inventory(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT *, DATE_FORMAT(created_at, '%d.%m.%Y') as date_formatted FROM inventory ORDER BY item_name ASC")
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@app.post("/api/inventory")
def add_inventory_item(data: InventoryItemDto, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO inventory (item_name, amount, min_amount, unit, location, status, requester) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (data.item_name, data.amount, data.min_amount, data.unit, data.location, data.status, data.requester))
    conn.commit(); cur.close(); conn.close()
    return {"status": "added"}

@app.delete("/api/inventory/{item_id}")
def delete_inventory_item(item_id: int, request: Request):
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM inventory WHERE id = %s", (item_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "deleted"}

# --- MANNSCHAFT & DRIVER AKTEN ---
@app.get("/api/personnel/list")
def list_personnel(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, name, rank, membership_status, phone, email, is_agt, is_maschinist, is_gf, 0 as has_picture FROM personnel ORDER BY name ASC")
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@app.post("/api/personnel/add")
def add_personnel_member(data: dict, request: Request):
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT IGNORE INTO personnel (name, rank, membership_status, is_agt, is_maschinist, is_gf) VALUES (%s, %s, %s, %s, %s, %s)",
                (data.get("name"), data.get("rank",""), data.get("membership_status","Aktiv"), bool(data.get("is_agt")), bool(data.get("is_maschinist")), bool(data.get("is_gf"))))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

# --- SYSTEM PARAMETERS ---
@app.get("/api/settings")
def get_registry_settings(request: Request):
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT setting_key, setting_value FROM settings")
    res = {row['setting_key']: row['setting_value'] for row in cur.fetchall()}
    cur.close(); conn.close()
    return res

@app.post("/api/settings")
def update_registry_settings(data: dict, request: Request):
    conn = get_db_connection(); cur = conn.cursor()
    for k, v in data.items():
        cur.execute("INSERT INTO settings (setting_key, setting_value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE setting_value=%s", (k, str(v), str(v)))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

# --- CORE DIENSTBUCH ROUTEN ---
@app.get("/groups")
def get_legacy_groups(request: Request):
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM groups_table ORDER BY name")
    r = cur.fetchall(); cur.close(); conn.close(); return r

@app.get("/api/vehicles")
def get_legacy_vehicles(request: Request):
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, name, radio_name, status, milage FROM vehicles ORDER BY name ASC")
    r = cur.fetchall(); cur.close(); conn.close(); return r

@app.put("/api/vehicles/{v_id}/status")
def update_vehicle_status(v_id: int, data: VehicleStatusDto, request: Request):
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("UPDATE vehicles SET status = %s WHERE id = %s", (data.status, v_id))
    conn.commit(); cur.close(); conn.close()
    return {"status": "updated"}

@app.get("/groups/{group_id}/sessions")
def get_group_sessions(group_id: int, request: Request):
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, description, duration, DATE_FORMAT(date, '%Y-%m-%d') as date, category, instructors FROM sessions WHERE group_id = %s ORDER BY date DESC", (group_id,))
    res = cur.fetchall(); cur.close(); conn.close()
    return res

# --- FIXED ORDER ARGUMENT MAPPER ---
@app.get("/groups/{group_id}/attendance")
def get_group_attendance_matrix(group_id: int, request: Request, session_id: Optional[int] = None):
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    session_data = {"session_id": session_id, "description": "", "duration": 2.0, "category": "Übung", "date": datetime.now().strftime("%Y-%m-%d"), "leader_signature": None, "instructors": ""}
    if session_id:
        cur.execute("SELECT id as session_id, description, duration, DATE_FORMAT(date, '%Y-%m-%d') as date, category, leader_signature, instructors FROM sessions WHERE id = %s", (session_id,))
        row = cur.fetchone()
        if row: session_data = row
            
    cur.execute("SELECT * FROM personnel ORDER BY name ASC")
    persons = cur.fetchall()
    for p in persons:
        p['is_present'] = False
        p['vehicle'] = ""
        p['g26_expired'] = False
    cur.close(); conn.close()
    return {**session_data, "persons": persons}

@app.post("/attendance")
def save_attendance_report(data: LegacySessionPayload, request: Request):
    conn = get_db_connection(); cur = conn.cursor()
    s_id = data.session_id
    if s_id:
        cur.execute("UPDATE sessions SET date=%s, duration=%s, description=%s, instructors=%s, category=%s WHERE id=%s", (data.date, data.duration, data.description, data.instructors, data.category, s_id))
        cur.execute("DELETE FROM attendance WHERE session_id = %s", (s_id,))
    else:
        cur.execute("INSERT INTO sessions (group_id, description, duration, date, category, instructors) VALUES (%s, %s, %s, %s, %s, %s)", (data.group_id, data.description, data.duration, data.date, data.category, data.instructors))
        s_id = cur.lastrowid
        
    for entry in data.entries:
        cur.execute("INSERT INTO attendance (session_id, person_id, is_present, vehicle) VALUES (%s, %s, %s, %s)", (s_id, entry.person_id, entry.is_present, entry.vehicle))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success", "session_id": s_id}

@app.get("/api/users/me/stats")
def get_my_global_fire_stats(year: int, request: Request):
    return {"hours": 42.5, "count": 18}

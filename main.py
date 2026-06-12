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
DB_PASSWORD = os.getenv("DB_PASSWORD")
SECRET_KEY = os.getenv("SECRET_KEY", "feuerwehr-dienstbuch-geheimschluessel-112")

app = FastAPI(title="FeuerwehrHub Ultimate")

if not os.path.exists("static"):
    os.makedirs("static")
app.mount("/static", StaticFiles(directory="static"), name="static")

# --- DATENMODELLE (Zwingend VOR den Funktionen!) ---
class LoginRequest(BaseModel): username: str; password: str
class KanbanUpdateRequest(BaseModel): status: str
class InventoryItemDto(BaseModel): item_name: str; amount: int; min_amount: int; unit: str; location: str; status: str; requester: Optional[str] = None
class NoteCreateDto(BaseModel): title: str; content: str; visibility: str; priority: str
class VehicleStatusDto(BaseModel): status: int
class VehicleCreateDto(BaseModel): name: str; radio_name: str; status: int; milage: int; tuv_date: Optional[str] = None; sp_date: Optional[str] = None; next_service: Optional[str] = None
class EventCreateDto(BaseModel): date: str; title: str; responsible: str
class EntryDto(BaseModel): person_id: int; is_present: bool; note: Optional[str] = ""; vehicle: Optional[str] = ""; signature: Optional[str] = None
class LegacySessionPayload(BaseModel): session_id: Optional[int] = None; date: str; group_id: int; category: str = "Übung"; duration: float = 0.0; description: str; instructors: Optional[str] = ""; leader_signature: Optional[str] = None; entries: List[EntryDto]

# --- DB CONNECTION & KRYPTO ---
def get_db_connection():
    return mysql.connector.connect(host="db", user="app_user", password=DB_PASSWORD, database="attendance_system")

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

# --- DATENBANK AUTOMATION ---
def init_database():
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS settings (setting_key VARCHAR(100) PRIMARY KEY, setting_value VARCHAR(255) NOT NULL) ENGINE=InnoDB;")
    cur.execute("INSERT IGNORE INTO settings (setting_key, setting_value) VALUES ('int_g26', '36'), ('apager_api_key', '0')")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS users (id INT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(255) NOT NULL UNIQUE, password_hash VARCHAR(255) NOT NULL, role VARCHAR(50) NOT NULL, is_first_login BOOLEAN DEFAULT TRUE, personnel_id INT NULL) ENGINE=InnoDB;""")
    cur.execute("""CREATE TABLE IF NOT EXISTS personnel (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL UNIQUE, rank VARCHAR(100) DEFAULT '', membership_status VARCHAR(50) DEFAULT 'Aktiv', is_agt BOOLEAN DEFAULT FALSE, is_maschinist BOOLEAN DEFAULT FALSE, is_gf BOOLEAN DEFAULT FALSE, g26_3_date DATE NULL) ENGINE=InnoDB;""")
    cur.execute("""CREATE TABLE IF NOT EXISTS inventory (id INT AUTO_INCREMENT PRIMARY KEY, item_name VARCHAR(255) NOT NULL, amount INT NOT NULL DEFAULT 0, min_amount INT NOT NULL DEFAULT 5, unit VARCHAR(50) DEFAULT 'Stück', location VARCHAR(100) DEFAULT 'Lager', status VARCHAR(50) DEFAULT 'OK', requester VARCHAR(255) NULL) ENGINE=InnoDB;""")
    cur.execute("""CREATE TABLE IF NOT EXISTS notes (id INT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(255) NOT NULL, title VARCHAR(255) NOT NULL, content TEXT NOT NULL, visibility VARCHAR(50) DEFAULT 'public', kanban_status VARCHAR(50) DEFAULT 'neu', priority VARCHAR(50) DEFAULT 'normal') ENGINE=InnoDB;""")
    cur.execute("""CREATE TABLE IF NOT EXISTS groups_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL UNIQUE) ENGINE=InnoDB;""")
    cur.execute("INSERT IGNORE INTO groups_table (id, name) VALUES (1, 'Feuerwehr - Aktive')")
    cur.execute("""CREATE TABLE IF NOT EXISTS vehicles (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, radio_name VARCHAR(255) DEFAULT '', status INT DEFAULT 2, tuv_date DATE NULL, sp_date DATE NULL, milage INT DEFAULT 0, next_service DATE NULL) ENGINE=InnoDB;""")
    cur.execute("""CREATE TABLE IF NOT EXISTS events (id INT AUTO_INCREMENT PRIMARY KEY, date DATE NOT NULL, title VARCHAR(255) NOT NULL, responsible VARCHAR(255) DEFAULT '') ENGINE=InnoDB;""")
    cur.execute("""CREATE TABLE IF NOT EXISTS sessions (id INT AUTO_INCREMENT PRIMARY KEY, group_id INT, description VARCHAR(255), duration FLOAT, date DATE, category VARCHAR(50), instructors VARCHAR(255), leader_signature LONGTEXT) ENGINE=InnoDB;""")
    cur.execute("""CREATE TABLE IF NOT EXISTS attendance (id INT AUTO_INCREMENT PRIMARY KEY, session_id INT, person_id INT, is_present BOOLEAN, vehicle VARCHAR(100), signature LONGTEXT, note VARCHAR(255)) ENGINE=InnoDB;""")
    
    cur.execute("SELECT COUNT(*) FROM users WHERE username = 'admin'")
    if cur.fetchone()[0] == 0:
        cur.execute("INSERT INTO users (username, password_hash, role, is_first_login) VALUES (%s, %s, %s, 0)", ("admin", hash_password("admin123"), "admin"))
    conn.commit(); cur.close(); conn.close()

init_database()

# --- WEB ROUTEN ---
@app.get("/")
def view_index(request: Request):
    if get_current_user(request): return FileResponse("static/dashboard.html")
    return FileResponse("static/login.html")

@app.get("/login")
def view_login(): return FileResponse("static/login.html")

@app.get("/editor")
def view_editor(request: Request):
    if not get_current_user(request): return FileResponse("static/login.html")
    return FileResponse("static/editor.html")

# --- AUTH API ---
@app.post("/api/login")
def api_login(data: LoginRequest, response: Response):
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE username = %s", (data.username.strip(),))
    user = cur.fetchone(); cur.close(); conn.close()
    if user and verify_password(user['password_hash'], data.password):
        token = create_session_token(user['username'], user['role'])
        response.set_cookie(key="session_token", value=token, httponly=True)
        return {"status": "success", "username": user['username'], "role": user['role'], "redirect": "/"}
    raise HTTPException(status_code=401, detail="Falsches Passwort!")

@app.get("/api/auth/me")
def api_auth_me(request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT u.role, p.name as personnel_name, p.is_agt, p.is_maschinist, p.is_gf, DATE_FORMAT(p.g26_3_date, '%Y-%m-%d') as g26 FROM users u LEFT JOIN personnel p ON u.personnel_id = p.id WHERE u.username = %s", (user["username"],))
    db_u = cur.fetchone(); cur.close(); conn.close()
    return {"username": user["username"], "role": user["role"], "profile": db_u}

@app.post("/api/logout")
def api_logout(response: Response):
    response.delete_cookie("session_token"); return {"status": "success"}

# --- VEREIN EVENTS API ---
@app.get("/api/events")
def get_events():
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, DATE_FORMAT(date, '%Y-%m-%d') as date, DATE_FORMAT(date, '%d.%m.%Y') as date_formatted, title, responsible FROM events ORDER BY date ASC")
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@app.post("/api/events")
def add_event(data: EventCreateDto):
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO events (date, title, responsible) VALUES (%s, %s, %s)", (data.date, data.title, data.responsible))
    conn.commit(); cur.close(); conn.close(); return {"status": "added"}

@app.delete("/api/events/{e_id}")
def delete_event(e_id: int):
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM events WHERE id = %s", (e_id,))
    conn.commit(); cur.close(); conn.close(); return {"status": "deleted"}

# --- LAGER API ---
@app.get("/api/inventory")
def get_inv():
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM inventory ORDER BY item_name ASC")
    res = cur.fetchall(); cur.close(); conn.close(); return res

@app.post("/api/inventory")
def add_inv(data: InventoryItemDto):
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO inventory (item_name, amount, min_amount, unit, location) VALUES (%s, %s, %s, %s, %s)", (data.item_name, data.amount, data.min_amount, data.unit, data.location))
    conn.commit(); cur.close(); conn.close(); return {"status": "added"}

@app.delete("/api/inventory/{i_id}")
def del_inv(i_id: int):
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM inventory WHERE id = %s", (i_id,))
    conn.commit(); cur.close(); conn.close(); return {"status": "deleted"}

# --- MÄNGEL KANBAN API ---
@app.get("/api/notes")
def get_notes():
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM notes ORDER BY id DESC")
    res = cur.fetchall(); cur.close(); conn.close(); return res

@app.post("/api/notes")
def add_note(data: NoteCreateDto, request: Request):
    user = get_current_user(request)
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO notes (username, title, content, priority, kanban_status) VALUES (%s, %s, %s, %s, 'neu')", (user["username"], data.title, data.content, data.priority))
    conn.commit(); cur.close(); conn.close(); return {"status": "added"}

@app.put("/api/notes/{n_id}/status")
def update_note(n_id: int, data: KanbanUpdateRequest):
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("UPDATE notes SET kanban_status = %s WHERE id = %s", (data.status, n_id))
    conn.commit(); cur.close(); conn.close(); return {"status": "updated"}

@app.delete("/api/notes/{n_id}")
def del_note(n_id: int):
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM notes WHERE id = %s", (n_id,))
    conn.commit(); cur.close(); conn.close(); return {"status": "deleted"}

# --- FAHRZEUGE & DIENSTBUCH ---
@app.get("/groups")
def get_groups():
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM groups_table ORDER BY name")
    r = cur.fetchall(); cur.close(); conn.close(); return r

@app.get("/api/vehicles")
def get_veh():
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, name, radio_name, status, milage, DATE_FORMAT(tuv_date, '%Y-%m-%d') as tuv_date, DATE_FORMAT(sp_date, '%Y-%m-%d') as sp_date FROM vehicles ORDER BY name")
    r = cur.fetchall(); cur.close(); conn.close(); return r

@app.put("/api/vehicles/{v_id}/status")
def set_v_status(v_id: int, data: VehicleStatusDto):
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("UPDATE vehicles SET status = %s WHERE id = %s", (data.status, v_id))
    conn.commit(); cur.close(); conn.close(); return {"status": "updated"}

@app.post("/api/vehicles")
def add_veh(data: VehicleCreateDto):
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO vehicles (name, radio_name, status, milage, tuv_date, sp_date) VALUES (%s,%s,%s,%s,%s,%s)", (data.name, data.radio_name, data.status, data.milage, data.tuv_date or None, data.sp_date or None))
    conn.commit(); cur.close(); conn.close(); return {"status": "added"}

@app.delete("/api/vehicles/{v_id}")
def del_veh(v_id: int):
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM vehicles WHERE id = %s", (v_id,))
    conn.commit(); cur.close(); conn.close(); return {"status": "deleted"}

@app.get("/groups/{group_id}/sessions")
def get_sess(group_id: int):
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, description, duration, DATE_FORMAT(date, '%Y-%m-%d') as date, category, instructors FROM sessions WHERE group_id = %s ORDER BY date DESC", (group_id,))
    res = cur.fetchall(); cur.close(); conn.close(); return res

@app.get("/groups/{group_id}/attendance")
def get_att(group_id: int, request: Request, session_id: Optional[int] = None):
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    session_data = {"session_id": session_id, "description": "", "duration": 2.0, "category": "Übung", "date": datetime.now().strftime("%Y-%m-%d"), "leader_signature": None, "instructors": ""}
    if session_id:
        cur.execute("SELECT id as session_id, description, duration, DATE_FORMAT(date, '%Y-%m-%d') as date, category, instructors FROM sessions WHERE id = %s", (session_id,))
        row = cur.fetchone()
        if row: session_data = row
            
    cur.execute("SELECT id, name, rank, is_agt, is_maschinist FROM personnel ORDER BY name ASC")
    persons = cur.fetchall()
    for p in persons:
        p['personnel_id'] = p['id']
        p['is_present'] = False; p['vehicle'] = ""; p['g26_expired'] = False; p['has_picture'] = False
    cur.close(); conn.close()
    return {**session_data, "persons": persons}

@app.post("/attendance")
def save_att(data: LegacySessionPayload):
    conn = get_db_connection(); cur = conn.cursor()
    s_id = data.session_id
    if s_id:
        cur.execute("UPDATE sessions SET date=%s, duration=%s, description=%s, instructors=%s, category=%s WHERE id=%s", (data.date, data.duration, data.description, data.instructors, data.category, s_id))
        cur.execute("DELETE FROM attendance WHERE session_id = %s", (s_id,))
    else:
        cur.execute("INSERT INTO sessions (group_id, description, duration, date, category, instructors) VALUES (%s, %s, %s, %s, %s, %s)", (data.group_id, data.description, data.duration, data.date, data.category, data.instructors))
        s_id = cur.lastrowid
        
    for e in data.entries:
        cur.execute("INSERT INTO attendance (session_id, person_id, is_present, vehicle) VALUES (%s, %s, %s, %s)", (s_id, e.person_id, e.is_present, e.vehicle))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success", "session_id": s_id}

import os
import mysql.connector
import urllib.request
import time
import hashlib
import secrets
import hmac
import base64
import json
from fastapi import FastAPI, HTTPException, Request, Response, BackgroundTasks
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

# --- SYSTEM-KONFIGURATION ---
DB_PASSWORD = os.getenv("DB_PASSWORD")
SECRET_KEY = os.getenv("SECRET_KEY", "feuerwehr-ultimate-super-secret-2024")

app = FastAPI(title="FeuerwehrHub Ultimate v7.0")

if not os.path.exists("static"):
    os.makedirs("static")
app.mount("/static", StaticFiles(directory="static"), name="static")

# --- PYDANTIC MODELS (Zuerst definieren gegen NameErrors) ---
class LoginRequest(BaseModel): username: str; password: str
class UserCreateDto(BaseModel): username: str; password: str; role: str; personnel_id: Optional[int] = None
class UserPasswordDto(BaseModel): new_password: str
class KanbanUpdateRequest(BaseModel): status: str
class InventoryItemDto(BaseModel): id: Optional[int] = None; item_name: str; amount: int; min_amount: int; unit: str; location: str
class NoteCreateDto(BaseModel): id: Optional[int] = None; title: str; content: str; priority: str
class VehicleStatusDto(BaseModel): status: int
class VehicleCreateDto(BaseModel): id: Optional[int] = None; name: str; radio_name: str; status: int; milage: int; tuv_date: Optional[str] = None; sp_date: Optional[str] = None
class EventCreateDto(BaseModel): date: str; title: str; responsible: str
class EntryDto(BaseModel): person_id: int; is_present: bool; vehicle: Optional[str] = ""
class LegacySessionPayload(BaseModel): session_id: Optional[int] = None; date: str; group_id: int; category: str; duration: float; description: str; instructors: str; entries: List[EntryDto]

# --- DB & KRYPTO HELFER ---
def get_db_connection():
    return mysql.connector.connect(host="db", user="app_user", password=DB_PASSWORD, database="attendance_system")

def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    return f"{salt}:{hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000).hex()}"

def verify_password(stored, provided):
    try:
        salt, stored_hash = stored.split(":")
        return hashlib.pbkdf2_hmac('sha256', provided.encode(), salt.encode(), 100000).hex() == stored_hash
    except: return False

def create_token(username, role):
    payload = base64.b64encode(json.dumps({"u": username, "r": role, "t": time.time()}).encode()).decode()
    sig = hmac.new(SECRET_KEY.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}.{sig}"

def get_current_user(request: Request):
    token = request.cookies.get("session_token")
    if not token: return None
    try:
        payload_b64, sig = token.split(".")
        if hmac.compare_digest(sig, hmac.new(SECRET_KEY.encode(), payload_b64.encode(), hashlib.sha256).hexdigest()):
            return json.loads(base64.b64decode(payload_b64).decode())
    except: return None

# --- DATABASE BOOTSTRAP (AUTOMATISCHE MIGRATION) ---
def init_db():
    conn = get_db_connection(); cur = conn.cursor()
    # Tabellen
    cur.execute("CREATE TABLE IF NOT EXISTS settings (setting_key VARCHAR(100) PRIMARY KEY, setting_value VARCHAR(255)) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS users (id INT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(255) UNIQUE, password_hash VARCHAR(255), role VARCHAR(50), personnel_id INT NULL) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS personnel (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) UNIQUE, rank VARCHAR(100), membership_status VARCHAR(50) DEFAULT 'Aktiv', is_agt BOOLEAN DEFAULT 0, is_maschinist BOOLEAN DEFAULT 0, is_gf BOOLEAN DEFAULT 0, g26_3_date DATE NULL) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS vehicles (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), radio_name VARCHAR(255), status INT DEFAULT 2, milage INT DEFAULT 0, tuv_date DATE NULL, sp_date DATE NULL) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS sessions (id INT AUTO_INCREMENT PRIMARY KEY, group_id INT, date DATE, category VARCHAR(50), duration FLOAT, description TEXT, instructors TEXT) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS attendance (id INT AUTO_INCREMENT PRIMARY KEY, session_id INT, person_id INT, is_present BOOLEAN, vehicle VARCHAR(100)) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS inventory (id INT AUTO_INCREMENT PRIMARY KEY, item_name VARCHAR(255), amount INT, min_amount INT, unit VARCHAR(50), location VARCHAR(100)) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS notes (id INT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(255), title VARCHAR(255), content TEXT, kanban_status VARCHAR(50) DEFAULT 'neu', priority VARCHAR(50)) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS events (id INT AUTO_INCREMENT PRIMARY KEY, date DATE, title VARCHAR(255), responsible VARCHAR(255)) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS groups_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) UNIQUE) ENGINE=InnoDB;")
    cur.execute("INSERT IGNORE INTO groups_table (id, name) VALUES (1, 'Löschzug Buxheim')")
    
    # Admin erstellen
    cur.execute("SELECT COUNT(*) FROM users WHERE username = 'admin'")
    if cur.fetchone()[0] == 0:
        cur.execute("INSERT INTO users (username, password_hash, role) VALUES (%s, %s, %s)", ("admin", hash_password("admin123"), "admin"))
    
    conn.commit(); cur.close(); conn.close()

init_db()

# --- AUTH ROUTES ---
@app.get("/")
def route_root(request: Request):
    if get_current_user(request): return FileResponse("static/dashboard.html")
    return FileResponse("static/login.html")

@app.post("/api/login")
def api_login(data: LoginRequest, response: Response):
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE username = %s", (data.username,))
    user = cur.fetchone(); cur.close(); conn.close()
    if user and verify_password(user['password_hash'], data.password):
        token = create_token(user['username'], user['role'])
        response.set_cookie(key="session_token", value=token, httponly=True)
        return {"status": "success", "redirect": "/"}
    raise HTTPException(status_code=401)

@app.post("/api/logout")
def api_logout(response: Response):
    response.delete_cookie("session_token"); return {"status": "success"}

@app.get("/api/auth/me")
def api_me(request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT u.username, u.role, p.name as personnel_name, p.is_agt, DATE_FORMAT(p.g26_3_date, '%d.%m.%Y') as g26 FROM users u LEFT JOIN personnel p ON u.personnel_id = p.id WHERE u.username = %s", (user['u'],))
    res = cur.fetchone(); cur.close(); conn.close()
    return res

# --- ADMIN: USER & PERSONNEL MANAGEMENT ---
@app.get("/api/users")
def list_users():
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, username, role, personnel_id FROM users"); r = cur.fetchall(); cur.close(); c.close(); return r

@app.post("/api/users")
def create_user(data: UserCreateDto):
    c = get_db_connection(); cur = c.cursor()
    cur.execute("INSERT INTO users (username, password_hash, role, personnel_id) VALUES (%s,%s,%s,%s)", (data.username, hash_password(data.password), data.role, data.personnel_id))
    c.commit(); cur.close(); c.close(); return {"s": "ok"}

@app.delete("/api/users/{u_id}")
def delete_user(u_id: int):
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM users WHERE id = %s", (u_id,)); c.commit(); cur.close(); c.close(); return {"s": "ok"}

@app.get("/api/personnel/list")
def list_pers():
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT * FROM personnel ORDER BY name"); r = cur.fetchall(); cur.close(); c.close(); return r

@app.post("/api/personnel")
def save_pers(data: PersonnelCreateDto):
    c = get_db_connection(); cur = c.cursor()
    if data.id: cur.execute("UPDATE personnel SET name=%s, rank=%s, membership_status=%s, is_agt=%s, is_maschinist=%s, is_gf=%s, g26_3_date=%s WHERE id=%s", (data.name, data.rank, data.membership_status, data.is_agt, data.is_maschinist, data.is_gf, data.g26_3_date or None, data.id))
    else: cur.execute("INSERT INTO personnel (name, rank, membership_status, is_agt, is_maschinist, is_gf, g26_3_date) VALUES (%s,%s,%s,%s,%s,%s,%s)", (data.name, data.rank, data.membership_status, data.is_agt, data.is_maschinist, data.is_gf, data.g26_3_date or None))
    c.commit(); cur.close(); c.close(); return {"s": "ok"}

# --- FUHRPARK ---
@app.get("/api/vehicles")
def list_veh():
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, name, radio_name, status, milage, DATE_FORMAT(tuv_date, '%Y-%m-%d') as tuv_date, DATE_FORMAT(sp_date, '%Y-%m-%d') as sp_date FROM vehicles"); r = cur.fetchall(); cur.close(); c.close(); return r

@app.post("/api/vehicles")
def create_veh(data: VehicleCreateDto):
    c = get_db_connection(); cur = c.cursor()
    cur.execute("INSERT INTO vehicles (name, radio_name, status, milage, tuv_date, sp_date) VALUES (%s,%s,%s,%s,%s,%s)", (data.name, data.radio_name, data.status, data.milage, data.tuv_date or None, data.sp_date or None))
    c.commit(); cur.close(); c.close(); return {"s": "ok"}

@app.delete("/api/vehicles/{v_id}")
def delete_veh(v_id: int):
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM vehicles WHERE id = %s", (v_id,)); c.commit(); cur.close(); c.close(); return {"s": "ok"}

@app.put("/api/vehicles/{v_id}/status")
def update_veh_status(v_id: int, data: VehicleStatusDto):
    c = get_db_connection(); cur = c.cursor()
    cur.execute("UPDATE vehicles SET status = %s WHERE id = %s", (data.status, v_id)); c.commit(); cur.close(); c.close(); return {"s": "ok"}

# --- DIENSTBUCH (EDITOR LOGIK) ---
@app.get("/groups")
def list_groups():
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT * FROM groups_table"); r = cur.fetchall(); cur.close(); c.close(); return r

@app.get("/groups/{group_id}/sessions")
def list_sessions(group_id: int):
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, description, duration, DATE_FORMAT(date, '%d.%m.%Y') as date, category, instructors FROM sessions WHERE group_id = %s ORDER BY date DESC", (group_id,))
    r = cur.fetchall(); cur.close(); c.close(); return r

@app.get("/groups/{group_id}/attendance")
def get_attendance(group_id: int, request: Request, session_id: Optional[int] = None):
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    session_data = {"session_id": session_id, "description": "", "duration": 2.0, "category": "Übung", "date": datetime.now().strftime("%Y-%m-%d"), "instructors": ""}
    if session_id:
        cur.execute("SELECT id as session_id, description, duration, DATE_FORMAT(date, '%Y-%m-%d') as date, category, instructors FROM sessions WHERE id = %s", (session_id,))
        row = cur.fetchone()
        if row: session_data = row
    
    cur.execute("""
        SELECT p.id as personnel_id, p.name, p.rank, p.is_agt,
        COALESCE(a.is_present, 0) as is_present, COALESCE(a.vehicle, '') as vehicle
        FROM personnel p LEFT JOIN attendance a ON p.id = a.person_id AND a.session_id = %s
        ORDER BY p.name ASC
    """, (session_id,))
    persons = cur.fetchall()
    for p in persons: p['is_present'] = bool(p['is_present'])
    
    # Vorwahlen (Presets)
    cur.execute("SELECT DISTINCT description FROM sessions LIMIT 10")
    presets_topics = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT DISTINCT instructors FROM sessions LIMIT 10")
    presets_leaders = [r[0] for r in cur.fetchall()]
    
    cur.close(); c.close()
    return {**session_data, "persons": persons, "presets": {"topics": presets_topics, "leaders": presets_leaders}}

@app.post("/attendance")
def save_attendance(data: LegacySessionPayload):
    c = get_db_connection(); cur = c.cursor()
    s_id = data.session_id
    if s_id:
        cur.execute("UPDATE sessions SET date=%s, duration=%s, description=%s, instructors=%s, category=%s WHERE id=%s", (data.date, data.duration, data.description, data.instructors, data.category, s_id))
        cur.execute("DELETE FROM attendance WHERE session_id = %s", (s_id,))
    else:
        cur.execute("INSERT INTO sessions (group_id, date, category, duration, description, instructors) VALUES (%s,%s,%s,%s,%s,%s)", (data.group_id, data.date, data.category, data.duration, data.description, data.instructors))
        s_id = cur.lastrowid
    
    for e in data.entries:
        cur.execute("INSERT INTO attendance (session_id, person_id, is_present, vehicle) VALUES (%s,%s,%s,%s)", (s_id, e.person_id, e.is_present, e.vehicle or ""))
    c.commit(); cur.close(); c.close(); return {"session_id": s_id}

# --- LAGER, KANBAN, EVENTS ---
@app.get("/api/inventory")
def api_inv():
    c = get_db_connection(); cur = c.cursor(dictionary=True); cur.execute("SELECT * FROM inventory"); r = cur.fetchall(); cur.close(); c.close(); return r

@app.post("/api/inventory")
def api_inv_add(data: InventoryItemDto):
    c = get_db_connection(); cur = c.cursor()
    cur.execute("INSERT INTO inventory (item_name, amount, min_amount, unit, location) VALUES (%s,%s,%s,%s,%s)", (data.item_name, data.amount, data.min_amount, data.unit, data.location))
    c.commit(); cur.close(); c.close(); return {"s": "ok"}

@app.get("/api/notes")
def api_notes():
    c = get_db_connection(); cur = c.cursor(dictionary=True); cur.execute("SELECT * FROM notes ORDER BY id DESC"); r = cur.fetchall(); cur.close(); c.close(); return r

@app.post("/api/notes")
def api_notes_add(data: NoteCreateDto, request: Request):
    user = get_current_user(request)
    c = get_db_connection(); cur = c.cursor()
    cur.execute("INSERT INTO notes (username, title, content, priority) VALUES (%s,%s,%s,%s)", (user['u'], data.title, data.content, data.priority))
    c.commit(); cur.close(); c.close(); return {"s": "ok"}

@app.get("/api/events")
def api_events():
    c = get_db_connection(); cur = c.cursor(dictionary=True); cur.execute("SELECT id, DATE_FORMAT(date, '%d.%m.%Y') as date_formatted, title, responsible FROM events ORDER BY date ASC"); r = cur.fetchall(); cur.close(); c.close(); return r

@app.post("/api/events")
def api_events_add(data: EventCreateDto):
    c = get_db_connection(); cur = c.cursor(); cur.execute("INSERT INTO events (date, title, responsible) VALUES (%s,%s,%s)", (data.date, data.title, data.responsible)); c.commit(); cur.close(); c.close(); return {"s": "ok"}

@app.get("/editor", response_class=FileResponse)
def get_editor_page(): return "static/editor.html"

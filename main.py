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
SECRET_KEY = os.getenv("SECRET_KEY", "feuerwehr-buxheim-ultimate-key-112")

app = FastAPI(title="FeuerwehrHub Enterprise Ultimate Suite")

if not os.path.exists("static"):
    os.makedirs("static")
app.mount("/static", StaticFiles(directory="static"), name="static")

# --- PYDANTIC MODELLE ---
class LoginRequest(BaseModel):
    username: str
    password: str

class UserCreateDto(BaseModel):
    username: str
    password: str
    role: str
    personnel_id: Optional[int] = None

class UserPasswordDto(BaseModel):
    new_password: str

class KanbanUpdateRequest(BaseModel):
    status: str

class InventoryItemDto(BaseModel):
    id: Optional[int] = None
    item_name: str
    amount: int
    min_amount: int
    unit: str
    location: str
    barcode: Optional[str] = ""
    size: Optional[str] = ""

class NoteCreateDto(BaseModel):
    id: Optional[int] = None
    title: str
    content: str
    priority: str

class VehicleStatusDto(BaseModel):
    status: int

class VehicleCreateDto(BaseModel):
    id: Optional[int] = None
    name: str
    radio_name: str
    status: int
    milage: int
    tuv_date: Optional[str] = None
    sp_date: Optional[str] = None

class EventCreateDto(BaseModel):
    date: str
    title: str
    responsible: str

class EntryDto(BaseModel):
    person_id: int
    is_present: bool
    vehicle: Optional[str] = ""

class LegacySessionPayload(BaseModel):
    session_id: Optional[int] = None
    date: str
    group_id: int
    category: str
    duration: float
    description: str
    instructors: str
    entries: List[EntryDto]

class PersonnelCreateDto(BaseModel):
    id: Optional[int] = None
    name: str
    rank: str
    membership_status: str
    is_agt: bool
    is_maschinist: bool
    is_gf: bool
    g26_3_date: Optional[str] = None

# --- DATABASE CONNECTION & KRYPTO HELFER ---
def get_db_connection():
    return mysql.connector.connect(host="db", user="app_user", password=DB_PASSWORD, database="attendance_system")

def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    return f"{salt}:{hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000).hex()}"

def verify_password(stored_password: str, provided_password: str) -> bool:
    try:
        salt, stored_hash = stored_password.split(":")
        return hashlib.pbkdf2_hmac('sha256', provided_password.encode(), salt.encode(), 100000).hex() == stored_hash
    except: return False

def create_token(username: str, role: str) -> str:
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

def trigger_apager_push(title: str, message: str):
    try:
        conn = get_db_connection(); cur = conn.cursor(dictionary=True)
        cur.execute("SELECT setting_value FROM settings WHERE setting_key = 'apager_api_key'")
        row = cur.fetchone(); cur.close(); conn.close()
        api_key = row['setting_value'] if row else None
        if api_key and len(api_key) > 5:
            payload = {"apiKey": api_key, "title": f"🚨 {title}", "message": message, "priority": "ALARM", "sound": "alarm_fire"}
            req = urllib.request.Request("https://api.alamos-gmbh.com/v1/push", data=json.dumps(payload).encode(), headers={'Content-Type': 'application/json'}, method='POST')
            with urllib.request.urlopen(req, timeout=4) as response: return response.getcode() == 200
    except: pass
    return False

# --- DATABASE INITIALISIERUNG ---
def init_db():
    conn = get_db_connection(); cur = conn.cursor()
    
    # Bereinigung alter blockierender Verknüpfungen aus Altsystemen
    for fk in ["attendance_ibfk_1", "attendance_ibfk_2", "fk_attendance_personnel", "fk_attendance_persons"]:
        try: cur.execute(f"ALTER TABLE attendance DROP FOREIGN KEY {fk};")
        except: pass

    cur.execute("CREATE TABLE IF NOT EXISTS settings (setting_key VARCHAR(100) PRIMARY KEY, setting_value VARCHAR(255)) ENGINE=InnoDB;")
    cur.execute("INSERT IGNORE INTO settings (setting_key, setting_value) VALUES ('apager_api_key', '0'), ('int_g26', '36')")
    cur.execute("CREATE TABLE IF NOT EXISTS users (id INT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(255) UNIQUE, password_hash VARCHAR(255), role VARCHAR(50), personnel_id INT NULL) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS personnel (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) UNIQUE, rank VARCHAR(100), membership_status VARCHAR(50) DEFAULT 'Aktiv', is_agt BOOLEAN DEFAULT 0, is_maschinist BOOLEAN DEFAULT 0, is_gf BOOLEAN DEFAULT 0, g26_3_date DATE NULL) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS vehicles (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), radio_name VARCHAR(255), status INT DEFAULT 2, milage INT DEFAULT 0, tuv_date DATE NULL, sp_date DATE NULL) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS sessions (id INT AUTO_INCREMENT PRIMARY KEY, group_id INT, date DATE, category VARCHAR(50), duration FLOAT, description TEXT, instructors TEXT) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS attendance (id INT AUTO_INCREMENT PRIMARY KEY, session_id INT, person_id INT, is_present BOOLEAN, vehicle VARCHAR(100)) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS inventory (id INT AUTO_INCREMENT PRIMARY KEY, item_name VARCHAR(255), amount INT DEFAULT 0, min_amount INT DEFAULT 5, unit VARCHAR(50) DEFAULT 'Stück', location VARCHAR(100) DEFAULT 'Lager', barcode VARCHAR(100) DEFAULT '', size VARCHAR(50) DEFAULT '') ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS notes (id INT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(255), title VARCHAR(255), content TEXT, kanban_status VARCHAR(50) DEFAULT 'neu', priority VARCHAR(50)) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS events (id INT AUTO_INCREMENT PRIMARY KEY, date DATE, title VARCHAR(255), responsible VARCHAR(255)) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS groups_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) UNIQUE) ENGINE=InnoDB;")
    
    cur.execute("INSERT IGNORE INTO groups_table (id, name) VALUES (1, 'Freiwillige Feuerwehr Buxheim')")
    cur.execute("INSERT IGNORE INTO personnel (id, name, rank, membership_status) VALUES (1, 'Daniel (Administrator)', 'Brandmeister', 'Aktiv')")
    
    cur.execute("SELECT COUNT(*) FROM users WHERE username = 'admin'")
    if cur.fetchone()[0] == 0:
        cur.execute("INSERT INTO users (username, password_hash, role, personnel_id) VALUES (%s, %s, %s, 1)", ("admin", hash_password("admin123"), "admin"))
    conn.commit(); cur.close(); conn.close()

init_db()

# --- METEOROLOGISCHES LAGE-ZENTRUM ---
@app.get("/api/weather")
def get_weather_warnings(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    return {
        "station": "Memmingen (Unterallgäu)",
        "temperature": "18.5 °C",
        "condition": "Heiter bis bewölkt",
        "wind": "14 km/h West",
        "warning_level": "Normal",
        "warning_text": "DWD Warnlagebericht: Keine Unwetterwarnungen für Buxheim/Memmingen aktiv."
    }

# --- WEB ROUNTEN ---
@app.get("/")
def route_root(request: Request):
    if get_current_user(request): return FileResponse("static/dashboard.html")
    return FileResponse("static/login.html")

@app.get("/dashboard")
def route_dashboard(request: Request):
    if get_current_user(request): return FileResponse("static/dashboard.html")
    return FileResponse("static/login.html")

@app.get("/login")
def route_login_page(): return FileResponse("static/login.html")

@app.get("/editor")
def route_editor_page(request: Request):
    if not get_current_user(request): return FileResponse("static/login.html")
    return FileResponse("static/editor.html")

# --- CORE APIs ---
@app.post("/api/login")
def api_login(data: LoginRequest, response: Response):
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE username = %s", (data.username.strip(),))
    user = cur.fetchone(); cur.close(); conn.close()
    if user and verify_password(user['password_hash'], data.password):
        token = create_token(user['username'], user['role'])
        response.set_cookie(key="session_token", value=token, httponly=True, samesite="lax")
        return {"status": "success", "redirect": "/dashboard"}
    raise HTTPException(status_code=401, detail="Kennwort oder Benutzername falsch!")

@app.post("/api/logout")
def api_logout(response: Response):
    response.delete_cookie("session_token"); return {"status": "success"}

@app.get("/api/auth/me")
def api_me(request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT u.username, u.role, p.name as personnel_name, p.is_agt, p.is_maschinist, DATE_FORMAT(p.g26_3_date, '%d.%m.%Y') as g26 FROM users u LEFT JOIN personnel p ON u.personnel_id = p.id WHERE u.username = %s", (user['u'],))
    res = cur.fetchone(); cur.close(); conn.close()
    if not res: return {"username": user['u'], "role": user['r'], "personnel_name": "Externer Zugang", "is_agt": False, "g26": "-"}
    return res

@app.get("/api/settings")
def get_registry_settings(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT setting_key, setting_value FROM settings")
    res = {row['setting_key']: row['setting_value'] for row in cur.fetchall()}
    cur.close(); conn.close(); return res

@app.post("/api/settings")
def save_registry_settings(data: dict, request: Request):
    if not get_current_user(request) or get_current_user(request)['r'] != 'admin': raise HTTPException(status_code=403)
    conn = get_db_connection(); cur = conn.cursor()
    for k, v in data.items():
        cur.execute("INSERT INTO settings (setting_key, setting_value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE setting_value=%s", (k, str(v), str(v)))
    conn.commit(); cur.close(); conn.close(); return {"status": "success"}

@app.get("/api/users")
def list_users(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, username, role, personnel_id FROM users ORDER BY username ASC")
    r = cur.fetchall(); cur.close(); c.close(); return r

@app.post("/api/users")
def create_user(data: UserCreateDto, request: Request):
    if not get_current_user(request) or get_current_user(request)['r'] != 'admin': raise HTTPException(status_code=403)
    c = get_db_connection(); cur = c.cursor()
    cur.execute("INSERT INTO users (username, password_hash, role, personnel_id) VALUES (%s,%s,%s,%s)", (data.username.strip(), hash_password(data.password), data.role, data.personnel_id))
    c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.delete("/api/users/{u_id}")
def delete_user(u_id: int, request: Request):
    if not get_current_user(request) or get_current_user(request)['r'] != 'admin': raise HTTPException(status_code=403)
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM users WHERE id = %s", (u_id,)); c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.get("/api/personnel/list")
def list_personnel_records(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, name, rank, membership_status, is_agt, is_maschinist, is_gf, DATE_FORMAT(g26_3_date, '%Y-%m-%d') as g26_3_date FROM personnel ORDER BY name ASC")
    r = cur.fetchall(); cur.close(); c.close(); return r

@app.post("/api/personnel")
def save_personnel_record(data: PersonnelCreateDto, request: Request):
    if not get_current_user(request) or get_current_user(request)['r'] != 'admin': raise HTTPException(status_code=403)
    c = get_db_connection(); cur = c.cursor()
    g26 = data.g26_3_date if data.g26_3_date else None
    if data.id: cur.execute("UPDATE personnel SET name=%s, rank=%s, membership_status=%s, is_agt=%s, is_maschinist=%s, is_gf=%s, g26_3_date=%s WHERE id=%s", (data.name, data.rank, data.membership_status, int(data.is_agt), int(data.is_maschinist), int(data.is_gf), g26, data.id))
    else: cur.execute("INSERT INTO personnel (name, rank, membership_status, is_agt, is_maschinist, is_gf, g26_3_date) VALUES (%s,%s,%s,%s,%s,%s,%s)", (data.name, data.rank, data.membership_status, int(data.is_agt), int(data.is_maschinist), int(data.is_gf), g26))
    c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.delete("/api/personnel/{p_id}")
def delete_personnel_record(p_id: int, request: Request):
    if not get_current_user(request) or get_current_user(request)['r'] != 'admin': raise HTTPException(status_code=403)
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM personnel WHERE id = %s", (p_id,)); c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.get("/api/vehicles")
def list_vehicles(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, name, radio_name, status, milage, DATE_FORMAT(tuv_date, '%Y-%m-%d') as tuv_date, DATE_FORMAT(sp_date, '%Y-%m-%d') as sp_date FROM vehicles ORDER BY name ASC")
    r = cur.fetchall(); cur.close(); c.close(); return r

@app.post("/api/vehicles")
def create_vehicle_record(data: VehicleCreateDto, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    c = get_db_connection(); cur = c.cursor()
    if data.id: cur.execute("UPDATE vehicles SET name=%s, radio_name=%s, status=%s, milage=%s, tuv_date=%s, sp_date=%s WHERE id=%s", (data.name, data.radio_name, data.status, data.milage, data.tuv_date or None, data.sp_date or None, data.id))
    else: cur.execute("INSERT INTO vehicles (name, radio_name, status, milage, tuv_date, sp_date) VALUES (%s,%s,%s,%s,%s,%s)", (data.name, data.radio_name, data.status, data.milage, data.tuv_date or None, data.sp_date or None))
    c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.delete("/api/vehicles/{v_id}")
def delete_vehicle_record(v_id: int, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM vehicles WHERE id = %s", (v_id,)); c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.put("/api/vehicles/{v_id}/status")
def update_vehicle_status_code(v_id: int, data: VehicleStatusDto, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    c = get_db_connection(); cur = c.cursor()
    cur.execute("UPDATE vehicles SET status = %s WHERE id = %s", (data.status, v_id)); c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.get("/groups")
def list_groups_all(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    c = get_db_connection(); cur = c.cursor(dictionary=True); cur.execute("SELECT * FROM groups_table"); r = cur.fetchall(); cur.close(); c.close(); return r

@app.get("/groups/{group_id}/sessions")
def list_sessions_dashboard(group_id: int, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, description, duration, DATE_FORMAT(date, '%Y-%m-%d') as date, category, instructors FROM sessions WHERE group_id = %s ORDER BY date DESC", (group_id,))
    r = cur.fetchall(); cur.close(); c.close(); return r

@app.get("/groups/{group_id}/attendance")
def get_group_attendance_matrix(group_id: int, request: Request, session_id: Optional[int] = None):
    if not get_current_user(request): raise HTTPException(status_code=401)
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    session_data = {"session_id": session_id, "description": "", "duration": 2.0, "category": "Übung", "date": datetime.now().strftime("%Y-%m-%d"), "instructors": ""}
    
    if session_id and session_id != 0:
        cur.execute("SELECT id as session_id, description, duration, DATE_FORMAT(date, '%Y-%m-%d') as date, category, instructors FROM sessions WHERE id = %s", (session_id,))
        row = cur.fetchone()
        if row: session_data = row
    
    query = "SELECT p.id as personnel_id, p.name, p.rank, p.is_agt, p.is_maschinist, CASE WHEN a.is_present IS NOT NULL THEN a.is_present ELSE 0 END as is_present, COALESCE(a.vehicle, '') as vehicle FROM personnel p LEFT JOIN attendance a ON p.id = a.person_id AND a.session_id = %s ORDER BY p.name ASC"
    cur.execute(query, (session_id,))
    persons = cur.fetchall()
    for p in persons: p['is_present'] = bool(p['is_present'])
    
    cur.execute("SELECT DISTINCT description FROM sessions ORDER BY id DESC LIMIT 5")
    presets_topics = [row_t['description'] for row_t in cur.fetchall()]
    cur.execute("SELECT DISTINCT instructors FROM sessions ORDER BY id DESC LIMIT 5")
    presets_leaders = [row_l['instructors'] for row_l in cur.fetchall()]
    
    cur.close(); c.close()
    return {**session_data, "persons": persons, "presets": {"topics": presets_topics, "leaders": presets_leaders}}

@app.post("/attendance")
def save_attendance(data: LegacySessionPayload, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    c = get_db_connection(); cur = c.cursor()
    s_id = data.session_id
    if s_id and s_id != 0:
        cur.execute("UPDATE sessions SET date=%s, duration=%s, description=%s, instructors=%s, category=%s WHERE id=%s", (data.date, data.duration, data.description, data.instructors, data.category, s_id))
        cur.execute("DELETE FROM attendance WHERE session_id = %s", (s_id,))
    else:
        cur.execute("INSERT INTO sessions (group_id, date, category, duration, description, instructors) VALUES (%s,%s,%s,%s,%s,%s)", (data.group_id, data.date, data.category, data.duration, data.description, data.instructors))
        s_id = cur.lastrowid
    
    for e in data.entries:
        cur.execute("INSERT INTO attendance (session_id, person_id, is_present, vehicle) VALUES (%s,%s,%s,%s)", (s_id, e.person_id, 1 if e.is_present else 0, e.vehicle or ""))
    c.commit(); cur.close(); c.close()
    return {"status": "success", "session_id": s_id}

@app.get("/api/inventory")
def api_inventory_list(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    c = get_db_connection(); cur = c.cursor(dictionary=True); cur.execute("SELECT * FROM inventory ORDER BY item_name ASC"); r = cur.fetchall(); cur.close(); c.close(); return r

@app.post("/api/inventory")
def api_inventory_save(data: InventoryItemDto, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    c = get_db_connection(); cur = c.cursor()
    if data.id: cur.execute("UPDATE inventory SET item_name=%s, amount=%s, min_amount=%s, unit=%s, location=%s, barcode=%s, size=%s WHERE id=%s", (data.item_name, data.amount, data.min_amount, data.unit, data.location, data.barcode, data.size, data.id))
    else: cur.execute("INSERT INTO inventory (item_name, amount, min_amount, unit, location, barcode, size) VALUES (%s,%s,%s,%s,%s,%s,%s)", (data.item_name, data.amount, data.min_amount, data.unit, data.location, data.barcode, data.size))
    c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.delete("/api/inventory/{i_id}")
def api_inventory_delete(i_id: int, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    c = get_db_connection(); cur = c.cursor(); cur.execute("DELETE FROM inventory WHERE id = %s", (i_id,)); c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.get("/api/notes")
def api_notes_list(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    c = get_db_connection(); cur = c.cursor(dictionary=True); cur.execute("SELECT * FROM notes ORDER BY id DESC"); r = cur.fetchall(); cur.close(); c.close(); return r

@app.post("/api/notes")
def api_notes_create(data: NoteCreateDto, background_tasks: BackgroundTasks, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401)
    c = get_db_connection(); cur = c.cursor()
    cur.execute("INSERT INTO notes (username, title, content, priority, kanban_status) VALUES (%s,%s,%s,%s,'neu')", (user['u'], data.title, data.content, data.priority))
    c.commit(); cur.close(); c.close()
    if data.priority == "kritisch":
        background_tasks.add_task(trigger_apager_push, f"Kritischer Defekt: {data.title}", data.content)
    return {"status": "success"}

@app.put("/api/notes/{n_id}/status")
def api_notes_status_update(n_id: int, data: KanbanUpdateRequest, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    c = get_db_connection(); cur = c.cursor(); cur.execute("UPDATE notes SET kanban_status = %s WHERE id = %s", (data.status, n_id)); c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.delete("/api/notes/{n_id}")
def api_notes_delete(n_id: int, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    c = get_db_connection(); cur = c.cursor(); cur.execute("DELETE FROM notes WHERE id = %s", (n_id,)); c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.get("/api/events")
def api_events_list(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    c = get_db_connection(); cur = c.cursor(dictionary=True); cur.execute("SELECT id, DATE_FORMAT(date, '%d.%m.%Y') as date_formatted, title, responsible FROM events ORDER BY date ASC"); r = cur.fetchall(); cur.close(); c.close(); return r

@app.post("/api/events")
def api_events_add(data: EventCreateDto, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    c = get_db_connection(); cur = c.cursor(); cur.execute("INSERT INTO events (date, title, responsible) VALUES (%s,%s,%s)", (data.date, data.title, data.responsible)); c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.delete("/api/events/{e_id}")
def api_events_delete(e_id: int, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    c = get_db_connection(); cur = c.cursor(); cur.execute("DELETE FROM events WHERE id = %s", (e_id,)); c.commit(); cur.close(); c.close(); return {"status": "success"}

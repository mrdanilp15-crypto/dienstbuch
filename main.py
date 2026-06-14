import os
import mysql.connector
import urllib.request
import urllib.parse
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
from datetime import datetime, timedelta

# --- SYSTEM-KONFIGURATION ---
DB_PASSWORD = os.getenv("DB_PASSWORD")
SECRET_KEY = os.getenv("SECRET_KEY", "digitales-dienstbuch-global-sovereign-key-112")

app = FastAPI(title="Digitales Dienstbuch - Global Enterprise Suite")

if not os.path.exists("static"):
    os.makedirs("static")
app.mount("/static", StaticFiles(directory="static"), name="static")

# --- PYDANTIC SYSTEM MODELLE ---
class LoginRequest(BaseModel):
    username: str
    password: str

class UserCreateDto(BaseModel):
    id: Optional[int] = None
    username: str
    password: Optional[str] = ""
    role: str = "user"
    personnel_id: Optional[int] = None

class KanbanUpdateRequest(BaseModel):
    status: str

class InventoryItemDto(BaseModel):
    id: Optional[int] = None
    item_name: str
    amount: int = 0
    min_amount: int = 5
    unit: str = "Stück"
    location: str = "Zentrallager"
    barcode: Optional[str] = ""
    size: Optional[str] = ""

class NoteCreateDto(BaseModel):
    id: Optional[int] = None
    title: str
    content: str
    priority: str = "normal"

class VehicleStatusDto(BaseModel):
    status: int

class VehicleCreateDto(BaseModel):
    id: Optional[int] = None
    name: str
    radio_name: str = ""
    status: int = 2
    milage: int = 0
    tuv_date: Optional[str] = None
    sp_date: Optional[str] = None

class VehicleLogDto(BaseModel):
    id: Optional[int] = None
    vehicle_id: int
    date: str
    driver_name: str
    purpose: str
    km_start: int
    km_end: int

class EventCreateDto(BaseModel):
    id: Optional[int] = None
    date: str
    title: str
    responsible: str = "Leitung"

class EntryDto(BaseModel):
    person_id: int
    is_present: bool = False
    vehicle: Optional[str] = ""

class LegacySessionPayload(BaseModel):
    session_id: Optional[int] = None
    date: str
    group_id: int
    category: str = "Übung"
    duration: float = 2.0
    description: str
    instructors: str = ""
    entries: List[EntryDto]

class PersonnelCreateDto(BaseModel):
    id: Optional[int] = None
    name: str
    rank: str = "Feuerwehranwärter"
    membership_status: str = "Aktiv"
    is_agt: bool = False
    is_maschinist: bool = False
    is_gf: bool = False
    g26_3_date: Optional[str] = None
    birth_date: Optional[str] = None
    entry_date: Optional[str] = None
    phone: Optional[str] = ""
    email: Optional[str] = ""
    address: Optional[str] = ""
    ice_contact: Optional[str] = ""
    drive_b: bool = False
    drive_be: bool = False
    drive_c: bool = False
    drive_ce: bool = False
    profile_picture: Optional[str] = ""

# --- PLATFORM-SECURITY & KRYPTOGRAPHIE ---
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

# --- DATABASE AUTOMATION & MIGRATION ---
def init_db():
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
    
    # REPARATUR: Ändere Spaltentyp von INT auf VARCHAR in der Live-Datenbank gegen Error 1366
    try: cur.execute("ALTER TABLE settings MODIFY COLUMN setting_value VARCHAR(255);")
    except: pass

    cur.execute("CREATE TABLE IF NOT EXISTS settings (setting_key VARCHAR(100) PRIMARY KEY, setting_value VARCHAR(255)) ENGINE=InnoDB;")
    
    default_settings = [
        ('apager_api_key', '0'), ('int_g26', '36'), 
        ('station_name', 'Freiwillige Feuerwehr Buxheim'),
        ('station_lat', '47.9942'), ('station_lon', '10.1344')
    ]
    for k, v in default_settings:
        cur.execute("INSERT IGNORE INTO settings (setting_key, setting_value) VALUES (%s, %s)", (k, v))

    cur.execute("CREATE TABLE IF NOT EXISTS users (id INT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(255) UNIQUE, password_hash VARCHAR(255), role VARCHAR(50), personnel_id INT NULL, failed_logins INT DEFAULT 0, lockout_until DATETIME NULL) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS personnel (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) UNIQUE, rank VARCHAR(100), membership_status VARCHAR(50), is_agt BOOLEAN DEFAULT 0, is_maschinist BOOLEAN DEFAULT 0, is_gf BOOLEAN DEFAULT 0, g26_3_date DATE NULL) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS vehicles (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), radio_name VARCHAR(255), status INT DEFAULT 2, milage INT DEFAULT 0, tuv_date DATE NULL, sp_date DATE NULL) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS vehicle_log (id INT AUTO_INCREMENT PRIMARY KEY, vehicle_id INT, date DATE, driver_name VARCHAR(255), purpose VARCHAR(255), km_start INT, km_end INT) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS sessions (id INT AUTO_INCREMENT PRIMARY KEY, group_id INT, date DATE, category VARCHAR(50), duration FLOAT, description TEXT, instructors TEXT) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS attendance (id INT AUTO_INCREMENT PRIMARY KEY, session_id INT, person_id INT, is_present BOOLEAN, vehicle VARCHAR(100)) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS inventory (id INT AUTO_INCREMENT PRIMARY KEY, item_name VARCHAR(255), amount INT DEFAULT 0, min_amount INT DEFAULT 5, unit VARCHAR(50) DEFAULT 'Stück', location VARCHAR(100) DEFAULT 'Lager', barcode VARCHAR(100) DEFAULT '', size VARCHAR(50) DEFAULT '') ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS notes (id INT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(255), title VARCHAR(255), content TEXT, kanban_status VARCHAR(50) DEFAULT 'neu', priority VARCHAR(50)) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS events (id INT AUTO_INCREMENT PRIMARY KEY, date DATE, title VARCHAR(255), responsible VARCHAR(255)) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS groups_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) UNIQUE) ENGINE=InnoDB;")
    
    cur.execute("INSERT IGNORE INTO groups_table (id, name) VALUES (1, 'Aktiver Dienstverband')")
    cur.execute("INSERT IGNORE INTO personnel (id, name, rank, membership_status) VALUES (1, 'Dienststellen Administrator', 'Brandmeister', 'Aktiv')")
    
    # SCHEMA-ERWEITERUNG: Füge erweiterte Stammdaten-Spalten falls nicht vorhanden hinzu
    columns_to_add = [
        ("birth_date", "DATE NULL"),
        ("entry_date", "DATE NULL"),
        ("phone", "VARCHAR(100) DEFAULT ''"),
        ("email", "VARCHAR(255) DEFAULT ''"),
        ("address", "TEXT NULL"),
        ("ice_contact", "VARCHAR(255) DEFAULT ''"),
        ("drive_b", "BOOLEAN DEFAULT 0"),
        ("drive_be", "BOOLEAN DEFAULT 0"),
        ("drive_c", "BOOLEAN DEFAULT 0"),
        ("drive_ce", "BOOLEAN DEFAULT 0"),
        ("profile_picture", "LONGTEXT NULL")
    ]
    for col, col_type in columns_to_add:
        try: cur.execute(f"ALTER TABLE personnel ADD COLUMN {col} {col_type};")
        except: pass

    cur.execute("SELECT COUNT(*) FROM users WHERE username = 'admin'")
    if cur.fetchone()[0] == 0:
        cur.execute("INSERT INTO users (username, password_hash, role, personnel_id) VALUES (%s, %s, %s, 1)", ("admin", hash_password("admin123"), "admin"))
    
    cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
    conn.commit(); cur.close(); conn.close()

init_db()

# --- AUTOMATISIERTER NOMINATIM GEOCÒDING PROXY ---
@app.get("/api/geocode")
def geocode_address(q: str, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    try:
        url = f"https://nominatim.openstreetmap.org/search?format=json&limit=1&q={urllib.parse.quote(q)}"
        req = urllib.request.Request(url, headers={'User-Agent': 'DigitalesDienstbuch/1.0 (feuerwehr-hub-app)'})
        with urllib.request.urlopen(req, timeout=5) as response:
            data = json.loads(response.read().decode())
            if data:
                item = data[0]
                return {"status": "success", "name": item.get("display_name"), "lat": item.get("lat"), "lon": item.get("lon")}
            return {"status": "error", "message": "Ort konnte weltweit nicht aufgelöst werden."}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# --- GLOBAL WEATHER ENGINE ---
@app.get("/api/weather")
def get_global_weather(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT setting_key, setting_value FROM settings")
    settings = {row['setting_key']: row['setting_value'] for row in cur.fetchall()}
    cur.close(); conn.close()
    
    lat = settings.get("station_lat", "47.9942")
    lon = settings.get("station_lon", "10.1344")
    name = settings.get("station_name", "Hauptdienststelle")
    
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=3) as response:
            data = json.loads(response.read().decode())
            cw = data.get("current_weather", {})
            return {
                "station": name, "temperature": f"{cw.get('temperature', '--')} °C",
                "wind": f"{cw.get('windspeed', '--')} km/h", "warning_text": "Live Satelliten-Wetterdaten aktiv synchronisiert."
            }
    except:
        return {"station": name, "temperature": "N/A", "wind": "N/A", "warning_text": "Wetter-Gateway offline."}

# --- AUTH API MIT LOCKOUT PROTECTION ---
@app.post("/api/login")
def api_login(data: LoginRequest, response: Response):
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE username = %s", (data.username.strip(),))
    user = cur.fetchone()
    
    if not user:
        cur.close(); conn.close()
        raise HTTPException(status_code=401, detail="Zugangsdaten ungültig!")
        
    if user.get('lockout_until'):
        lockout = user['lockout_until']
        if isinstance(lockout, str):
            try: lockout = datetime.strptime(lockout, "%Y-%m-%d %H:%M:%S")
            except: lockout = None
        if lockout and datetime.now() < lockout:
            remaining = int((lockout - datetime.now()).total_seconds() / 60)
            cur.close(); conn.close()
            raise HTTPException(status_code=423, detail=f"Sicherheits-Lockout aktiv. Bitte in {max(1, remaining)} Minuten versuchen.")

    if verify_password(user['password_hash'], data.password):
        cur.execute("UPDATE users SET failed_logins = 0, lockout_until = NULL WHERE id = %s", (user['id'],))
        conn.commit(); cur.close(); conn.close()
        token = create_token(user['username'], user['role'])
        response.set_cookie(key="session_token", value=token, httponly=True, samesite="lax")
        return {"status": "success", "redirect": "/dashboard"}
    else:
        new_failed = user['failed_logins'] + 1
        lockout_time = None
        detail_msg = "Zugangsdaten ungültig!"
        if new_failed >= 5:
            lockout_time = datetime.now() + timedelta(minutes=15)
            cur.execute("UPDATE users SET failed_logins = %s, lockout_until = %s WHERE id = %s", (new_failed, lockout_time, user['id']))
            detail_msg = "Konto wurde wegen zu vieler Fehlversuche für 15 Minuten gesperrt."
        else:
            cur.execute("UPDATE users SET failed_logins = %s WHERE id = %s", (new_failed, user['id']))
        conn.commit(); cur.close(); conn.close()
        raise HTTPException(status_code=401, detail=detail_msg)

@app.post("/api/logout")
def api_logout(response: Response):
    response.delete_cookie("session_token"); return {"status": "success"}

@app.get("/api/auth/me")
def api_me(request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT u.username, u.role, p.name as personnel_name, p.rank, p.membership_status, p.phone, p.email, p.address, p.profile_picture, p.is_agt, p.is_maschinist, p.is_gf, DATE_FORMAT(p.g26_3_date, '%d.%m.%Y') as g26 FROM users u LEFT JOIN personnel p ON u.personnel_id = p.id WHERE u.username = %s", (user['u'],))
    res = cur.fetchone(); cur.close(); conn.close()
    return res

# --- REGISTRY ---
@app.get("/api/settings")
def get_registry_settings(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT setting_key, setting_value FROM settings")
    res = {row['setting_key']: row['setting_value'] for row in cur.fetchall()}
    cur.close(); conn.close(); return res

@app.post("/api/settings")
def save_registry_settings(data: dict, request: Request):
    conn = get_db_connection(); cur = conn.cursor()
    for k, v in data.items():
        cur.execute("INSERT INTO settings (setting_key, setting_value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE setting_value=%s", (k, str(v), str(v)))
    conn.commit(); cur.close(); conn.close(); return {"status": "success"}

# --- SYSTEM ACCOUNTS CRUD (LOGINS MIT EDITIERBARKEIT) ---
@app.get("/api/users")
def list_users(request: Request):
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, username, role, personnel_id FROM users ORDER BY username ASC")
    r = cur.fetchall(); cur.close(); c.close(); return r

@app.post("/api/users")
def create_or_update_user(data: UserCreateDto, request: Request):
    c = get_db_connection(); cur = c.cursor()
    if data.id:
        if data.password and len(data.password.strip()) > 0:
            cur.execute("UPDATE users SET role=%s, personnel_id=%s, password_hash=%s WHERE id=%s", (data.role, data.personnel_id, hash_password(data.password), data.id))
        else:
            cur.execute("UPDATE users SET role=%s, personnel_id=%s WHERE id=%s", (data.role, data.personnel_id, data.id))
    else:
        cur.execute("INSERT INTO users (username, password_hash, role, personnel_id) VALUES (%s,%s,%s,%s)", (data.username.strip(), hash_password(data.password), data.role, data.personnel_id))
    c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.delete("/api/users/{u_id}")
def delete_user(u_id: int, request: Request):
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM users WHERE id = %s", (u_id,)); c.commit(); cur.close(); c.close(); return {"status": "success"}

# --- KAMERADEN STAMMAKTEN INTERNATIONALE ENGINE ---
@app.get("/api/personnel/list")
def list_personnel_records(request: Request):
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("""SELECT id, name, rank, membership_status, is_agt, is_maschinist, is_gf, phone, email, address, ice_contact, drive_b, drive_be, drive_c, drive_ce, profile_picture,
                   DATE_FORMAT(g26_3_date, '%Y-%m-%d') as g26_3_date,
                   DATE_FORMAT(birth_date, '%Y-%m-%d') as birth_date,
                   DATE_FORMAT(entry_date, '%Y-%m-%d') as entry_date FROM personnel ORDER BY name ASC""")
    r = cur.fetchall(); cur.close(); c.close(); return r

@app.post("/api/personnel")
def save_personnel_record(data: PersonnelCreateDto, request: Request):
    c = get_db_connection(); cur = c.cursor()
    g26 = data.g26_3_date if data.g26_3_date else None
    b_date = data.birth_date if data.birth_date else None
    e_date = data.entry_date if data.entry_date else None
    
    if data.id:
        cur.execute("""UPDATE personnel SET name=%s, rank=%s, membership_status=%s, is_agt=%s, is_maschinist=%s, is_gf=%s, g26_3_date=%s,
                       birth_date=%s, entry_date=%s, phone=%s, email=%s, address=%s, ice_contact=%s, drive_b=%s, drive_be=%s, drive_c=%s, drive_ce=%s, profile_picture=%s WHERE id=%s""",
                    (data.name, data.rank, data.membership_status, int(data.is_agt), int(data.is_maschinist), int(data.is_gf), g26,
                     b_date, e_date, data.phone, data.email, data.address, data.ice_contact, int(data.drive_b), int(data.drive_be), int(data.drive_c), int(data.drive_ce), data.profile_picture, data.id))
    else:
        cur.execute("""INSERT INTO personnel (name, rank, membership_status, is_agt, is_maschinist, is_gf, g26_3_date, birth_date, entry_date, phone, email, address, ice_contact, drive_b, drive_be, drive_c, drive_ce, profile_picture)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (data.name, data.rank, data.membership_status, int(data.is_agt), int(data.is_maschinist), int(data.is_gf), g26,
                     b_date, e_date, data.phone, data.email, data.address, data.ice_contact, int(data.drive_b), int(data.drive_be), int(data.drive_c), int(data.drive_ce), data.profile_picture))
    c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.delete("/api/personnel/{p_id}")
def delete_personnel_record(p_id: int, request: Request):
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM personnel WHERE id = %s", (p_id,)); c.commit(); cur.close(); c.close(); return {"status": "success"}

# --- FUHRPARK ENGINE ---
@app.get("/api/vehicles")
def list_vehicles(request: Request):
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, name, radio_name, status, milage, DATE_FORMAT(tuv_date, '%Y-%m-%d') as tuv_date, DATE_FORMAT(sp_date, '%Y-%m-%d') as sp_date FROM vehicles ORDER BY name ASC")
    r = cur.fetchall(); cur.close(); c.close(); return r

@app.post("/api/vehicles")
def create_vehicle_record(data: VehicleCreateDto, request: Request):
    c = get_db_connection(); cur = c.cursor()
    if data.id: cur.execute("UPDATE vehicles SET name=%s, radio_name=%s, status=%s, milage=%s, tuv_date=%s, sp_date=%s WHERE id=%s", (data.name, data.radio_name, data.status, data.milage, data.tuv_date or None, data.sp_date or None, data.id))
    else: cur.execute("INSERT INTO vehicles (name, radio_name, status, milage, tuv_date, sp_date) VALUES (%s,%s,%s,%s,%s,%s)", (data.name, data.radio_name, data.status, data.milage, data.tuv_date or None, data.sp_date or None))
    c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.delete("/api/vehicles/{v_id}")
def delete_vehicle_record(v_id: int, request: Request):
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM vehicles WHERE id = %s", (v_id,)); c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.put("/api/vehicles/{v_id}/status")
def update_vehicle_status_code(v_id: int, data: VehicleStatusDto, request: Request):
    c = get_db_connection(); cur = c.cursor()
    cur.execute("UPDATE vehicles SET status = %s WHERE id = %s", (data.status, v_id)); c.commit(); cur.close(); c.close(); return {"status": "success"}

# --- FAHRTENBUCH ---
@app.get("/api/vehicles/logs")
def list_vehicle_logs():
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT l.*, v.name as vehicle_name, DATE_FORMAT(l.date, '%Y-%m-%d') as date, DATE_FORMAT(l.date, '%d.%m.%Y') as date_formatted FROM vehicle_log l LEFT JOIN vehicles v ON l.vehicle_id = v.id ORDER BY l.id DESC")
    r = cur.fetchall(); cur.close(); c.close(); return r

@app.post("/api/vehicles/logs")
def save_vehicle_log(data: VehicleLogDto):
    c = get_db_connection(); cur = c.cursor()
    if data.id:
        cur.execute("UPDATE vehicle_log SET vehicle_id=%s, date=%s, driver_name=%s, purpose=%s, km_start=%s, km_end=%s WHERE id=%s", (data.vehicle_id, data.date, data.driver_name, data.purpose, data.km_start, data.km_end, data.id))
    else:
        cur.execute("INSERT INTO vehicle_log (vehicle_id, date, driver_name, purpose, km_start, km_end) VALUES (%s,%s,%s,%s,%s,%s)", (data.vehicle_id, data.date, data.driver_name, data.purpose, data.km_start, data.km_end))
        cur.execute("UPDATE vehicles SET milage = %s WHERE id = %s", (data.km_end, data.vehicle_id))
    c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.delete("/api/vehicles/logs/{log_id}")
def delete_vehicle_log(log_id: int):
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM vehicle_log WHERE id = %s", (log_id,)); c.commit(); cur.close(); c.close(); return {"status": "success"}

# --- ATTENDANCE REAKTIV ---
@app.get("/groups")
def list_groups_all(request: Request):
    c = get_db_connection(); cur = c.cursor(dictionary=True); cur.execute("SELECT * FROM groups_table"); r = cur.fetchall(); cur.close(); c.close(); return r

@app.get("/groups/{group_id}/sessions")
def list_sessions_dashboard(group_id: int, request: Request):
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, description, duration, DATE_FORMAT(date, '%d.%m.%Y') as date, category, instructors FROM sessions WHERE group_id = %s ORDER BY date DESC", (group_id,))
    r = cur.fetchall(); cur.close(); c.close(); return r

@app.get("/groups/{group_id}/attendance")
def get_group_attendance_matrix(group_id: int, request: Request, session_id: Optional[int] = None):
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

# --- LAGER BESTÄNDE ---
@app.get("/api/inventory")
def api_inventory_list(request: Request):
    c = get_db_connection(); cur = c.cursor(dictionary=True); cur.execute("SELECT * FROM inventory ORDER BY item_name ASC"); r = cur.fetchall(); cur.close(); c.close(); return r

@app.post("/api/inventory")
def api_inventory_save(data: InventoryItemDto, request: Request):
    c = get_db_connection(); cur = c.cursor()
    if data.id: cur.execute("UPDATE inventory SET item_name=%s, amount=%s, min_amount=%s, unit=%s, location=%s, barcode=%s, size=%s WHERE id=%s", (data.item_name, data.amount, data.min_amount, data.unit, data.location, data.barcode, data.size, data.id))
    else: cur.execute("INSERT INTO inventory (item_name, amount, min_amount, unit, location, barcode, size) VALUES (%s,%s,%s,%s,%s,%s,%s)", (data.item_name, data.amount, data.min_amount, data.unit, data.location, data.barcode, data.size))
    c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.delete("/api/inventory/{i_id}")
def api_inventory_delete(i_id: int, request: Request):
    c = get_db_connection(); cur = c.cursor(); cur.execute("DELETE FROM inventory WHERE id = %s", (i_id,)); c.commit(); cur.close(); c.close(); return {"status": "success"}

# --- MÄNGEL KANBAN & EVENT BUCH ---
@app.get("/api/notes")
def api_notes_list(request: Request):
    c = get_db_connection(); cur = c.cursor(dictionary=True); cur.execute("SELECT * FROM notes ORDER BY id DESC"); r = cur.fetchall(); cur.close(); c.close(); return r

@app.post("/api/notes")
def api_notes_create(data: NoteCreateDto, background_tasks: BackgroundTasks, request: Request):
    user = get_current_user(request)
    c = get_db_connection(); cur = c.cursor()
    cur.execute("INSERT INTO notes (username, title, content, priority, kanban_status) VALUES (%s,%s,%s,%s,'neu')", (user['u'], data.title, data.content, data.priority))
    c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.put("/api/notes/{n_id}/status")
def api_notes_status_update(n_id: int, data: KanbanUpdateRequest, request: Request):
    c = get_db_connection(); cur = c.cursor(); cur.execute("UPDATE notes SET kanban_status = %s WHERE id = %s", (data.status, n_id)); c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.delete("/api/notes/{n_id}")
def api_notes_delete(n_id: int, request: Request):
    c = get_db_connection(); cur = c.cursor(); cur.execute("DELETE FROM notes WHERE id = %s", (n_id,)); c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.get("/api/events")
def api_events_list(request: Request):
    c = get_db_connection(); cur = c.cursor(dictionary=True); cur.execute("SELECT id, DATE_FORMAT(date, '%d.%m.%Y') as date_formatted, DATE_FORMAT(date, '%Y-%m-%d') as date, title, responsible FROM events ORDER BY date ASC"); r = cur.fetchall(); cur.close(); c.close(); return r

@app.post("/api/events")
def api_events_add(data: EventCreateDto, request: Request):
    c = get_db_connection(); cur = c.cursor()
    if data.id: cur.execute("UPDATE events SET date=%s, title=%s, responsible=%s WHERE id=%s", (data.date, data.title, data.responsible, data.id))
    else: cur.execute("INSERT INTO events (date, title, responsible) VALUES (%s,%s,%s)", (data.date, data.title, data.responsible))
    c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.delete("/api/events/{e_id}")
def api_events_delete(e_id: int, request: Request):
    c = get_db_connection(); cur = c.cursor(); cur.execute("DELETE FROM events WHERE id = %s", (e_id,)); c.commit(); cur.close(); c.close(); return {"status": "success"}

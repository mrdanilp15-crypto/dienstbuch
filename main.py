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
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
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

# --- PYDANTIC SYSTEM MODELLE (Zwingend ganz oben definiert) ---
class LoginRequest(BaseModel):
    username: str
    password: str

class UserCreateDto(BaseModel):
    id: Optional[int] = None
    username: str
    password: str
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
    phone: Optional[str] = ""
    email: Optional[str] = ""
    address: Optional[str] = ""
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

# --- DATABASE SOVEREIGN AUTOMATION & SCHEMA SANIERUNG ---
def init_db():
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
    
    # Altreste blockierender Tabellen entfernen, um Datenintegrität herzustellen
    try: cur.execute("DROP TABLE IF EXISTS attendance;")
    except: pass
    try: cur.execute("DROP TABLE IF EXISTS persons;")
    except: pass

    cur.execute("CREATE TABLE IF NOT EXISTS settings (setting_key VARCHAR(100) PRIMARY KEY, setting_value VARCHAR(255)) ENGINE=InnoDB;")
    
    default_settings = [
        ('apager_api_key', '0'), ('int_g26', '36'), 
        ('station_name', 'Dienststelle Hauptwache'),
        ('station_lat', '47.9942'), ('station_lon', '10.1344')
    ]
    for k, v in default_settings:
        cur.execute("INSERT IGNORE INTO settings (setting_key, setting_value) VALUES (%s, %s)", (k, v))

    cur.execute("CREATE TABLE IF NOT EXISTS users (id INT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(255) UNIQUE, password_hash VARCHAR(255), role VARCHAR(50), personnel_id INT NULL, failed_logins INT DEFAULT 0, lockout_until DATETIME NULL) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS personnel (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) UNIQUE, rank VARCHAR(100), membership_status VARCHAR(50), is_agt BOOLEAN DEFAULT 0, is_maschinist BOOLEAN DEFAULT 0, is_gf BOOLEAN DEFAULT 0, g26_3_date DATE NULL, phone VARCHAR(100) DEFAULT '', email VARCHAR(255) DEFAULT '', address TEXT NULL, profile_picture LONGTEXT NULL) ENGINE=InnoDB;")
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
    
    cur.execute("SELECT COUNT(*) FROM users WHERE username = 'admin'")
    if cur.fetchone()[0] == 0:
        cur.execute("INSERT INTO users (username, password_hash, role, personnel_id) VALUES (%s, %s, %s, 1)", ("admin", hash_password("admin123"), "admin"))
    
    cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
    conn.commit(); cur.close(); conn.close()

init_db()

# --- AUTOMATISIERTE BERIKHTS- & KPI STATISTIK ENGINE ---
@app.get("/api/reports/summary")
def get_kpi_summary(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    
    cur.execute("SELECT COALESCE(SUM(duration), 0) as total_hours, COUNT(*) as total_sessions FROM sessions")
    sess_stats = cur.fetchone()
    
    cur.execute("SELECT COUNT(*) as total_count FROM personnel")
    pers_stats = cur.fetchone()
    
    cur.execute("SELECT COUNT(*) as total_count FROM vehicles")
    veh_stats = cur.fetchone()
    
    cur.close(); conn.close()
    return {
        "total_hours": float(sess_stats["total_hours"]),
        "total_sessions": sess_stats["total_sessions"],
        "crew_strength": pers_stats["total_count"],
        "vehicle_count": veh_stats["total_count"]
    }

# --- GLOBAL OPEN-METEO WEATHER ENGINE ---
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
                "wind": f"{cw.get('windspeed', '--')} km/h", "warning_text": "Live Satelliten-Wetter aktiv synchronisiert."
            }
    except:
        return {"station": name, "temperature": "N/A", "wind": "N/A", "warning_text": "Wetter-Gateway offline."}

# --- APPLIKATIONS ROUTEN ---
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

# --- AUTH SEKTION MIT ACCOUNT LOCKOUT BRUTE-FORCE PROTECTION ---
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
    cur.execute("SELECT u.username, u.role, p.name as personnel_name, p.rank, p.membership_status, p.phone, p.email, p.address, p.profile_picture, p.is_agt, p.is_maschinist, p.is_gf FROM users u LEFT JOIN personnel p ON u.personnel_id = p.id WHERE u.username = %s", (user['u'],))
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

# --- SYSTEM ACCOUNTS CRUD (LOGINS) ---
@app.get("/api/users")
def list_users(request: Request):
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, username, role, personnel_id FROM users ORDER BY username ASC")
    r = cur.fetchall(); cur.close(); c.close(); return r

@app.post("/api/users")
def create_user(data: UserCreateDto, request: Request):
    c = get_db_connection(); cur = c.cursor()
    if data.id:
        cur.execute("UPDATE users SET role=%s, personnel_id=%s WHERE id=%s", (data.role, data.personnel_id, data.id))
    else:
        cur.execute("INSERT INTO users (username, password_hash, role, personnel_id) VALUES (%s,%s,%s,%s)", (data.username.strip(), hash_password(data.password), data.role, data.personnel_id))
    c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.delete("/api/users/{u_id}")
def delete_user(u_id: int, request: Request):
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM users WHERE id = %s", (u_id,)); c.commit(); cur.close(); c.close(); return {"status": "success"}

# --- KAMERADEN & PERSONALAKTEN CRUD ---
@app.get("/api/personnel/list")
def list_personnel_records(request: Request):
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, name, rank, membership_status, is_agt, is_maschinist, is_gf, phone, email, address, profile_picture, DATE_FORMAT(g26_3_date, '%Y-%m-%d') as g26_3_date FROM personnel ORDER BY name ASC")
    r = cur.fetchall(); cur.close(); c.close(); return r

@app.post("/api/personnel")
def save_personnel_record(data: PersonnelCreateDto, request: Request):
    c = get_db_connection(); cur = c.cursor()
    g26 = data.g26_3_date if data.g26_3_date else None
    if data.id:
        cur.execute("UPDATE personnel SET name=%s, rank=%s, membership_status=%s, is_agt=%s, is_maschinist=%s, is_gf=%s, g26_3_date=%s, phone=%s, email=%s, address=%s, profile_picture=%s WHERE id=%s", (data.name, data.rank, data.membership_status, int(data.is_agt), int(data.is_maschinist), int(data.is_gf), g26, data.phone, data.email, data.address, data.profile

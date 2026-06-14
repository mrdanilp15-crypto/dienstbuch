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
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timedelta

# --- SYSTEM-KONFIGURATION ---
DB_PASSWORD = os.getenv("DB_PASSWORD", "feuerwehr")
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
    location: str = "Lager"
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

# --- SECURTY UTILS & CRYPTO ---
def get_db_connection():
    return mysql.connector.connect(host="db", user="app_user", password=DB_PASSWORD, database="attendance_system")

def hash_password(p: str) -> str:
    s = secrets.token_hex(16)
    return f"{s}:{hashlib.pbkdf2_hmac('sha256', p.encode(), s.encode(), 100000).hex()}"

def verify_password(stored, prov) -> bool:
    try:
        s, h = stored.split(":")
        return hashlib.pbkdf2_hmac('sha256', prov.encode(), s.encode(), 100000).hex() == h
    except: return False

def create_token(u: str, r: str) -> str:
    p = base64.b64encode(json.dumps({"u": u, "r": r, "t": time.time()}).encode()).decode()
    return f"{p}.{hmac.new(SECRET_KEY.encode(), p.encode(), hashlib.sha256).hexdigest()}"

def get_current_user(req: Request):
    t = req.cookies.get("session_token")
    if not t: return None
    try:
        p, sig = t.split(".")
        if hmac.compare_digest(sig, hmac.new(SECRET_KEY.encode(), p.encode(), hashlib.sha256).hexdigest()):
            return json.loads(base64.b64decode(p).decode())
    except: return None

# --- DATABASE SCHEMASANIERUNG ---
def init_db():
    try:
        c = get_db_connection(); cur = c.cursor()
        cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
        try: cur.execute("ALTER TABLE settings MODIFY COLUMN setting_value VARCHAR(255);")
        except: pass
        try: cur.execute("DROP TABLE IF EXISTS attendance;")
        except: pass
        cur.execute("CREATE TABLE IF NOT EXISTS settings (setting_key VARCHAR(100) PRIMARY KEY, setting_value VARCHAR(255)) ENGINE=InnoDB;")
        for k, v in [('apager_api_key', '0'), ('int_g26', '36'), ('station_name', 'Freiwillige Feuerwehr Buxheim'), ('station_lat', '47.9942'), ('station_lon', '10.1344')]:
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
        for col, ct in [("birth_date", "DATE NULL"), ("entry_date", "DATE NULL"), ("phone", "VARCHAR(100) DEFAULT ''"), ("email", "VARCHAR(255) DEFAULT ''"), ("address", "TEXT NULL"), ("ice_contact", "VARCHAR(255) DEFAULT ''"), ("drive_b", "BOOLEAN DEFAULT 0"), ("drive_be", "BOOLEAN DEFAULT 0"), ("drive_c", "BOOLEAN DEFAULT 0"), ("drive_ce", "BOOLEAN DEFAULT 0"), ("profile_picture", "LONGTEXT NULL")]:
            try: cur.execute(f"ALTER TABLE personnel ADD COLUMN {col} {ct};")
            except: pass
        cur.execute("SELECT COUNT(*) FROM users WHERE username = 'admin'")
        if cur.fetchone()[0] == 0: cur.execute("INSERT INTO users (username, password_hash, role, personnel_id) VALUES (%s, %s, %s, 1)", ("admin", hash_password("admin123"), "admin"))
        cur.execute("SET FOREIGN_KEY_CHECKS = 1;"); c.commit(); cur.close(); c.close()
    except: pass

init_db()

# --- WEB SEITEN INTERFACES ---
@app.get("/")
def route_root(r: Request): return FileResponse("static/dashboard.html") if get_current_user(r) else FileResponse("static/login.html")
@app.get("/dashboard")
def route_dash(r: Request): return FileResponse("static/dashboard.html") if get_current_user(r) else FileResponse("static/login.html")
@app.get("/login")
def route_log(): return FileResponse("static/login.html")
@app.get("/editor")
def route_edit(r: Request): return FileResponse("static/editor.html") if get_current_user(r) else FileResponse("static/login.html")

# --- AUTH API MIT BRUTE FORCE PROTECTION ---
@app.post("/api/login")
def api_login(d: LoginRequest, res: Response):
    c = get_db_connection(); cur = c.cursor(dictionary=True); cur.execute("SELECT * FROM users WHERE username = %s", (d.username.strip(),))
    u = cur.fetchone()
    if not u: cur.close(); c.close(); raise HTTPException(401, "Ungültig")
    if u.get('lockout_until') and u['lockout_until'] and datetime.now() < u['lockout_until']:
        cur.close(); c.close(); raise HTTPException(423, "Konto gesperrt.")
    if verify_password(u['password_hash'], d.password):
        cur.execute("UPDATE users SET failed_logins = 0, lockout_until = NULL WHERE id = %s", (u['id'],))
        c.commit(); cur.close(); c.close(); token = create_token(u['username'], u['role'])
        res.set_cookie(key="session_token", value=token, httponly=True, samesite="lax"); return {"status": "success", "redirect": "/dashboard"}
    else:
        nf = u['failed_logins'] + 1; lo = datetime.now() + timedelta(minutes=15) if nf >= 5 else None
        if lo: cur.execute("UPDATE users SET failed_logins = %s, lockout_until = %s WHERE id = %s", (nf, lo, u['id']))
        else: cur.execute("UPDATE users SET failed_logins = %s WHERE id = %s", (nf, u['id']))
        c.commit(); cur.close(); c.close(); raise HTTPException(401, "Ungültig")

@app.post("/api/logout")
def api_logout(res: Response): res.delete_cookie("session_token"); return {"status": "success"}

@app.get("/api/auth/me")
def api_me(r: Request):
    u = get_current_user(r); if not u: raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT u.username, u.role, p.name as personnel_name, p.rank, p.membership_status, p.phone, p.email, p.address, p.profile_picture, p.is_agt, p.is_maschinist, p.is_gf, DATE_FORMAT(p.g26_3_date, '%d.%m.%Y') as g26 FROM users u LEFT JOIN personnel p ON u.personnel_id = p.id WHERE u.username = %s", (u['u'],))
    res = cur.fetchone(); cur.close(); c.close(); return res

@app.get("/api/geocode")
def geocode(q: str, r: Request):
    if not get_current_user(r): raise HTTPException(401)
    try:
        url = f"https://nominatim.openstreetmap.org/search?format=json&limit=1&q={urllib.parse.quote(q)}"
        req = urllib.request.Request(url, headers={'User-Agent': 'DigitalesDienstbuch/1.0'})
        with urllib.request.urlopen(req, timeout=5) as res:
            data = json.loads(res.read().decode())
            if data: return {"status": "success", "name": data[0].get("display_name"), "lat": data[0].get("lat"), "lon": data[0].get("lon")}
    except: pass
    return {"status": "error", "message": "Fehler."}

@app.get("/api/weather")
def get_weather(r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor(dictionary=True); cur.execute("SELECT setting_key, setting_value FROM settings")
    s = {row['setting_key']: row['setting_value'] for row in cur.fetchall()}; cur.close(); c.close()
    lat, lon, name = s.get("station_lat", "47.9942"), s.get("station_lon", "10.1344"), s.get("station_name", "Hauptdienststelle")
    try:
        with urllib.request.urlopen(f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true", timeout=3) as res:
            cw = json.loads(res.read().decode()).get("current_weather", {})
            return {"station": name, "temperature": f"{cw.get('temperature', '--')} °C", "wind": f"{cw.get('windspeed', '--')} km/h", "warning_text": "Live-Wetter synchronisiert."}
    except: return {"station": name, "temperature": "N/A", "wind": "N/A", "warning_text": "API Offline."}

    # --- SYSTEM SETTINGS & CONFIGURATION ---
@app.get("/api/settings")
def get_settings(r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor(dictionary=True); cur.execute("SELECT setting_key, setting_value FROM settings")
    res = {row['setting_key']: row['setting_value'] for row in cur.fetchall()}; cur.close(); c.close(); return res

@app.post("/api/settings")
def save_settings(d: dict, r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor()
    for k, v in d.items(): cur.execute("INSERT INTO settings (setting_key, setting_value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE setting_value=%s", (k, str(v), str(v)))
    c.commit(); cur.close(); c.close(); return {"status": "success"}

# --- SYSTEM ACCOUNTS CRUD (USER LOGINS) ---
@app.get("/api/users")
def list_users(r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor(dictionary=True); cur.execute("SELECT id, username, role, personnel_id FROM users ORDER BY username ASC")
    res = cur.fetchall(); cur.close(); c.close(); return res

@app.post("/api/users")
def save_user(d: UserCreateDto, r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor()
    if d.id:
        if d.password and len(d.password.strip()) > 0: cur.execute("UPDATE users SET role=%s, personnel_id=%s, password_hash=%s WHERE id=%s", (d.role, d.personnel_id, hash_password(d.password), d.id))
        else: cur.execute("UPDATE users SET role=%s, personnel_id=%s WHERE id=%s", (d.role, d.personnel_id, d.id))
    else: cur.execute("INSERT INTO users (username, password_hash, role, personnel_id) VALUES (%s,%s,%s,%s)", (d.username.strip(), hash_password(d.password), d.role, d.personnel_id))
    c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.delete("/api/users/{u_id}")
def del_user(u_id: int, r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor(); cur.execute("DELETE FROM users WHERE id = %s", (u_id,)); c.commit(); cur.close(); c.close(); return {"status": "success"}

# --- PERSONAL REGISTER CRUD (AKTE 3.0) ---
@app.get("/api/personnel/list")
def list_pers(r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("""SELECT id, name, rank, membership_status, is_agt, is_maschinist, is_gf, phone, email, address, ice_contact, drive_b, drive_be, drive_c, drive_ce, profile_picture,
                   DATE_FORMAT(g26_3_date, '%Y-%m-%d') as g26_3_date, DATE_FORMAT(birth_date, '%Y-%m-%d') as birth_date, DATE_FORMAT(entry_date, '%Y-%m-%d') as entry_date FROM personnel ORDER BY name ASC""")
    res = cur.fetchall(); cur.close(); c.close(); return res

@app.post("/api/personnel")
def save_pers(d: PersonnelCreateDto, r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor()
    g26, bd, ed = d.g26_3_date or None, d.birth_date or None, d.entry_date or None
    if d.id: cur.execute("""UPDATE personnel SET name=%s, rank=%s, membership_status=%s, is_agt=%s, is_maschinist=%s, is_gf=%s, g26_3_date=%s, birth_date=%s, entry_date=%s, phone=%s, email=%s, address=%s, ice_contact=%s, drive_b=%s, drive_be=%s, drive_c=%s, drive_ce=%s, profile_picture=%s WHERE id=%s""", (d.name, d.rank, d.membership_status, int(d.is_agt), int(d.is_maschinist), int(d.is_gf), g26, bd, ed, d.phone, d.email, d.address, d.ice_contact, int(d.drive_b), int(d.drive_be), int(d.drive_c), int(d.drive_ce), d.profile_picture, d.id))
    else: cur.execute("""INSERT INTO personnel (name, rank, membership_status, is_agt, is_maschinist, is_gf, g26_3_date, birth_date, entry_date, phone, email, address, ice_contact, drive_b, drive_be, drive_c, drive_ce, profile_picture) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (d.name, d.rank, d.membership_status, int(d.is_agt), int(d.is_maschinist), int(d.is_gf), g26, bd, ed, d.phone, d.email, d.address, d.ice_contact, int(d.drive_b), int(d.drive_be), int(d.drive_c), int(d.drive_ce), d.profile_picture))
    c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.delete("/api/personnel/{p_id}")
def del_pers(p_id: int, r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor(); cur.execute("DELETE FROM personnel WHERE id = %s", (p_id,)); c.commit(); cur.close(); c.close(); return {"status": "success"}

# --- FUHRPARK ASSETS CRUD ---
@app.get("/api/vehicles")
def list_vehicles(r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor(dictionary=True); cur.execute("SELECT id, name, radio_name, status, milage, DATE_FORMAT(tuv_date, '%Y-%m-%d') as tuv_date, DATE_FORMAT(sp_date, '%Y-%m-%d') as sp_date FROM vehicles ORDER BY name ASC")
    res = cur.fetchall(); cur.close(); c.close(); return res

@app.post("/api/vehicles")
def save_vehicle(d: VehicleCreateDto, r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor()
    if d.id: cur.execute("UPDATE vehicles SET name=%s, radio_name=%s, status=%s, milage=%s, tuv_date=%s, sp_date=%s WHERE id=%s", (d.name, d.radio_name, d.status, d.milage, d.tuv_date or None, d.sp_date or None, d.id))
    else: cur.execute("INSERT INTO vehicles (name, radio_name, status, milage, tuv_date, sp_date) VALUES (%s,%s,%s,%s,%s,%s)", (d.name, d.radio_name, d.status, d.milage, d.tuv_date or None, d.sp_date or None))
    c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.delete("/api/vehicles/{v_id}")
def del_vehicle(v_id: int, r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor(); cur.execute("DELETE FROM vehicles WHERE id = %s", (v_id,)); c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.put("/api/vehicles/{v_id}/status")
def vehicle_status(v_id: int, d: VehicleStatusDto, r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor(); cur.execute("UPDATE vehicles SET status = %s WHERE id = %s", (d.status, v_id)); c.commit(); cur.close(); c.close(); return {"status": "success"}

# --- DIGITALES FAHRTENBUCH CRUD ---
@app.get("/api/vehicles/logs")
def list_logs():
    c = get_db_connection(); cur = c.cursor(dictionary=True); cur.execute("SELECT l.*, v.name as vehicle_name, DATE_FORMAT(l.date, '%Y-%m-%d') as date, DATE_FORMAT(l.date, '%d.%m.%Y') as date_formatted FROM vehicle_log l LEFT JOIN vehicles v ON l.vehicle_id = v.id ORDER BY l.id DESC")
    res = cur.fetchall(); cur.close(); c.close(); return res

@app.post("/api/vehicles/logs")
def save_log(d: VehicleLogDto):
    c = get_db_connection(); cur = c.cursor()
    if d.id: cur.execute("UPDATE vehicle_log SET vehicle_id=%s, date=%s, driver_name=%s, purpose=%s, km_start=%s, km_end=%s WHERE id=%s", (d.vehicle_id, d.date, d.driver_name, d.purpose, d.km_start, d.km_end, d.id))
    else:
        cur.execute("INSERT INTO vehicle_log (vehicle_id, date, driver_name, purpose, km_start, km_end) VALUES (%s,%s,%s,%s,%s,%s)", (d.vehicle_id, d.date, d.driver_name, d.purpose, d.km_start, d.km_end))
        cur.execute("UPDATE vehicles SET milage = %s WHERE id = %s", (d.km_end, d.vehicle_id))
    c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.delete("/api/vehicles/logs/{log_id}")
def del_log(log_id: int):
    c = get_db_connection(); cur = c.cursor(); cur.execute("DELETE FROM vehicle_log WHERE id = %s", (log_id,)); c.commit(); cur.close(); c.close(); return {"status": "success"}

# --- DIGITALES DIENSTBUCH & ATTENDANCE ENGINE ---
@app.get("/groups")
def list_groups(r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor(dictionary=True); cur.execute("SELECT * FROM groups_table"); res = cur.fetchall(); cur.close(); c.close(); return res

@app.get("/groups/{group_id}/sessions")
def list_sessions(group_id: int, r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor(dictionary=True); cur.execute("SELECT id, description, duration, DATE_FORMAT(date, '%d.%m.%Y') as date, category, instructors FROM sessions WHERE group_id = %s ORDER BY date DESC", (group_id,)); res = cur.fetchall(); cur.close(); c.close(); return res

@app.get("/groups/{group_id}/attendance")
def get_attendance(group_id: int, r: Request, session_id: Optional[int] = None):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    sd = {"session_id": session_id, "description": "", "duration": 2.0, "category": "Übung", "date": datetime.now().strftime("%Y-%m-%d"), "instructors": ""}
    if session_id and session_id != 0:
        cur.execute("SELECT id as session_id, description, duration, DATE_FORMAT(date, '%Y-%m-%d') as date, category, instructors FROM sessions WHERE id = %s", (session_id,))
        row = cur.fetchone()
        if row: sd = row
    cur.execute("SELECT p.id as personnel_id, p.name, p.rank, CASE WHEN a.is_present IS NOT NULL THEN a.is_present ELSE 0 END as is_present, COALESCE(a.vehicle, '') as vehicle FROM personnel p LEFT JOIN attendance a ON p.id = a.person_id AND a.session_id = %s ORDER BY p.name ASC", (session_id,))
    persons = cur.fetchall()
    for p in persons: p['is_present'] = bool(p['is_present'])
    cur.execute("SELECT DISTINCT description FROM sessions ORDER BY id DESC LIMIT 5")
    pt = [row_t['description'] for row_t in cur.fetchall()]
    cur.execute("SELECT DISTINCT instructors FROM sessions ORDER BY id DESC LIMIT 5")
    pl = [row_l['instructors'] for row_l in cur.fetchall()]
    cur.close(); c.close(); return {**sd, "persons": persons, "presets": {"topics": pt, "leaders": pl}}

@app.post("/attendance")
def save_attendance(d: LegacySessionPayload, r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor()
    s_id = d.session_id
    if s_id and s_id != 0:
        cur.execute("UPDATE sessions SET date=%s, duration=%s, description=%s, instructors=%s, category=%s WHERE id=%s", (d.date, d.duration, d.description, d.instructors, d.category, s_id))
        cur.execute("DELETE FROM attendance WHERE session_id = %s", (s_id,))
    else:
        cur.execute("INSERT INTO sessions (group_id, date, category, duration, description, instructors) VALUES (%s,%s,%s,%s,%s,%s)", (d.group_id, d.date, d.category, d.duration, d.description, d.instructors))
        s_id = cur.lastrowid
    for e in d.entries: cur.execute("INSERT INTO attendance (session_id, person_id, is_present, vehicle) VALUES (%s,%s,%s,%s)", (s_id, e.person_id, 1 if e.is_present else 0, e.vehicle or ""))
    c.commit(); cur.close(); c.close(); return {"status": "success", "session_id": s_id}

# --- INVENTAR / SMART-KLEIDERKAMMER CRUD ---
@app.get("/api/inventory")
def list_inv(r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor(dictionary=True); cur.execute("SELECT * FROM inventory ORDER BY item_name ASC"); res = cur.fetchall(); cur.close(); c.close(); return res

@app.post("/api/inventory")
def save_inv(d: InventoryItemDto, r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor()
    if d.id: cur.execute("UPDATE inventory SET item_name=%s, amount=%s, min_amount=%s, unit=%s, location=%s, barcode=%s, size=%s WHERE id=%s", (d.item_name, d.amount, d.min_amount, d.unit, d.location, d.barcode, d.size, d.id))
    else: cur.execute("INSERT INTO inventory (item_name, amount, min_amount, unit, location, barcode, size) VALUES (%s,%s,%s,%s,%s,%s,%s)", (d.item_name, d.amount, d.min_amount, d.unit, d.location, d.barcode, d.size))
    c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.delete("/api/inventory/{i_id}")
def del_inv(i_id: int, r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor(); cur.execute("DELETE FROM inventory WHERE id = %s", (i_id,)); c.commit(); cur.close(); c.close(); return {"status": "success"}

# --- WHITEBOARD MÄNGEL KANBAN ---
@app.get("/api/notes")
def list_notes(r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor(dictionary=True); cur.execute("SELECT * FROM notes ORDER BY id DESC"); res = cur.fetchall(); cur.close(); c.close(); return res

@app.post("/api/notes")
def save_note(d: NoteCreateDto, r: Request):
    u = get_current_user(r); if not u: raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor()
    cur.execute("INSERT INTO notes (username, title, content, priority, kanban_status) VALUES (%s,%s,%s,%s,'neu')", (u['u'], d.title, d.content, d.priority))
    c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.put("/api/notes/{n_id}/status")
def status_note(n_id: int, d: KanbanUpdateRequest, r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor(); cur.execute("UPDATE notes SET kanban_status = %s WHERE id = %s", (d.status, n_id)); c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.delete("/api/notes/{n_id}")
def del_note(n_id: int, r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor(); cur.execute("DELETE FROM notes WHERE id = %s", (n_id,)); c.commit(); cur.close(); c.close(); return {"status": "success"}

# --- VEREINSKALENDER & DIENSTPLANUNG CRUD ---
@app.get("/api/events")
def list_events(r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor(dictionary=True); cur.execute("SELECT id, DATE_FORMAT(date, '%d.%m.%Y') as date_formatted, DATE_FORMAT(date, '%Y-%m-%d') as date, title, responsible FROM events ORDER BY date ASC"); res = cur.fetchall(); cur.close(); c.close(); return res

@app.post("/api/events")
def save_event(d: EventCreateDto, r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor()
    if d.id: cur.execute("UPDATE events SET date=%s, title=%s, responsible=%s WHERE id=%s", (d.date, d.title, d.responsible, d.id))
    else: cur.execute("INSERT INTO events (date, title, responsible) VALUES (%s,%s,%s)", (d.date, d.title, d.responsible))
    c.commit(); cur.close(); c.close(); return {"status": "success"}

@app.delete("/api/events/{e_id}")
def del_event(e_id: int, r: Request):
    if not get_current_user(r): raise HTTPException(401)
    c = get_db_connection(); cur = c.cursor(); cur.execute("DELETE FROM events WHERE id = %s", (e_id,)); c.commit(); cur.close(); c.close(); return {"status": "success"}

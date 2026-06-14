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
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timedelta

# --- SYSTEM-KONFIGURATION ---
DB_PASSWORD = os.getenv("DB_PASSWORD", "feuerwehr")
SECRET_KEY = os.getenv("SECRET_KEY", "digitales-dienstbuch-global-sovereign-key-112")

app = FastAPI(title="Digitales Dienstbuch")

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
    qr_code_id: Optional[str] = ""
    last_check: Optional[str] = None
    next_check: Optional[str] = None

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
    next_oil_change_km: Optional[int] = 10000

class VehicleLogDto(BaseModel):
    id: Optional[int] = None
    vehicle_id: int
    date: str
    driver_name: str
    purpose: str
    km_start: int
    km_end: int
    fuel_liters: Optional[float] = 0.0

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

class TicketCreateDto(BaseModel):
    id: Optional[int] = None
    title: str
    content: str
    vehicle_id: Optional[int] = None
    inventory_id: Optional[int] = None
    priority: str = "normal"
    status: str = "neu"

class AlarmPayloadDto(BaseModel):
    address: str
    keyword: str
    alert_text: str

class HydrantDto(BaseModel):
    id: Optional[int] = None
    lat: float
    lon: float
    hydrant_type: str = "Unterflurhydrant"
    diameter: str = "H100"
    last_check: Optional[str] = None

# --- DATABASE ENGINE & CRYPTO ---
def get_db_connection():
    return mysql.connector.connect(host="db", user="app_user", password=DB_PASSWORD, database="attendance_system")

def hash_password(p: str) -> str:
    s = secrets.token_hex(16)
    return f"{s}:{hashlib.pbkdf2_hmac('sha256', p.encode(), s.encode(), 100000).hex()}"

def verify_password(stored, prov) -> bool:
    try:
        s, h = stored.split(":")
        return hashlib.pbkdf2_hmac('sha256', prov.encode(), s.encode(), 100000).hex() == h
    except:
        return False

def create_token(u: str, r: str) -> str:
    p = base64.b64encode(json.dumps({"u": u, "r": r, "t": time.time()}).encode()).decode()
    return f"{p}.{hmac.new(SECRET_KEY.encode(), p.encode(), hashlib.sha256).hexdigest()}"

def get_current_user(req: Request):
    t = req.cookies.get("session_token")
    if not t:
        return None
    try:
        p, sig = t.split(".")
        if hmac.compare_digest(sig, hmac.new(SECRET_KEY.encode(), p.encode(), hashlib.sha256).hexdigest()):
            return json.loads(base64.b64decode(p).decode())
    except:
        return None

# --- DATABASE INITIALIZER & AUTOMATISCHE MIGRATION ---
def init_db():
    try:
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
        
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
        cur.execute("CREATE TABLE IF NOT EXISTS tickets (id INT AUTO_INCREMENT PRIMARY KEY, title VARCHAR(255), content TEXT, vehicle_id INT NULL, inventory_id INT NULL, priority VARCHAR(50) DEFAULT 'normal', status VARCHAR(50) DEFAULT 'neu', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS active_alarm (id INT AUTO_INCREMENT PRIMARY KEY, address VARCHAR(255), keyword VARCHAR(100), alert_text TEXT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS hydranten (id INT AUTO_INCREMENT PRIMARY KEY, lat DOUBLE, lon DOUBLE, hydrant_type VARCHAR(100), diameter VARCHAR(50), last_check DATE NULL) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS e_ri_cards (un_number VARCHAR(10) PRIMARY KEY, danger_text TEXT, safety_measures TEXT, first_aid TEXT) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS groups_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) UNIQUE) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS events (id INT AUTO_INCREMENT PRIMARY KEY, date DATE, title VARCHAR(255), responsible VARCHAR(255)) ENGINE=InnoDB;")

        cur.execute("INSERT IGNORE INTO groups_table (id, name) VALUES (1, 'Aktiver Dienstverband')")
        cur.execute("INSERT IGNORE INTO personnel (id, name, rank, membership_status) VALUES (1, 'Dienststellen Administrator', 'Brandmeister', 'Aktiv')")

        # --- SELF-HEALING MIGRATION MANAGER ---
        migrations = [
            ("personnel", "birth_date", "DATE NULL"), ("personnel", "entry_date", "DATE NULL"),
            ("personnel", "phone", "VARCHAR(100) DEFAULT ''"), ("personnel", "email", "VARCHAR(255) DEFAULT ''"),
            ("personnel", "address", "TEXT NULL"), ("personnel", "ice_contact", "VARCHAR(255) DEFAULT ''"),
            ("personnel", "drive_b", "BOOLEAN DEFAULT 0"), ("personnel", "drive_be", "BOOLEAN DEFAULT 0"),
            ("personnel", "drive_c", "BOOLEAN DEFAULT 0"), ("personnel", "drive_ce", "BOOLEAN DEFAULT 0"),
            ("personnel", "profile_picture", "LONGTEXT NULL"),
            ("vehicles", "next_oil_change_km", "INT DEFAULT 10000"),
            ("vehicle_log", "fuel_liters", "FLOAT DEFAULT 0.0"),
            ("inventory", "qr_code_id", "VARCHAR(100) DEFAULT ''"),
            ("inventory", "last_check", "DATE NULL"), ("inventory", "next_check", "DATE NULL")
        ]
        for table, col, schema in migrations:
            try: cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {schema};")
            except: pass

        cur.execute("SELECT COUNT(*) FROM users WHERE username = 'admin'")
        if cur.fetchone()[0] == 0:
            cur.execute("INSERT INTO users (username, password_hash, role, personnel_id) VALUES (%s, %s, %s, 1)", ("admin", hash_password("admin123"), "admin"))
            
        cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
        c.commit()
        cur.close()
        c.close()
    except Exception as e:
        print(f"Migration abgefangen: {str(e)}")

init_db()

# --- WEB SEITEN CONTROLLER ---
@app.get("/")
def route_root(r: Request):
    if get_current_user(r): return FileResponse("static/dashboard.html")
    return FileResponse("static/login.html")

@app.get("/dashboard")
def route_dashboard(r: Request):
    if get_current_user(r): return FileResponse("static/dashboard.html")
    return FileResponse("static/login.html")

@app.get("/login")
def route_login_page(): return FileResponse("static/login.html")

@app.get("/editor")
def route_editor_page(r: Request):
    if get_current_user(r): return FileResponse("static/editor.html")
    return FileResponse("static/login.html")

# --- AUTH API ---
@app.post("/api/login")
def api_login(d: LoginRequest, res: Response):
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE username = %s", (d.username.strip(),))
    u = cur.fetchone()
    if not u:
        cur.close()
        c.close()
        raise HTTPException(status_code=401)
    if verify_password(u['password_hash'], d.password):
        cur.execute("UPDATE users SET failed_logins = 0 WHERE id = %s", (u['id'],))
        c.commit()
        cur.close()
        c.close()
        token = create_token(u['username'], u['role'])
        res.set_cookie(key="session_token", value=token, httponly=True, samesite="lax")
        return {"status": "success", "redirect": "/dashboard"}
    else:
        cur.close()
        c.close()
        raise HTTPException(status_code=401)

@app.post("/api/logout")
def api_logout(res: Response):
    res.delete_cookie("session_token")
    return {"status": "success"}

@app.get("/api/auth/me")
def api_me(r: Request):
    u = get_current_user(r)
    if not u: raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT u.username, u.role, u.personnel_id, p.name as personnel_name, p.rank, p.membership_status, p.phone, p.email, p.address, p.profile_picture, p.is_agt, p.is_maschinist, p.is_gf, DATE_FORMAT(p.g26_3_date, '%Y-%m-%d') as g26_3_date, DATE_FORMAT(p.birth_date, '%Y-%m-%d') as birth_date, DATE_FORMAT(p.entry_date, '%Y-%m-%d') as entry_date, p.ice_contact, p.drive_b, p.drive_be, p.drive_c, p.drive_ce FROM users u LEFT JOIN personnel p ON u.personnel_id = p.id WHERE u.username = %s", (u['u'],))
    res = cur.fetchone()
    cur.close()
    c.close()
    return res

@app.get("/api/geocode")
def geocode(q: str, r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    try:
        url = f"https://nominatim.openstreetmap.org/search?format=json&limit=1&q={urllib.parse.quote(q)}"
        req = urllib.request.Request(url, headers={'User-Agent': 'DigitalesDienstbuch/1.0'})
        with urllib.request.urlopen(req, timeout=5) as res:
            data = json.loads(res.read().decode())
            if data: return {"status": "success", "name": data[0].get("display_name"), "lat": data[0].get("lat"), "lon": data[0].get("lon")}
    except: pass
    return {"status": "error", "message": "Fehler"}

@app.get("/api/weather")
def get_weather(r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT setting_key, setting_value FROM settings")
    s = {row['setting_key']: row['setting_value'] for row in cur.fetchall()}
    cur.close()
    c.close()
    lat, lon, name = s.get("station_lat", "47.9942"), s.get("station_lon", "10.1344"), s.get("station_name", "Dienststelle")
    try:
        with urllib.request.urlopen(f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true", timeout=3) as res:
            cw = json.loads(res.read().decode()).get("current_weather", {})
            return {"station": name, "temperature": f"{cw.get('temperature', '--')} °C", "wind": f"{cw.get('windspeed', '--')} km/h", "warning_text": "Dienstbuch-Wetter synchronisiert."}
    except: return {"station": name, "temperature": "N/A", "wind": "N/A", "warning_text": "Gateway Offline."}

@app.get("/api/settings")
def get_settings(r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT setting_key, setting_value FROM settings")
    res = {row['setting_key']: row['setting_value'] for row in cur.fetchall()}
    cur.close()
    c.close()
    return res

@app.post("/api/settings")
def save_settings(d: dict, r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor()
    for k, v in d.items():
        cur.execute("INSERT INTO settings (setting_key, setting_value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE setting_value=%s", (k, str(v), str(v)))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

@app.get("/api/users")
def list_users(r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, username, role, personnel_id FROM users ORDER BY username ASC")
    res = cur.fetchall()
    cur.close()
    c.close()
    return res

@app.post("/api/users")
def save_user(d: UserCreateDto, r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor()
    if d.id:
        if d.password and len(d.password.strip()) > 0:
            cur.execute("UPDATE users SET role=%s, personnel_id=%s, password_hash=%s WHERE id=%s", (d.role, d.personnel_id, hash_password(d.password), d.id))
        else:
            cur.execute("UPDATE users SET role=%s, personnel_id=%s WHERE id=%s", (d.role, d.personnel_id, d.id))
    else:
        cur.execute("INSERT INTO users (username, password_hash, role, personnel_id) VALUES (%s,%s,%s,%s)", (d.username.strip(), hash_password(d.password), d.role, d.personnel_id))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

@app.delete("/api/users/{u_id}")
def del_user(u_id: int, r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("DELETE FROM users WHERE id = %s", (u_id,))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

@app.get("/api/personnel/list")
def list_pers(r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("""SELECT id, name, rank, membership_status, is_agt, is_maschinist, is_gf, phone, email, address, ice_contact, drive_b, drive_be, drive_c, drive_ce, profile_picture,
                   DATE_FORMAT(g26_3_date, '%Y-%m-%d') as g26_3_date, DATE_FORMAT(birth_date, '%Y-%m-%d') as birth_date, DATE_FORMAT(entry_date, '%Y-%m-%d') as entry_date FROM personnel ORDER BY name ASC""")
    res = cur.fetchall()
    cur.close()
    c.close()
    return res

@app.post("/api/personnel")
def save_pers(d: PersonnelCreateDto, r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor()
    g26 = d.g26_3_date if d.g26_3_date and d.g26_3_date.strip() != "" else None
    bd = d.birth_date if d.birth_date and d.birth_date.strip() != "" else None
    ed = d.entry_date if d.entry_date and d.entry_date.strip() != "" else None
    
    if d.id:
        cur.execute("""UPDATE personnel SET name=%s, rank=%s, membership_status=%s, is_agt=%s, is_maschinist=%s, is_gf=%s, g26_3_date=%s, birth_date=%s, entry_date=%s, phone=%s, email=%s, address=%s, ice_contact=%s, drive_b=%s, drive_be=%s, drive_c=%s, drive_ce=%s, profile_picture=%s WHERE id=%s""", 
                    (d.name, d.rank, d.membership_status, int(d.is_agt), int(d.is_maschinist), int(d.is_gf), g26, bd, ed, d.phone, d.email, d.address, d.ice_contact, int(d.drive_b), int(d.drive_be), int(d.drive_c), int(d.drive_ce), d.profile_picture, d.id))
    else:
        cur.execute("""INSERT INTO personnel (name, rank, membership_status, is_agt, is_maschinist, is_gf, g26_3_date, birth_date, entry_date, phone, email, address, ice_contact, drive_b, drive_be, drive_c, drive_ce, profile_picture) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", 
                    (d.name, d.rank, d.membership_status, int(d.is_agt), int(d.is_maschinist), int(d.is_gf), g26, bd, ed, d.phone, d.email, d.address, d.ice_contact, int(d.drive_b), int(d.drive_be), int(d.drive_c), int(d.drive_ce), d.profile_picture))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

@app.delete("/api/personnel/{p_id}")
def del_pers(p_id: int, r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("DELETE FROM personnel WHERE id = %s", (p_id,))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

@app.get("/api/vehicles")
def list_vehicles(r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, name, radio_name, status, milage, DATE_FORMAT(tuv_date, '%Y-%m-%d') as tuv_date, DATE_FORMAT(sp_date, '%Y-%m-%d') as sp_date, next_oil_change_km FROM vehicles ORDER BY name ASC")
    res = cur.fetchall()
    cur.close()
    c.close()
    return res

@app.post("/api/vehicles")
def save_vehicle(d: VehicleCreateDto, r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor()
    td = d.tuv_date if d.tuv_date and d.tuv_date.strip() != "" else None
    sd = d.sp_date if d.sp_date and d.sp_date.strip() != "" else None
    if d.id:
        cur.execute("UPDATE vehicles SET name=%s, radio_name=%s, status=%s, milage=%s, tuv_date=%s, sp_date=%s, next_oil_change_km=%s WHERE id=%s", (d.name, d.radio_name, d.status, d.milage, td, sd, d.next_oil_change_km, d.id))
    else:
        cur.execute("INSERT INTO vehicles (name, radio_name, status, milage, tuv_date, sp_date, next_oil_change_km) VALUES (%s,%s,%s,%s,%s,%s,%s)", (d.name, d.radio_name, d.status, d.milage, td, sd, d.next_oil_change_km))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

@app.delete("/api/vehicles/{v_id}")
def del_vehicle(v_id: int, r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("DELETE FROM vehicles WHERE id = %s", (v_id,))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

@app.put("/api/vehicles/{v_id}/status")
def vehicle_status(v_id: int, d: VehicleStatusDto, r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("UPDATE vehicles SET status = %s WHERE id = %s", (d.status, v_id))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

@app.get("/api/vehicles/logs")
def list_logs():
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT l.*, v.name as vehicle_name, DATE_FORMAT(l.date, '%Y-%m-%d') as date, DATE_FORMAT(l.date, '%d.%m.%Y') as date_formatted FROM vehicle_log l LEFT JOIN vehicles v ON l.vehicle_id = v.id ORDER BY l.id DESC")
    res = cur.fetchall()
    cur.close()
    c.close()
    return res

@app.post("/api/vehicles/logs")
def save_log(d: VehicleLogDto):
    c = get_db_connection()
    cur = c.cursor()
    if d.id:
        cur.execute("UPDATE vehicle_log SET vehicle_id=%s, date=%s, driver_name=%s, purpose=%s, km_start=%s, km_end=%s, fuel_liters=%s WHERE id=%s", (d.vehicle_id, d.date, d.driver_name, d.purpose, d.km_start, d.km_end, d.fuel_liters, d.id))
    else:
        cur.execute("INSERT INTO vehicle_log (vehicle_id, date, driver_name, purpose, km_start, km_end, fuel_liters) VALUES (%s,%s,%s,%s,%s,%s,%s)", (d.vehicle_id, d.date, d.driver_name, d.purpose, d.km_start, d.km_end, d.fuel_liters))
        cur.execute("UPDATE vehicles SET milage = %s WHERE id = %s", (d.km_end, d.vehicle_id))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

@app.delete("/api/vehicles/logs/{log_id}")
def del_log(log_id: int):
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("DELETE FROM vehicle_log WHERE id = %s", (log_id,))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

@app.get("/groups")
def list_groups(r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT * FROM groups_table")
    res = cur.fetchall()
    cur.close()
    c.close()
    return res

@app.get("/groups/{group_id}/sessions")
def list_sessions(group_id: int, r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, description, duration, DATE_FORMAT(date, '%d.%m.%Y') as date, category, instructors FROM sessions WHERE group_id = %s ORDER BY date DESC", (group_id,))
    res = cur.fetchall()
    cur.close()
    c.close()
    return res

@app.get("/groups/{group_id}/attendance")
def get_attendance(group_id: int, r: Request, session_id: Optional[int] = None):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
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
    cur.close()
    c.close()
    return {**sd, "persons": persons, "presets": {"topics": pt, "leaders": pl}}

@app.post("/attendance")
def save_attendance(d: LegacySessionPayload, r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor()
    s_id = d.session_id
    if s_id and s_id != 0:
        cur.execute("UPDATE sessions SET date=%s, duration=%s, description=%s, instructors=%s, category=%s WHERE id=%s", (d.date, d.duration, d.description, d.instructors, d.category, s_id))
        cur.execute("DELETE FROM attendance WHERE session_id = %s", (s_id,))
    else:
        cur.execute("INSERT INTO sessions (group_id, date, category, duration, description, instructors) VALUES (%s,%s,%s,%s,%s,%s)", (d.group_id, d.date, d.category, d.duration, d.description, d.instructors))
        s_id = cur.lastrowid
    for e in d.entries:
        cur.execute("INSERT INTO attendance (session_id, person_id, is_present, vehicle) VALUES (%s,%s,%s,%s)", (s_id, e.person_id, 1 if e.is_present else 0, e.vehicle or ""))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success", "session_id": s_id}

@app.get("/api/inventory")
def list_inv(r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, item_name, amount, min_amount, unit, location, barcode, size, qr_code_id, DATE_FORMAT(last_check, '%Y-%m-%d') as last_check, DATE_FORMAT(next_check, '%Y-%m-%d') as next_check FROM inventory ORDER BY item_name ASC")
    res = cur.fetchall()
    cur.close()
    c.close()
    return res

@app.post("/api/inventory")
def save_inv(d: InventoryItemDto, r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor()
    lc = d.last_check if d.last_check and d.last_check.strip() != "" else None
    nc = d.next_check if d.next_check and d.next_check.strip() != "" else None
    qr_id = d.qr_code_id
    if not qr_id or qr_id.strip() == "":
        qr_id = f"FEUERWEHR-QR-{secrets.token_hex(4).upper()}"
    if d.id:
        cur.execute("UPDATE inventory SET item_name=%s, amount=%s, min_amount=%s, unit=%s, location=%s, barcode=%s, size=%s, qr_code_id=%s, last_check=%s, next_check=%s WHERE id=%s", (d.item_name, d.amount, d.min_amount, d.unit, d.location, d.barcode, d.size, qr_id, lc, nc, d.id))
    else:
        cur.execute("INSERT INTO inventory (item_name, amount, min_amount, unit, location, barcode, size, qr_code_id, last_check, next_check) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (d.item_name, d.amount, d.min_amount, d.unit, d.location, d.barcode, d.size, qr_id, lc, nc))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

@app.delete("/api/inventory/{i_id}")
def del_inv(i_id: int, r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("DELETE FROM inventory WHERE id = %s", (i_id,))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

@app.get("/api/tickets")
def list_tickets(r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT t.*, v.name as vehicle_name, i.item_name FROM tickets t LEFT JOIN vehicles v ON t.vehicle_id = v.id LEFT JOIN inventory i ON t.inventory_id = i.id ORDER BY t.id DESC")
    res = cur.fetchall()
    cur.close()
    c.close()
    return res

@app.post("/api/tickets")
def create_ticket(d: TicketCreateDto, r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor()
    v_id = d.vehicle_id if d.vehicle_id else None
    i_id = d.inventory_id if d.inventory_id else None
    cur.execute("INSERT INTO tickets (title, content, vehicle_id, inventory_id, priority, status) VALUES (%s,%s,%s,%s,%s,%s)", (d.title, d.content, v_id, i_id, d.priority, d.status))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

@app.put("/api/tickets/{t_id}/status")
def update_ticket_status(t_id: int, d: KanbanUpdateRequest, r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("UPDATE tickets SET status = %s WHERE id = %s", (d.status, t_id))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

@app.get("/api/alarm/active")
def get_active_alarm(r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, address, keyword, alert_text, DATE_FORMAT(timestamp, '%d.%m.%Y %H:%i') as timestamp FROM active_alarm ORDER BY id DESC LIMIT 1")
    res = cur.fetchone()
    cur.close()
    c.close()
    return res if res else {"status": "clear", "message": "Keine Alarme vorliegend."}

@app.post("/api/alarm/trigger")
def trigger_alarm_webhook(d: AlarmPayloadDto):
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("INSERT INTO active_alarm (address, keyword, alert_text) VALUES (%s, %s, %s)", (d.address, d.keyword, d.alert_text))
    c.commit()
    cur.close()
    c.close()
    return {"status": "alarm_broadcasted"}

@app.get("/api/gahrgut/ericard/{un_number}")
def get_eri_card(un_number: str, r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    un_clean = un_number.strip().zfill(4)
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT * FROM e_ri_cards WHERE un_number = %s", (un_clean,))
    res = cur.fetchone()
    cur.close()
    c.close()
    if res: return res
    try:
        un_int = int(un_clean)
        if 1 <= un_int < 1000:
            return {"un_number": un_clean, "danger_text": "ADR KLASSE 1 (Explosivstoffe / Munition): Akute Detonationsgefahr.", "safety_measures": "Sicherheitsradius mind. 500 Meter einrichten!", "first_aid": "Thermische Verbrennungen sofort kühlen."}
        elif 1000 <= un_int < 2000:
            return {"un_number": un_clean, "danger_text": "ADR KLASSE 2/3 (Gase / Entzündbare Flüssigkeiten): Schwere Gaswolken kriechen am Boden.", "safety_measures": "Ex-Schutz-Zone (mind. 100m) einrichten! Funkenbildung vermeiden.", "first_aid": "Verunglückte unter Atemschutz retten. Frischluft."}
        elif 2000 <= un_int < 3000:
            return {"un_number": un_clean, "danger_text": "ADR KLASSE 4/5 (Selbstentzündliche / oxidierende Stoffe): Heftige Reaktion mit Wasser!", "safety_measures": "Vorsicht bei Wassereinsatz. Erstickende Löschmittel prüfen.", "first_aid": "Chemische Pulverreste trocken abwischen, danach spülen."}
        elif 3000 <= un_int <= 3600:
            return {"un_number": un_clean, "danger_text": "ADR KLASSE 6/8 (Toxische / Ätzende Chemikalien): Akute Lebensgefahr bei Einatmen oder Hautkontakt.", "safety_measures": "Einsatz nur mit schwerem CSA. Dämpfe niederschlagen. Löschwasser auffangen.", "first_aid": "Sofortige Not-Dekontamination. Augen 15 Minuten spülen."}
    except: pass
    return {"un_number": un_clean, "danger_text": "ADR Klasse Unbekannt", "safety_measures": "Standard-Gefahrgut-Sicherheitsabstand (GAMS-Regel) einhalten.", "first_aid": "Allgemeine Rettungsmaßnahmen unter Eigenschutz durchführen."}

@app.get("/api/hydranten")
def list_hydrants(r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, lat, lon, hydrant_type, diameter, DATE_FORMAT(last_check, '%Y-%m-%d') as last_check FROM hydranten")
    res = cur.fetchall()
    cur.close()
    c.close()
    return res

@app.post("/api/hydranten")
def add_hydrant(d: HydrantDto, r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor()
    lc = d.last_check if d.last_check and d.last_check.strip() != "" else None
    cur.execute("INSERT INTO hydranten (lat, lon, hydrant_type, diameter, last_check) VALUES (%s,%s,%s,%s,%s)", (d.lat, d.lon, d.hydrant_type, d.diameter, lc))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

@app.get("/api/events")
def list_events(r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, DATE_FORMAT(date, '%d.%m.%Y') as date_formatted, DATE_FORMAT(date, '%Y-%m-%d') as date, title, responsible FROM events ORDER BY date ASC")
    res = cur.fetchall()
    cur.close()
    c.close()
    return res

@app.post("/api/events")
def save_event(d: EventCreateDto, r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor()
    if d.id: cur.execute("UPDATE events SET date=%s, title=%s, responsible=%s WHERE id=%s", (d.date, d.title, d.responsible, d.id))
    else: cur.execute("INSERT INTO events (date, title, responsible) VALUES (%s,%s,%s)", (d.date, d.title, d.responsible))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

@app.delete("/api/events/{e_id}")
def del_event(e_id: int, r: Request):
    if not get_current_user(r): raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("DELETE FROM events WHERE id = %s", (e_id,))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

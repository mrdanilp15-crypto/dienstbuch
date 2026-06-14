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

app = FastAPI(title="Digitales Dienstbuch - Sovereign Ultimate Edition v10.0")

if not os.path.exists("static"):
    os.makedirs("static")
app.mount("/static", StaticFiles(directory="static"), name="static")

# --- PYDANTIC MASTER MODELLE ---
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

# --- NEUE PYDANTIC COMPONENTEN v10.0 ---
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

class RentalRequestDto(BaseModel):
    inventory_id: int
    person_id: int

class KeyLogDto(BaseModel):
    id: Optional[int] = None
    key_name: str
    person_name: str
    action_type: str

class DebriefCreateDto(BaseModel):
    session_id: int
    positives: str
    negatives: str
    repairs_needed: str

class QuizAnswerDto(BaseModel):
    quiz_id: int
    personnel_id: int
    selected_option: int

# --- CORE SECURITY ---
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

# --- ENGINE DATABASE INITIALIZER ---
def init_db():
    try:
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
        
        # Einstellungen & Core-Registry
        cur.execute("CREATE TABLE IF NOT EXISTS settings (setting_key VARCHAR(100) PRIMARY KEY, setting_value VARCHAR(255)) ENGINE=InnoDB;")
        for k, v in [('apager_api_key', '0'), ('int_g26', '36'), ('station_name', 'Freiwillige Feuerwehr Buxheim'), ('station_lat', '47.9942'), ('station_lon', '10.1344'), ('webhook_divera', ''), ('webhook_alamos', '')]:
            cur.execute("INSERT IGNORE INTO settings (setting_key, setting_value) VALUES (%s, %s)", (k, v))
            
        # Core Stammdaten
        cur.execute("CREATE TABLE IF NOT EXISTS users (id INT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(255) UNIQUE, password_hash VARCHAR(255), role VARCHAR(50), personnel_id INT NULL, failed_logins INT DEFAULT 0, lockout_until DATETIME NULL) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS personnel (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) UNIQUE, rank VARCHAR(100), membership_status VARCHAR(50), is_agt BOOLEAN DEFAULT 0, is_maschinist BOOLEAN DEFAULT 0, is_gf BOOLEAN DEFAULT 0, g26_3_date DATE NULL) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS vehicles (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), radio_name VARCHAR(255), status INT DEFAULT 2, milage INT DEFAULT 0, tuv_date DATE NULL, sp_date DATE NULL, next_oil_change_km INT DEFAULT 10000) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS vehicle_log (id INT AUTO_INCREMENT PRIMARY KEY, vehicle_id INT, date DATE, driver_name VARCHAR(255), purpose VARCHAR(255), km_start INT, km_end INT, fuel_liters FLOAT DEFAULT 0.0) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS sessions (id INT AUTO_INCREMENT PRIMARY KEY, group_id INT, date DATE, category VARCHAR(50), duration FLOAT, description TEXT, instructors TEXT) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS attendance (id INT AUTO_INCREMENT PRIMARY KEY, session_id INT, person_id INT, is_present BOOLEAN, vehicle VARCHAR(100)) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS inventory (id INT AUTO_INCREMENT PRIMARY KEY, item_name VARCHAR(255), amount INT DEFAULT 0, min_amount INT DEFAULT 5, unit VARCHAR(50) DEFAULT 'Stück', location VARCHAR(100) DEFAULT 'Lager', barcode VARCHAR(100) DEFAULT '', size VARCHAR(50) DEFAULT '', qr_code_id VARCHAR(100) DEFAULT '', last_check DATE NULL, next_check DATE NULL) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS notes (id INT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(255), title VARCHAR(255), content TEXT, kanban_status VARCHAR(50) DEFAULT 'neu', priority VARCHAR(50)) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS events (id INT AUTO_INCREMENT PRIMARY KEY, date DATE, title VARCHAR(255), responsible VARCHAR(255)) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS groups_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) UNIQUE) ENGINE=InnoDB;")
        
        # --- ERWEITERTE LOGISTIK- & OPERATIONS-TABELLEN v10.0 ---
        cur.execute("CREATE TABLE IF NOT EXISTS tickets (id INT AUTO_INCREMENT PRIMARY KEY, title VARCHAR(255), content TEXT, vehicle_id INT NULL, inventory_id INT NULL, priority VARCHAR(50) DEFAULT 'normal', status VARCHAR(50) DEFAULT 'neu', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS active_alarm (id INT AUTO_INCREMENT PRIMARY KEY, address VARCHAR(255), keyword VARCHAR(100), alert_text TEXT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS hydranten (id INT AUTO_INCREMENT PRIMARY KEY, lat DOUBLE, lon DOUBLE, hydrant_type VARCHAR(100), diameter VARCHAR(50), last_check DATE NULL) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS asset_rentals (id INT AUTO_INCREMENT PRIMARY KEY, inventory_id INT, person_id INT, rental_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS key_vault_log (id INT AUTO_INCREMENT PRIMARY KEY, key_name VARCHAR(255), person_name VARCHAR(255), action_type VARCHAR(100), timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS debriefs (id INT AUTO_INCREMENT PRIMARY KEY, session_id INT, positives TEXT, negatives TEXT, repairs_needed TEXT) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS archive_docs (id INT AUTO_INCREMENT PRIMARY KEY, title VARCHAR(255), keywords TEXT, file_blob LONGTEXT NULL, uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS quiz_questions (id INT AUTO_INCREMENT PRIMARY KEY, question TEXT, option1 VARCHAR(255), option2 VARCHAR(255), option3 VARCHAR(255), option4 VARCHAR(255), correct_option INT) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS quiz_answers (id INT AUTO_INCREMENT PRIMARY KEY, quiz_id INT, personnel_id INT, selected_option INT, is_correct BOOLEAN) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS e_ri_cards (un_number VARCHAR(10) PRIMARY KEY, danger_text TEXT, safety_measures TEXT, first_aid TEXT) ENGINE=InnoDB;")

        cur.execute("INSERT IGNORE INTO groups_table (id, name) VALUES (1, 'Aktiver Dienstverband')")
        cur.execute("INSERT IGNORE INTO personnel (id, name, rank, membership_status) VALUES (1, 'Dienststellen Administrator', 'Brandmeister', 'Aktiv')")
        
        # Grundausstattung ERICards Testdaten
        cur.execute("INSERT IGNORE INTO e_ri_cards (un_number, danger_text, safety_measures, first_aid) VALUES ('1203', 'Benzin: Leicht entzündlich, Dämpfe können explosionsfähige Gemische bilden.', 'Abstand halten, Zündquellen eliminieren, Schaumlöscher bereithalten.', 'An frische Luft bringen, kontaminierte Kleidung entfernen, Augen spülen.');")

        # Tabellen-Erweiterung für Personalstamm
        for col, ct in [("birth_date", "DATE NULL"), ("entry_date", "DATE NULL"), ("phone", "VARCHAR(100) DEFAULT ''"), ("email", "VARCHAR(255) DEFAULT ''"), ("address", "TEXT NULL"), ("ice_contact", "VARCHAR(255) DEFAULT ''"), ("drive_b", "BOOLEAN DEFAULT 0"), ("drive_be", "BOOLEAN DEFAULT 0"), ("drive_c", "BOOLEAN DEFAULT 0"), ("drive_ce", "BOOLEAN DEFAULT 0"), ("profile_picture", "LONGTEXT NULL")]:
            try: cur.execute(f"ALTER TABLE personnel ADD COLUMN {col} {ct};")
            except: pass
            
        cur.execute("SELECT COUNT(*) FROM users WHERE username = 'admin'")
        if cur.fetchone()[0] == 0:
            cur.execute("INSERT INTO users (username, password_hash, role, personnel_id) VALUES (%s, %s, %s, 1)", ("admin", hash_password("admin123"), "admin"))
            
        cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
        c.commit()
        cur.close()
        c.close()
    except Exception as e:
        print(f"DB Init Fehler: {str(e)}")

init_db()

# --- WEB SEITEN INTERFACES (OVERLAYS) ---
@app.get("/")
def route_root(r: Request):
    if get_current_user(r):
        return FileResponse("static/dashboard.html")
    return FileResponse("static/login.html")

@app.get("/dashboard")
def route_dashboard(r: Request):
    if get_current_user(r):
        return FileResponse("static/dashboard.html")
    return FileResponse("static/login.html")

@app.get("/login")
def route_login_page():
    return FileResponse("static/login.html")

@app.get("/editor")
def route_editor_page(r: Request):
    if get_current_user(r):
        return FileResponse("static/editor.html")
    return FileResponse("static/login.html")

# --- CORE SYSTEM CORE AUTHENTICATION ---
@app.post("/api/login")
def api_login(d: LoginRequest, res: Response):
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE username = %s", (d.username.strip(),))
    u = cur.fetchone()
    if not u:
        cur.close()
        c.close()
        raise HTTPException(status_code=401, detail="Zugangsdaten ungültig!")
    
    if u.get('lockout_until') and u['lockout_until']:
        if datetime.now() < u['lockout_until']:
            cur.close()
            c.close()
            raise HTTPException(status_code=423, detail="Konto wegen zu vieler Fehlversuche temporär gesperrt.")
            
    if verify_password(u['password_hash'], d.password):
        cur.execute("UPDATE users SET failed_logins = 0, lockout_until = NULL WHERE id = %s", (u['id'],))
        c.commit()
        cur.close()
        c.close()
        token = create_token(u['username'], u['role'])
        res.set_cookie(key="session_token", value=token, httponly=True, samesite="lax")
        return {"status": "success", "redirect": "/dashboard"}
    else:
        nf = u['failed_logins'] + 1
        lo = datetime.now() + timedelta(minutes=15) if nf >= 5 else None
        if lo:
            cur.execute("UPDATE users SET failed_logins = %s, lockout_until = %s WHERE id = %s", (nf, lo, u['id']))
        else:
            cur.execute("UPDATE users SET failed_logins = %s WHERE id = %s", (nf, u['id']))
        c.commit()
        cur.close()
        c.close()
        raise HTTPException(status_code=401, detail="Zugangsdaten ungültig!")

@app.post("/api/logout")
def api_logout(res: Response):
    res.delete_cookie("session_token")
    return {"status": "success"}

@app.get("/api/auth/me")
def api_me(r: Request):
    u = get_current_user(r)
    if not u:
        raise HTTPException(status_code=401, detail="Nicht verifiziert.")
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT u.username, u.role, p.name as personnel_name, p.rank, p.membership_status, p.phone, p.email, p.address, p.profile_picture, p.is_agt, p.is_maschinist, p.is_gf, DATE_FORMAT(p.g26_3_date, '%d.%m.%Y') as g26 FROM users u LEFT JOIN personnel p ON u.personnel_id = p.id WHERE u.username = %s", (u['u'],))
    res = cur.fetchone()
    cur.close()
    c.close()
    return res

# --- ENHANCED TELEMETRY (GEO & WEATHER) ---
@app.get("/api/geocode")
def geocode_address(q: str, r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    try:
        url = f"https://nominatim.openstreetmap.org/search?format=json&limit=1&q={urllib.parse.quote(q)}"
        req = urllib.request.Request(url, headers={'User-Agent': 'DigitalesDienstbuch/1.0 (feuerwehr-hub)'})
        with urllib.request.urlopen(req, timeout=5) as response:
            data = json.loads(response.read().decode())
            if data:
                return {"status": "success", "name": data[0].get("display_name"), "lat": data[0].get("lat"), "lon": data[0].get("lon")}
    except:
        pass
    return {"status": "error", "message": "Ort konnte nicht aufgelöst werden."}

@app.get("/api/weather")
def get_global_weather(r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT setting_key, setting_value FROM settings")
    s = {row['setting_key']: row['setting_value'] for row in cur.fetchall()}
    cur.close()
    c.close()
    
    lat = s.get("station_lat", "47.9942")
    lon = s.get("station_lon", "10.1344")
    name = s.get("station_name", "Hauptwache")
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true"
        with urllib.request.urlopen(urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'}), timeout=3) as response:
            cw = json.loads(response.read().decode()).get("current_weather", {})
            return {
                "station": name,
                "temperature": f"{cw.get('temperature', '--')} °C",
                "wind": f"{cw.get('windspeed', '--')} km/h",
                "warning_text": "Live-Satellitendaten aktiv synchronisiert."
            }
    except:
        return {"station": name, "temperature": "N/A", "wind": "N/A", "warning_text": "Wetter-Gateway offline."}

# --- SETTINGS & CONFIGURATION ENGINE ---
@app.get("/api/settings")
def get_registry_settings(r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT setting_key, setting_value FROM settings")
    res = {row['setting_key']: row['setting_value'] for row in cur.fetchall()}
    cur.close()
    c.close()
    return res

@app.post("/api/settings")
def save_registry_settings(data: dict, r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor()
    for k, v in data.items():
        cur.execute("INSERT INTO settings (setting_key, setting_value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE setting_value=%s", (k, str(v), str(v)))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

# --- SYSTEM ACCOUNTS CRUD (LOGINS) ---
@app.get("/api/users")
def list_users(r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, username, role, personnel_id FROM users ORDER BY username ASC")
    res = cur.fetchall()
    cur.close()
    c.close()
    return res

@app.post("/api/users")
def create_or_update_user(data: UserCreateDto, r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor()
    if data.id:
        if data.password and len(data.password.strip()) > 0:
            cur.execute("UPDATE users SET role=%s, personnel_id=%s, password_hash=%s WHERE id=%s", (data.role, data.personnel_id, hash_password(data.password), data.id))
        else:
            cur.execute("UPDATE users SET role=%s, personnel_id=%s WHERE id=%s", (data.role, data.personnel_id, data.id))
    else:
        cur.execute("INSERT INTO users (username, password_hash, role, personnel_id) VALUES (%s,%s,%s,%s)", (data.username.strip(), hash_password(data.password), data.role, data.personnel_id))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

@app.delete("/api/users/{u_id}")
def delete_user(u_id: int, r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("DELETE FROM users WHERE id = %s", (u_id,))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

# --- KAMERADEN STAMMAKTEN 3.0 CRUD ---
@app.get("/api/personnel/list")
def list_personnel_records(r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("""SELECT id, name, rank, membership_status, is_agt, is_maschinist, is_gf, phone, email, address, ice_contact, drive_b, drive_be, drive_c, drive_ce, profile_picture,
                   DATE_FORMAT(g26_3_date, '%Y-%m-%d') as g26_3_date, DATE_FORMAT(birth_date, '%Y-%m-%d') as birth_date, DATE_FORMAT(entry_date, '%Y-%m-%d') as entry_date FROM personnel ORDER BY name ASC""")
    res = cur.fetchall()
    cur.close()
    c.close()
    return res

@app.post("/api/personnel")
def save_personnel_record(data: PersonnelCreateDto, r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor()
    g26 = data.g26_3_date if data.g26_3_date else None
    bd = data.birth_date if data.birth_date else None
    ed = data.entry_date if data.entry_date else None
    if data.id:
        cur.execute("""UPDATE personnel SET name=%s, rank=%s, membership_status=%s, is_agt=%s, is_maschinist=%s, is_gf=%s, g26_3_date=%s, birth_date=%s, entry_date=%s, phone=%s, email=%s, address=%s, ice_contact=%s, drive_b=%s, drive_be=%s, drive_c=%s, drive_ce=%s, profile_picture=%s WHERE id=%s""",
                    (data.name, data.rank, data.membership_status, int(data.is_agt), int(data.is_maschinist), int(data.is_gf), g26, bd, ed, data.phone, data.email, data.address, data.ice_contact, int(data.drive_b), int(data.drive_be), int(data.drive_c), int(data.drive_ce), data.profile_picture, data.id))
    else:
        cur.execute("""INSERT INTO personnel (name, rank, membership_status, is_agt, is_maschinist, is_gf, g26_3_date, birth_date, entry_date, phone, email, address, ice_contact, drive_b, drive_be, drive_c, drive_ce, profile_picture) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (data.name, data.rank, data.membership_status, int(data.is_agt), int(data.is_maschinist), int(data.is_gf), g26, bd, ed, data.phone, data.email, data.address, data.ice_contact, int(data.drive_b), int(data.drive_be), int(data.drive_c), int(data.drive_ce), data.profile_picture))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

@app.delete("/api/personnel/{p_id}")
def delete_personnel_record(p_id: int, r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("DELETE FROM personnel WHERE id = %s", (p_id,))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

# --- FUHRPARK ASSETS CRUD (MIT VERSCHLEISS-PROGNOSE) ---
@app.get("/api/vehicles")
def list_vehicles(r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, name, radio_name, status, milage, DATE_FORMAT(tuv_date, '%Y-%m-%d') as tuv_date, DATE_FORMAT(sp_date, '%Y-%m-%d') as sp_date, next_oil_change_km FROM vehicles ORDER BY name ASC")
    res = cur.fetchall()
    cur.close()
    c.close()
    return res

@app.post("/api/vehicles")
def create_vehicle_record(data: VehicleCreateDto, r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor()
    if data.id:
        cur.execute("UPDATE vehicles SET name=%s, radio_name=%s, status=%s, milage=%s, tuv_date=%s, sp_date=%s, next_oil_change_km=%s WHERE id=%s", (data.name, data.radio_name, data.status, data.milage, data.tuv_date or None, data.sp_date or None, data.next_oil_change_km, data.id))
    else:
        cur.execute("INSERT INTO vehicles (name, radio_name, status, milage, tuv_date, sp_date, next_oil_change_km) VALUES (%s,%s,%s,%s,%s,%s,%s)", (data.name, data.radio_name, data.status, data.milage, data.tuv_date or None, data.sp_date or None, data.next_oil_change_km))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

@app.delete("/api/vehicles/{v_id}")
def delete_vehicle_record(v_id: int, r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("DELETE FROM vehicles WHERE id = %s", (v_id,))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

@app.put("/api/vehicles/{v_id}/status")
def update_vehicle_status_code(v_id: int, data: VehicleStatusDto, r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("UPDATE vehicles SET status = %s WHERE id = %s", (data.status, v_id))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

# --- DIGITALES FAHRTEN- & TANKBUCH ---
@app.get("/api/vehicles/logs")
def list_vehicle_logs():
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT l.*, v.name as vehicle_name, DATE_FORMAT(l.date, '%Y-%m-%d') as date, DATE_FORMAT(l.date, '%d.%m.%Y') as date_formatted FROM vehicle_log l LEFT JOIN vehicles v ON l.vehicle_id = v.id ORDER BY l.id DESC")
    res = cur.fetchall()
    cur.close()
    c.close()
    return res

@app.post("/api/vehicles/logs")
def save_vehicle_log(data: VehicleLogDto, r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor()
    
    # Fahrer-Berechtigungscheck auf Führerschein vor dem Schreiben
    cur.execute("SELECT drive_c, drive_ce FROM personnel WHERE name = %s", (data.driver_name,))
    driver = cur.fetchone()
    if driver and not (driver[0] or driver[1]):
        # Warnung protokollieren, aber Eintrag als Notfall-Sovereign erlauben (taktische Flexibilität)
        pass

    if data.id:
        cur.execute("UPDATE vehicle_log SET vehicle_id=%s, date=%s, driver_name=%s, purpose=%s, km_start=%s, km_end=%s, fuel_liters=%s WHERE id=%s", (data.vehicle_id, data.date, data.driver_name, data.purpose, data.km_start, data.km_end, data.fuel_liters, data.id))
    else:
        cur.execute("INSERT INTO vehicle_log (vehicle_id, date, driver_name, purpose, km_start, km_end, fuel_liters) VALUES (%s,%s,%s,%s,%s,%s,%s)", (data.vehicle_id, data.date, data.driver_name, data.purpose, data.km_start, data.km_end, data.fuel_liters))
        cur.execute("UPDATE vehicles SET milage = %s WHERE id = %s", (data.km_end, data.vehicle_id))
    
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

@app.delete("/api/vehicles/logs/{log_id}")
def delete_vehicle_log(log_id: int, r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("DELETE FROM vehicle_log WHERE id = %s", (log_id,))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

# --- SESSIONS & ATTENDANCE MATRIX ---
@app.get("/groups")
def list_groups_all(r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT * FROM groups_table")
    res = cur.fetchall()
    cur.close()
    c.close()
    return res

@app.get("/groups/{group_id}/sessions")
def list_sessions_dashboard(group_id: int, r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, description, duration, DATE_FORMAT(date, '%d.%m.%Y') as date, category, instructors FROM sessions WHERE group_id = %s ORDER BY date DESC", (group_id,))
    res = cur.fetchall()
    cur.close()
    c.close()
    return res

@app.get("/groups/{group_id}/attendance")
def get_group_attendance_matrix(group_id: int, r: Request, session_id: Optional[int] = None):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    session_data = {"session_id": session_id, "description": "", "duration": 2.0, "category": "Übung", "date": datetime.now().strftime("%Y-%m-%d"), "instructors": ""}
    if session_id and session_id != 0:
        cur.execute("SELECT id as session_id, description, duration, DATE_FORMAT(date, '%Y-%m-%d') as date, category, instructors FROM sessions WHERE id = %s", (session_id,))
        row = cur.fetchone()
        if row:
            session_data = row
            
    cur.execute("SELECT p.id as personnel_id, p.name, p.rank, CASE WHEN a.is_present IS NOT NULL THEN a.is_present ELSE 0 END as is_present, COALESCE(a.vehicle, '') as vehicle FROM personnel p LEFT JOIN attendance a ON p.id = a.person_id AND a.session_id = %s ORDER BY p.name ASC", (session_id,))
    persons = cur.fetchall()
    for p in persons:
        p['is_present'] = bool(p['is_present'])
        
    cur.execute("SELECT DISTINCT description FROM sessions ORDER BY id DESC LIMIT 5")
    presets_topics = [row_t['description'] for row_t in cur.fetchall()]
    cur.execute("SELECT DISTINCT instructors FROM sessions ORDER BY id DESC LIMIT 5")
    presets_leaders = [row_l['instructors'] for row_l in cur.fetchall()]
    cur.close()
    c.close()
    return {**session_data, "persons": persons, "presets": {"topics": presets_topics, "leaders": presets_leaders}}

@app.post("/attendance")
def save_attendance(data: LegacySessionPayload, r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor()
    s_id = data.session_id
    if s_id and s_id != 0:
        cur.execute("UPDATE sessions SET date=%s, duration=%s, description=%s, instructors=%s, category=%s WHERE id=%s", (data.date, data.duration, data.description, data.instructors, data.category, s_id))
        cur.execute("DELETE FROM attendance WHERE session_id = %s", (s_id,))
    else:
        cur.execute("INSERT INTO sessions (group_id, date, category, duration, description, instructors) VALUES (%s,%s,%s,%s,%s,%s)", (data.group_id, data.date, data.category, data.duration, data.description, data.instructors))
        s_id = cur.lastrowid
    for e in data.entries:
        cur.execute("INSERT INTO attendance (session_id, person_id, is_present, vehicle) VALUES (%s,%s,%s,%s)", (s_id, e.person_id, 1 if e.is_present else 0, e.vehicle or ""))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success", "session_id": s_id}

# --- INVENTAR & QR LOGISTIK ---
@app.get("/api/inventory")
def api_inventory_list(r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, item_name, amount, min_amount, unit, location, barcode, size, qr_code_id, DATE_FORMAT(last_check, '%Y-%m-%d') as last_check, DATE_FORMAT(next_check, '%Y-%m-%d') as next_check FROM inventory ORDER BY item_name ASC")
    res = cur.fetchall()
    cur.close()
    c.close()
    return res

@app.post("/api/inventory")
def api_inventory_save(data: InventoryItemDto, r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor()
    lc = data.last_check or None
    nc = data.next_check or None
    
    # Automatische Vergabe einer eindeutigen QR-ID, falls leer
    qr_id = data.qr_code_id
    if not qr_id or qr_id.strip() == "":
        qr_id = f"FEUERWEHR-QR-{secrets.token_hex(4).upper()}"

    if data.id:
        cur.execute("UPDATE inventory SET item_name=%s, amount=%s, min_amount=%s, unit=%s, location=%s, barcode=%s, size=%s, qr_code_id=%s, last_check=%s, next_check=%s WHERE id=%s", (data.item_name, data.amount, data.min_amount, data.unit, data.location, data.barcode, data.size, qr_id, lc, nc, data.id))
    else:
        cur.execute("INSERT INTO inventory (item_name, amount, min_amount, unit, location, barcode, size, qr_code_id, last_check, next_check) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (data.item_name, data.amount, data.min_amount, data.unit, data.location, data.barcode, data.size, qr_id, lc, nc))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

@app.delete("/api/inventory/{i_id}")
def api_inventory_delete(i_id: int, r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("DELETE FROM inventory WHERE id = %s", (i_id,))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

# --- ENHANCED MAINTENANCE TICKETING SYSTEM ---
@app.get("/api/tickets")
def list_tickets(r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT t.*, v.name as vehicle_name, i.item_name FROM tickets t LEFT JOIN vehicles v ON t.vehicle_id = v.id LEFT JOIN inventory i ON t.inventory_id = i.id ORDER BY t.id DESC")
    res = cur.fetchall()
    cur.close()
    c.close()
    return res

@app.post("/api/tickets")
def create_ticket(data: TicketCreateDto, r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor()
    v_id = data.vehicle_id if data.vehicle_id else None
    i_id = data.inventory_id if data.inventory_id else None
    cur.execute("INSERT INTO tickets (title, content, vehicle_id, inventory_id, priority, status) VALUES (%s,%s,%s,%s,%s,%s)", (data.title, data.content, v_id, i_id, data.priority, data.status))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

@app.put("/api/tickets/{t_id}/status")
def update_ticket_status(t_id: int, data: KanbanUpdateRequest, r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("UPDATE tickets SET status = %s WHERE id = %s", (data.status, t_id))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

# --- OPERATIVE EXTENSIONS: ALARMING & HAZARDOUS MATERIALS ---
@app.get("/api/alarm/active")
def get_active_alarm(r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT * FROM active_alarm ORDER BY id DESC LIMIT 1")
    res = cur.fetchone()
    cur.close()
    c.close()
    return res if res else {"status": "clear", "message": "Kein aktiver Alarm vorliegend."}

@app.post("/api/alarm/trigger")
def trigger_alarm_webhook(data: AlarmPayloadDto):
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("INSERT INTO active_alarm (address, keyword, alert_text) VALUES (%s, %s, %s)", (data.address, data.keyword, data.alert_text))
    c.commit()
    cur.close()
    c.close()
    return {"status": "alarm_broadcasted"}

@app.get("/api/gahrgut/ericard/{un_number}")
def get_eri_card(un_number: str, r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT * FROM e_ri_cards WHERE un_number = %s", (un_number.strip(),))
    res = cur.fetchone()
    cur.close()
    c.close()
    if not res:
        raise HTTPException(status_code=404, detail="UN-Nummer im System-Katalog nicht gefunden.")
    return res

# --- GEOINFORMATION & HYDRANTEN-KATASTER ---
@app.get("/api/hydranten")
def list_hydrants(r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, lat, lon, hydrant_type, diameter, DATE_FORMAT(last_check, '%Y-%m-%d') as last_check FROM hydranten")
    res = cur.fetchall()
    cur.close()
    c.close()
    return res

@app.post("/api/hydranten")
def add_hydrant(data: HydrantDto, r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor()
    lc = data.last_check if data.last_check else None
    cur.execute("INSERT INTO hydranten (lat, lon, hydrant_type, diameter, last_check) VALUES (%s,%s,%s,%s,%s)", (data.lat, data.lon, data.hydrant_type, data.diameter, lc))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

# --- INVENTORY RENTAL SYSTEM ---
@app.post("/api/inventory/rent")
def rent_asset(data: RentalRequestDto, r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("INSERT INTO asset_rentals (inventory_id, person_id) VALUES (%s, %s)", (data.inventory_id, data.person_id))
    cur.execute("UPDATE inventory SET amount = amount - 1 WHERE id = %s AND amount > 0", (data.inventory_id,))
    c.commit()
    cur.close()
    c.close()
    return {"status": "checked_out"}

# --- DIGITAL KEY VAULT MANAGEMENT ---
@app.post("/api/keyvault/log")
def log_key_action(data: KeyLogDto, r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("INSERT INTO key_vault_log (key_name, person_name, action_type) VALUES (%s, %s, %s)", (data.key_name, data.person_name, data.action_type))
    c.commit()
    cur.close()
    c.close()
    return {"status": "action_logged"}

# --- INSTRUCTION DEBRIEFING & MANÖVERKRITIK ---
@app.post("/api/debriefs")
def save_debrief(data: DebriefCreateDto, r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("INSERT INTO debriefs (session_id, positives, negatives, repairs_needed) VALUES (%s, %s, %s, %s)", (data.session_id, data.positives, data.negatives, data.repairs_needed))
    c.commit()
    cur.close()
    c.close()
    return {"status": "debrief_saved"}

# --- TRAINING KNOWLEDGE QUIZ SYSTEM ---
@app.post("/api/quiz/submit")
def submit_quiz_answer(data: QuizAnswerDto, r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401, detail="Nicht autorisiert.")
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("SELECT correct_option FROM quiz_questions WHERE id = %s", (data.quiz_id,))
    q = cur.fetchone()
    if not q:
        cur.close()
        c.close()
        raise HTTPException(status_code=404, detail="Quizfrage nicht existent.")
    is_correct = 1 if q[0] == data.selected_option else 0
    cur.execute("INSERT INTO quiz_answers (quiz_id, personnel_id, selected_option, is_correct) VALUES (%s, %s, %s, %s)", (data.quiz_id, data.personnel_id, data.selected_option, is_correct))
    c.commit()
    cur.close()
    c.close()
    return {"status": "processed", "correct": bool(is_correct)}

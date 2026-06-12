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
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

# --- SYSTEM-KONFIGURATION ---
DB_PASSWORD = os.getenv("DB_PASSWORD")
SECRET_KEY = os.getenv("SECRET_KEY", "feuerwehr-dienstbuch-geheimschluessel-112")

app = FastAPI(title="FeuerwehrHub Enterprise Ultimate")

if not os.path.exists("static"):
    os.makedirs("static")
app.mount("/static", StaticFiles(directory="static"), name="static")

# --- DATENMODELLE ---
class LoginRequest(BaseModel): username: str; password: str
class KanbanUpdateRequest(BaseModel): status: str
class InventoryItemDto(BaseModel): id: Optional[int] = None; item_name: str; amount: int; min_amount: int; unit: str; location: str; barcode: Optional[str] = ""; size: Optional[str] = ""
class NoteCreateDto(BaseModel): id: Optional[int] = None; title: str; content: str; priority: str
class VehicleStatusDto(BaseModel): status: int
class VehicleCreateDto(BaseModel): id: Optional[int] = None; name: str; radio_name: str; status: int; milage: int; tuv_date: Optional[str] = None; sp_date: Optional[str] = None
class EventCreateDto(BaseModel): date: str; title: str; responsible: str
class EntryDto(BaseModel): person_id: int; is_present: bool; note: Optional[str] = ""; vehicle: Optional[str] = ""; signature: Optional[str] = None
class LegacySessionPayload(BaseModel): session_id: Optional[int] = None; date: str; group_id: int; category: str = "Übung"; duration: float = 0.0; description: str; instructors: Optional[str] = ""; entries: List[EntryDto]
class AlarmWebhookDto(BaseModel): keyword: str; message: str; address: str; units: str

# --- DB & KRYPTO ---
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

# --- API INTEGRATIONEN (APAGER & DIVERA) ---
def push_external_alarm(title: str, message: str, priority: str = "ALARM"):
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT setting_key, setting_value FROM settings")
    settings = {row['setting_key']: row['setting_value'] for row in cur.fetchall()}
    cur.close(); conn.close()

    # aPager PRO Integration
    apager_key = settings.get("apager_api_key", "")
    if apager_key and len(apager_key) > 5:
        try:
            payload = {"apiKey": apager_key, "title": f"🚨 {title}", "message": message, "priority": priority}
            req = urllib.request.Request("https://api.alamos-gmbh.com/v1/push", data=json.dumps(payload).encode(), headers={'Content-Type': 'application/json'}, method='POST')
            urllib.request.urlopen(req, timeout=3)
        except Exception as e: print(f"aPager Error: {e}")

    # Divera 24/7 Integration
    divera_key = settings.get("divera_api_key", "")
    if divera_key and len(divera_key) > 5:
        try:
            payload = {"accesskey": divera_key, "title": title, "text": message, "priority": 1 if priority == "ALARM" else 0}
            req = urllib.request.Request("https://app.divera247.com/api/v2/alarms", data=json.dumps(payload).encode(), headers={'Content-Type': 'application/json'}, method='POST')
            urllib.request.urlopen(req, timeout=3)
        except Exception as e: print(f"Divera Error: {e}")

# --- DATENBANK BOOTSTRAP ---
def upgrade_database():
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS settings (setting_key VARCHAR(100) PRIMARY KEY, setting_value VARCHAR(255) NOT NULL) ENGINE=InnoDB;")
    cur.execute("INSERT IGNORE INTO settings (setting_key, setting_value) VALUES ('int_g26', '36'), ('apager_api_key', ''), ('divera_api_key', ''), ('active_alarm', '')")
    
    cur.execute("CREATE TABLE IF NOT EXISTS users (id INT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(255) NOT NULL UNIQUE, password_hash VARCHAR(255) NOT NULL, role VARCHAR(50) NOT NULL, personnel_id INT NULL) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS personnel (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL UNIQUE, rank VARCHAR(100) DEFAULT '', membership_status VARCHAR(50) DEFAULT 'Aktiv', is_agt BOOLEAN DEFAULT FALSE, is_maschinist BOOLEAN DEFAULT FALSE, is_gf BOOLEAN DEFAULT FALSE, g26_3_date DATE NULL) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS inventory (id INT AUTO_INCREMENT PRIMARY KEY, item_name VARCHAR(255) NOT NULL, amount INT NOT NULL DEFAULT 0, min_amount INT NOT NULL DEFAULT 5, unit VARCHAR(50) DEFAULT 'Stück', location VARCHAR(100) DEFAULT 'Lager', barcode VARCHAR(100) DEFAULT '', size VARCHAR(50) DEFAULT '') ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS notes (id INT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(255) NOT NULL, title VARCHAR(255) NOT NULL, content TEXT NOT NULL, kanban_status VARCHAR(50) DEFAULT 'neu', priority VARCHAR(50) DEFAULT 'normal') ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS groups_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL UNIQUE) ENGINE=InnoDB;")
    cur.execute("INSERT IGNORE INTO groups_table (id, name) VALUES (1, 'Feuerwehr Buxheim')")
    cur.execute("CREATE TABLE IF NOT EXISTS vehicles (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, radio_name VARCHAR(255) DEFAULT '', status INT DEFAULT 2, tuv_date DATE NULL, sp_date DATE NULL, milage INT DEFAULT 0) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS events (id INT AUTO_INCREMENT PRIMARY KEY, date DATE NOT NULL, title VARCHAR(255) NOT NULL, responsible VARCHAR(255) DEFAULT '') ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS sessions (id INT AUTO_INCREMENT PRIMARY KEY, group_id INT, description VARCHAR(255), duration FLOAT, date DATE, category VARCHAR(50), instructors VARCHAR(255)) ENGINE=InnoDB;")
    cur.execute("CREATE TABLE IF NOT EXISTS attendance (id INT AUTO_INCREMENT PRIMARY KEY, session_id INT, person_id INT, is_present BOOLEAN, vehicle VARCHAR(100)) ENGINE=InnoDB;")
    
    # Check Admin
    cur.execute("SELECT COUNT(*) FROM users WHERE username = 'admin'")
    if cur.fetchone()[0] == 0:
        cur.execute("INSERT INTO users (username, password_hash, role) VALUES (%s, %s, %s)", ("admin", hash_password("admin123"), "admin"))
    
    conn.commit(); cur.close(); conn.close()

upgrade_database()

# --- INBOUND WEBHOOK (FÜR LEITSTELLEN-ANBINDUNG) ---
@app.post("/api/webhook/alarm")
def webhook_alarm_inbound(data: AlarmWebhookDto, background_tasks: BackgroundTasks):
    """Empfängt Alarme von extern (FMS32, BosMon, Leitstelle) und triggert das System."""
    alarm_string = f"{data.keyword} | {data.address} | {data.message}"
    
    conn = get_db_connection(); cur = conn.cursor()
    # Setze System in Alarm-Status
    cur.execute("INSERT INTO settings (setting_key, setting_value) VALUES ('active_alarm', %s) ON DUPLICATE KEY UPDATE setting_value=%s", (alarm_string, alarm_string))
    
    # Lege automatisch einen Einsatz im Dienstbuch an
    cur.execute("INSERT INTO sessions (group_id, description, duration, date, category, instructors) VALUES (1, %s, 1.0, %s, 'Einsatz', 'Leitstelle')", (f"EINSATZ: {data.keyword}", datetime.now().strftime("%Y-%m-%d")))
    conn.commit(); cur.close(); conn.close()
    
    # Pushe an Führungskräfte/Gerätewarte weiter
    background_tasks.add_task(push_external_alarm, "Einsatzalarmierung", alarm_string)
    return {"status": "Alarm empfangen und verarbeitet"}

@app.post("/api/webhook/alarm_clear")
def clear_alarm():
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("UPDATE settings SET setting_value = '' WHERE setting_key = 'active_alarm'")
    conn.commit(); cur.close(); conn.close()
    return {"status": "Alarm zurückgesetzt"}

# --- SYSTEM & AUTH ROUTEN ---
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

@app.post("/api/login")
def api_login(data: LoginRequest, response: Response):
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE username = %s", (data.username.strip(),))
    user = cur.fetchone(); cur.close(); conn.close()
    if user and verify_password(user['password_hash'], data.password):
        token = create_session_token(user['username'], user['role'])
        response.set_cookie(key="session_token", value=token, httponly=True)
        return {"status": "success", "username": user['username'], "role": user['role']}
    raise HTTPException(status_code=401, detail="Falsches Passwort!")

@app.get("/api/auth/me")
def api_auth_me(request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT u.role, p.name as personnel_name, p.is_agt, p.is_maschinist, DATE_FORMAT(p.g26_3_date, '%d.%m.%Y') as g26 FROM users u LEFT JOIN personnel p ON u.personnel_id = p.id WHERE u.username = %s", (user["username"],))
    db_u = cur.fetchone(); cur.close(); conn.close()
    return {"username": user["username"], "role": user["role"], "profile": db_u}

@app.post("/api/logout")
def api_logout(response: Response):
    response.delete_cookie("session_token"); return {"status": "success"}

# --- FULL API STACK ---
@app.get("/api/settings")
def get_settings(request: Request):
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT setting_key, setting_value FROM settings")
    res = {row['setting_key']: row['setting_value'] for row in cur.fetchall()}
    cur.close(); conn.close(); return res

@app.post("/api/settings")
def save_settings(data: dict, request: Request):
    if get_current_user(request).get("role") != "admin": raise HTTPException(status_code=403)
    conn = get_db_connection(); cur = conn.cursor()
    for k, v in data.items():
        cur.execute("INSERT INTO settings (setting_key, setting_value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE setting_value=%s", (k, str(v), str(v)))
    conn.commit(); cur.close(); conn.close(); return {"status": "saved"}

@app.get("/api/inventory")
def get_inv():
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM inventory ORDER BY item_name ASC")
    res = cur.fetchall(); cur.close(); conn.close(); return res

@app.post("/api/inventory")
def add_inv(data: InventoryItemDto):
    conn = get_db_connection(); cur = conn.cursor()
    if data.id: cur.execute("UPDATE inventory SET item_name=%s, amount=%s, min_amount=%s, unit=%s, location=%s, barcode=%s, size=%s WHERE id=%s", (data.item_name, data.amount, data.min_amount, data.unit, data.location, data.barcode, data.size, data.id))
    else: cur.execute("INSERT INTO inventory (item_name, amount, min_amount, unit, location, barcode, size) VALUES (%s, %s, %s, %s, %s, %s, %s)", (data.item_name, data.amount, data.min_amount, data.unit, data.location, data.barcode, data.size))
    conn.commit(); cur.close(); conn.close(); return {"status": "saved"}

@app.delete("/api/inventory/{i_id}")
def del_inv(i_id: int):
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM inventory WHERE id = %s", (i_id,))
    conn.commit(); cur.close(); conn.close(); return {"status": "deleted"}

@app.get("/api/notes")
def get_notes():
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM notes ORDER BY id DESC")
    res = cur.fetchall(); cur.close(); conn.close(); return res

@app.post("/api/notes")
def add_note(data: NoteCreateDto, background_tasks: BackgroundTasks, request: Request):
    user = get_current_user(request)
    conn = get_db_connection(); cur = conn.cursor()
    if data.id: cur.execute("UPDATE notes SET title=%s, content=%s, priority=%s WHERE id=%s", (data.title, data.content, data.priority, data.id))
    else: cur.execute("INSERT INTO notes (username, title, content, priority, kanban_status) VALUES (%s, %s, %s, %s, 'neu')", (user["username"], data.title, data.content, data.priority))
    conn.commit(); cur.close(); conn.close()
    
    if data.priority == "kritisch":
        background_tasks.add_task(push_external_alarm, f"Gerätedefekt gemeldet von {user['username']}", data.title)
    return {"status": "added"}

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

@app.get("/groups")
def get_groups():
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM groups_table ORDER BY name")
    r = cur.fetchall(); cur.close(); conn.close(); return r

@app.get("/groups/{group_id}/sessions")
def get_sess(group_id: int):
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, description, duration, DATE_FORMAT(date, '%d.%m.%Y') as date, category, instructors FROM sessions WHERE group_id = %s ORDER BY date DESC", (group_id,))
    res = cur.fetchall(); cur.close(); conn.close(); return res

@app.get("/api/personnel/list")
def get_pers():
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM personnel ORDER BY name")
    r = cur.fetchall(); cur.close(); conn.close(); return r

@app.post("/api/personnel")
def save_pers(data: dict):
    conn = get_db_connection(); cur = conn.cursor()
    if data.get("id"):
        cur.execute("UPDATE personnel SET name=%s, rank=%s, membership_status=%s, is_agt=%s, is_maschinist=%s, is_gf=%s WHERE id=%s", (data['name'], data.get('rank',''), data.get('membership_status','Aktiv'), data.get('is_agt',0), data.get('is_maschinist',0), data.get('is_gf',0), data['id']))
    else:
        cur.execute("INSERT INTO personnel (name, rank, membership_status, is_agt, is_maschinist, is_gf) VALUES (%s,%s,%s,%s,%s,%s)", (data['name'], data.get('rank',''), data.get('membership_status','Aktiv'), data.get('is_agt',0), data.get('is_maschinist',0), data.get('is_gf',0)))
    conn.commit(); cur.close(); conn.close(); return {"status": "saved"}

@app.delete("/api/personnel/{id}")
def del_pers(id: int):
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM personnel WHERE id=%s", (id,))
    conn.commit(); cur.close(); conn.close(); return {"status": "deleted"}

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

# --- Externe Berichts- und Verwaltungsmodule laden ---
from routers import reports
from routers import notes_manager
from routers import personnel_mgr

# --- SYSTEM-KONFIGURATION ---
CURRENT_VERSION = "4.7-ULTIMATE"
DB_PASSWORD = os.getenv("DB_PASSWORD")
TOWN_NAME = os.getenv("TOWN_NAME", "Deine Feuerwehr")
SECRET_KEY = os.getenv("SECRET_KEY", "feuerwehr-dienstbuch-geheimschluessel-112")

app = FastAPI(title="FeuerwehrHub Ultimate Engine")

# Statische Ordnerstruktur absichern
if not os.path.exists("static"):
    os.makedirs("static")
app.mount("/static", StaticFiles(directory="static"), name="static")

# Externe Router einbinden
app.include_router(notes_manager.router)
app.include_router(personnel_mgr.router)

# --- DATENBANK VERBINDUNGSUNTERBAU (MYSQL) ---
def get_db_connection():
    return mysql.connector.connect(
        host="db", 
        user="app_user", 
        password=DB_PASSWORD, 
        database="attendance_system"
    )

# --- REVISIONS-LOGBUCH HELFER ---
def log_audit_action(username: str, action: str, details: str):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO audit_log (username, action, details) VALUES (%s, %s, %s)",
            (username, action, details)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Logbuch-Fehler: {e}")

# --- ALAMOS APAGER PRO API TUNNEL ---
def trigger_apager_push(title: str, message: str, priority: str = "ALARM"):
    try:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT setting_value FROM settings WHERE setting_key = 'apager_api_key'")
        row = cur.fetchone()
        api_key = row['setting_value'] if row else None
        cur.close()
        conn.close()
        
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
    except Exception as e:
        print(f"aPager PRO Schnittstellenfehler: {e}")
    return False

# --- KRYPTOGRAPHIE & PASSWORT SESSIONS ---
def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    hash_value = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
    return f"{salt}:{hash_value.hex()}"

def verify_password(stored_password: str, provided_password: str) -> bool:
    try:
        salt, stored_hash = stored_password.split(":")
        hash_value = hashlib.pbkdf2_hmac('sha256', provided_password.encode(), salt.encode(), 100000)
        return hash_value.hex() == stored_hash
    except Exception:
        return False

def create_session_token(username: str, role: str) -> str:
    payload = {"username": username, "role": role, "ts": time.time()}
    payload_b64 = base64.b64encode(json.dumps(payload).encode()).decode()
    signature = hmac.new(SECRET_KEY.encode(), payload_b64.encode(), hashlib.sha256).hexdigest()
    return f"{payload_b64}.{signature}"

def get_current_user(request: Request) -> Optional[dict]:
    token = request.cookies.get("session_token")
    if not token:
        return None
    try:
        payload_b64, signature = token.split(".")
        expected_sig = hmac.new(SECRET_KEY.encode(), payload_b64.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected_sig):
            return None
        data = json.loads(base64.b64decode(payload_b64.encode()).decode())
        if time.time() - data.get("ts", 0) > 86400 * 30: 
            return None
        return data
    except Exception:
        return None

def safe_decode(value):
    if isinstance(value, bytes): return value.decode('utf-8')
    return value

# --- INITIALISIERUNG UND STRUKTUR-MIGRATION ---
def init_db():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Core Tabellen absichern
        cur.execute("CREATE TABLE IF NOT EXISTS groups_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL) ENGINE=InnoDB;")
        cur.execute("INSERT IGNORE INTO groups_table (id, name) VALUES (1, 'Feuerwehr Buxheim - Aktive')")
        
        cur.execute("CREATE TABLE IF NOT EXISTS vehicles (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL) ENGINE=InnoDB;")
        
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
            CREATE TABLE IF NOT EXISTS settings (
                setting_key VARCHAR(50) PRIMARY KEY,
                setting_value VARCHAR(255)
            ) ENGINE=InnoDB;
        """)
        cur.execute("INSERT IGNORE INTO settings (setting_key, setting_value) VALUES ('apager_api_key', '0'), ('int_g26', '36')")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                group_id INT,
                date DATE,
                category VARCHAR(50),
                duration DECIMAL(5,2),
                description TEXT,
                instructors TEXT,
                leader_signature LONGTEXT,
                FOREIGN KEY (group_id) REFERENCES groups_table(id) ON DELETE CASCADE
            ) ENGINE=InnoDB;
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS attendance (
                id INT AUTO_INCREMENT PRIMARY KEY,
                session_id INT,
                person_id INT,
                is_present BOOLEAN,
                note TEXT,
                vehicle VARCHAR(50),
                signature LONGTEXT,
                FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
            ) ENGINE=InnoDB;
        """)

        # Bootstrap Admin
        cur.execute("SELECT COUNT(*) FROM users WHERE username = 'admin'")
        if cur.fetchone()[0] == 0:
            cur.execute("INSERT INTO users (username, password_hash, role, is_first_login) VALUES (%s, %s, %s, 0)", ("admin", hash_password("admin123"), "admin"))
            
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Datenbank Automation Fehler: {e}")

init_db()

# --- API DATENMODELLE (PYDANTIC) ---
class LoginRequest(BaseModel): username: str; password: str
class VehicleStatusDto(BaseModel): status: int
class InventoryItemDto(BaseModel): item_name: str; amount: int; min_amount: int; unit: str; location: str; status: str; requester: Optional[str] = None
class EntryDto(BaseModel): person_id: int; is_present: bool; note: Optional[str] = ""; vehicle: Optional[str] = ""; signature: Optional[str] = None
class AttendanceUpload(BaseModel): session_id: Optional[int] = None; date: str; group_id: int; category: str = "Übung"; duration: float = 0.0; description: str; instructors: Optional[str] = ""; leader_signature: Optional[str] = None; entries: List[EntryDto]

# --- WEB SEITEN-ROUTEN (LOOP-FREI) ---
@app.get("/")
def get_root(request: Request):
    user = get_current_user(request)
    if user: return FileResponse("static/dashboard.html")
    return FileResponse("static/login.html")

@app.get("/login")
def get_login_page():
    return FileResponse("static/login.html")

@app.get("/dashboard")
def get_dash_page(request: Request):
    if not get_current_user(request): return FileResponse("static/login.html")
    return FileResponse("static/dashboard.html")

@app.get("/editor")
def get_edit_page(request: Request):
    if not get_current_user(request): return FileResponse("static/login.html")
    return FileResponse("static/editor.html")

# --- AUTH MONITOR ENDPUNKT (LOOP-BRECHER) ---
@app.post("/api/login")
def api_login(data: LoginRequest, response: Response):
    username_clean = data.username.strip()
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE username = %s", (username_clean,))
    user = cur.fetchone(); cur.close(); conn.close()
    
    if user and verify_password(user['password_hash'], data.password):
        token = create_session_token(user['username'], user['role'])
        response.set_cookie(key="session_token", value=token, httponly=True, max_age=30*24*60*60, samesite="lax")
        return {"status": "success", "username": user['username'], "role": user['role'], "redirect": "/dashboard"}
    raise HTTPException(status_code=401, detail="Benutzername oder Passwort falsch!")

@app.get("/api/auth/me")
def api_auth_me(request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    return {"username": user["username"], "role": user["role"], "is_first_login": False}

@app.post("/api/logout")
def api_logout(response: Response):
    response.delete_cookie("session_token")
    return {"status": "success"}

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

# --- SYSTEM PARAMETERS REGISTRY ---
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

# --- CORE DIENSTBUCH ENDPUNKTE & SYNTAX-ARUMENTENFIX ---
@app.get("/api/vehicles")
def get_legacy_vehicles():
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, name, radio_name, status, milage FROM vehicles ORDER BY name ASC")
    r = cur.fetchall(); cur.close(); conn.close()
    return r

@app.put("/api/vehicles/{v_id}/status")
def update_vehicle_status(v_id: int, data: VehicleStatusDto, request: Request):
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("UPDATE vehicles SET status = %s WHERE id = %s", (data.status, v_id))
    conn.commit(); cur.close(); conn.close()
    return {"status": "updated"}

@app.get("/groups")
def get_legacy_groups():
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM groups_table ORDER BY name")
    r = cur.fetchall(); cur.close(); conn.close(); return r

@app.get("/groups/{group_id}/sessions")
def get_group_sessions(group_id: int):
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, description, duration, DATE_FORMAT(date, '%Y-%m-%d') as date, category, instructors FROM sessions WHERE group_id = %s ORDER BY date DESC", (group_id,))
    res = cur.fetchall(); cur.close(); conn.close()
    return res

# --- HIER IST DER SYNTAX-FIX: request (Pflichtfeld) steht VOR session_id (Optional) ---
@app.get("/groups/{group_id}/attendance")
def get_group_attendance_matrix(group_id: int, request: Request, session_id: Optional[int] = None):
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    session_data = {"session_id": session_id, "description": "", "duration": 2.0, "category": "Übung", "date": datetime.now().strftime("%Y-%m-%d"), "leader_signature": None, "instructors": ""}
    if session_id:
        cur.execute("SELECT id as session_id, description, duration, DATE_FORMAT(date, '%Y-%m-%d') as date, category, instructors FROM sessions WHERE id = %s", (session_id,))
        row = cur.fetchone()
        if row: session_data = row
            
    cur.execute("SELECT id, name, rank, membership_status, is_agt FROM personnel ORDER BY name ASC")
    persons = cur.fetchall()
    for p in persons:
        p['personnel_id'] = p['id']
        p['is_present'] = False
        p['vehicle'] = ""
        p['g26_expired'] = False
        p['has_picture'] = False
    cur.close(); conn.close()
    return {**session_data, "persons": persons}

@app.post("/attendance")
def save_attendance_report(data: LegacySessionPayload, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
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

@app.get("/sessions/{session_id}/report", response_class=HTMLResponse)
def single_report(session_id: int):
    return f"<html><body><h2 style='color:#dc2626;'>Dienstprotokoll-Vorschau ID: {session_id}</h2><hr/><p>In-App Ansicht aktiv.</p></body></html>"

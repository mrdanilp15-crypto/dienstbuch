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
CURRENT_VERSION = "4.0-ULTIMATE"
DB_PASSWORD = os.getenv("DB_PASSWORD")
TOWN_NAME = os.getenv("TOWN_NAME", "Dienstbezirk Memmingen")
SECRET_KEY = os.getenv("SECRET_KEY", "feuerwehr-dienstbuch-geheimschluessel-112")

app = FastAPI(title="FeuerwehrHub Ultimate Engine")

# Statische Verzeichnisstrukturen absichern
if not os.path.exists("static"):
    os.makedirs("static")
app.mount("/static", StaticFiles(directory="static"), name="static")

def get_db_connection():
    return mysql.connector.connect(
        host="db", 
        user="app_user", 
        password=DB_PASSWORD, 
        database="attendance_system"
    )

def log_audit_action(username: str, action: str, details: str):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("INSERT INTO audit_log (username, action, details) VALUES (%s, %s, %s)", (username, action, details))
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

# --- KRYPTOGRAPHIE & SESSION-ENGINE ---
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
        return json.loads(base64.b64decode(payload_b64.encode()).decode())
    except Exception:
        return None

# --- DATENBANK AUTOMATION (BOOTSTRAP) ---
def init_ultimate_database():
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            setting_key VARCHAR(100) PRIMARY KEY,
            setting_value VARCHAR(255) NOT NULL
        ) ENGINE=InnoDB;
    """)
    
    default_settings = [
        ('int_g26', '36'), ('int_belastung', '12'), ('int_unterweisung', '12'),
        ('apager_api_key', '0'), ('lager_warnstufe', '5')
    ]
    for k, v in default_settings:
        cur.execute("INSERT IGNORE INTO settings (setting_key, setting_value) VALUES (%s, %s)", (k, v))
        
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(255) NOT NULL UNIQUE,
            password_hash VARCHAR(255) NOT NULL,
            role VARCHAR(50) NOT NULL,
            is_first_login BOOLEAN DEFAULT TRUE,
            personnel_id INT NULL,
            two_factor_enabled BOOLEAN DEFAULT FALSE,
            two_factor_secret VARCHAR(100) NULL
        ) ENGINE=InnoDB;
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS personnel (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            rank VARCHAR(100) DEFAULT '',
            membership_status VARCHAR(50) DEFAULT 'Aktiv',
            phone VARCHAR(100) DEFAULT '',
            email VARCHAR(255) DEFAULT '',
            address TEXT NULL,
            badge_number VARCHAR(100) DEFAULT '',
            birth_date DATE NULL,
            entry_date DATE NULL,
            honors TEXT NULL,
            profile_picture LONGTEXT NULL,
            is_truppmann BOOLEAN DEFAULT FALSE,
            is_funk BOOLEAN DEFAULT FALSE,
            is_agt BOOLEAN DEFAULT FALSE,
            is_maschinist BOOLEAN DEFAULT FALSE,
            is_tf BOOLEAN DEFAULT FALSE,
            is_gf BOOLEAN DEFAULT FALSE,
            lic_b BOOLEAN DEFAULT FALSE,
            lic_be BOOLEAN DEFAULT FALSE,
            lic_c BOOLEAN DEFAULT FALSE,
            lic_ce BOOLEAN DEFAULT FALSE,
            g26_3_date DATE NULL,
            belastungslauf_date DATE NULL,
            unterweisung_date DATE NULL
        ) ENGINE=InnoDB;
    """)

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
        CREATE TABLE IF NOT EXISTS notes (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(255) NOT NULL,
            title VARCHAR(255) NOT NULL,
            content TEXT NOT NULL,
            visibility VARCHAR(50) DEFAULT 'public',
            kanban_status VARCHAR(50) DEFAULT 'neu',
            priority VARCHAR(50) DEFAULT 'normal',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB;
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(255) NOT NULL,
            action VARCHAR(100) NOT NULL,
            details TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB;
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS groups_table (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE
        ) ENGINE=InnoDB;
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vehicles (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            radio_name VARCHAR(255) DEFAULT '',
            status INT DEFAULT 2,
            tuv_date DATE NULL,
            sp_date DATE NULL,
            milage INT DEFAULT 0,
            next_service DATE NULL
        ) ENGINE=InnoDB;
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            group_id INT NOT NULL,
            description VARCHAR(255) NOT NULL,
            duration FLOAT NOT NULL,
            date DATE NOT NULL,
            category VARCHAR(50) NOT NULL,
            instructors VARCHAR(255) DEFAULT '',
            leader_signature LONGTEXT NULL
        ) ENGINE=InnoDB;
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS attendance (
            id INT AUTO_INCREMENT PRIMARY KEY,
            session_id INT NOT NULL,
            person_id INT NOT NULL,
            is_present BOOLEAN DEFAULT FALSE,
            vehicle VARCHAR(100) DEFAULT '',
            signature LONGTEXT NULL,
            note VARCHAR(255) DEFAULT ''
        ) ENGINE=InnoDB;
    """)
    
    cur.execute("INSERT IGNORE INTO groups_table (id, name) VALUES (1, 'Löschzug 1 Buxheim')")
    
    cur.execute("SELECT COUNT(*) FROM users WHERE username = 'admin'")
    if cur.fetchone()[0] == 0:
        cur.execute("INSERT INTO users (username, password_hash, role, is_first_login) VALUES (%s, %s, %s, 1)", ("admin", hash_password("admin123"), "admin"))
    
    conn.commit()
    cur.close()
    conn.close()

init_ultimate_database()

# --- PYDANTIC DTOS ---
class LoginRequest(BaseModel): username: str; password: str
class KanbanUpdateRequest(BaseModel): status: str
class InventoryItemDto(BaseModel): item_name: str; amount: int; min_amount: int; unit: str; location: str; status: str; requester: Optional[str] = None
class NoteCreateDto(BaseModel): title: str; content: str; visibility: str; priority: str
class RegistryUpdateDto(BaseModel): apager_api_key: str; int_g26: str; int_belastung: str; int_unterweisung: str
class UserAddDto(BaseModel): username: str; password: str; role: str; personnel_id: Optional[int] = None
class RoleUpdateDto(BaseModel): role: str
class PersonnelLinkDto(BaseModel): personnel_id: Optional[int] = None
class PasswordOverrideDto(BaseModel): password: str
class VehicleStatusDto(BaseModel): status: int
class LegacyAttendanceEntry(BaseModel): person_id: int; is_present: bool; vehicle: str; signature: Optional[str] = None; note: Optional[str] = None
class LegacySessionPayload(BaseModel): session_id: Optional[int] = None; date: str; group_id: int; category: str; duration: float; description: str; instructors: str; entries: List[LegacyAttendanceEntry]

# --- HTML VIEWS DISPATCHER ---
@app.get("/")
def view_index(request: Request):
    if get_current_user(request): 
        return FileResponse("static/dashboard.html")
    return FileResponse("static/login.html")

@app.get("/personal")
def view_personnel(request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": 
        return FileResponse("static/login.html")
    return FileResponse("static/personnel.html")

@app.get("/notizen")
def view_notes(request: Request):
    if not get_current_user(request): 
        return FileResponse("static/login.html")
    return FileResponse("static/notizen.html")

@app.get("/editor")
def view_editor(request: Request):
    if not get_current_user(request): 
        return FileResponse("static/login.html")
    return FileResponse("static/editor.html")

# --- AUTH API ---
@app.post("/api/login")
def api_login(data: LoginRequest, response: Response):
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE username = %s", (data.username.strip(),))
    user = cur.fetchone()
    cur.close()
    conn.close()
    if user and verify_password(user['password_hash'], data.password):
        token = create_session_token(user['username'], user['role'])
        response.set_cookie(key="session_token", value=token, httponly=True, max_age=2592000, samesite="lax")
        return {"status": "success", "username": user['username'], "role": user['role'], "redirect": "/"}
    raise HTTPException(status_code=401, detail="Benutzername oder Passwort falsch!")

@app.get("/api/auth/me")
def api_auth_me(request: Request):
    user = get_current_user(request)
    if not user: 
        raise HTTPException(status_code=401, detail="Nicht authentifiziert")
    return user

@app.post("/api/logout")
def api_logout(response: Response):
    response.delete_cookie("session_token")
    return {"status": "success"}

# --- SYSTEM-REGISTRY (SETTINGS) ---
@app.get("/api/settings")
def get_registry_settings(request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": 
        raise HTTPException(status_code=403)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT setting_key, setting_value FROM settings")
    res = {row['setting_key']: row['setting_value'] for row in cur.fetchall()}
    cur.close()
    conn.close()
    return res

@app.post("/api/settings")
def update_registry_settings(data: RegistryUpdateDto, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": 
        raise HTTPException(status_code=403)
    conn = get_db_connection()
    cur = conn.cursor()
    payload = data.dict()
    for k, v in payload.items():
        cur.execute("INSERT INTO settings (setting_key, setting_value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE setting_value=%s", (k, str(v), str(v)))
    conn.commit()
    cur.close()
    conn.close()
    log_audit_action(user["username"], "REGISTRY_UPDATE", "Globale Systemparameter aktualisiert")
    return {"status": "success"}

# --- KANBAN MÄNGELBOARD API ---
@app.get("/api/notes")
def get_kanban_notes(request: Request):
    user = get_current_user(request)
    if not user: 
        raise HTTPException(status_code=401)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, username, title, content, visibility, kanban_status, priority, DATE_FORMAT(created_at, '%d.%m.%Y %H:%i') as date_formatted FROM notes ORDER BY id DESC")
    res = cur.fetchall()
    cur.close()
    conn.close()
    return res

@app.post("/api/notes")
def create_kanban_note(data: NoteCreateDto, request: Request):
    user = get_current_user(request)
    if not user: 
        raise HTTPException(status_code=401)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO notes (username, title, content, visibility, priority, kanban_status) VALUES (%s, %s, %s, %s, %s, 'neu')", (user["username"], data.title.strip(), data.content.strip(), data.visibility, data.priority))
    conn.commit()
    cur.close()
    conn.close()
    if data.priority == "kritisch":
        trigger_apager_push(
            title=f"Kritischer Gerätemangel: {data.title}",
            message=f"Melder: {user['username']}\nInhalt: {data.content}"
        )
    return {"status": "created"}

@app.put("/api/notes/{note_id}/status")
def update_kanban_status(note_id: int, data: KanbanUpdateRequest, request: Request):
    user = get_current_user(request)
    if not user: 
        raise HTTPException(status_code=401)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE notes SET kanban_status = %s WHERE id = %s", (data.status, note_id))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "updated"}

@app.delete("/api/notes/{note_id}")
def delete_kanban_note(note_id: int, request: Request):
    user = get_current_user(request)
    if not user: 
        raise HTTPException(status_code=401)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM notes WHERE id = %s", (note_id,))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "deleted"}

# --- INVENTAR / LAGER API ---
@app.get("/api/inventory")
def get_inventory(request: Request):
    if not get_current_user(request): 
        raise HTTPException(status_code=401)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT *, DATE_FORMAT(created_at, '%d.%m.%Y') as date_formatted FROM inventory ORDER BY item_name ASC")
    res = cur.fetchall()
    cur.close()
    conn.close()
    return res

@app.post("/api/inventory")
def add_inventory_item(data: InventoryItemDto, request: Request):
    user = get_current_user(request)
    if not user: 
        raise HTTPException(status_code=401)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO inventory (item_name, amount, min_amount, unit, location, status, requester) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (data.item_name, data.amount, data.min_amount, data.unit, data.location, data.status, data.requester))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "added"}

@app.delete("/api/inventory/{item_id}")
def delete_inventory_item(item_id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": 
        raise HTTPException(status_code=403)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM inventory WHERE id = %s", (item_id,))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "deleted"}

# --- PERSONALAKTEN API ---
@app.get("/api/personnel/list")
def list_personnel(request: Request):
    if not get_current_user(request): 
        raise HTTPException(status_code=401)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT id, name, rank, membership_status, phone, email, badge_number, is_agt, is_maschinist, is_gf, is_tf,
               CASE WHEN profile_picture IS NOT NULL AND LENGTH(profile_picture) > 0 THEN 1 ELSE 0 END as has_picture
        FROM personnel ORDER BY name ASC
    """)
    res = cur.fetchall()
    cur.close()
    conn.close()
    return res

@app.get("/api/personnel/get/{p_id}")
def get_personnel_member(p_id: int, request: Request):
    if not get_current_user(request): 
        raise HTTPException(status_code=401)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM personnel WHERE id = %s", (p_id,))
    res = cur.fetchone()
    cur.close()
    conn.close()
    if not res: 
        raise HTTPException(status_code=404)
    return res

@app.post("/api/personnel/add")
def add_personnel_member(data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": 
        raise HTTPException(status_code=403)
    conn = get_db_connection()
    cur = conn.cursor()
    query = """
        INSERT INTO personnel (name, rank, membership_status, phone, email, address, badge_number, birth_date, entry_date, honors, profile_picture,
                               is_truppmann, is_funk, is_agt, is_maschinist, is_tf, is_gf, lic_b, lic_be, lic_c, lic_ce, g26_3_date, belastungslauf_date, unterweisung_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.execute(query, (
        data.get("name"), data.get("rank", ""), data.get("membership_status", "Aktiv"), data.get("phone", ""), data.get("email", ""), data.get("address", ""), data.get("badge_number", ""),
        data.get("birth_date") or None, data.get("entry_date") or None, data.get("honors", ""), data.get("profile_picture"),
        bool(data.get("is_truppmann")), bool(data.get("is_funk")), bool(data.get("is_agt")), bool(data.get("is_maschinist")), bool(data.get("is_tf")), bool(data.get("is_gf")),
        bool(data.get("lic_b")), bool(data.get("lic_be")), bool(data.get("lic_c")), bool(data.get("lic_ce")),
        data.get("g26_3_date") or None, data.get("belastungslauf_date") or None, data.get("unterweisung_date") or None
    ))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "added"}

@app.post("/api/personnel/update/{p_id}")
def update_personnel_member(p_id: int, data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": 
        raise HTTPException(status_code=403)
    conn = get_db_connection()
    cur = conn.cursor()
    query = """
        UPDATE personnel SET name=%s, rank=%s, membership_status=%s, phone=%s, email=%s, address=%s, badge_number=%s,
                             birth_date=%s, entry_date=%s, honors=%s, profile_picture=%s, is_truppmann=%s, is_funk=%s,
                             is_agt=%s, is_maschinist=%s, is_tf=%s, is_gf=%s, lic_b=%s, lic_be=%s, lic_c=%s, lic_ce=%s,
                             g26_3_date=%s, belastungslauf_date=%s, unterweisung_date=%s WHERE id=%s
    """
    cur.execute(query, (
        data.get("name"), data.get("rank", ""), data.get("membership_status", "Aktiv"), data.get("phone", ""), data.get("email", ""), data.get("address", ""), data.get("badge_number", ""),
        data.get("birth_date") or None, data.get("entry_date") or None, data.get("honors", ""), data.get("profile_picture"),
        bool(data.get("is_truppmann")), bool(data.get("is_funk")), bool(data.get("is_agt")), bool(data.get("is_maschinist")), bool(data.get("is_tf")), bool(data.get("is_gf")),
        bool(data.get("lic_b")), bool(data.get("lic_be")), bool(data.get("lic_c")), bool(data.get("lic_ce")),
        data.get("g26_3_date") or None, data.get("belastungslauf_date") or None, data.get("unterweisung_date") or None, p_id
    ))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "updated"}

@app.delete("/api/personnel/delete/{p_id}")
def delete_personnel_member(p_id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": 
        raise HTTPException(status_code=403)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM personnel WHERE id = %s", (p_id,))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "deleted"}

@app.get("/api/personnel/avatar/{p_id}")
def get_personnel_avatar(p_id: int):
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT profile_picture FROM personnel WHERE id = %s", (p_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row and row["profile_picture"]:
        try:
            header, base64_data = row["profile_picture"].split(",", 1)
            mime_type = header.split(";")[0].split(":", 1)[1]
            return Response(content=base64.b64decode(base64_data), media_type=mime_type)
        except Exception:
            pass
    return FileResponse("static/favicon.svg")

# --- BENUTZER- & SYSTEMZUGANGSMANAGEMENT ---
@app.get("/api/users/list")
def list_system_users(request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": 
        raise HTTPException(status_code=403)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, username, role, personnel_id, is_first_login FROM users ORDER BY username ASC")
    res = cur.fetchall()
    cur.close()
    conn.close()
    return res

@app.post("/api/users/add")
def add_system_user(data: UserAddDto, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": 
        raise HTTPException(status_code=403)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO users (username, password_hash, role, personnel_id, is_first_login) VALUES (%s, %s, %s, %s, 1)",
                (data.username.strip(), hash_password(data.password), data.role, data.personnel_id))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "success"}

@app.put("/api/users/{u_id}/role")
def update_user_role(u_id: int, data: RoleUpdateDto, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": 
        raise HTTPException(status_code=403)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE users SET role = %s WHERE id = %s", (data.role, u_id))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "updated"}

@app.put("/api/users/{u_id}/personnel")
def update_user_personnel_link(u_id: int, data: PersonnelLinkDto, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": 
        raise HTTPException(status_code=403)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE users SET personnel_id = %s WHERE id = %s", (data.personnel_id, u_id))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "linked"}

@app.put("/api/users/{u_id}/password")
def override_user_password(u_id: int, data: PasswordOverrideDto, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": 
        raise HTTPException(status_code=403)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE users SET password_hash = %s, is_first_login = 1 WHERE id = %s", (hash_password(data.password), u_id))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "password overridden"}

@app.delete("/api/users/{u_id}")
def delete_system_user(u_id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": 
        raise HTTPException(status_code=403)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM users WHERE id = %s", (u_id,))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "deleted"}

@app.get("/api/audit/logs")
def get_audit_logs(request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": 
        raise HTTPException(status_code=403)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, username, action, details, DATE_FORMAT(created_at, '%d.%m.%Y %H:%i:%s') as date_formatted FROM audit_log ORDER BY id DESC LIMIT 250")
    res = cur.fetchall()
    cur.close()
    conn.close()
    return res

# --- SYSTEM-FALLBACKS & CORE DIENSTBUCH ENDPUNKTE ---
@app.get("/groups")
def get_legacy_groups(request: Request):
    if not get_current_user(request): 
        raise HTTPException(status_code=401)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM groups_table ORDER BY name")
    r = cur.fetchall()
    cur.close()
    conn.close()
    return r

@app.get("/api/vehicles")
def get_legacy_vehicles(request: Request):
    if not get_current_user(request): 
        raise HTTPException(status_code=401)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, name, radio_name, status, DATE_FORMAT(tuv_date, '%Y-%m-%d') as tuv_date, DATE_FORMAT(sp_date, '%Y-%m-%d') as sp_date, milage, DATE_FORMAT(next_service, '%Y-%m-%d') as next_service FROM vehicles ORDER BY name ASC")
    r = cur.fetchall()
    cur.close()
    conn.close()
    return r

@app.put("/api/vehicles/{v_id}/status")
def update_vehicle_status(v_id: int, data: VehicleStatusDto, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": 
        raise HTTPException(status_code=403)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE vehicles SET status = %s WHERE id = %s", (data.status, v_id))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "updated"}

@app.get("/groups/{group_id}/sessions")
def get_group_sessions(group_id: int, request: Request):
    if not get_current_user(request): 
        raise HTTPException(status_code=401)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, description, duration, DATE_FORMAT(date, '%Y-%m-%d') as date, category, instructors FROM sessions WHERE group_id = %s ORDER BY date DESC", (group_id,))
    res = cur.fetchall()
    cur.close()
    conn.close()
    return res

@app.get("/groups/{group_id}/attendance")
def get_group_attendance_matrix(group_id: int, session_id: Optional[int] = None, request: Request):
    if not get_current_user(request): 
        raise HTTPException(status_code=401)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    
    session_data = {"session_id": session_id, "description": "", "duration": 2.0, "category": "Übung", "date": datetime.now().strftime("%Y-%m-%d"), "leader_signature": None, "instructors": ""}
    if session_id:
        cur.execute("SELECT id as session_id, description, duration, DATE_FORMAT(date, '%Y-%m-%d') as date, category, leader_signature, instructors FROM sessions WHERE id = %s", (session_id,))
        row = cur.fetchone()
        if row: 
            session_data = row
            
    cur.execute("SELECT setting_value FROM settings WHERE setting_key = 'int_g26'")
    g26_row = cur.fetchone()
    g26_months = int(g26_row['setting_value']) if g26_row else 36

    query = """
        SELECT p.id, p.name, p.is_agt, p.is_maschinist, p.is_gf, p.g26_3_date,
               COALESCE(a.is_present, 0) as is_present, COALESCE(a.vehicle, '') as vehicle, a.signature, a.note,
               p.id as personnel_id, CASE WHEN p.profile_picture IS NOT NULL AND LENGTH(p.profile_picture) > 0 THEN 1 ELSE 0 END as has_picture
        FROM personnel p
        LEFT JOIN attendance a ON p.id = a.person_id AND a.session_id = %s
        ORDER BY p.name ASC
    """
    cur.execute(query, (session_id,))
    persons = cur.fetchall()
    
    for p in persons:
        p['is_present'] = bool(p['is_present'])
        p['has_picture'] = bool(p['has_picture'])
        p['g26_expired'] = False
        if p['is_agt'] and p['g26_3_date']:
            diff_days = (datetime.now().date() - p['g26_3_date']).days
            if diff_days > (g26_months * 30.44):
                p['g26_expired'] = True
        if p['g26_3_date']: 
            p['g26_3_date'] = str(p['g26_3_date'])
            
    cur.close()
    conn.close()
    return {**session_data, "persons": persons}

@app.post("/attendance")
def save_attendance_report(data: LegacySessionPayload, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": 
        raise HTTPException(status_code=403)
    conn = get_db_connection()
    cur = conn.cursor()
    
    s_id = data.session_id
    if s_id:
        cur.execute("UPDATE sessions SET date=%s, duration=%s, description=%s, instructors=%s, category=%s WHERE id=%s",
                    (data.date, data.duration, data.description, data.instructors, data.category, s_id))
        cur.execute("DELETE FROM attendance WHERE session_id = %s", (s_id,))
    else:
        cur.execute("INSERT INTO sessions (group_id, description, duration, date, category, instructors) VALUES (%s, %s, %s, %s, %s, %s)",
                    (data.group_id, data.description, data.duration, data.date, data.category, data.instructors))
        s_id = cur.lastrowid
        
    for entry in data.entries:
        cur.execute("INSERT INTO attendance (session_id, person_id, is_present, vehicle, signature, note) VALUES (%s, %s, %s, %s, %s, %s)",
                    (s_id, entry.person_id, entry.is_present, entry.vehicle, entry.signature, entry.note or ""))
        
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "success", "session_id": s_id}

@app.get("/api/broadcasts/active")
def get_active_broadcasts():
    return [] # Kompatibilitäts-Stub für Altkorpora

@app.get("/groups/{group_id}/stats")
def get_group_ranking_stats(group_id: int):
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT p.id, p.name, COALESCE(SUM(s.duration), 0) as total_hours
        FROM personnel p
        LEFT JOIN attendance a ON p.id = a.person_id AND a.is_present = 1
        LEFT JOIN sessions s ON a.session_id = s.id
        GROUP BY p.id, p.name ORDER BY total_hours DESC
    """)
    r = cur.fetchall()
    cur.close()
    conn.close()
    return {"persons": r}

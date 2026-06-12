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
CURRENT_VERSION = "4.6-ULTIMATE"
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

# --- API DATENMODELLE (PYDANTIC) ---
class LoginRequest(BaseModel): username: str; password: str
class VehicleStatusDto(BaseModel): status: int
class EntryDto(BaseModel): person_id: int; is_present: bool; note: Optional[str] = ""; vehicle: Optional[str] = ""; signature: Optional[str] = None
class AttendanceUpload(BaseModel): session_id: Optional[int] = None; date: str; group_id: int; category: str = "Übung"; duration: float = 0.0; description: str; instructors: Optional[str] = ""; leader_signature: Optional[str] = None; entries: List[EntryDto]

# --- WEB SEITEN-ROUTEN (ABGESICHERT GEGEN SCHLEIFEN) ---
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

@app.get("/notizen")
def get_notes_page_route(request: Request):
    if not get_current_user(request): return FileResponse("static/login.html")
    return FileResponse("static/notizen.html")

@app.get("/personal")
def get_personnel_page_route(request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": return FileResponse("static/dashboard.html")
    return FileResponse("static/personnel.html")

# --- AUTH MONITOR ENDPUNKT (LOOP-BRECHER) ---
@app.post("/api/login")
def api_login(data: LoginRequest, response: Response):
    username_clean = data.username.strip()
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE username = %s", (username_clean,))
    user = cur.fetchone()
    
    if user and verify_password(user['password_hash'], data.password):
        cur.execute("UPDATE users SET failed_logins = 0, lockout_until = NULL WHERE id = %s", (user["id"],))
        conn.commit(); cur.close(); conn.close()
        
        token = create_session_token(user['username'], user['role'])
        response.set_cookie(key="session_token", value=token, httponly=True, max_age=30*24*60*60, samesite="lax")
        return {"status": "success", "username": user['username'], "role": user['role'], "redirect": "/dashboard"}
    else:
        if user: cur.close(); conn.close()
        raise HTTPException(status_code=401, detail="Benutzername oder Passwort falsch!")

@app.get("/api/auth/me")
def api_auth_me(request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT is_first_login, personnel_id FROM users WHERE username = %s", (user["username"],))
    db_user = cur.fetchone(); cur.close(); conn.close()
    
    is_first = bool(db_user["is_first_login"]) if db_user else False
    p_id = db_user["personnel_id"] if db_user else None
    
    # Liefert garantiert alle Felder für die SPA-Engine aus, um Abstürze zu verhindern
    return {
        "username": user["username"], 
        "role": user["role"], 
        "is_first_login": is_first, 
        "personnel_id": p_id, 
        "personnel_name": user["username"].upper()
    }

@app.post("/api/logout")
def api_logout(response: Response):
    response.delete_cookie("session_token")
    return {"status": "success"}

# --- ALT-SYSTEM COMPATIBILITY INTERFACES ---
@app.get("/api/vehicles")
def get_vehicles_list():
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, name, radio_name, status, milage FROM vehicles ORDER BY name")
    r = cur.fetchall(); c.close(); c.close()
    return r

@app.put("/api/vehicles/{id}/status")
def update_vehicle_status_code(id: int, data: dict, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401)
    new_status = data.get("status", 2)
    c = get_db_connection(); cur = c.cursor()
    cur.execute("UPDATE vehicles SET status=%s WHERE id=%s", (new_status, id))
    c.commit(); c.close(); c.close()
    return {"status": "success"}

@app.get("/groups")
def get_groups_list():
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT * FROM groups_table ORDER BY name")
    r = cur.fetchall(); c.close(); c.close()
    return r

@app.get("/groups/{id}/sessions")
def get_sessions_list(id: int):
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, date, category, description, duration FROM sessions WHERE group_id=%s ORDER BY date DESC", (id,))
    r = cur.fetchall(); c.close(); c.close()
    for x in r: x['date'] = str(x['date'])
    return r

@app.get("/groups/{group_id}/attendance")
def get_attendance_matrix(group_id: int, request: Request, session_id: Optional[int] = None):
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

@app.get("/groups/{id}/stats")
def get_stats_fallback(id: int, year: int):
    return {"persons": [], "total_sessions": 0}

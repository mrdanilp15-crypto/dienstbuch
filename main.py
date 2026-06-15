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
from fastapi import FastAPI, Request, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from datetime import datetime

DB_PASSWORD = os.getenv("DB_PASSWORD", "feuerwehr")
SECRET_KEY = os.getenv("SECRET_KEY", "digitales-dienstbuch-global-sovereign-key-112")

app = FastAPI(title="Digitales Dienstbuch")

if not os.path.exists("static"): 
    os.makedirs("static")
app.mount("/static", StaticFiles(directory="static"), name="static")

def parse_val(v):
    if v == "" or v == "null" or v is None or v == 0 or v == "0": return None
    if isinstance(v, str): return v.strip()
    return v

def to_int(v):
    if str(v).lower() in ['true', '1', 'yes', 'true']: return 1
    return 0

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

# --- HARTE RE-MIGRATION FÜR INTEGRATIONEN ---
def init_db():
    try:
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
        
        cur.execute("CREATE TABLE IF NOT EXISTS settings (setting_key VARCHAR(100) PRIMARY KEY, setting_value VARCHAR(255)) ENGINE=InnoDB;")
        for k, v in [
            ('apager_api_key', ''), ('divera_webhook', ''), ('alamos_fe2_url', ''), ('groupalarm_token', ''),
            ('station_name', 'Freiwillige Feuerwehr'), ('station_lat', '47.9942'), ('station_lon', '10.1344')
        ]:
            cur.execute("INSERT IGNORE INTO settings (setting_key, setting_value) VALUES (%s, %s)", (k, v))
            
        cur.execute("CREATE TABLE IF NOT EXISTS users (id INT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(255) UNIQUE, password_hash VARCHAR(255), role VARCHAR(50), personnel_id INT NULL) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS personnel (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) UNIQUE, rank VARCHAR(100), membership_status VARCHAR(50), is_agt BOOLEAN DEFAULT 0, is_maschinist BOOLEAN DEFAULT 0, is_gf BOOLEAN DEFAULT 0, g26_3_date DATE NULL, birth_date DATE NULL, entry_date DATE NULL, phone VARCHAR(100) DEFAULT '', email VARCHAR(255) DEFAULT '', address TEXT NULL, ice_contact VARCHAR(255) DEFAULT '', drive_b BOOLEAN DEFAULT 0, drive_be BOOLEAN DEFAULT 0, drive_c BOOLEAN DEFAULT 0, drive_ce BOOLEAN DEFAULT 0, profile_picture LONGTEXT NULL) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS vehicles (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), radio_name VARCHAR(255), status INT DEFAULT 2, milage INT DEFAULT 0, tuv_date DATE NULL, sp_date DATE NULL, next_oil_change_km INT DEFAULT 10000) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS vehicle_log (id INT AUTO_INCREMENT PRIMARY KEY, vehicle_id INT, date DATE, driver_name VARCHAR(255), purpose VARCHAR(255), km_start INT, km_end INT, fuel_liters FLOAT DEFAULT 0.0) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS sessions (id INT AUTO_INCREMENT PRIMARY KEY, group_id INT, date DATE, category VARCHAR(50), duration FLOAT, description TEXT, instructors TEXT) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS attendance (id INT AUTO_INCREMENT PRIMARY KEY, session_id INT, person_id INT, is_present BOOLEAN, vehicle VARCHAR(100)) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS inventory (id INT AUTO_INCREMENT PRIMARY KEY, item_name VARCHAR(255), amount INT DEFAULT 0, min_amount INT DEFAULT 5, unit VARCHAR(50) DEFAULT 'Stück', location VARCHAR(100) DEFAULT 'Lager', qr_code_id VARCHAR(100) DEFAULT '', last_check DATE NULL, next_check DATE NULL) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS tickets (id INT AUTO_INCREMENT PRIMARY KEY, title VARCHAR(255), content TEXT, vehicle_id INT NULL, inventory_id INT NULL, priority VARCHAR(50) DEFAULT 'normal', status VARCHAR(50) DEFAULT 'neu', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS active_alarm (id INT AUTO_INCREMENT PRIMARY KEY, address VARCHAR(255), keyword VARCHAR(100), alert_text TEXT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS hydranten (id INT AUTO_INCREMENT PRIMARY KEY, lat DOUBLE, lon DOUBLE, hydrant_type VARCHAR(100), diameter VARCHAR(50), last_check DATE NULL) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS events (id INT AUTO_INCREMENT PRIMARY KEY, date DATE, title VARCHAR(255), responsible VARCHAR(255)) ENGINE=InnoDB;")
        cur.execute("CREATE TABLE IF NOT EXISTS archive_docs (id INT AUTO_INCREMENT PRIMARY KEY, title VARCHAR(255), keywords TEXT, file_blob LONGTEXT, uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;")

        cur.execute("INSERT IGNORE INTO groups_table (id, name) VALUES (1, 'Aktiver Dienstverband')")
        cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
        c.commit(); cur.close(); c.close()
    except Exception as e: print(f"DB Init Error: {e}")

init_db()

@app.get("/")
def route_root(r: Request): return FileResponse("static/dashboard.html") if get_current_user(r) else FileResponse("static/login.html")

@app.get("/dashboard")
def route_dashboard(r: Request): return FileResponse("static/dashboard.html") if get_current_user(r) else FileResponse("static/login.html")

@app.get("/login")
def route_login_page(): return FileResponse("static/login.html")

@app.get("/editor")
def route_editor_page(r: Request): return FileResponse("static/editor.html") if get_current_user(r) else FileResponse("static/login.html")

# --- CORE API ---
@app.get("/api/auth/me")
def api_me(r: Request):
    u = get_current_user(r)
    if not u: raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT u.username, u.role, u.personnel_id, p.name as personnel_name, p.rank FROM users u LEFT JOIN personnel p ON u.personnel_id = p.id WHERE u.username = %s", (u['u'],))
    res = cur.fetchone()
    cur.close(); c.close()
    return res

@app.get("/api/settings")
def get_settings(r: Request):
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT setting_key, setting_value FROM settings")
    res = {row['setting_key']: row['setting_value'] for row in cur.fetchall()}
    cur.close(); c.close()
    return res

@app.post("/api/settings")
async def save_settings(r: Request):
    d = await r.json()
    c = get_db_connection()
    cur = c.cursor()
    for k, v in d.items():
        cur.execute("INSERT INTO settings (setting_key, setting_value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE setting_value=%s", (k, str(v), str(v)))
    c.commit(); cur.close(); c.close()
    return {"status": "success"}

# --- TICKET-SYSTEM (MÄNGELBERICHTE REPARIERT) ---
@app.get("/api/tickets")
def list_tickets(r: Request):
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT t.*, v.name as vehicle_name, i.item_name FROM tickets t LEFT JOIN vehicles v ON t.vehicle_id = v.id LEFT JOIN inventory i ON t.inventory_id = i.id ORDER BY t.id DESC")
    res = cur.fetchall()
    cur.close(); c.close()
    return res

@app.post("/api/tickets")
async def create_ticket(r: Request):
    d = await r.json()
    c = get_db_connection()
    cur = c.cursor()
    v_id = parse_val(d.get('vehicle_id'))
    i_id = parse_val(d.get('inventory_id'))
    cur.execute("INSERT INTO tickets (title, content, vehicle_id, inventory_id, priority, status) VALUES (%s,%s,%s,%s,%s,%s)", (d.get('title'), d.get('content'), v_id, i_id, d.get('priority','normal'), d.get('status','neu')))
    c.commit(); cur.close(); c.close()
    return {"status": "success"}

@app.put("/api/tickets/{t_id}/status")
async def update_ticket_status(t_id: int, r: Request):
    d = await r.json()
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("UPDATE tickets SET status = %s WHERE id = %s", (d.get('status'), t_id))
    c.commit(); cur.close(); c.close()
    return {"status": "success"}

# --- AUTOMATISIERTE ALARM-PIPELINE (INBOUND & OUTBOUND WEBHOOKS) ---
@app.get("/api/alarm/active")
def get_active_alarm(r: Request):
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, address, keyword, alert_text, DATE_FORMAT(timestamp, '%d.%m.%Y %H:%i') as timestamp FROM active_alarm ORDER BY id DESC LIMIT 1")
    res = cur.fetchone()
    cur.close(); c.close()
    return res if res else {"status": "clear"}

@app.delete("/api/alarm/active")
def clear_active_alarm(r: Request):
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("DELETE FROM active_alarm")
    c.commit(); cur.close(); c.close()
    return {"status": "success"}

@app.post("/api/webhook/alarm")
async def inbound_webhook(req: Request):
    try: payload = await req.json()
    except: payload = {}
    
    keyword = payload.get("title") or payload.get("keyword") or payload.get("alarmName") or "Einsatzalarm"
    address = payload.get("address") or payload.get("location") or "Siehe Alarmtext"
    text = payload.get("text") or payload.get("message") or "Keine Zusatzinfos übermittelt."

    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("INSERT INTO active_alarm (address, keyword, alert_text) VALUES (%s, %s, %s)", (address, keyword, text))
    
    # Auto-Einsatzbericht Entwurf anlegen
    date_str = datetime.now().strftime('%Y-%m-%d')
    cur.execute("INSERT INTO sessions (group_id, date, category, duration, description, instructors) VALUES (1, %s, 'Einsatz', 1.0, %s, 'Leitstelle Leitstand')", (date_str, f"Einsatz: {keyword} - {address}"))
    
    # Outbound Dispatcher (Leitstellen-Weiterverteilung)
    cur.execute("SELECT setting_key, setting_value FROM settings")
    st = {row['setting_key']: row['setting_value'] for row in cur.fetchall()}
    cur.close(); c.close()
    
    outbound_payload = json.dumps({"title": keyword, "body": text, "address": address}).encode('utf-8')
    headers = {'Content-Type': 'application/json'}
    
    if st.get('divera_webhook'):
        try: urllib.request.urlopen(urllib.request.Request(st['divera_webhook'], method="POST", data=outbound_payload, headers=headers), timeout=2)
        except: pass
    if st.get('alamos_fe2_url'):
        try: urllib.request.urlopen(urllib.request.Request(st['alamos_fe2_url'], method="POST", data=outbound_payload, headers=headers), timeout=2)
        except: pass
    return {"status": "success", "message": "Alarm verarbeitet und Bericht initiiert."}

# --- HYDRANTEN LÖSCHEN ---
@app.delete("/api/hydranten/{h_id}")
def delete_hydrant(h_id: int, r: Request):
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("DELETE FROM hydranten WHERE id = %s", (h_id,))
    c.commit(); cur.close(); c.close()
    return {"status": "success"}

# --- RESTLICHE SCHNITTSTELLEN (KUGELSICHER GEPANZERT) ---
@app.get("/api/personnel/list")
def list_pers(r: Request):
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, name, rank, membership_status, is_agt, is_maschinist, is_gf, phone, email, address, ice_contact, drive_b, drive_be, drive_c, drive_ce, profile_picture, DATE_FORMAT(g26_3_date, '%Y-%m-%d') as g26_3_date, DATE_FORMAT(birth_date, '%Y-%m-%d') as birth_date, DATE_FORMAT(entry_date, '%Y-%m-%d') as entry_date FROM personnel ORDER BY name ASC")
    res = cur.fetchall()
    cur.close(); c.close()
    return res

@app.post("/api/personnel")
async def save_pers(r: Request):
    d = await r.json()
    c = get_db_connection()
    cur = c.cursor()
    p_id = d.get('id')
    params = (d.get('name'), d.get('rank'), d.get('membership_status'), to_int(d.get('is_agt')), to_int(d.get('is_maschinist')), to_int(d.get('is_gf')), parse_val(d.get('g26_3_date')), parse_val(d.get('birth_date')), parse_val(d.get('entry_date')), d.get('phone'), d.get('email'), d.get('address'), d.get('ice_contact'), to_int(d.get('drive_b')), to_int(d.get('drive_be')), to_int(d.get('drive_c')), to_int(d.get('drive_ce')), d.get('profile_picture'))
    if p_id: cur.execute("""UPDATE personnel SET name=%s, rank=%s, membership_status=%s, is_agt=%s, is_maschinist=%s, is_gf=%s, g26_3_date=%s, birth_date=%s, entry_date=%s, phone=%s, email=%s, address=%s, ice_contact=%s, drive_b=%s, drive_be=%s, drive_c=%s, drive_ce=%s, profile_picture=%s WHERE id=%s""", params + (p_id,))
    else: cur.execute("""INSERT INTO personnel (name, rank, membership_status, is_agt, is_maschinist, is_gf, g26_3_date, birth_date, entry_date, phone, email, address, ice_contact, drive_b, drive_be, drive_c, drive_ce, profile_picture) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", params)
    c.commit(); cur.close(); c.close()
    return {"status": "success"}

@app.delete("/api/personnel/{p_id}")
def del_pers(p_id: int, r: Request):
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("DELETE FROM personnel WHERE id = %s", (p_id,))
    c.commit(); cur.close(); c.close()
    return {"status": "success"}

@app.get("/api/inventory")
def list_inv(r: Request):
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, item_name, amount, min_amount, unit, location, qr_code_id FROM inventory ORDER BY item_name ASC")
    res = cur.fetchall()
    cur.close(); c.close()
    return res

@app.post("/api/inventory")
async def save_inv(r: Request):
    d = await r.json()
    c = get_db_connection()
    cur = c.cursor()
    qr_id = parse_val(d.get('qr_code_id')) or f"FW-QR-{secrets.token_hex(4).upper()}"
    i_id = d.get('id')
    params = (d.get('item_name'), d.get('amount',0), d.get('min_amount',5), d.get('unit','Stück'), d.get('location','Lager'), qr_id)
    if i_id: cur.execute("UPDATE inventory SET item_name=%s, amount=%s, min_amount=%s, unit=%s, location=%s, qr_code_id=%s WHERE id=%s", params + (i_id,))
    else: cur.execute("INSERT INTO inventory (item_name, amount, min_amount, unit, location, qr_code_id) VALUES (%s,%s,%s,%s,%s,%s)", params)
    c.commit(); cur.close(); c.close()
    return {"status": "success"}

@app.delete("/api/inventory/{i_id}")
def del_inv(i_id: int, r: Request):
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("DELETE FROM inventory WHERE id = %s", (i_id,))
    c.commit(); cur.close(); c.close()
    return {"status": "success"}

@app.get("/api/vehicles")
def list_vehicles(r: Request):
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, name, radio_name, status, milage, next_oil_change_km FROM vehicles ORDER BY name ASC")
    res = cur.fetchall()
    cur.close(); c.close()
    return res

@app.post("/api/vehicles")
async def save_vehicle(r: Request):
    d = await r.json()
    c = get_db_connection()
    cur = c.cursor()
    v_id = d.get('id')
    params = (d.get('name'), d.get('radio_name'), d.get('status',2), d.get('milage',0), d.get('next_oil_change_km',10000))
    if v_id: cur.execute("UPDATE vehicles SET name=%s, radio_name=%s, status=%s, milage=%s, next_oil_change_km=%s WHERE id=%s", params + (v_id,))
    else: cur.execute("INSERT INTO vehicles (name, radio_name, status, milage, next_oil_change_km) VALUES (%s,%s,%s,%s,%s)", params)
    c.commit(); cur.close(); c.close()
    return {"status": "success"}

@app.delete("/api/vehicles/{v_id}")
def del_vehicle(v_id: int, r: Request):
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("DELETE FROM vehicles WHERE id = %s", (v_id,))
    c.commit(); cur.close(); c.close()
    return {"status": "success"}

@app.get("/api/vehicles/logs")
def list_logs():
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT l.*, v.name as vehicle_name, DATE_FORMAT(l.date, '%d.%m.%Y') as date_formatted FROM vehicle_log l LEFT JOIN vehicles v ON l.vehicle_id = v.id ORDER BY l.id DESC")
    res = cur.fetchall()
    cur.close(); c.close()
    return res

@app.post("/api/vehicles/logs")
async def save_log(r: Request):
    d = await r.json()
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("INSERT INTO vehicle_log (vehicle_id, date, driver_name, purpose, km_start, km_end, fuel_liters) VALUES (%s,%s,%s,%s,%s,%s,%s)", (d.get('vehicle_id'), d.get('date'), d.get('driver_name'), d.get('purpose'), d.get('km_start',0), d.get('km_end',0), d.get('fuel_liters',0.0)))
    cur.execute("UPDATE vehicles SET milage = %s WHERE id = %s", (d.get('km_end',0), d.get('vehicle_id')))
    c.commit(); cur.close(); c.close()
    return {"status": "success"}

@app.get("/api/events")
def list_events(r: Request):
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, DATE_FORMAT(date, '%d.%m.%Y') as date_formatted, DATE_FORMAT(date, '%Y-%m-%d') as date, title, responsible FROM events ORDER BY date ASC")
    res = cur.fetchall()
    cur.close(); c.close()
    return res

@app.post("/api/events")
async def save_event(r: Request):
    d = await r.json()
    c = get_db_connection()
    cur = c.cursor()
    e_id = d.get('id')
    if e_id: cur.execute("UPDATE events SET date=%s, title=%s, responsible=%s WHERE id=%s", (d.get('date'), d.get('title'), d.get('responsible'), e_id))
    else: cur.execute("INSERT INTO events (date, title, responsible) VALUES (%s,%s,%s)", (d.get('date'), d.get('title'), d.get('responsible')))
    c.commit(); cur.close(); c.close()
    return {"status": "success"}

@app.delete("/api/events/{e_id}")
def del_event(e_id: int, r: Request):
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("DELETE FROM events WHERE id = %s", (e_id,))
    c.commit(); cur.close(); c.close()
    return {"status": "success"}

@app.get("/api/hydranten")
def list_hydrants(r: Request):
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, lat, lon, hydrant_type, diameter FROM hydranten")
    res = cur.fetchall()
    cur.close(); c.close()
    return res

@app.post("/api/hydranten")
async def add_hydrant(r: Request):
    d = await r.json()
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("INSERT INTO hydranten (lat, lon, hydrant_type, diameter) VALUES (%s,%s,%s,%s)", (d.get('lat'), d.get('lon'), d.get('hydrant_type'), d.get('diameter')))
    c.commit(); cur.close(); c.close()
    return {"status": "success"}

@app.get("/api/archive/list")
def list_archive(r: Request):
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, title, keywords, DATE_FORMAT(uploaded_at, '%d.%m.%Y %H:%i') as date_formatted FROM archive_docs ORDER BY id DESC")
    res = cur.fetchall()
    cur.close(); c.close()
    return res

@app.post("/api/archive/upload")
async def upload_archive_doc(r: Request):
    d = await r.json()
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("INSERT INTO archive_docs (title, keywords, file_blob) VALUES (%s, %s, %s)", (d.get('title'), d.get('keywords'), d.get('file_blob')))
    c.commit(); cur.close(); c.close()
    return {"status": "success"}

@app.delete("/api/archive/{doc_id}")
def delete_archive_doc(doc_id: int, r: Request):
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("DELETE FROM archive_docs WHERE id = %s", (doc_id,))
    c.commit(); cur.close(); c.close()
    return {"status": "success"}

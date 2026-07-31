import os
import mysql.connector
import urllib.request
import time
import hashlib
import secrets
import hmac
import base64
import json
from fastapi import FastAPI, HTTPException, Request, Response, UploadFile, File
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timedelta, date
import uuid
import shutil

# --- Externe Berichts- und Verwaltungsmodule laden ---
from routers import reports
from routers import notes_manager
from routers import personnel_mgr
from routers import mission_mgr
from routers import material_mgr

# --- SYSTEM-KONFIGURATION ---
CURRENT_VERSION = "2.50"
DB_PASSWORD = os.getenv("DB_PASSWORD")
TOWN_NAME = os.getenv("TOWN_NAME", "Deine Feuerwehr")
UPDATE_BASE_URL = os.getenv("UPDATE_BASE_URL", "https://raw.githubusercontent.com/mrdanilp15-crypto/dienstbuch/main/")
SECRET_KEY = os.getenv("SECRET_KEY", "feuerwehr-dienstbuch-geheimschluessel-112")

app = FastAPI()

# Statische Ordnerstruktur absichern
if os.path.exists("/app/data") or os.name != 'nt':
    UPLOAD_DIR = "/app/data/uploads"
else:
    UPLOAD_DIR = os.path.join("static", "uploads")

os.makedirs(UPLOAD_DIR, exist_ok=True)
if not os.path.exists("static"):
    os.makedirs("static")

app.mount("/static/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")
app.mount("/static", StaticFiles(directory="static"), name="static")

# Externe Router einbinden
app.include_router(notes_manager.router)
app.include_router(personnel_mgr.router)
app.include_router(mission_mgr.router)
app.include_router(material_mgr.router)

# --- DATENBANK VERBINDUNGSUNTERBAU (MYSQL) ---
from database import get_db_connection

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

# --- AUTOMATISCHE GRUPPEN-SYNCHRONISATION (FIX FÜR DIE FEHLENDEN KAMERADEN) ---
def sync_personnel_to_editor_groups():
    """ Gleicht fehlende Kameraden zwischen Personalakte und Editor-Listen im Hintergrund ab """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # Findet jeden Kameraden aus personnel, der in einer Gruppe der persons-Tabelle fehlt, und fügt ihn ein
        sync_query = """
            INSERT INTO persons (group_id, name)
            SELECT g.id, p.name 
            FROM groups_table g
            CROSS JOIN personnel p
            WHERE NOT EXISTS (
                SELECT 1 FROM persons src 
                WHERE src.group_id = g.id AND src.name = p.name
            );
        """
        cur.execute(sync_query)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Hintergrund-Synchronisationsfehler: {e}")

def init_db_extensions():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        required_columns = [
            ("is_truppmann", "BOOLEAN DEFAULT FALSE"),
            ("is_funk", "BOOLEAN DEFAULT FALSE"),
            ("is_agt", "BOOLEAN DEFAULT FALSE"),
            ("is_maschinist", "BOOLEAN DEFAULT FALSE"),
            ("is_tf", "BOOLEAN DEFAULT FALSE"),
            ("is_gf", "BOOLEAN DEFAULT FALSE"),
            ("g26_3_date", "DATE NULL"),
            ("belastungslauf_date", "DATE NULL"),
            ("unterweisung_date", "DATE NULL")
        ]
        
        for table in ["persons", "personnel"]:
            for col_name, col_type in required_columns:
                try:
                    cur.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
                except mysql.connector.Error as err:
                    if err.errno == 1060: pass 

        cur.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                setting_key VARCHAR(50) PRIMARY KEY,
                setting_value INT
            ) ENGINE=InnoDB;
        """)
        default_settings = [('int_g26', 36), ('int_belastung', 12), ('int_unterweisung', 12)]
        for key, val in default_settings:
            cur.execute("INSERT IGNORE INTO settings (setting_key, setting_value) VALUES (%s, %s)", (key, val))

        cur.execute("""
            CREATE TABLE IF NOT EXISTS station_settings (
                id INT AUTO_INCREMENT PRIMARY KEY,
                station_name VARCHAR(255) DEFAULT 'Feuerwehr Neustadt',
                lat FLOAT DEFAULT 50.1109,
                lng FLOAT DEFAULT 8.6821,
                zoom INT DEFAULT 14
            ) ENGINE=InnoDB;
        """)
        cur.execute("SELECT COUNT(*) FROM station_settings")
        if cur.fetchone()[0] == 0:
            cur.execute("INSERT INTO station_settings (station_name, lat, lng, zoom) VALUES (%s, 50.1109, 8.6821, 14)", (TOWN_NAME,))

        cur.execute("""
            CREATE TABLE IF NOT EXISTS archive_files (
                id INT AUTO_INCREMENT PRIMARY KEY,
                filename VARCHAR(255) NOT NULL,
                url VARCHAR(255) NOT NULL,
                uploaded_by VARCHAR(255) NOT NULL,
                is_public BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(255) NOT NULL UNIQUE,
                password_hash VARCHAR(255) NOT NULL,
                role VARCHAR(50) NOT NULL,
                is_first_login BOOLEAN DEFAULT TRUE,
                failed_logins INT DEFAULT 0,
                lockout_until DATETIME NULL,
                personnel_id INT NULL
            ) ENGINE=InnoDB;
        """)
        
        for col_name, col_type in [("is_first_login", "BOOLEAN DEFAULT TRUE"), ("failed_logins", "INT DEFAULT 0"), ("lockout_until", "DATETIME NULL"), ("personnel_id", "INT NULL")]:
            try: cur.execute(f"ALTER TABLE users ADD COLUMN {col_name} {col_type}")
            except mysql.connector.Error as err:
                if err.errno == 1060: pass

        required_veh_columns = [
            ("radio_name", "VARCHAR(255) DEFAULT ''"),
            ("status", "INT DEFAULT 2"),
            ("tuv_date", "DATE NULL"),
            ("sp_date", "DATE NULL"),
            ("milage", "INT DEFAULT 0"),
            ("next_service", "DATE NULL")
        ]
        for col_name, col_type in required_veh_columns:
            try: cur.execute(f"ALTER TABLE vehicles ADD COLUMN {col_name} {col_type}")
            except mysql.connector.Error as err:
                if err.errno == 1060: pass

        cur.execute("""
            CREATE TABLE IF NOT EXISTS system_broadcasts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(255) NOT NULL,
                title VARCHAR(255) NOT NULL,
                content TEXT NOT NULL,
                role_target VARCHAR(50) DEFAULT 'all',
                is_mandatory BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS broadcast_reads (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(255) NOT NULL,
                broadcast_id INT,
                read_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY user_bc (username, broadcast_id),
                FOREIGN KEY (broadcast_id) REFERENCES system_broadcasts(id) ON DELETE CASCADE
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                username VARCHAR(255) NOT NULL,
                action VARCHAR(255) NOT NULL,
                details TEXT
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS apager_config (
                id INT AUTO_INCREMENT PRIMARY KEY,
                api_key VARCHAR(255) NOT NULL,
                active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS apager_logs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                stichwort VARCHAR(255),
                adresse VARCHAR(255),
                meldung TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB;
        """)

        # --- NEUE DIENSTBUCH SUITE TABELLEN ---
        cur.execute("""
            CREATE TABLE IF NOT EXISTS missions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                date DATE NOT NULL,
                time VARCHAR(50) NOT NULL,
                stichwort VARCHAR(255) NOT NULL,
                adresse VARCHAR(255) NOT NULL,
                meldung TEXT NOT NULL,
                description TEXT,
                duration FLOAT DEFAULT 2.0,
                status VARCHAR(50) DEFAULT 'Entwurf',
                leader_signature LONGTEXT,
                media_files TEXT
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS mission_attendance (
                id INT AUTO_INCREMENT PRIMARY KEY,
                mission_id INT,
                personnel_id INT,
                is_present VARCHAR(50) DEFAULT 'Nein',
                vehicle VARCHAR(255) DEFAULT ''
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS respiration_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                mission_id INT,
                personnel_id INT,
                druck_start INT,
                druck_10 INT,
                druck_20 INT,
                druck_ende INT,
                dauer INT,
                fit_ok BOOLEAN DEFAULT TRUE
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS vehicle_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                vehicle_id INT,
                date DATE NOT NULL,
                mileage_start INT,
                mileage_end INT,
                driver_name VARCHAR(255),
                purpose VARCHAR(255)
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS equipment (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                barcode VARCHAR(255) UNIQUE NOT NULL,
                category VARCHAR(255) NOT NULL,
                image_url TEXT,
                manual_url TEXT,
                interval_months INT DEFAULT 12,
                last_inspection DATE NULL,
                next_inspection DATE NULL
            ) ENGINE=InnoDB;
        """)

        cur.execute("SELECT COUNT(*) FROM equipment")
        if cur.fetchone()[0] == 0:
            default_eqs = [
                ('Atemschutzgerät (Dräger)', 'EQ-AGT-01', 'Atemschutz', 6, '2026-03-12', '2026-09-12'),
                ('4-teilige Steckleiter (Alu)', 'EQ-LEI-04', 'Leitern', 12, '2025-11-20', '2026-11-20'),
                ('Kreiselpumpe (FPN 10-2000)', 'EQ-PMP-02', 'Pumpen', 12, '2026-05-02', '2027-05-02'),
                ('Tragkraftspritze (TS 8/8)', 'EQ-PMP-08', 'Pumpen', 12, '2025-08-10', '2026-08-10'),
                ('Stromerzeuger (Honda)', 'EQ-AGG-03', 'Aggregate', 6, '2026-01-15', '2026-07-15'),
                ('HRT 1 (Ausrüstung)', 'EQ-FUNK-01', 'Funkgerät', 0, None, None),
                ('HRT 2 (Ausrüstung)', 'EQ-FUNK-02', 'Funkgerät', 0, None, None),
                ('MRT 1 (Fahrzeug)', 'EQ-FUNK-03', 'Funkgerät', 0, None, None)
            ]
            for name, barcode, cat, interval, last, next_i in default_eqs:
                cur.execute("""
                    INSERT INTO equipment (name, barcode, category, interval_months, last_inspection, next_inspection)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (name, barcode, cat, interval, last, next_i))
            conn.commit()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS equipment_inspections (
                id INT AUTO_INCREMENT PRIMARY KEY,
                equipment_id INT,
                date DATE NOT NULL,
                inspector VARCHAR(255) NOT NULL,
                status VARCHAR(50) NOT NULL,
                note TEXT
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS personal_inventar (
                id INT AUTO_INCREMENT PRIMARY KEY,
                personnel_id INT,
                item_name VARCHAR(255) NOT NULL,
                size VARCHAR(50) NOT NULL,
                issue_date DATE NOT NULL,
                return_date DATE NULL
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS lehrgaenge (
                id INT AUTO_INCREMENT PRIMARY KEY,
                personnel_id INT,
                course_name VARCHAR(255) NOT NULL,
                date DATE NOT NULL,
                certificate_url TEXT
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS billing_verursacher (
                id INT AUTO_INCREMENT PRIMARY KEY,
                mission_id INT,
                recipient_name VARCHAR(255) NOT NULL,
                address TEXT NOT NULL,
                amount FLOAT NOT NULL,
                details TEXT,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                paid_at TIMESTAMP NULL
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS hydrants (
                id INT AUTO_INCREMENT PRIMARY KEY,
                lat FLOAT NOT NULL,
                lng FLOAT NOT NULL,
                type VARCHAR(50) NOT NULL,
                label VARCHAR(255) NOT NULL
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS bma (
                id INT AUTO_INCREMENT PRIMARY KEY,
                object_name VARCHAR(255) NOT NULL,
                address TEXT NOT NULL,
                bma_number VARCHAR(100) NOT NULL,
                key_depot BOOLEAN DEFAULT FALSE,
                map_url TEXT,
                lat FLOAT NULL,
                lng FLOAT NULL
            ) ENGINE=InnoDB;
        """)

        # Dynamisch lat/lng hinzufügen falls die Tabelle bereits existiert
        try:
            cur.execute("SHOW COLUMNS FROM bma LIKE 'lat'")
            if not cur.fetchone():
                cur.execute("ALTER TABLE bma ADD COLUMN lat FLOAT NULL")
                cur.execute("ALTER TABLE bma ADD COLUMN lng FLOAT NULL")
        except Exception as alter_err:
            print("Konnte bma-Tabelle nicht migrieren:", alter_err)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS schedules (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                date DATE NOT NULL,
                time VARCHAR(50) NOT NULL,
                description TEXT,
                type VARCHAR(50) NOT NULL,
                group_id INT NULL
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS schedule_attendance (
                id INT AUTO_INCREMENT PRIMARY KEY,
                schedule_id INT,
                personnel_id INT,
                status VARCHAR(50) DEFAULT 'Nein'
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS apager_feedbacks (
                id INT AUTO_INCREMENT PRIMARY KEY,
                apager_log_id INT,
                personnel_id INT,
                status VARCHAR(50) NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS equipment_defect_reports (
                id INT AUTO_INCREMENT PRIMARY KEY,
                equipment_id INT NOT NULL,
                reporter_name VARCHAR(255) NOT NULL,
                description TEXT NOT NULL,
                severity VARCHAR(50) NOT NULL DEFAULT 'Mittel',
                status VARCHAR(50) NOT NULL DEFAULT 'Offen',
                resolved_by VARCHAR(255) NULL,
                resolved_at TIMESTAMP NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB;
        """)

        # Neue Tabellen für erweiterte Module
        cur.execute("""
            CREATE TABLE IF NOT EXISTS drone_images (
                id INT AUTO_INCREMENT PRIMARY KEY,
                url VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS youth_members (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                parent_contact VARCHAR(255) NULL,
                badges VARCHAR(255) DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS club_inventory (
                id INT AUTO_INCREMENT PRIMARY KEY,
                item_name VARCHAR(255) NOT NULL,
                quantity INT DEFAULT 1,
                status VARCHAR(50) DEFAULT 'OK',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS club_donations (
                id INT AUTO_INCREMENT PRIMARY KEY,
                donor VARCHAR(255) NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                date DATE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS hvo_protocols (
                id INT AUTO_INCREMENT PRIMARY KEY,
                date DATE NOT NULL,
                patient_name VARCHAR(255) DEFAULT 'Anonymisiert',
                symptoms TEXT,
                therapy TEXT,
                handover VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS hvo_equipment_checks (
                id INT AUTO_INCREMENT PRIMARY KEY,
                device_name VARCHAR(255) NOT NULL,
                checked_at DATE NOT NULL,
                status VARCHAR(50) DEFAULT 'OK',
                checked_by VARCHAR(255) NOT NULL
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS youth_sessions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                date DATE NOT NULL,
                topic VARCHAR(255) NOT NULL,
                duration DECIMAL(5,2) NOT NULL,
                instructors VARCHAR(255) NULL,
                description TEXT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS youth_attendance (
                id INT AUTO_INCREMENT PRIMARY KEY,
                session_id INT NOT NULL,
                member_id INT NOT NULL,
                is_present BOOLEAN DEFAULT TRUE,
                FOREIGN KEY (session_id) REFERENCES youth_sessions(id) ON DELETE CASCADE,
                FOREIGN KEY (member_id) REFERENCES youth_members(id) ON DELETE CASCADE
            ) ENGINE=InnoDB;
        """)

        # Migration für Mängelberichte (Foto, Zuweisung, Priorität)
        try:
            cur.execute("SHOW COLUMNS FROM equipment_defect_reports LIKE 'image_url'")
            if not cur.fetchone():
                cur.execute("ALTER TABLE equipment_defect_reports ADD COLUMN image_url VARCHAR(255) NULL")
                cur.execute("ALTER TABLE equipment_defect_reports ADD COLUMN assigned_to VARCHAR(255) NULL")
                cur.execute("ALTER TABLE equipment_defect_reports ADD COLUMN priority VARCHAR(50) NOT NULL DEFAULT 'Mittel'")
        except Exception as mig_err:
            print("Fehler bei defect reports Migration:", mig_err)

        cur.execute("SELECT COUNT(*) FROM users WHERE username = 'admin'")
        if cur.fetchone()[0] == 0:
            default_admin_hash = hash_password("admin123")
            cur.execute(
                "INSERT INTO users (username, password_hash, role, is_first_login) VALUES (%s, %s, %s, 1)",
                ("admin", default_admin_hash, "admin")
            )
            log_audit_action("SYSTEM", "INITIALISIERUNG", "Standard-Admin 'admin' mit Kennwort 'admin123' angelegt.")

        conn.commit()
        cur.close()
        conn.close()
        
        # Startet den Abgleich direkt beim Hochfahren des Containers
        sync_personnel_to_editor_groups()
        notes_manager.init_notes_db()
        personnel_mgr.init_personnel_db()
    except Exception as e:
        print(f"init_db_extensions Fehler: {e}")
        print(f"Fehler bei DB-Erweiterung: {e}")

def init_db():
    max_retries = 10
    for i in range(max_retries):
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS groups_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL) ENGINE=InnoDB;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS persons (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    group_id INT,
                    name VARCHAR(255) NOT NULL,
                    FOREIGN KEY (group_id) REFERENCES groups_table(id) ON DELETE CASCADE
                ) ENGINE=InnoDB;
            """)
            cur.execute("CREATE TABLE IF NOT EXISTS vehicles (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL) ENGINE=InnoDB;")
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
                    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE,
                    FOREIGN KEY (person_id) REFERENCES persons(id) ON DELETE CASCADE
                ) ENGINE=InnoDB;
            """)
            conn.commit()
            cur.close()
            conn.close()
            init_db_extensions()
            break
        except Exception as e:
            time.sleep(5)

init_db()

def safe_decode(value):
    if isinstance(value, bytes): return value.decode('utf-8')
    return value

# --- API DATENMODELLE (PYDANTIC) ---
class PersonData(BaseModel): name: str
class VehicleData(BaseModel): 
    name: str
    radio_name: Optional[str] = ""
    status: Optional[int] = 2
    tuv_date: Optional[str] = None
    sp_date: Optional[str] = None
    milage: Optional[int] = 0
    next_service: Optional[str] = None

class EntryDto(BaseModel): 
    person_id: int; is_present: bool; note: Optional[str] = ""; 
    vehicle: Optional[str] = ""; signature: Optional[str] = None
class AttendanceUpload(BaseModel): 
    session_id: Optional[int] = None; date: str; group_id: int; category: str = "Übung"; 
    duration: float = 0.0; description: str; instructors: Optional[str] = ""; 
    leader_signature: Optional[str] = None; entries: List[EntryDto]
class GroupData(BaseModel): name: str

class LoginRequest(BaseModel):
    username: str
    password: str

class UserCreateRequest(BaseModel):
    username: str
    password: str
    role: str
    personnel_id: Optional[int] = None

class BroadcastCreateRequest(BaseModel):
    title: str
    content: str
    role_target: str = "all"
    is_mandatory: bool = False

# --- WEB SEITEN-ROUTEN ---
@app.get("/", response_class=FileResponse)
def get_login(request: Request):
    user = get_current_user(request)
    if user: return FileResponse("static/dashboard.html")
    return FileResponse("static/login.html")

@app.get("/login", response_class=FileResponse)
def get_login_explicit(request: Request):
    user = get_current_user(request)
    if user: return FileResponse("static/dashboard.html")
    return FileResponse("static/login.html")

@app.get("/dashboard", response_class=FileResponse)
def get_dash(request: Request):
    if not get_current_user(request):
        eq_barcode = request.query_params.get("eq_barcode")
        if eq_barcode:
            return RedirectResponse(url=f"/login?eq_barcode={eq_barcode}", status_code=302)
        return FileResponse("static/login.html")
    return FileResponse("static/dashboard.html")

@app.get("/editor", response_class=FileResponse)
def get_edit(request: Request):
    if not get_current_user(request): return FileResponse("static/login.html")
    return FileResponse("static/editor.html")

@app.get("/notizen", response_class=FileResponse)
def get_notes_page(request: Request):
    if not get_current_user(request): return FileResponse("static/login.html")
    return FileResponse("static/notizen.html")

@app.get("/personal", response_class=FileResponse)
def get_personnel_page(request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": return FileResponse("static/dashboard.html")
    # Führt die Synchronisation aus, sobald der Admin den Personalreiter öffnet
    sync_personnel_to_editor_groups()
    return FileResponse("static/personnel.html") 

@app.get("/favicon.ico", include_in_schema=False)
async def favicon(): return FileResponse("static/favicon.svg") if os.path.exists("static/favicon.svg") else Response(status_code=204)

# --- AUTHENTIFIZIERUNG UND LOGIN SPERREN ---
@app.post("/api/login")
def api_login(data: LoginRequest, response: Response):
    username_clean = data.username.strip()
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE username = %s", (username_clean,))
    user = cur.fetchone()
    
    if user:
        if user["lockout_until"] and datetime.now() < user["lockout_until"]:
            cur.close(); conn.close()
            remaining = (user["lockout_until"] - datetime.now()).seconds // 60 + 1
            log_audit_action("SYSTEM", "LOGIN_VERSUCH_GESPERRT", f"Anmeldeversuch auf gesperrtes Konto '{username_clean}'.")
            raise HTTPException(status_code=423, detail=f"Konto gesperrt. Bitte in {remaining} Min. versuchen.")
            
        if verify_password(user['password_hash'], data.password):
            cur.execute("UPDATE users SET failed_logins = 0, lockout_until = NULL WHERE id = %s", (user["id"],))
            conn.commit(); cur.close(); conn.close()
            
            token = create_session_token(user['username'], user['role'])
            response.set_cookie(key="session_token", value=token, httponly=True, max_age=30*24*60*60, samesite="lax")
            log_audit_action(user['username'], "LOGIN", "Erfolgreich eingeloggt.")
            return {"status": "success", "username": user['username'], "role": user['role'], "is_first_login": bool(user['is_first_login']), "redirect": "/dashboard"}
        else:
            failed = user["failed_logins"] + 1
            lockout = datetime.now() + timedelta(minutes=15) if failed >= 5 else None
            cur.execute("UPDATE users SET failed_logins = %s, lockout_until = %s WHERE id = %s", (failed, lockout, user["id"],))
            conn.commit(); cur.close(); conn.close()
            if failed >= 5:
                log_audit_action("SYSTEM", "KONTO_GESPERRT", f"Konto '{username_clean}' wegen zu vieler Fehllogins für 15 Min. gesperrt.")
                raise HTTPException(status_code=423, detail="Konto wegen zu vieler Fehllogins für 15 Min. gesperrt.")
            log_audit_action("SYSTEM", "LOGIN_FEHLVERSUCH", f"Falsches Passwort für Benutzer '{username_clean}' ({failed}/5).")
            raise HTTPException(status_code=401, detail=f"Passwort falsch! ({failed}/5)")
    else:
        cur.close(); conn.close()
        log_audit_action("SYSTEM", "LOGIN_BENUTZER_UNBEKANNT", f"Anmeldeversuch mit nicht existierendem Namen '{username_clean}'.")
        raise HTTPException(status_code=401, detail="Benutzername existiert nicht!")

@app.get("/api/auth/me")
def api_auth_me(request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT u.is_first_login, u.personnel_id, p.name as personnel_name 
        FROM users u 
        LEFT JOIN personnel p ON u.personnel_id = p.id 
        WHERE u.username = %s
    """, (user["username"],))
    db_user = cur.fetchone(); cur.close(); conn.close()
    
    is_first = bool(db_user["is_first_login"]) if db_user else False
    p_id = db_user["personnel_id"] if db_user else None
    p_name = db_user["personnel_name"] if db_user else None
    return {"username": user["username"], "role": user["role"], "is_first_login": is_first, "personnel_id": p_id, "personnel_name": p_name}

@app.post("/api/logout")
def api_logout(response: Response):
    response.delete_cookie("session_token", path="/")
    return {"status": "success"}

@app.put("/api/auth/change-password")
def user_change_self_password(data: dict, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    old_pw = data.get("old_password")
    new_pw = data.get("new_password")
    
    if not old_pw or not new_pw or len(new_pw.strip()) < 4:
        raise HTTPException(status_code=400, detail="Eingaben ungültig oder Passwort zu kurz!")
        
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT password_hash FROM users WHERE username = %s", (user["username"],))
    db_user = cur.fetchone()
    
    if not db_user or not verify_password(db_user['password_hash'], old_pw):
        cur.close(); conn.close()
        raise HTTPException(status_code=400, detail="Das aktuelle Passwort ist nicht korrekt!")
        
    new_hash = hash_password(new_pw.strip())
    cur.execute("UPDATE users SET password_hash = %s, is_first_login = 0 WHERE username = %s", (new_hash, user["username"]))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "PASSWORT_ÄNDERUNG", "Eigenes Passwort erfolgreich aktualisiert.")
    return {"status": "success"}

# --- ERWEITERTE BENUTZERSTEUERUNG (ADMINS) ---
@app.get("/api/users/list")
def list_users(request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, username, role, is_first_login, personnel_id FROM users ORDER BY username ASC")
    users = cur.fetchall(); cur.close(); conn.close()
    return users

@app.put("/api/users/{user_id}/reset-password")
def admin_reset_user_password(user_id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT username FROM users WHERE id = %s", (user_id,))
    target_user = cur.fetchone()
    if not target_user: cur.close(); conn.close(); raise HTTPException(status_code=404, detail="Benutzer nicht gefunden")
    
    p_hash = hash_password("admin123")
    cur.execute("UPDATE users SET password_hash = %s, is_first_login = 1 WHERE id = %s", (p_hash, user_id))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "PASSWORT_RESET", f"Passwort für '{target_user['username']}' auf 'admin123' zurückgesetzt.")
    return {"status": "success", "message": "Passwort auf 'admin123' zurückgesetzt. Erstanmeldung erforderlich."}

# --- DATENBANK SICHERUNG (EXPORT & IMPORT) ---
@app.get("/api/admin/backup/export")
def export_database_backup(request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Keine Berechtigung (Admin erforderlich)")

    tables_to_export = [
        "users", "personnel", "groups_table", "persons", "sessions", "attendance",
        "vehicles", "log_rides", "hvo_checks", "equipment", "inspections",
        "equipment_defect_reports", "notes", "archive_files", "youth_sessions",
        "youth_attendance", "settings", "audit_log", "apager_config", "apager_logs",
        "broadcasts", "schedules"
    ]

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    backup_tables = {}

    for table in tables_to_export:
        try:
            cur.execute(f"SELECT * FROM `{table}`")
            rows = cur.fetchall()
            for r in rows:
                for k, v in r.items():
                    if isinstance(v, (datetime, date)):
                        r[k] = str(v)
                    elif isinstance(v, bytes):
                        r[k] = v.decode('utf-8', errors='ignore')
            backup_tables[table] = rows
        except Exception as e:
            print(f"Export warning for table {table}: {e}")

    cur.close()
    conn.close()

    filename = f"dienstbuch_backup_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.json"
    backup_data = {
        "app_name": "Dienstbuch",
        "version": CURRENT_VERSION,
        "exported_at": datetime.now().isoformat(),
        "exported_by": user["username"],
        "tables": backup_tables
    }

    log_audit_action(user["username"], "DATENBANK-BACKUP", f"Datenbank-Sicherung '{filename}' erstellt.")

    json_str = json.dumps(backup_data, ensure_ascii=False, indent=2)
    return Response(
        content=json_str,
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

@app.post("/api/admin/backup/import")
async def import_database_backup(request: Request, file: UploadFile = File(...)):
    user = get_current_user(request)
    if not user or user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Keine Berechtigung (Admin erforderlich)")

    try:
        content = await file.read()
        backup_data = json.loads(content.decode('utf-8'))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Ungültiges JSON-Backup: {e}")

    if not isinstance(backup_data, dict) or "tables" not in backup_data:
        raise HTTPException(status_code=400, detail="Ungültiges Backup-Format (Schlüssel 'tables' fehlt).")

    tables = backup_data["tables"]
    conn = get_db_connection()
    cur = conn.cursor()

    imported_count = 0
    try:
        for table_name, rows in tables.items():
            if not rows or not isinstance(rows, list):
                continue
            
            for row in rows:
                cols = list(row.keys())
                placeholders = ", ".join(["%s"] * len(cols))
                col_names = ", ".join([f"`{c}`" for c in cols])
                updates = ", ".join([f"`{c}`=VALUES(`{c}`)" for c in cols])
                
                query = f"INSERT INTO `{table_name}` ({col_names}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
                vals = [row[c] for c in cols]
                try:
                    cur.execute(query, vals)
                    imported_count += 1
                except Exception as row_err:
                    print(f"Import row error in {table_name}: {row_err}")

        conn.commit()
    except Exception as e:
        conn.rollback()
        cur.close()
        conn.close()
        raise HTTPException(status_code=500, detail=f"Fehler beim Importieren: {e}")

    cur.close()
    conn.close()

    sync_personnel_to_editor_groups()
    log_audit_action(user["username"], "DATENBANK-IMPORT", f"Backup-Datei '{file.filename}' erfolgreich importiert ({imported_count} Datensätze).")

    return {"status": "success", "imported_rows": imported_count}

@app.post("/api/users/add")
def add_user(data: UserCreateRequest, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    try:
        p_hash = hash_password(data.password)
        cur.execute("INSERT INTO users (username, password_hash, role, is_first_login, personnel_id) VALUES (%s, %s, %s, 1, %s)", (data.username.strip(), p_hash, data.role, data.personnel_id or None))
        conn.commit()
        log_audit_action(user["username"], "NUTZER_ANLEGEN", f"Konto für '{data.username.strip()}' verknüpft mit Personal-ID {data.personnel_id} erstellt.")
    except mysql.connector.Error as err:
        if err.errno == 1062: raise HTTPException(status_code=400, detail="Benutzername existiert bereits!")
        raise HTTPException(status_code=500, detail=str(err))
    finally: cur.close(); conn.close()
    return {"status": "success"}

@app.put("/api/users/{user_id}/role")
def update_user_role(user_id: int, data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": raise HTTPException(status_code=403, detail="Keine Berechtigung")
    new_role = data.get("role")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("UPDATE users SET role = %s WHERE id = %s", (new_role, user_id))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@app.put("/api/users/{user_id}/personnel")
def update_user_personnel_relation(user_id: int, data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": 
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    new_pid = data.get("personnel_id")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("UPDATE users SET personnel_id = %s WHERE id = %s", (new_pid, user_id))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "VERKNÜPFUNG_ÄNDERN", f"User-ID {user_id} wurde mit Personal-ID {new_pid} verknüpft.")
    return {"status": "success"}

@app.put("/api/users/{user_id}/password")
def change_user_password(user_id: int, data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": raise HTTPException(status_code=403, detail="Keine Berechtigung")
    new_pw = data.get("password")
    p_hash = hash_password(new_pw.strip())
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("UPDATE users SET password_hash = %s, is_first_login = 1 WHERE id = %s", (p_hash, user_id))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@app.delete("/api/users/{user_id}")
def delete_user(user_id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

# --- FEUERWACHE STANDORT EINSTELLUNGEN ---
@app.get("/api/settings/station")
def get_station_settings():
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT station_name, lat, lng, zoom FROM station_settings LIMIT 1")
    row = cur.fetchone(); cur.close(); conn.close()
    if not row:
        return {"station_name": TOWN_NAME, "lat": 50.1109, "lng": 8.6821, "zoom": 14}
    return row

@app.put("/api/settings/station")
def update_station_settings(data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    
    station_name = data.get("station_name", "Feuerwehr").strip()
    try:
        lat = float(data.get("lat", 50.1109))
        lng = float(data.get("lng", 8.6821))
        zoom = int(data.get("zoom", 14))
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Ungültige Koordinaten")
        
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("SELECT id FROM station_settings LIMIT 1")
    row = cur.fetchone()
    if row:
        cur.execute("""
            UPDATE station_settings 
            SET station_name = %s, lat = %s, lng = %s, zoom = %s
            WHERE id = %s
        """, (station_name, lat, lng, zoom, row[0]))
    else:
        cur.execute("""
            INSERT INTO station_settings (station_name, lat, lng, zoom)
            VALUES (%s, %s, %s, %s)
        """, (station_name, lat, lng, zoom))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "WACHE_EINSTELLUNGEN", f"Standort-Einstellungen aktualisiert: {station_name}")
    return {"status": "success"}

# --- DATEI UPLOAD SYSTEM ---
@app.post("/api/upload")
async def upload_file(request: Request, file: UploadFile = File(...)):
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht angemeldet")
    
    ext = os.path.splitext(file.filename)[1]
    filename = f"{uuid.uuid4()}{ext}"
    filepath = os.path.join(UPLOAD_DIR, filename)
    
    with open(filepath, "wb") as buffer:
        content = await file.read()
        buffer.write(content)
        
    url = f"/static/uploads/{filename}"
    return {"url": url, "filename": file.filename}

# --- DOCUMENT ARCHIVE SYSTEM ---
@app.post("/api/archive/upload")
async def upload_archive_file(request: Request, file: UploadFile = File(...), is_public: bool = False):
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht angemeldet")
    
    ext = os.path.splitext(file.filename)[1]
    filename = f"{uuid.uuid4()}{ext}"
    filepath = os.path.join(UPLOAD_DIR, filename)
    
    with open(filepath, "wb") as buffer:
        content = await file.read()
        buffer.write(content)
        
    url = f"/static/uploads/{filename}"
    
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO archive_files (filename, url, uploaded_by, is_public) VALUES (%s, %s, %s, %s)",
        (file.filename, url, user["username"], 1 if is_public else 0)
    )
    conn.commit(); cur.close(); conn.close()
    
    log_audit_action(user["username"], "ARCHIV_DATEI_HOCHGELADEN", f"Datei '{file.filename}' hochgeladen (Öffentlich: {is_public}).")
    return {"status": "success", "url": url}

@app.get("/api/archive/files")
def get_archive_files(request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht angemeldet")
        
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    # Everyone only sees public files or files they uploaded themselves!
    cur.execute("""
        SELECT id, filename, url, uploaded_by, is_public, DATE_FORMAT(created_at, '%d.%m.%Y %H:%i') as created_at
        FROM archive_files
        WHERE is_public = 1 OR uploaded_by = %s
        ORDER BY id DESC
    """, (user["username"],))
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@app.delete("/api/archive/files/{file_id}")
def delete_archive_file(file_id: int, request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht angemeldet")
        
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT uploaded_by, filename, is_public FROM archive_files WHERE id = %s", (file_id,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close()
        raise HTTPException(status_code=404, detail="Datei nicht gefunden")
        
    is_owner = (row["uploaded_by"] == user["username"])
    is_privileged = (user["role"] in ("admin", "leitung"))
    
    if not is_owner and not (is_privileged and row["is_public"]):
        cur.close(); conn.close()
        raise HTTPException(status_code=403, detail="Keine Berechtigung zum Löschen dieser Datei")
        
    cur.execute("DELETE FROM archive_files WHERE id = %s", (file_id,))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "ARCHIV_DATEI_GELOESCHT", f"Datei '{row['filename']}' gelöscht.")
    return {"status": "success"}

# --- AUDIT-LOG ROUTE (REVISIONS-PROTOKOLL) ---
@app.get("/api/audit/logs")
def get_audit_logs(request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, DATE_FORMAT(created_at, '%d.%m.%Y %H:%i') as date_formatted, username, action, details FROM audit_log ORDER BY id DESC LIMIT 150")
    logs = cur.fetchall(); cur.close(); conn.close()
    return logs

# --- INTERNES BROADCAST SYSTEM ---
@app.get("/api/broadcasts/active")
def list_active_broadcasts(request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    
    query = """
        SELECT b.id, b.title, b.content, b.is_mandatory, b.username as autor,
               DATE_FORMAT(b.created_at, '%d.%m.%Y') as datum,
               0 as gelesen
        FROM system_broadcasts b
        LEFT JOIN broadcast_reads r ON b.id = r.broadcast_id AND r.username = %s
        WHERE (b.role_target = 'all' OR b.role_target = %s)
          AND r.id IS NULL
        ORDER BY b.created_at DESC LIMIT 10
    """
    cur.execute(query, (user["username"], user["role"]))
    broadcasts = cur.fetchall(); cur.close(); conn.close()
    return broadcasts

@app.post("/api/broadcasts")
def create_broadcast(data: BroadcastCreateRequest, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO system_broadcasts (username, title, content, role_target, is_mandatory) VALUES (%s, %s, %s, %s, %s)",
        (user["username"], data.title.strip(), data.content.strip(), data.role_target, 1 if data.is_mandatory else 0)
    )
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@app.post("/api/broadcasts/{id}/read")
def mark_broadcast_as_read(id: int, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT IGNORE INTO broadcast_reads (username, broadcast_id) VALUES (%s, %s)", (user["username"], id))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@app.delete("/api/broadcasts/{id}")
def delete_broadcast(id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": 
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM system_broadcasts WHERE id = %s", (id,))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "BROADCAST_LOESCHEN", f"Meldung ID {id} wurde unwiderruflich entfernt.")
    return {"status": "success"}

# --- FAHRZEUG POOL APIS ---
@app.get("/api/vehicles")
def get_vehicles(request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, name, radio_name, status, tuv_date, sp_date, milage, next_service FROM vehicles ORDER BY name")
    r = cur.fetchall(); c.close()
    for v in r:
        if v['tuv_date']: v['tuv_date'] = str(v['tuv_date'])
        if v['sp_date']: v['sp_date'] = str(v['sp_date'])
        if v['next_service']: v['next_service'] = str(v['next_service'])
    return r

@app.post("/api/vehicles")
def create_vehicle(v: VehicleData, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("""
        INSERT INTO vehicles (name, radio_name, status, tuv_date, sp_date, milage, next_service) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (v.name, v.radio_name, v.status or 2, v.tuv_date or None, v.sp_date or None, v.milage or 0, v.next_service or None))
    c.commit(); c.close()
    log_audit_action(user["username"], "FAHRZEUG_ANLEGEN", f"Fahrzeug '{v.name}' in Flotte aufgenommen.")
    return {"status": "created"}

@app.put("/api/vehicles/{id}")
def update_vehicle(id: int, v: VehicleData, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("""
        UPDATE vehicles 
        SET name=%s, radio_name=%s, status=%s, tuv_date=%s, sp_date=%s, milage=%s, next_service=%s 
        WHERE id=%s
    """, (v.name, v.radio_name, v.status or 2, v.tuv_date or None, v.sp_date or None, v.milage or 0, v.next_service or None, id))
    c.commit(); c.close()
    return {"status": "updated"}

@app.put("/api/vehicles/{id}/status")
def update_vehicle_status(id: int, data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt")
    new_status = data.get("status", 2)
    c = get_db_connection(); cur = c.cursor()
    cur.execute("UPDATE vehicles SET status=%s WHERE id=%s", (new_status, id))
    c.commit(); c.close()
    log_audit_action(user["username"], "FUNKSTATUS", f"Fahrzeug ID {id} auf BOS Status {new_status} gesetzt.")
    return {"status": "status updated"}

@app.delete("/api/vehicles/{id}")
def delete_vehicle(id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM vehicles WHERE id=%s", (id,))
    c.commit(); c.close()
    return {"status": "deleted"}

# --- GRUPPEN & DIENST-STRUKTUREN ---
@app.get("/groups")
def get_groups(request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    c=get_db_connection(); cur=c.cursor(dictionary=True)
    cur.execute("SELECT * FROM groups_table ORDER BY name")
    r=cur.fetchall(); c.close(); return r

@app.put("/groups/{id}")
def update_group(id: int, g: GroupData, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("UPDATE groups_table SET name=%s WHERE id=%s", (g.name, id))
    c.commit(); cur.close(); c.close()
    return {"status": "updated"}

@app.post("/groups")
def create_group(g: GroupData, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt")
    c=get_db_connection(); cur=c.cursor()
    cur.execute("INSERT INTO groups_table (name) VALUES (%s)", (g.name,))
    c.commit(); c.close(); return {"status": "created"}

@app.delete("/groups/{id}")
def delete_group(id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM groups_table WHERE id=%s", (id,))
    c.commit(); c.close(); return {"status": "deleted"}

@app.get("/groups/{id}/sessions")
def get_sessions(id: int, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, date, category, description, duration, leader_signature FROM sessions WHERE group_id=%s ORDER BY date DESC, id DESC", (id,))
    r = cur.fetchall(); c.close()
    for x in r: 
        x['date'] = str(x['date'])
        x['is_signed'] = bool(x['leader_signature'] and len(str(x['leader_signature'])) > 100)
        if 'leader_signature' in x: del x['leader_signature']
    return r

@app.get("/groups/{id}/stats")
def get_stats(id: int, year: int, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT COUNT(*) as total FROM sessions WHERE group_id=%s AND YEAR(date)=%s", (id, year))
    max_s = cur.fetchone()['total'] or 0
    sql = """SELECT p.id as person_id, p.name, 
             SUM(CASE WHEN a.is_present=1 AND s.id IS NOT NULL THEN 1 ELSE 0 END) as present_count, 
             SUM(CASE WHEN a.is_present=1 AND s.id IS NOT NULL THEN s.duration ELSE 0 END) as total_hours 
             FROM persons p LEFT JOIN attendance a ON p.id=a.person_id 
             LEFT JOIN sessions s ON a.session_id=s.id AND YEAR(s.date)=%s AND s.group_id=%s 
             WHERE p.group_id=%s GROUP BY p.id, p.name ORDER BY total_hours DESC"""
    cur.execute(sql, (year, id, id)); p = cur.fetchall(); c.close()
    return {"persons": p, "total_sessions": max_s}

@app.get("/groups/{group_id}/attendance")
async def get_attendance(group_id: int, request: Request, session_id: Optional[int] = None):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    try:
        session_data = {"session_id": session_id, "description": "", "duration": 2.0, "category": "Übung", "date": datetime.now().strftime("%Y-%m-%d"), "leader_signature": None, "instructors": ""}
        if session_id:
            cur.execute("SELECT id as session_id, description, duration, date, category, leader_signature, instructors FROM sessions WHERE id = %s", (session_id,))
            row = cur.fetchone()
            if row:
                session_data = row
                session_data['date'] = str(session_data['date'])
                if session_data.get('leader_signature'): session_data['leader_signature'] = safe_decode(session_data['leader_signature'])

        cur.execute("SELECT setting_value FROM settings WHERE setting_key = 'int_g26'")
        g26_row = cur.fetchone()
        g26_allowed_months = g26_row['setting_value'] if g26_row else 36

        query = """SELECT p.id, p.name, COALESCE(a.is_present, 0) as is_present, COALESCE(a.note, '') as note, 
                          COALESCE(a.vehicle, '') as vehicle, a.signature, pl.id AS personnel_id, 
                          CASE WHEN pl.profile_picture IS NOT NULL AND LENGTH(pl.profile_picture) > 0 THEN 1 ELSE 0 END AS has_picture,
                          pl.g26_3_date, pl.is_agt
                   FROM persons p 
                   LEFT JOIN attendance a ON p.id = a.person_id AND a.session_id = %s 
                   LEFT JOIN personnel pl ON p.name = pl.name 
                   WHERE p.group_id = %s ORDER BY p.name"""
        cur.execute(query, (session_id, group_id))
        persons = cur.fetchall()
        
        for p in persons:
            p['signature'] = safe_decode(p['signature'])
            p['is_present'] = bool(p['is_present'])
            p['has_picture'] = bool(p.get('has_picture', 0))
            
            p['g26_expired'] = False
            if p.get('is_agt') and p.get('g26_3_date'):
                g26_date = p['g26_3_date']
                if g26_date:
                    diff_days = (datetime.now().date() - g26_date).days
                    if diff_days > (g26_allowed_months * 30.44):
                        p['g26_expired'] = True
            if p.get('g26_3_date'):
                p['g26_3_date'] = str(p['g26_3_date'])

        return {**session_data, "persons": persons}
    finally: cur.close(); conn.close()

@app.post("/attendance")
async def save_attendance(payload: AttendanceUpload, request: Request):
    user = get_current_user(request)
    if not user or user["role"] in ("mannschaft", "geratewart"): raise HTTPException(status_code=403, detail="Schreibgeschützt")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    try:
        if payload.session_id:
            cur.execute("""UPDATE sessions SET date=%s, description=%s, duration=%s, category=%s, instructors=%s, leader_signature=%s WHERE id=%s""",(payload.date, payload.description, payload.duration, payload.category, payload.instructors, payload.leader_signature, payload.session_id))
            session_id = payload.session_id
        else:
            cur.execute("""INSERT INTO sessions (group_id, date, description, duration, category, instructors, leader_signature) VALUES (%s, %s, %s, %s, %s, %s, %s)""",(payload.group_id, payload.date, payload.description, payload.duration, payload.category, payload.instructors, payload.leader_signature))
            session_id = cur.lastrowid
        cur.execute("DELETE FROM attendance WHERE session_id = %s", (session_id,))
        for entry in payload.entries:
            cur.execute("INSERT INTO attendance (session_id, person_id, is_present, note, vehicle, signature) VALUES (%s, %s, %s, %s, %s, %s)",(session_id, entry.person_id, 1 if entry.is_present else 0, entry.note or "", entry.vehicle or "", entry.signature))
        conn.commit(); return {"status": "success", "session_id": session_id}
    except Exception as e: conn.rollback(); raise HTTPException(status_code=500, detail=str(e))
    finally: cur.close(); conn.close()

@app.get("/groups/{group_id}/topics")
def get_topics(group_id: int, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("SELECT DISTINCT description FROM sessions WHERE group_id=%s AND description IS NOT NULL LIMIT 50", (group_id,))
    r = [row[0] for row in cur.fetchall()]; c.close(); return r

@app.get("/groups/{group_id}/instructors")
def get_instructors(group_id: int, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("SELECT DISTINCT instructors FROM sessions WHERE group_id=%s AND instructors IS NOT NULL LIMIT 50", (group_id,))
    r = [row[0] for row in cur.fetchall()]; c.close(); return r

@app.post("/sessions/{session_id}/leader_signature")
async def save_leader_sig(session_id: int, data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] in ("mannschaft", "geratewart"): raise HTTPException(status_code=403, detail="Schreibgeschützt")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("UPDATE sessions SET leader_signature=%s WHERE id=%s", (data.get("signature"), session_id))
    c.commit(); c.close(); return {"status": "success"}

# --- EINTRÄGE / DIENSTE PERMANENT LÖSCHEN ---
@app.delete("/sessions/{session_id}")
def delete_session(session_id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] in ("mannschaft", "geratewart"): 
        raise HTTPException(status_code=403, detail="Schreibgeschützt")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM sessions WHERE id = %s", (session_id,))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "EINTRAG_LOESCHEN", f"Diensteintrag ID {session_id} wurde unwiderruflich gelöscht.")
    return {"status": "success"}

# --- BERICHTE & JAHRESBERICHTE SYSTEM ---
@app.get("/sessions/{session_id}/report", response_class=HTMLResponse)
def single_report(session_id: int, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT s.*, g.name as gname FROM sessions s JOIN groups_table g ON s.group_id = g.id WHERE s.id=%s", (session_id,))
    s = cur.fetchone()
    if s and s['leader_signature']: s['leader_signature'] = safe_decode(s['leader_signature'])
    cur.execute("SELECT p.name, a.is_present, a.note, a.vehicle, a.signature FROM attendance a JOIN persons p ON a.person_id = p.id WHERE a.session_id=%s ORDER BY p.name", (session_id,))
    persons = cur.fetchall(); c.close()
    for p in persons: p['signature'] = safe_decode(p['signature'])
    return f"<html><head><meta charset='UTF-8'><style>{reports.get_report_styles()}</style></head><body>{reports.generate_single_report(s, persons, TOWN_NAME)}</body></html>"

@app.get("/groups/{group_id}/print_view", response_class=HTMLResponse)
def year_report(group_id: int, year: int, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT name FROM groups_table WHERE id=%s", (group_id,))
    gname_res = cur.fetchone(); gname = gname_res['name'] if gname_res else "Unbekannt"
    cur.execute("SELECT COUNT(*) as total FROM sessions WHERE group_id=%s AND YEAR(date)=%s", (group_id, year))
    max_s = cur.fetchone()['total'] or 0
    cur.execute("SELECT s.*, g.name as gname FROM sessions s JOIN groups_table g ON s.group_id = g.id WHERE s.group_id=%s AND YEAR(s.date)=%s ORDER BY s.date ASC, s.id ASC", (group_id, year))
    sessions_list = cur.fetchall()
    html_body = ""; p_stats = {}; cat_sums = {"Übung": 0.0, "Einsatz": 0.0, "Sonstiges": 0.0}
    for s in sessions_list:
        if s['leader_signature']: s['leader_signature'] = safe_decode(s['leader_signature'])
        cur.execute("SELECT p.name, a.is_present, a.note, a.vehicle, a.signature FROM attendance a JOIN persons p ON a.person_id = p.id WHERE a.session_id=%s ORDER BY p.name", (s['id'],))
        persons = cur.fetchall()
        for p in persons: p['signature'] = safe_decode(p['signature'])
        html_body += reports.generate_single_report(s, persons, TOWN_NAME)
        cat = s['category'] if s['category'] in cat_sums else "Sonstiges"
        cat_sums[cat] += float(s['duration'])
        for p in persons:
            if p['name'] not in p_stats: p_stats[p['name']] = {"Übung": 0.0, "Einsatz": 0.0, "Sonstiges": 0.0, "total_h": 0.0, "p": 0}
            if p['is_present']: p_stats[p['name']]["p"] += 1; p_stats[p['name']][cat] += float(s['duration']); p_stats[p['name']]["total_h"] += float(s['duration'])
    for n in p_stats: p_stats[n]['q'] = round((p_stats[n]['p'] / max_s) * 100) if max_s > 0 else 0
    html_body += reports.generate_year_report(gname, year, p_stats, cat_sums, TOWN_NAME)
    c.close()
    return f"<html><head><meta charset='UTF-8'><style>{reports.get_report_styles()}</style></head><body>{html_body}</body></html>"

# --- GLOBALER NUTZERSTUNDEN-ABGLEICH ---
@app.get("/api/users/me/stats")
def get_my_global_fire_stats(year: int, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT p.name, u.personnel_id FROM users u LEFT JOIN personnel p ON u.personnel_id = p.id WHERE u.username = %s", (user["username"],))
    res = cur.fetchone()
    
    klarnat_name = None
    if res and res["name"]:
        klarnat_name = res["name"]
    else:
        # Fallback: Suche nach Namen, der dem Benutzernamen ähnelt
        cur.execute("SELECT id, name FROM personnel WHERE LOWER(name) LIKE %s", (f"%{user['username'].lower()}%",))
        fallback = cur.fetchone()
        if fallback:
            # Auto-bind
            cur.execute("UPDATE users SET personnel_id = %s WHERE username = %s", (fallback["id"], user["username"]))
            conn.commit()
            klarnat_name = fallback["name"]
            
    if not klarnat_name:
        cur.close(); conn.close()
        return {"hours": 0, "count": 0, "unlinked": True}
        
    query = """
        SELECT COALESCE(SUM(s.duration), 0) as total_hours, COUNT(DISTINCT s.id) as present_count
        FROM attendance a JOIN sessions s ON a.session_id = s.id JOIN persons p ON a.person_id = p.id
        WHERE p.name = %s AND YEAR(s.date) = %s AND a.is_present = 1
    """
    cur.execute(query, (klarnat_name, year))
    stats = cur.fetchone(); cur.close(); conn.close()
    return {"hours": float(stats["total_hours"]) if stats else 0.0, "count": stats["present_count"] if stats else 0, "unlinked": False, "name": klarnat_name}

@app.get("/api/users/me/sessions")
def get_my_sessions(year: int, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT p.name FROM users u LEFT JOIN personnel p ON u.personnel_id = p.id WHERE u.username = %s", (user["username"],))
    res = cur.fetchone()
    if not res or not res["name"]:
        cur.close(); conn.close()
        return []
    
    query = """
        SELECT s.date, s.category, s.description, s.duration
        FROM attendance a 
        JOIN sessions s ON a.session_id = s.id 
        JOIN persons p ON a.person_id = p.id
        WHERE p.name = %s AND YEAR(s.date) = %s AND a.is_present = 1
        ORDER BY s.date DESC
    """
    cur.execute(query, (res["name"], year))
    sessions = cur.fetchall(); cur.close(); conn.close()
    for s in sessions:
        s["date"] = str(s["date"])
    return sessions

@app.put("/api/users/me/bind-personnel")
def bind_self_personnel(data: dict, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    pid = data.get("personnel_id")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("UPDATE users SET personnel_id = %s WHERE username = %s", (pid, user["username"]))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

# --- ALARMIERUNG (APAGER PRO WEBHOOK & CONFIG) ---
import uuid

@app.get("/api/apager/config")
def get_apager_config(request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM apager_config LIMIT 1")
    row = cur.fetchone()
    if not row:
        api_key = uuid.uuid4().hex
        cur.execute("INSERT INTO apager_config (api_key) VALUES (%s)", (api_key,))
        conn.commit()
        cur.close(); conn.close()
        return {"api_key": api_key, "active": True}
    cur.close(); conn.close()
    return {"api_key": row['api_key'], "active": bool(row['active'])}

@app.post("/api/apager/config")
def regenerate_apager_key(request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": raise HTTPException(status_code=403, detail="Keine Berechtigung")
    new_key = uuid.uuid4().hex
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM apager_config")
    cur.execute("INSERT INTO apager_config (api_key) VALUES (%s)", (new_key,))
    conn.commit(); cur.close(); conn.close()
    return {"api_key": new_key, "active": True}

@app.get("/api/apager/logs")
def get_apager_logs(request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM apager_logs ORDER BY created_at DESC LIMIT 50")
    r = cur.fetchall(); cur.close(); conn.close()
    import datetime
    now = datetime.datetime.now()
    for log in r:
        if isinstance(log.get('created_at'), datetime.datetime):
            diff_sec = (now - log['created_at']).total_seconds()
            log['diff_min'] = diff_sec / 60.0
        else:
            log['diff_min'] = 999.0
        log['created_at'] = str(log['created_at'])
    return r

@app.post("/api/apager/webhook")
async def apager_webhook(api_key: str, req: Request):
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id FROM apager_config WHERE api_key = %s AND active = 1", (api_key,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close()
        raise HTTPException(status_code=401, detail="Ungültiger API-Key.")
    
    try:
        data = await req.json()
    except:
        cur.close(); conn.close()
        raise HTTPException(status_code=400, detail="Ungültiges JSON.")
        
    stichwort = data.get("stichwort", "Alarmierung")
    adresse = data.get("adresse", "Unbekannter Ort")
    meldung = data.get("meldung", "Keine weiteren Details.")
    
    cur.execute("""
        INSERT INTO apager_logs (stichwort, adresse, meldung)
        VALUES (%s, %s, %s)
    """, (stichwort, adresse, meldung))
    
    # --- AUTO-EINSATZERÖFFNUNG & VORBEFÜLLUNG ---
    today = datetime.now().date().isoformat()
    now_time = datetime.now().strftime("%H:%M")
    cur.execute("""
        INSERT INTO missions (date, time, stichwort, adresse, meldung, description, duration, status)
        VALUES (%s, %s, %s, %s, %s, '', 2.0, 'Entwurf')
    """, (today, now_time, stichwort, adresse, meldung))
    
    conn.commit(); cur.close(); conn.close()
    return {"status": "success", "message": "Alarm erfolgreich verarbeitet und Einsatz angelegt."}

# --- APAGER FEEDBACKS ENDPOINTS ---
@app.get("/api/apager/feedbacks")
def get_apager_feedbacks(request: Request):
    check_user = get_current_user(request)
    if not check_user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT af.*, p.name, p.is_agt, p.is_maschinist, p.is_gf, p.is_tf
        FROM apager_feedbacks af
        JOIN personnel p ON af.personnel_id = p.id
        ORDER BY af.updated_at DESC LIMIT 50
    """)
    res = cur.fetchall(); cur.close(); conn.close()
    for row in res:
        row['updated_at'] = str(row['updated_at'])
    return res

@app.post("/api/apager/feedbacks")
def submit_apager_feedback(status: str, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT personnel_id FROM users WHERE username = %s", (user["username"],))
    row = cur.fetchone()
    if not row or not row["personnel_id"]:
        cur.close(); conn.close()
        raise HTTPException(status_code=400, detail="Kein Kamerad mit diesem Login verknüpft.")
    
    personnel_id = row["personnel_id"]
    
    # Letzten Alarm holen
    cur.execute("SELECT id FROM apager_logs ORDER BY created_at DESC LIMIT 1")
    alarm = cur.fetchone()
    alarm_id = alarm["id"] if alarm else None
    
    cur.execute("""
        INSERT INTO apager_feedbacks (apager_log_id, personnel_id, status)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE status = %s
    """, (alarm_id, personnel_id, status, status))
    
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

# --- TEST ALARM ---
@app.post("/api/apager/test-alarm")
async def send_test_alarm(data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "geratewart"):
        raise HTTPException(status_code=403, detail="Nur Admins/Leitung/Gerätewarte können Test-Alarme senden.")
    stichwort = "[TEST] " + (data.get("stichwort") or "Probealarm")
    adresse = data.get("adresse") or "Übungsgelände"
    meldung = data.get("meldung") or "Dies ist ein Test-Alarm – keine echte Gefahr!"
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO apager_logs (stichwort, adresse, meldung) VALUES (%s, %s, %s)",
        (stichwort, adresse, meldung)
    )
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "TEST_ALARM", f"Test-Alarm '{stichwort}' bei {adresse} ausgelöst.")
    return {"status": "success", "message": "Test-Alarm wurde im Protokoll erfasst."}

# --- MÄNGELMELDER (GERÄTEWART - ERWEITERT) ---
@app.post("/api/material/defect-reports")
def create_defect_report(data: dict, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    eq_id = data.get("equipment_id")
    desc = data.get("description", "").strip()
    severity = data.get("severity", "Mittel")
    image_url = data.get("image_url")
    assigned_to = data.get("assigned_to")
    priority = data.get("priority", "Mittel")
    if not eq_id or not desc:
        raise HTTPException(status_code=400, detail="Gerät und Beschreibung erforderlich.")
    reporter = data.get("reporter_name") or user["username"]
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO equipment_defect_reports (equipment_id, reporter_name, description, severity, image_url, assigned_to, priority) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (eq_id, reporter, desc, severity, image_url, assigned_to, priority)
    )
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "MANGEL_MELDEN", f"Mangel für Gerät-ID {eq_id} gemeldet: {desc[:60]}")
    return {"status": "success"}

@app.get("/api/material/defect-reports")
def get_defect_reports(request: Request, status: str = "Offen"):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    if status == "alle":
        cur.execute("""
            SELECT dr.*, e.name as equipment_name, e.barcode
            FROM equipment_defect_reports dr
            LEFT JOIN equipment e ON dr.equipment_id = e.id
            ORDER BY dr.created_at DESC LIMIT 100
        """)
    else:
        cur.execute("""
            SELECT dr.*, e.name as equipment_name, e.barcode
            FROM equipment_defect_reports dr
            LEFT JOIN equipment e ON dr.equipment_id = e.id
            WHERE dr.status = %s
            ORDER BY dr.created_at DESC LIMIT 100
        """, (status,))
    rows = cur.fetchall(); cur.close(); conn.close()
    for r in rows:
        for k in ["created_at", "resolved_at"]:
            if r.get(k): r[k] = str(r[k])
    return rows

@app.put("/api/material/defect-reports/{report_id}")
def resolve_defect_report(report_id: int, data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "geratewart"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    new_status = data.get("status", "Erledigt")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute(
        "UPDATE equipment_defect_reports SET status = %s, resolved_by = %s, resolved_at = NOW() WHERE id = %s",
        (new_status, user["username"], report_id)
    )
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "MANGEL_ERLEDIGT", f"Mangel-Meldung ID {report_id} als '{new_status}' markiert.")
    return {"status": "success"}

@app.delete("/api/material/defect-reports/{report_id}")
def delete_defect_report(report_id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "geratewart"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM equipment_defect_reports WHERE id = %s", (report_id,))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "MANGEL_GELOESCHT", f"Mangel-Meldung ID {report_id} gelöscht.")
    return {"status": "success"}

# --- DRONE MAP IMAGES ---
@app.get("/api/material/drone-images")
def get_drone_images(request: Request):
    check_auth = get_current_user(request)
    if not check_auth: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM drone_images ORDER BY id DESC")
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@app.post("/api/material/drone-images")
def add_drone_image(data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "geratewart"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    url = data.get("url")
    if not url: raise HTTPException(status_code=400, detail="URL erforderlich")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO drone_images (url) VALUES (%s)", (url,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@app.delete("/api/material/drone-images/{img_id}")
def delete_drone_image(img_id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "geratewart"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM drone_images WHERE id = %s", (img_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

# --- JUGENDFEUERWERKE & VEREIN ---
@app.get("/api/jugend/members")
def get_youth_members(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM youth_members ORDER BY name ASC")
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@app.post("/api/jugend/members")
def add_youth_member(data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "jugendwarte"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    name = data.get("name")
    parent = data.get("parent_contact", "")
    badges = data.get("badges", "")
    if not name: raise HTTPException(status_code=400, detail="Name erforderlich")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO youth_members (name, parent_contact, badges) VALUES (%s, %s, %s)", (name, parent, badges))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@app.delete("/api/jugend/members/{m_id}")
def delete_youth_member(m_id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "jugendwarte"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM youth_members WHERE id = %s", (m_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

# --- JUGEND-DIENSTBERICHTE ---
@app.get("/api/jugend/sessions")
def get_youth_sessions(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM youth_sessions ORDER BY date DESC, id DESC")
    sessions = cur.fetchall()
    for s in sessions:
        if isinstance(s["date"], date):
            s["date"] = str(s["date"])
        cur.execute("""
            SELECT ya.member_id, ya.is_present, ym.name
            FROM youth_attendance ya
            JOIN youth_members ym ON ya.member_id = ym.id
            WHERE ya.session_id = %s
        """, (s["id"],))
        s["attendance"] = cur.fetchall()
    cur.close(); conn.close()
    return sessions

@app.post("/api/jugend/sessions")
def add_youth_session(data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "jugendwarte"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    sess_date = data.get("date")
    topic = data.get("topic")
    duration = float(data.get("duration", 2.0))
    instructors = data.get("instructors", "")
    description = data.get("description", "")
    attendance = data.get("attendance", {})
    if not sess_date or not topic:
        raise HTTPException(status_code=400, detail="Datum und Thema erforderlich")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO youth_sessions (date, topic, duration, instructors, description)
        VALUES (%s, %s, %s, %s, %s)
    """, (sess_date, topic, duration, instructors, description))
    session_id = cur.lastrowid
    cur.execute("SELECT id FROM youth_members")
    member_ids = [row[0] for row in cur.fetchall()]
    for m_id in member_ids:
        is_pres = attendance.get(str(m_id)) or attendance.get(m_id) or False
        cur.execute("""
            INSERT INTO youth_attendance (session_id, member_id, is_present)
            VALUES (%s, %s, %s)
        """, (session_id, m_id, 1 if is_pres else 0))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success", "session_id": session_id}

@app.delete("/api/jugend/sessions/{s_id}")
def delete_youth_session(s_id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "jugendwarte"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM youth_sessions WHERE id = %s", (s_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@app.get("/api/verein/inventory")
def get_club_inventory(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM club_inventory ORDER BY item_name ASC")
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@app.post("/api/verein/inventory")
def add_club_inventory(data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    item = data.get("item_name")
    qty = int(data.get("quantity", 1))
    status = data.get("status", "OK")
    if not item: raise HTTPException(status_code=400, detail="Bezeichnung erforderlich")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO club_inventory (item_name, quantity, status) VALUES (%s, %s, %s)", (item, qty, status))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@app.delete("/api/verein/inventory/{i_id}")
def delete_club_inventory(i_id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM club_inventory WHERE id = %s", (i_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@app.get("/api/verein/donations")
def get_donations(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT *, DATE_FORMAT(date, '%d.%m.%Y') as formatted_date FROM club_donations ORDER BY date DESC")
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@app.post("/api/verein/donations")
def add_donation(data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    donor = data.get("donor")
    amount = float(data.get("amount", 0))
    date_val = data.get("date")
    if not donor or not date_val: raise HTTPException(status_code=400, detail="Spender und Datum erforderlich")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO club_donations (donor, amount, date) VALUES (%s, %s, %s)", (donor, amount, date_val))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

# --- FIRST RESPONDER / HvO ---
@app.get("/api/hvo/protocols")
def get_hvo_protocols(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT *, DATE_FORMAT(date, '%d.%m.%Y') as formatted_date FROM hvo_protocols ORDER BY date DESC LIMIT 100")
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@app.post("/api/hvo/protocols")
def add_hvo_protocol(data: dict, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401, detail="Nicht angemeldet")
    date_val = data.get("date")
    symptoms = data.get("symptoms", "")
    therapy = data.get("therapy", "")
    handover = data.get("handover", "")
    if not date_val: raise HTTPException(status_code=400, detail="Datum erforderlich")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO hvo_protocols (date, symptoms, therapy, handover) VALUES (%s, %s, %s, %s)", (date_val, symptoms, therapy, handover))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@app.get("/api/hvo/checks")
def get_hvo_checks(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT *, DATE_FORMAT(checked_at, '%d.%m.%Y') as formatted_date FROM hvo_equipment_checks ORDER BY checked_at DESC")
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@app.post("/api/hvo/checks")
def add_hvo_check(data: dict, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    dev = data.get("device_name")
    status = data.get("status", "OK")
    if not dev: raise HTTPException(status_code=400, detail="Gerätename erforderlich")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO hvo_equipment_checks (device_name, checked_at, status, checked_by) VALUES (%s, NOW(), %s, %s)", (dev, status, user["username"]))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

# --- AI DRAFT SUMMARY GENERATOR ---
@app.post("/api/missions/ai-draft")
def get_ai_draft(data: dict, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401, detail="Nicht angemeldet")
    stichwort = data.get("stichwort", "Brandeinsatz")
    adresse = data.get("adresse", "Hauptstraße 12")
    meldung = data.get("meldung", "Rauchentwicklung")
    
    # Generiere einen ansprechenden Entwurf
    draft = f"Am Einsatzort ({adresse}) wurde nach Erkundung der Lage die Meldung '{meldung}' ({stichwort}) bestätigt. Die Mannschaft ging unter schwerem Atemschutz vor. Der Brand konnte rasch unter Kontrolle gebracht und gelöscht werden. Anschließend Belüftungsmaßnahmen durchgeführt. Übergabe an Eigentümer."
    return {"draft": draft}

# --- ICAL / central calendar CENTRAL EXPORT ---
@app.get("/api/calendar/export.ics", response_class=Response)
def export_calendar_ical():
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM schedules")
    schedules = cur.fetchall(); cur.close(); conn.close()
    
    import datetime
    ics = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//FF Dienstbuch//Calendar Export//DE\r\n"
    for s in schedules:
        d_val = s["date"] # date object
        t_val = s["time"] # "HH:MM" format
        try:
            h, m = map(int, t_val.split(":"))
            dt = datetime.datetime(d_val.year, d_val.month, d_val.day, h, m)
        except:
            dt = datetime.datetime(d_val.year, d_val.month, d_val.day, 19, 0)
        
        dt_str = dt.strftime("%Y%m%dT%H%M%S")
        dt_end_str = (dt + datetime.timedelta(hours=2)).strftime("%Y%m%dT%H%M%S")
        
        ics += "BEGIN:VEVENT\r\n"
        ics += f"UID:SCH{s['id']}@feuerwehr-dienstbuch.de\r\n"
        ics += f"DTSTAMP:{dt_str}\r\n"
        ics += f"DTSTART:{dt_str}\r\n"
        ics += f"DTEND:{dt_end_str}\r\n"
        ics += f"SUMMARY:{s['title']}\r\n"
        ics += f"DESCRIPTION:{s['description'] or ''} ({s['type']})\r\n"
        ics += "END:VEVENT\r\n"
    ics += "END:VCALENDAR\r\n"
    
    return Response(content=ics, media_type="text/calendar", headers={"Content-Disposition": "attachment; filename=feuerwehr_dienstplan.ics"})

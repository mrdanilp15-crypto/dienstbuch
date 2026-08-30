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
from typing import List, Optional, Union
from datetime import datetime, timedelta, date
import uuid
import shutil

from routers import reports
from routers import notes_manager
from routers import personnel_mgr
from routers import mission_mgr
from routers import material_mgr
from routers import ws_mgr
from routers import auth_mgr
from routers import users_mgr
from routers import apager_api
from routers import vehicles_api
from routers import admin_api
from routers import groups_api
from routers import material_api
from routers import jugend_api
from routers import verein_api
from routers import hvo_api
from routers import missions_api
from routers import calendar_api
from routers import push_api

# --- SYSTEM-KONFIGURATION ---
CURRENT_VERSION = "2.50"
DB_PASSWORD = os.getenv("DB_PASSWORD")
TOWN_NAME = os.getenv("TOWN_NAME", "Deine Feuerwehr")
UPDATE_BASE_URL = os.getenv("UPDATE_BASE_URL", "https://raw.githubusercontent.com/mrdanilp15-crypto/dienstbuch/main/")
SECRET_KEY = os.getenv("SECRET_KEY", "feuerwehr-dienstbuch-geheimschluessel-112")

from fastapi.middleware.gzip import GZipMiddleware

app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size=1000)

@app.middleware("http")
async def add_cache_control_headers(request: Request, call_next):
    response = await call_next(request)
    if request.url.path.startswith("/static/"):
        response.headers["Cache-Control"] = "public, max-age=86400, stale-while-revalidate=604800"
    return response

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
app.include_router(ws_mgr.router)
app.include_router(auth_mgr.router)
app.include_router(users_mgr.router)
app.include_router(apager_api.router)
app.include_router(vehicles_api.router)
app.include_router(admin_api.router)
app.include_router(groups_api.router)
app.include_router(material_api.router)
app.include_router(jugend_api.router)
app.include_router(verein_api.router)
app.include_router(hvo_api.router)
app.include_router(missions_api.router)
app.include_router(calendar_api.router)
app.include_router(push_api.router)

# --- DATENBANK VERBINDUNGSUNTERBAU (MYSQL) ---
from database import get_db_connection

# --- REVISIONS-LOGBUCH HELFER ---
from core.utils import log_audit_action, hash_password, verify_password, create_session_token, get_current_user


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
                zoom INT DEFAULT 14,
                ticker_text TEXT NULL
            ) ENGINE=InnoDB;
        """)
        try:
            cur.execute("SHOW COLUMNS FROM station_settings LIKE 'ticker_text'")
            if not cur.fetchone():
                cur.execute("ALTER TABLE station_settings ADD COLUMN ticker_text TEXT NULL")
        except Exception as alter_err:
            print("Konnte ticker_text Spalte nicht migrieren:", alter_err)

        cur.execute("SELECT COUNT(*) FROM station_settings")
        if cur.fetchone()[0] == 0:
            cur.execute("INSERT INTO station_settings (station_name, lat, lng, zoom, ticker_text) VALUES (%s, 50.1109, 8.6821, 14, %s)", (TOWN_NAME, "Willkommen im Gerätehaus • Bitte Ausbildungszeiten beachten"))

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
            CREATE TABLE IF NOT EXISTS push_subscriptions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(255) NOT NULL,
                endpoint TEXT NOT NULL,
                p256dh VARCHAR(255) NOT NULL,
                auth VARCHAR(255) NOT NULL,
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
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vehicle_checks (
                id INT AUTO_INCREMENT PRIMARY KEY,
                vehicle_id INT NOT NULL,
                date DATE NOT NULL,
                checker_name VARCHAR(255) NOT NULL,
                status VARCHAR(50) DEFAULT 'OK',
                items_checked JSON,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (vehicle_id) REFERENCES vehicles(id) ON DELETE CASCADE
            ) ENGINE=InnoDB;
        """)

        # Migration für youth_members (Extended Jugend-Akte)
        youth_cols = [
            ("birth_date", "DATE NULL"),
            ("entry_date", "DATE NULL"),
            ("phone", "VARCHAR(100) NULL"),
            ("email", "VARCHAR(150) NULL"),
            ("address", "VARCHAR(255) NULL"),
            ("notes", "TEXT NULL"),
            ("skills", "TEXT NULL"),
            ("lic_am", "TINYINT(1) DEFAULT 0"),
            ("lic_a1", "TINYINT(1) DEFAULT 0"),
            ("lic_b", "TINYINT(1) DEFAULT 0"),
            ("lic_l", "TINYINT(1) DEFAULT 0"),
            ("lic_t", "TINYINT(1) DEFAULT 0"),
            ("has_jf1", "TINYINT(1) DEFAULT 0"),
            ("has_jf2", "TINYINT(1) DEFAULT 0"),
            ("has_jf3", "TINYINT(1) DEFAULT 0"),
            ("has_wissentest", "TINYINT(1) DEFAULT 0"),
            ("has_leistungsspange", "TINYINT(1) DEFAULT 0"),
            ("has_jugendabzeichen", "TINYINT(1) DEFAULT 0"),
            ("has_mta_basis", "TINYINT(1) DEFAULT 0"),
            ("has_erste_hilfe", "TINYINT(1) DEFAULT 0"),
            ("has_funk", "TINYINT(1) DEFAULT 0"),
            ("profile_picture", "LONGTEXT NULL")
        ]
        for col_name, col_def in youth_cols:
            try:
                cur.execute(f"SHOW COLUMNS FROM youth_members LIKE '{col_name}'")
                if not cur.fetchone():
                    cur.execute(f"ALTER TABLE youth_members ADD COLUMN {col_name} {col_def}")
            except Exception as e:
                print(f"Migration youth_members col {col_name}: {e}")

        # Migration für Mängelberichte (Foto, Zuweisung, Priorität)
        try:
            cur.execute("SHOW COLUMNS FROM equipment_defect_reports LIKE 'image_url'")
            if not cur.fetchone():
                cur.execute("ALTER TABLE equipment_defect_reports ADD COLUMN image_url VARCHAR(255) NULL")
                cur.execute("ALTER TABLE equipment_defect_reports ADD COLUMN assigned_to VARCHAR(255) NULL")
                cur.execute("ALTER TABLE equipment_defect_reports ADD COLUMN priority VARCHAR(50) NOT NULL DEFAULT 'Mittel'")
        except Exception as mig_err:
            print("Fehler bei defect reports Migration:", mig_err)

        # Migration für Einsatzberichte (Gruppe / Einheit & Start-/Endzeit)
        try:
            cur.execute("SHOW COLUMNS FROM missions LIKE 'group_id'")
            if not cur.fetchone():
                cur.execute("ALTER TABLE missions ADD COLUMN group_id INT NULL")
        except Exception as m_err:
            print("Fehler bei missions group_id Migration:", m_err)

        for tbl, col_name, col_def in [
            ("missions", "end_time", "VARCHAR(50) DEFAULT ''"),
            ("sessions", "time", "VARCHAR(50) DEFAULT ''"),
            ("sessions", "end_time", "VARCHAR(50) DEFAULT ''")
        ]:
            try:
                cur.execute(f"SHOW COLUMNS FROM {tbl} LIKE '{col_name}'")
                if not cur.fetchone():
                    cur.execute(f"ALTER TABLE {tbl} ADD COLUMN {col_name} {col_def}")
            except Exception as mig_err:
                print(f"Fehler bei {tbl} {col_name} Migration:", mig_err)

        cur.execute("SELECT COUNT(*) FROM users WHERE username = 'admin'")
        if cur.fetchone()[0] == 0:
            default_admin_hash = hash_password("admin123")
            cur.execute(
                "INSERT INTO users (username, password_hash, role, is_first_login) VALUES (%s, %s, %s, 1)",
                ("admin", default_admin_hash, "admin")
            )
            log_audit_action("SYSTEM", "INITIALISIERUNG", "Standard-Admin 'admin' mit Kennwort 'admin123' angelegt.")

        # Performance SQL Indizes für schnelle Abfragezeiten
        indexes = [
            ("persons", "idx_persons_group_name", "(group_id, name)"),
            ("personnel", "idx_personnel_status_name", "(membership_status, name)"),
            ("attendance", "idx_attendance_sess_person", "(session_id, person_id)"),
            ("mission_attendance", "idx_mission_att_m_p", "(mission_id, personnel_id)"),
            ("sessions", "idx_sessions_g_date", "(group_id, date)"),
            ("missions", "idx_missions_date", "(date)")
        ]
        for tbl, idx_name, cols in indexes:
            try:
                cur.execute(f"CREATE INDEX {idx_name} ON {tbl} {cols}")
            except Exception:
                pass

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
            try:
                cur.execute("CREATE INDEX idx_persons_group_name ON persons (group_id, name);")
            except Exception:
                pass
            
            conn.commit()
            cur.close()
            conn.close()
            init_db_extensions()
            break
        except Exception as e:
            time.sleep(5)

init_db()

from core.models import safe_decode, PersonData, VehicleData, EntryDto, AttendanceUpload, GroupData
from pydantic import BaseModel

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

@app.get("/sw.js", response_class=FileResponse, include_in_schema=False)
def get_service_worker():
    return FileResponse("static/sw.js", media_type="application/javascript")

@app.get("/manifest.json", response_class=FileResponse, include_in_schema=False)
def get_manifest():
    return FileResponse("static/manifest.json", media_type="application/manifest+json")

# --- AUTHENTIFIZIERUNG UND LOGIN SPERREN ---
# Routes moved to routers/auth_mgr.py

# --- ERWEITERTE BENUTZERSTEUERUNG (ADMINS) ---
# Routes moved to routers/users_mgr.py

# --- DATENBANK SICHERUNG (EXPORT & IMPORT) ---
# Routes moved to routers/admin_api.py

# users_mgr routes for add, update role/personnel/password, delete have been moved

# --- FEUERWACHE STANDORT EINSTELLUNGEN ---
@app.get("/api/settings/station")
def get_station_settings():
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT station_name, lat, lng, zoom, ticker_text, iban, bic FROM station_settings LIMIT 1")
    except Exception:
        try:
            cur.execute("ALTER TABLE station_settings ADD COLUMN ticker_text TEXT NULL")
            conn.commit()
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE station_settings ADD COLUMN iban VARCHAR(100) NULL, ADD COLUMN bic VARCHAR(100) NULL")
            conn.commit()
        except Exception:
            pass
        cur.execute("SELECT station_name, lat, lng, zoom, ticker_text, iban, bic FROM station_settings LIMIT 1")
    row = cur.fetchone(); cur.close(); conn.close()
    if not row:
        return {"station_name": TOWN_NAME, "lat": 50.1109, "lng": 8.6821, "zoom": 14, "ticker_text": "Willkommen im Gerätehaus • Bitte Ausbildungszeiten beachten", "iban": "", "bic": ""}
    if not row.get("ticker_text"):
        row["ticker_text"] = "Willkommen im Gerätehaus • Bitte Ausbildungszeiten beachten"
    if not row.get("iban"):
        row["iban"] = ""
    if not row.get("bic"):
        row["bic"] = ""
    return row

@app.put("/api/settings/station")
def update_station_settings(data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    
    station_name = data.get("station_name", "Feuerwehr").strip()
    ticker_text = data.get("ticker_text", "").strip()
    iban = data.get("iban", "").strip()
    bic = data.get("bic", "").strip()
    try:
        lat = float(data.get("lat", 50.1109))
        lng = float(data.get("lng", 8.6821))
        zoom = int(data.get("zoom", 14))
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Ungültige Koordinaten")
        
    conn = get_db_connection(); cur = conn.cursor()
    try:
        cur.execute("ALTER TABLE station_settings ADD COLUMN ticker_text TEXT NULL")
        conn.commit()
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE station_settings ADD COLUMN iban VARCHAR(100) NULL, ADD COLUMN bic VARCHAR(100) NULL")
        conn.commit()
    except Exception:
        pass

    cur.execute("SELECT id FROM station_settings LIMIT 1")
    row = cur.fetchone()
    if row:
        cur.execute("""
            UPDATE station_settings 
            SET station_name = %s, lat = %s, lng = %s, zoom = %s, ticker_text = %s, iban = %s, bic = %s
            WHERE id = %s
        """, (station_name, lat, lng, zoom, ticker_text, iban, bic, row[0]))
    else:
        cur.execute("""
            INSERT INTO station_settings (station_name, lat, lng, zoom, ticker_text, iban, bic) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (station_name, lat, lng, zoom, ticker_text, iban, bic))
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
# Routes moved to routers/admin_api.py

# --- AUDIT-LOG ROUTE (REVISIONS-PROTOKOLL) ---
# Routes moved to routers/admin_api.py

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
# Routes moved to routers/vehicles_api.py


# --- AUTOMATED BACKUP ---
# Routes moved to routers/admin_api.py


# Trigger reload

# Trigger reload 2

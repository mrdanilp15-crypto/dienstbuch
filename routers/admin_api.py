from fastapi import APIRouter, HTTPException, Request, Response, UploadFile, File, Form
from fastapi.responses import FileResponse
from typing import Optional
import json
import os
import uuid
import datetime
import decimal
import shutil
import subprocess
from datetime import date

from database import get_db_connection
from core.utils import get_current_user, log_audit_action

router = APIRouter()

# Erlaubte Dateiendungen für Archiv-Uploads: verhindert, dass z.B. eine .html/.svg-Datei
# hochgeladen wird, die beim Aufruf über /static/uploads/... mit ihrem eigenen Content-Type
# (text/html, image/svg+xml) ausgeliefert würde und darin JavaScript im App-Origin ausführen
# könnte (gespeicherte XSS trotz httponly-Session-Cookie).
ALLOWED_ARCHIVE_EXTENSIONS = {
    ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff",
    ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".odt", ".ods",
    ".txt", ".csv", ".zip",
}
# Jeder angemeldete Nutzer (jede Rolle) darf hier hochladen - ohne Obergrenze könnte ein
# einzelner Upload (oder viele wiederholte) die Festplatte des Servers füllen (DoS).
_MAX_ARCHIVE_UPLOAD_SIZE = 25 * 1024 * 1024  # 25 MB

BACKUP_TABLES = [
    "users", "personnel", "groups_table", "persons", "sessions", "attendance",
    "vehicles", "vehicle_log", "vehicle_checks", "hvo_protocols", "hvo_equipment_checks",
    "equipment", "equipment_inspections", "equipment_defect_reports", "notes",
    "archive_files", "youth_sessions", "youth_attendance", "settings",
    "station_settings", "audit_log", "apager_config", "apager_logs",
    "apager_feedbacks", "system_broadcasts", "broadcast_reads", "schedules",
    "schedule_attendance", "missions", "mission_attendance", "respiration_log",
    "billing_verursacher", "personal_inventar", "lehrgaenge", "hydrants", "bma",
    "drone_images", "push_subscriptions", "fcm_tokens", "club_inventory", "club_donations"
]


def build_backup_payload(exported_by: str) -> dict:
    """Baut die vollstaendige JSON-Sicherung. Bewusst ohne Request-Objekt, damit auch die
    automatische Sicherung (ohne angemeldeten Nutzer) dieselben Daten erzeugt - das Ergebnis
    laesst sich ueber den vorhandenen Import wieder einspielen."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    backup_tables = {}

    for table in BACKUP_TABLES:
        try:
            cur.execute(f"SELECT * FROM `{table}`")
            rows = cur.fetchall()
            for r in rows:
                for k, v in r.items():
                    if isinstance(v, (datetime.datetime, date)):
                        r[k] = str(v)
                    elif isinstance(v, bytes):
                        r[k] = v.decode('utf-8', errors='ignore')
                    # DECIMAL-Spalten (z.B. missions.duration, sessions.duration,
                    # club_donations.amount) kommen aus mysql-connector als decimal.Decimal -
                    # json.dumps() kann das nicht serialisieren und wirft sonst einen
                    # unbehandelten TypeError, der hier als 500 Internal Server Error endet.
                    elif isinstance(v, decimal.Decimal):
                        r[k] = float(v)
            backup_tables[table] = rows
        except Exception as e:
            print(f"Export warning for table {table}: {e}")

    cur.close()
    conn.close()

    from main import CURRENT_VERSION
    return {
        "app_name": "Dienstbuch",
        "version": CURRENT_VERSION,
        "exported_at": datetime.datetime.now().isoformat(),
        "exported_by": exported_by,
        "tables": backup_tables
    }


@router.get("/api/admin/backup/export")
def export_database_backup(request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Keine Berechtigung (Admin erforderlich)")

    filename = f"dienstbuch_backup_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M')}.json"
    backup_data = build_backup_payload(user["username"])

    log_audit_action(user["username"], "DATENBANK-BACKUP", f"Datenbank-Sicherung '{filename}' erstellt.")

    json_str = json.dumps(backup_data, ensure_ascii=False, indent=2)
    return Response(
        content=json_str,
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

@router.post("/api/admin/backup/import")
async def import_database_backup(request: Request, file: UploadFile = File(...)):
    user = get_current_user(request)
    if not user or user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Keine Berechtigung (Admin erforderlich)")

    content = await file.read()

    # --- FALL 1: ALTE ODER FREMDE SQL-DATEI (.sql) ---
    if file.filename.lower().endswith(".sql"):
        try:
            sql_script = content.decode('utf-8', errors='ignore')
            conn = get_db_connection()
            cur = conn.cursor()
            statements = [s.strip() for s in sql_script.split(';') if s.strip()]
            executed_count = 0
            for stmt in statements:
                if stmt and not stmt.startswith("--") and not stmt.startswith("/*"):
                    try:
                        cur.execute(stmt)
                        executed_count += 1
                    except Exception as s_err:
                        print(f"SQL import statement notice: {s_err}")
            conn.commit()
            cur.close()
            conn.close()

            from main import sync_personnel_to_editor_groups, init_db_extensions
            sync_personnel_to_editor_groups()
            init_db_extensions()
            log_audit_action(user["username"], "DATENBANK-IMPORT-SQL", f"SQL-Datei '{file.filename}' erfolgreich importiert ({executed_count} Befehle ausgeführt).")
            return {"status": "success", "imported_rows": executed_count, "message": f"{executed_count} SQL-Befehle erfolgreich ausgeführt."}
        except Exception as sql_err:
            raise HTTPException(status_code=500, detail=f"Fehler beim Importieren der SQL-Datei: {sql_err}")

    # --- FALL 2: DIENSTBUCH JSON-BACKUP (.json) ---
    try:
        backup_data = json.loads(content.decode('utf-8'))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Ungültige Backup-Datei (.json oder .sql erwartet): {e}")

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

    from main import sync_personnel_to_editor_groups, init_db_extensions
    sync_personnel_to_editor_groups()
    init_db_extensions()
    log_audit_action(user["username"], "DATENBANK-IMPORT", f"Backup-Datei '{file.filename}' erfolgreich importiert ({imported_count} Datensätze).")

    return {"status": "success", "imported_rows": imported_count}

# Sicherungen müssen einen Neuaufbau des Containers überstehen. Das Arbeitsverzeichnis
# (/app) ist NICHT gemountet - dort abgelegte Sicherungen wären nach dem nächsten
# "docker compose up -d --build" spurlos weg, also genau dann, wenn man sie braucht.
# /app/data hängt dagegen am benannten Volume notes_data (siehe docker-compose.yml).
BACKUP_DIR = os.getenv("BACKUP_DIR", "/app/data/backups")
try:
    BACKUP_KEEP = max(1, int(os.getenv("BACKUP_KEEP", "14")))
except ValueError:
    BACKUP_KEEP = 14


def _backup_dir() -> str:
    """Beschreibbares Sicherungsverzeichnis. Fällt auf ./auto_backups zurück, wenn das
    Volume nicht existiert (lokale Entwicklung ausserhalb von Docker)."""
    for candidate in (BACKUP_DIR, os.path.join(os.getcwd(), "auto_backups")):
        try:
            os.makedirs(candidate, exist_ok=True)
            return candidate
        except Exception:
            continue
    raise RuntimeError("Kein beschreibbares Sicherungsverzeichnis gefunden")


def _prune_backups(backup_dir: str, keep: int) -> int:
    """Ältere Sicherungen entfernen, damit das Volume nicht unbemerkt vollläuft. Die
    Dateinamen enthalten einen sortierbaren Zeitstempel, neueste zuerst."""
    removed = 0
    for prefix in ("db_backup_", "uploads_backup_", "dienstbuch_backup_"):
        try:
            files = sorted((f for f in os.listdir(backup_dir) if f.startswith(prefix)), reverse=True)
        except Exception:
            continue
        for old in files[keep:]:
            try:
                os.remove(os.path.join(backup_dir, old))
                removed += 1
            except Exception as e:
                print(f"Backup-Aufräumen fehlgeschlagen für {old}: {e}")
    return removed


def run_auto_backup(username: str = "system") -> dict:
    """Erzeugt eine Sicherung. Bewusst ohne Request-Objekt, damit auch der Zeitplan im
    Hintergrund (siehe main.py) dieselbe Funktion nutzen kann."""
    backup_dir = _backup_dir()
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    db_dump_path = os.path.join(backup_dir, f"db_backup_{ts}.sql")

    db_host = os.getenv("DB_HOST", os.getenv("MYSQL_HOST", "db"))
    db_user = os.getenv("DB_USER", os.getenv("MYSQL_USER", "app_user"))
    db_pass = os.getenv("DB_PASSWORD") or os.getenv("DB_PASS") or os.getenv("MYSQL_PASSWORD") or "dein_app_passwort"
    db_name = os.getenv("DB_NAME", os.getenv("MYSQL_DATABASE", "attendance_system"))

    sql_dump_ok = False
    sql_dump_error = None
    try:
        # Passwort über MYSQL_PWD statt als -p<pass>-Kommandozeilenargument übergeben:
        # so landet es nicht in der Prozessliste, und Sonderzeichen (", $, `, ...) im
        # Passwort können den Aufruf nicht mehr als Shell-String fehlinterpretieren -
        # subprocess.run bekommt hier für beide Plattformen eine reine Argumentliste
        # statt eines zusammengebauten Shell-Kommandos (kein shell=True nötig).
        mysqldump_cmd = ["mysqldump", "-h", db_host, "-u", db_user, db_name]
        dump_env = os.environ.copy()
        dump_env["MYSQL_PWD"] = db_pass
        with open(db_dump_path, "w", encoding="utf-8") as f:
            subprocess.run(mysqldump_cmd, stdout=f, env=dump_env, check=True)
        sql_dump_ok = os.path.exists(db_dump_path) and os.path.getsize(db_dump_path) > 0
    except Exception as e:
        print(f"Error during mysqldump: {e}")
        sql_dump_error = str(e)

    uploads_file = None
    try:
        uploads_file = shutil.make_archive(os.path.join(backup_dir, f"uploads_backup_{ts}"),
                                           'zip', "static/uploads")
    except Exception as e:
        print(f"Upload-Sicherung fehlgeschlagen: {e}")

    # Rückfallebene: schlägt mysqldump fehl, darf eine AUTOMATISCHE Sicherung nicht
    # einfach ohne Datenbankstand zurückbleiben - das merkt sonst niemand. Der JSON-Stand
    # ist reines Python (kein externes Programm nötig) und lässt sich über den bereits
    # vorhandenen Import wieder einspielen.
    json_file = None
    if not sql_dump_ok:
        try:
            os.remove(db_dump_path)
        except Exception:
            pass
        try:
            json_file = os.path.join(backup_dir, f"dienstbuch_backup_{ts}.json")
            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(build_backup_payload(username), f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"JSON-Rückfallsicherung fehlgeschlagen: {e}")
            json_file = None

    _prune_backups(backup_dir, BACKUP_KEEP)

    if sql_dump_ok:
        log_audit_action(username, "AUTO_BACKUP",
                         f"Automatische Sicherung erstellt: {os.path.basename(db_dump_path)}")
        return {"status": "success", "directory": backup_dir,
                "sql_dump": db_dump_path, "uploads": uploads_file, "json_fallback": None}

    if json_file:
        log_audit_action(username, "AUTO_BACKUP_JSON",
                         f"mysqldump fehlgeschlagen ({sql_dump_error or 'leere Ausgabe'}), "
                         f"stattdessen JSON-Sicherung erstellt: {os.path.basename(json_file)}")
        return {"status": "success", "directory": backup_dir, "sql_dump": None,
                "uploads": uploads_file, "json_fallback": json_file,
                "warning": "mysqldump war nicht verfügbar - es wurde eine JSON-Sicherung "
                           "erstellt, die sich über den Import wieder einspielen lässt."}

    log_audit_action(username, "AUTO_BACKUP_FEHLGESCHLAGEN",
                     f"Weder mysqldump noch JSON-Sicherung möglich: {sql_dump_error or 'unbekannt'}")
    return {"status": "error", "directory": backup_dir, "sql_dump": None,
            "uploads": uploads_file, "json_fallback": None,
            "warning": "Die Sicherung ist fehlgeschlagen. Bitte Server-Protokoll prüfen."}


@router.post("/api/admin/backup/auto")
def auto_backup(request: Request):
    user = get_current_user(request)
    if not user or user.get("role") not in ["admin", "leitung"]:
        raise HTTPException(status_code=403, detail="Nicht berechtigt")
    return run_auto_backup(user["username"])


@router.get("/api/admin/backups")
def list_auto_backups(request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Keine Berechtigung (Admin erforderlich)")
    backup_dir = _backup_dir()
    files = []
    for name in sorted(os.listdir(backup_dir), reverse=True):
        path = os.path.join(backup_dir, name)
        if not os.path.isfile(path):
            continue
        st = os.stat(path)
        files.append({
            "name": name,
            "size": st.st_size,
            "created_at": datetime.datetime.fromtimestamp(st.st_mtime).isoformat()
        })
    return {"directory": backup_dir, "keep": BACKUP_KEEP, "files": files}


@router.get("/api/admin/backups/{name}")
def download_auto_backup(name: str, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Keine Berechtigung (Admin erforderlich)")
    backup_dir = _backup_dir()
    # Pfad-Ausbruch verhindern: nur Dateien direkt in diesem Verzeichnis herausgeben,
    # niemals etwas wie "../../etc/passwd" oder einen Symlink nach draussen.
    safe_name = os.path.basename(name)
    full_path = os.path.realpath(os.path.join(backup_dir, safe_name))
    if os.path.dirname(full_path) != os.path.realpath(backup_dir) or not os.path.isfile(full_path):
        raise HTTPException(status_code=404, detail="Sicherung nicht gefunden")
    log_audit_action(user["username"], "BACKUP_DOWNLOAD", f"Sicherung '{safe_name}' heruntergeladen.")
    return FileResponse(full_path, filename=safe_name, media_type="application/octet-stream")

@router.post("/api/archive/upload")
async def upload_archive_file(request: Request, file: UploadFile = File(...), is_public: bool = False, vehicle_id: Optional[int] = Form(None)):
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht angemeldet")

    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in ALLOWED_ARCHIVE_EXTENSIONS:
        raise HTTPException(status_code=400, detail=f"Dateityp '{ext}' nicht erlaubt.")

    from main import UPLOAD_DIR
    filename = f"{uuid.uuid4()}{ext}"
    filepath = os.path.join(UPLOAD_DIR, filename)

    size = 0
    try:
        with open(filepath, "wb") as buffer:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > _MAX_ARCHIVE_UPLOAD_SIZE:
                    raise HTTPException(status_code=413, detail="Datei zu groß (max. 25 MB).")
                buffer.write(chunk)
    except HTTPException:
        if os.path.exists(filepath):
            os.remove(filepath)
        raise

    url = f"/static/uploads/{filename}"

    conn = get_db_connection(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO archive_files (filename, url, uploaded_by, is_public, vehicle_id) VALUES (%s, %s, %s, %s, %s)",
        (file.filename, url, user["username"], 1 if is_public else 0, vehicle_id)
    )
    conn.commit(); cur.close(); conn.close()

    log_audit_action(user["username"], "ARCHIV_DATEI_HOCHGELADEN", f"Datei '{file.filename}' hochgeladen (Öffentlich: {is_public}).")
    return {"status": "success", "url": url}

@router.get("/api/archive/files")
def get_archive_files(request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht angemeldet")

    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT id, filename, url, uploaded_by, is_public, DATE_FORMAT(created_at, '%d.%m.%Y %H:%i') as created_at
        FROM archive_files
        WHERE (is_public = 1 OR uploaded_by = %s) AND vehicle_id IS NULL
        ORDER BY id DESC
    """, (user["username"],))
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@router.get("/api/vehicles/{vehicle_id}/documents")
def get_vehicle_documents(vehicle_id: int, request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT id, filename, url, uploaded_by, DATE_FORMAT(created_at, '%d.%m.%Y %H:%i') as created_at
        FROM archive_files
        WHERE vehicle_id = %s
        ORDER BY id DESC
    """, (vehicle_id,))
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@router.delete("/api/archive/files/{file_id}")
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

@router.get("/api/admin/stats")
def get_admin_stats(request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ["admin", "leitung"]:
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    
    cur.execute("SELECT DAYNAME(date) as day, COUNT(*) as count FROM missions GROUP BY DAYNAME(date)")
    missions_by_day_raw = cur.fetchall()
    
    # Fill missing days and sort properly
    days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    days_german = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]
    
    # Create mapping from raw to dict
    raw_dict = {row["day"]: row["count"] for row in missions_by_day_raw if row["day"]}
    
    missions_by_day = []
    for i, d in enumerate(days_order):
        missions_by_day.append({
            "day": days_german[i],
            "count": raw_dict.get(d, 0)
        })
        
    cur.execute("SELECT COUNT(*) as total FROM missions")
    total_missions = cur.fetchone()["total"]
    
    cur.execute("SELECT MONTHNAME(date) as month, COUNT(*) as count FROM missions GROUP BY MONTHNAME(date)")
    missions_by_month_raw = cur.fetchall()
    
    months_order = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
    months_german = ["Jan", "Feb", "Mär", "Apr", "Mai", "Jun", "Jul", "Aug", "Sep", "Okt", "Nov", "Dez"]
    
    month_dict = {row["month"]: row["count"] for row in missions_by_month_raw if row["month"]}
    missions_by_month = []
    for i, m in enumerate(months_order):
        missions_by_month.append({
            "month": months_german[i],
            "count": month_dict.get(m, 0)
        })
    
    cur.close()
    conn.close()
    
    return {
        "missions_by_day": missions_by_day,
        "missions_by_month": missions_by_month,
        "total_missions": total_missions
    }

@router.get("/api/search")
def global_search(q: str, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    q = (q or "").strip()
    if len(q) < 2:
        return {"personnel": [], "vehicles": [], "equipment": [], "documents": [], "notes": [], "missions": [], "bmas": []}
    like = f"%{q}%"
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)

    cur.execute("SELECT id, name, rank FROM personnel WHERE name LIKE %s ORDER BY name LIMIT 8", (like,))
    personnel = cur.fetchall()

    cur.execute("SELECT id, name, radio_name FROM vehicles WHERE name LIKE %s OR radio_name LIKE %s ORDER BY name LIMIT 8", (like, like))
    vehicles = cur.fetchall()

    cur.execute("SELECT id, name, barcode, category FROM equipment WHERE name LIKE %s OR barcode LIKE %s ORDER BY name LIMIT 8", (like, like))
    equipment = cur.fetchall()

    cur.execute("SELECT id, filename, url FROM archive_files WHERE filename LIKE %s AND is_public = 1 ORDER BY created_at DESC LIMIT 8", (like,))
    documents = cur.fetchall()

    # Gleiche Sichtbarkeits-Regel wie notes_manager.py list_notes(): eigene Notizen +
    # öffentliche + rollenspezifische - eine Suche darf keine fremden privaten Notizen
    # zutage fördern, die der Nutzer über die normale Notizbuch-Ansicht gar nicht sähe.
    cur.execute("""
        SELECT id, title, visibility FROM notes
        WHERE (title LIKE %s OR content LIKE %s)
          AND (username = %s OR visibility = 'public'
               OR (visibility = 'admin' AND %s IN ('admin', 'leitung'))
               OR (visibility = 'geratewart' AND %s IN ('geratewart', 'admin')))
        ORDER BY created_at DESC LIMIT 8
    """, (like, like, user["username"], user["role"], user["role"]))
    notes = cur.fetchall()

    cur.execute("SELECT id, stichwort, adresse, date FROM missions WHERE stichwort LIKE %s OR adresse LIKE %s ORDER BY date DESC LIMIT 8", (like, like))
    missions = cur.fetchall()
    for m in missions:
        if m.get("date"): m["date"] = str(m["date"])

    cur.execute("SELECT id, object_name, address FROM bma WHERE object_name LIKE %s OR address LIKE %s ORDER BY object_name LIMIT 8", (like, like))
    bmas = cur.fetchall()

    cur.close(); conn.close()
    return {"personnel": personnel, "vehicles": vehicles, "equipment": equipment, "documents": documents, "notes": notes, "missions": missions, "bmas": bmas}

@router.get("/api/admin/stats/attendance")
def get_attendance_stats(year: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ["admin", "leitung"]:
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    # Wiederverwendet dieselbe Stunden-Berechnung wie die Abrechnung (Dienst- + Einsatzstunden
    # pro Person), nur ohne Stundensatz - vermeidet eine zweite, abweichende Implementierung
    # derselben nicht-trivialen Query.
    from routers.mission_mgr import calculate_compensations
    comps = calculate_compensations(year, 0.0, request)
    return sorted([c for c in comps if c["total_hours"] > 0], key=lambda x: x["total_hours"], reverse=True)

@router.get("/api/admin/stats/due-soon")
def get_due_soon(request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ["admin", "leitung", "geratewart"]:
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    from core.reminders import get_due_items
    return get_due_items()

@router.get("/api/admin/audit-log")
def get_audit_log(request: Request, limit: int = 200):
    user = get_current_user(request)
    if not user or user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    limit = max(1, min(limit, 500))
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, created_at, username, action, details FROM audit_log ORDER BY created_at DESC LIMIT %s", (limit,))
    res = cur.fetchall(); cur.close(); conn.close()
    for r in res:
        r["created_at"] = str(r["created_at"])
    return res

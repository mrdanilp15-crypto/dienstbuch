from fastapi import APIRouter, HTTPException, Request, Response, UploadFile, File
import json
import os
import uuid
import datetime
import shutil
import subprocess
from datetime import date

from database import get_db_connection
from core.utils import get_current_user, log_audit_action

router = APIRouter()

@router.get("/api/admin/backup/export")
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
                    if isinstance(v, (datetime.datetime, date)):
                        r[k] = str(v)
                    elif isinstance(v, bytes):
                        r[k] = v.decode('utf-8', errors='ignore')
            backup_tables[table] = rows
        except Exception as e:
            print(f"Export warning for table {table}: {e}")

    cur.close()
    conn.close()

    from main import CURRENT_VERSION
    filename = f"dienstbuch_backup_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M')}.json"
    backup_data = {
        "app_name": "Dienstbuch",
        "version": CURRENT_VERSION,
        "exported_at": datetime.datetime.now().isoformat(),
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

@router.post("/api/admin/backup/auto")
def auto_backup(request: Request):
    user = get_current_user(request)
    if not user or user.get("role") not in ["admin", "leitung"]:
        raise HTTPException(status_code=403, detail="Nicht berechtigt")
    
    backup_dir = os.path.join(os.getcwd(), "auto_backups")
    os.makedirs(backup_dir, exist_ok=True)
    
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    db_dump_path = os.path.join(backup_dir, f"db_backup_{ts}.sql")
    
    db_host = os.getenv("DB_HOST", os.getenv("MYSQL_HOST", "db"))
    db_user = os.getenv("DB_USER", os.getenv("MYSQL_USER", "app_user"))
    db_pass = os.getenv("DB_PASSWORD") or os.getenv("DB_PASS") or os.getenv("MYSQL_PASSWORD") or "dein_app_passwort"
    db_name = os.getenv("DB_NAME", os.getenv("MYSQL_DATABASE", "attendance_system"))
    
    try:
        if os.name == 'nt':
            mysqldump_cmd = f"mysqldump -h {db_host} -u {db_user} -p{db_pass} {db_name} > {db_dump_path}"
            subprocess.run(["powershell", "-Command", mysqldump_cmd], check=True)
        else:
            mysqldump_cmd = ["mysqldump", "-h", db_host, "-u", db_user, f"-p{db_pass}", db_name]
            with open(db_dump_path, "w") as f:
                subprocess.run(mysqldump_cmd, stdout=f, check=True)
    except Exception as e:
        print(f"Error during mysqldump: {e}")
        pass
        
    zip_filename = f"backup_{ts}"
    zip_path = os.path.join(backup_dir, zip_filename)
    shutil.make_archive(zip_path, 'zip', "static/uploads")
    
    return {"status": "success", "backup_file": f"{zip_path}.zip", "sql_dump": db_dump_path}

@router.post("/api/archive/upload")
async def upload_archive_file(request: Request, file: UploadFile = File(...), is_public: bool = False):
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht angemeldet")
    
    from main import UPLOAD_DIR
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

@router.get("/api/archive/files")
def get_archive_files(request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht angemeldet")
        
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT id, filename, url, uploaded_by, is_public, DATE_FORMAT(created_at, '%d.%m.%Y %H:%i') as created_at
        FROM archive_files
        WHERE is_public = 1 OR uploaded_by = %s
        ORDER BY id DESC
    """, (user["username"],))
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

@router.get("/api/audit/logs")
def get_audit_logs(request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, DATE_FORMAT(created_at, '%d.%m.%Y %H:%i') as date_formatted, username, action, details FROM audit_log ORDER BY id DESC LIMIT 150")
    logs = cur.fetchall(); cur.close(); conn.close()
    return logs
@router.get("/api/admin/stats")
def get_admin_stats(request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    
    cur.execute("SELECT DAYNAME(date) as day, COUNT(*) as count FROM missions GROUP BY DAYNAME(date)")
    missions_by_day_raw = cur.fetchall()
    
    # Optional: Fill missing days and sort properly
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    missions_dict = {d: 0 for d in days}
    for row in missions_by_day_raw:
        if row["day"] in missions_dict:
            missions_dict[row["day"]] = row["count"]
            
    missions_by_day = [{"day": d, "count": missions_dict[d]} for d in days]
    
    cur.execute("SELECT COUNT(*) as count FROM missions")
    total_missions = cur.fetchone()["count"]
    
    cur.close()
    conn.close()
    
    return {
        "missions_by_day": missions_by_day,
        "missions_total": total_missions
    }

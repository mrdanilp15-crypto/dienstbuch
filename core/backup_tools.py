"""Nach jeder Sicherung: Probe-Wiederherstellung (echt, falls möglich), Status merken, Kopie an einen zweiten Ort (WebDAV).

Eine Sicherung, die sich nicht einspielen lässt, ist keine. Deshalb wird die frische SQL-Datei in eine
Wegwerf-Datenbank eingespielt (nur mit dem Root-Zugang aus ROOT_PASS), mit der Live-Datenbank verglichen und
die Wegwerf-Datenbank wieder gelöscht. Ohne Root-Zugang oder ohne mysql-Programm bleibt es bei einer
Plausibilitätsprüfung (Datei vollständig, alle Tabellen enthalten) - das steht dann auch so in der Anzeige.
"""
import datetime
import gzip
import os
import shlex
import shutil
import subprocess
import tempfile
import time
from urllib.parse import quote

import mysql.connector

from core.mailer import decrypt_secret, encrypt_secret
from database import get_db_connection


def _db_params():
    return dict(
        host=os.getenv("DB_HOST", os.getenv("MYSQL_HOST", "db")),
        port=int(os.getenv("DB_PORT", "3306") or 3306),
        name=os.getenv("DB_NAME", os.getenv("MYSQL_DATABASE", "attendance_system")),
    )


def _root_password() -> str:
    return os.getenv("ROOT_PASS") or os.getenv("MYSQL_ROOT_PASSWORD") or ""


def _mysql_cli():
    custom = os.getenv("MYSQL_CLI")  # nur für Tests: z.B. "docker exec -i demo_test_db mariadb"
    if custom:
        return shlex.split(custom), True
    exe = shutil.which("mariadb") or shutil.which("mysql")
    return ([exe] if exe else None), False


def _live_counts(tables=("users", "personnel")):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'")
    n_tables = cur.fetchone()[0]
    counts = {}
    for t in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM `{t}`")
            counts[t] = cur.fetchone()[0]
        except Exception:
            counts[t] = None
    cur.close()
    conn.close()
    return n_tables, counts


def plausibility_check(path: str) -> dict:
    """Datei vorhanden, nicht winzig, bis zum Ende geschrieben ("Dump completed") und alle Tabellen enthalten."""
    size = os.path.getsize(path)
    if size < 1024:
        return {"status": "error", "detail": f"Sicherungsdatei ist nur {size} Byte groß."}
    with open(path, "rb") as f:
        f.seek(max(0, size - 4096))
        tail = f.read().decode("utf-8", "ignore")
    tables_in_dump = 0
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if line.startswith("CREATE TABLE"):
                tables_in_dump += 1
    live, _ = _live_counts()
    if "Dump completed" not in tail:
        return {"status": "error", "detail": "Die Sicherungsdatei endet abrupt (Dump nicht vollständig)."}
    if tables_in_dump < live:
        return {"status": "error", "detail": f"Nur {tables_in_dump} von {live} Tabellen in der Sicherung."}
    return {"status": "ok", "detail": f"Datei vollständig, alle {live} Tabellen enthalten (Plausibilitätsprüfung)."}


def restore_test(path: str) -> dict:
    """Spielt die Sicherung in eine Wegwerf-Datenbank ein und vergleicht Tabellen- und Zeilenzahlen mit der Live-Datenbank."""
    root_pw = _root_password()
    cli, custom = _mysql_cli()
    if not root_pw or not cli:
        return None
    p = _db_params()
    scratch = f"restoretest_{int(time.time())}"
    conn = None
    try:
        conn = mysql.connector.connect(host=p["host"], port=p["port"], user="root", password=root_pw, connection_timeout=10)
        cur = conn.cursor()
        cur.execute(f"CREATE DATABASE `{scratch}` CHARACTER SET utf8mb4")
        env = dict(os.environ, MYSQL_PWD=root_pw)
        cmd = cli + (["-uroot", f"-p{root_pw}"] if custom else ["-h", p["host"], "-P", str(p["port"]), "-u", "root"]) + [scratch]
        with open(path, "rb") as f:
            r = subprocess.run(cmd, stdin=f, env=env, capture_output=True, timeout=600)
        if r.returncode != 0:
            return {"status": "error", "detail": "Einspielen fehlgeschlagen: " + r.stderr.decode("utf-8", "ignore")[:200]}
        live_tables, live_counts = _live_counts()
        cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = %s AND table_type = 'BASE TABLE'", (scratch,))
        got_tables = cur.fetchone()[0]
        diffs = []
        for t, n in live_counts.items():
            if n is None:
                continue
            cur.execute(f"SELECT COUNT(*) FROM `{scratch}`.`{t}`")
            m = cur.fetchone()[0]
            if abs(m - n) > max(2, n // 20):  # kleine Abweichung ist normal (Sicherung ist ein paar Sekunden älter)
                diffs.append(f"{t}: {m} statt {n}")
        if got_tables < live_tables:
            return {"status": "error", "detail": f"Nach dem Einspielen nur {got_tables} von {live_tables} Tabellen."}
        if diffs:
            return {"status": "warn", "detail": "Einspielen klappt, aber Zeilenzahlen weichen ab: " + "; ".join(diffs)}
        return {"status": "ok", "detail": f"Probe-Wiederherstellung erfolgreich: {got_tables} Tabellen eingespielt und mit der Live-Datenbank verglichen."}
    except Exception as e:
        return {"status": "error", "detail": f"Probe-Wiederherstellung nicht möglich: {str(e)[:200]}"}
    finally:
        try:
            if conn:
                cur = conn.cursor()
                cur.execute(f"DROP DATABASE IF EXISTS `{scratch}`")
                conn.close()
        except Exception:
            pass


def verify_backup(path: str) -> dict:
    basic = plausibility_check(path)
    if basic["status"] == "error":
        return basic
    real = restore_test(path)
    return real if real else basic


# --- Status ---------------------------------------------------------------------------------------------

def save_status(**fields):
    cols = ", ".join(fields)
    marks = ", ".join(["%s"] * len(fields))
    upd = ", ".join(f"{k} = VALUES({k})" for k in fields)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f"INSERT INTO backup_status (id, {cols}) VALUES (1, {marks}) ON DUPLICATE KEY UPDATE {upd}", tuple(fields.values()))
    conn.commit()
    cur.close()
    conn.close()


def get_status() -> dict:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM backup_status WHERE id = 1")
    row = cur.fetchone() or {}
    cur.close()
    conn.close()
    return row


# --- Kopie an einen zweiten Ort (WebDAV, z.B. Nextcloud, NAS, Hetzner Storage Box) -----------------------

def get_offsite_settings() -> dict:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM backup_settings WHERE id = 1")
    row = cur.fetchone() or {}
    cur.close()
    conn.close()
    return row


def _check_url(url: str):
    u = (url or "").strip().rstrip("/")
    local = u.startswith(("http://127.0.0.1", "http://localhost"))
    if not (u.startswith("https://") or local):
        raise ValueError("Die Adresse muss mit https:// beginnen.")
    return u


def webdav_upload(local_path: str, remote_name: str, cfg: dict):
    import requests
    base = _check_url(cfg["url"])
    auth = (cfg["username"], decrypt_secret(cfg["password_enc"]) if cfg.get("password_enc") else "")
    url = f"{base}/{quote(remote_name)}"
    for attempt in (1, 2):
        with open(local_path, "rb") as f:
            r = requests.put(url, data=f, auth=auth, timeout=900)
        if r.status_code in (200, 201, 204):
            return
        if r.status_code in (404, 409) and attempt == 1:
            requests.request("MKCOL", base, auth=auth, timeout=30)  # Zielordner anlegen und nochmal versuchen
            continue
        raise RuntimeError(f"Der Speicher antwortet mit Fehler {r.status_code}.")


def test_offsite(cfg: dict) -> dict:
    import requests
    try:
        base = _check_url(cfg["url"])
        auth = (cfg["username"], decrypt_secret(cfg["password_enc"]) if cfg.get("password_enc") else "")
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as t:
            t.write("Verbindungstest Dienstbuch " + datetime.datetime.now().isoformat())
            tmp = t.name
        try:
            webdav_upload(tmp, "verbindungstest.txt", cfg)
            requests.delete(f"{base}/verbindungstest.txt", auth=auth, timeout=30)
        finally:
            os.remove(tmp)
        return {"ok": True, "detail": "Verbindung klappt - Testdatei wurde hochgeladen und wieder entfernt."}
    except Exception as e:
        return {"ok": False, "detail": str(e)[:200]}


def run_offsite_copy(dump_path: str, uploads_path: str = None) -> dict:
    cfg = get_offsite_settings()
    if not cfg or not cfg.get("enabled"):
        return {"status": "off", "detail": "Keine Kopie an einen zweiten Ort eingerichtet."}
    try:
        gz = dump_path + ".gz"
        with open(dump_path, "rb") as src, gzip.open(gz, "wb", compresslevel=6) as dst:
            shutil.copyfileobj(src, dst)
        try:
            webdav_upload(gz, os.path.basename(gz), cfg)
        finally:
            os.remove(gz)
        extra = ""
        if uploads_path and os.path.exists(uploads_path) and os.path.getsize(uploads_path) < 300 * 1024 * 1024:
            webdav_upload(uploads_path, os.path.basename(uploads_path), cfg)
            extra = " (mit Uploads)"
        return {"status": "ok", "detail": f"Kopie hochgeladen{extra}: {os.path.basename(gz)}"}
    except Exception as e:
        return {"status": "error", "detail": str(e)[:300]}


def after_backup(dump_path: str, uploads_path: str = None):
    """Wird nach jeder erfolgreichen SQL-Sicherung aufgerufen. Fehler hier dürfen die Sicherung nie gefährden."""
    now = datetime.datetime.now()
    size = os.path.getsize(dump_path)
    ver = verify_backup(dump_path)
    off = run_offsite_copy(dump_path, uploads_path)
    save_status(last_run=now, dump_file=os.path.basename(dump_path), size_bytes=size,
                verify_status=ver["status"], verify_detail=ver["detail"][:500], verify_at=now,
                offsite_status=off["status"], offsite_detail=off["detail"][:500], offsite_at=now)
    from core.notify import notify_admins
    if ver["status"] == "error":
        notify_admins("Sicherung lässt sich nicht einspielen", ver["detail"][:300])
    if off["status"] == "error":
        notify_admins("Kopie der Sicherung an den zweiten Ort fehlgeschlagen", off["detail"][:300])
    return ver, off

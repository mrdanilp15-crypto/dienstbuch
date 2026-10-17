"""Betrieb und Datenschutz: Sicherungs-Status, Kopie an zweiten Ort, Aufbewahrung/Anonymisierung."""
import os
from datetime import datetime

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from core import backup_tools, retention
from core.mailer import encrypt_secret
from core.security import get_int_setting, set_int_setting
from core.utils import check_auth, log_audit_action
from database import get_db_connection

router = APIRouter(prefix="/api/admin", tags=["ops"])


def _fmt(dt):
    return dt.strftime("%d.%m.%Y %H:%M") if dt else None


# --- Sicherung ------------------------------------------------------------------------------------------

@router.get("/backup-status")
def backup_status(request: Request):
    check_auth(request, require_admin=True)
    st = backup_tools.get_status()
    cfg = backup_tools.get_offsite_settings()
    cli, _ = backup_tools._mysql_cli()
    real_check = bool(backup_tools._root_password() and cli)
    files = []
    try:
        from routers.admin_api import _backup_dir
        d = _backup_dir()
        for f in sorted((f for f in os.listdir(d) if f.startswith(("db_backup_", "dienstbuch_backup_"))), reverse=True)[:5]:
            p = os.path.join(d, f)
            files.append({"name": f, "mb": round(os.path.getsize(p) / 1024 / 1024, 2), "at": _fmt(datetime.fromtimestamp(os.path.getmtime(p)))})
    except Exception:
        pass
    return {
        "status": {k: (_fmt(v) if isinstance(v, datetime) else v) for k, v in st.items()},
        "real_restore_check": real_check,
        "offsite": {"enabled": bool(cfg.get("enabled")), "url": cfg.get("url") or "", "username": cfg.get("username") or "", "has_password": bool(cfg.get("password_enc"))},
        "files": files,
    }


class OffsiteIn(BaseModel):
    enabled: bool = False
    url: str = ""
    username: str = ""
    password: str = ""  # leer = bisheriges behalten


def _merged_cfg(data: OffsiteIn) -> dict:
    old = backup_tools.get_offsite_settings()
    return {"enabled": 1 if data.enabled else 0, "url": data.url.strip(), "username": data.username.strip(),
            "password_enc": encrypt_secret(data.password) if data.password else (old.get("password_enc") or "")}


@router.put("/backup-settings")
def save_backup_settings(data: OffsiteIn, request: Request):
    user = check_auth(request, require_admin=True)
    cfg = _merged_cfg(data)
    if cfg["enabled"]:
        try:
            backup_tools._check_url(cfg["url"])
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        if not cfg["username"]:
            raise HTTPException(status_code=400, detail="Bitte einen Benutzernamen angeben.")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO backup_settings (id, enabled, url, username, password_enc) VALUES (1, %s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE enabled = VALUES(enabled), url = VALUES(url), username = VALUES(username), password_enc = VALUES(password_enc)",
                (cfg["enabled"], cfg["url"], cfg["username"], cfg["password_enc"]))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "BACKUP_ZIEL_GEAENDERT", f"Kopie an zweiten Ort: {'an' if cfg['enabled'] else 'aus'} ({cfg['url']})")
    return {"status": "success"}


@router.post("/backup-settings/test")
def test_backup_target(data: OffsiteIn, request: Request):
    check_auth(request, require_admin=True)
    return backup_tools.test_offsite(_merged_cfg(data))


@router.post("/backup/run-now")
def backup_run_now(request: Request):
    """Jetzt sichern, prüfen und (falls eingerichtet) kopieren - dauert je nach Datenmenge einen Moment."""
    user = check_auth(request, require_admin=True)
    from routers.admin_api import run_auto_backup
    result = run_auto_backup(user["username"])
    return {"result": result.get("status"), "status": {k: (_fmt(v) if isinstance(v, datetime) else v) for k, v in backup_tools.get_status().items()}}


# --- Datenschutz / Aufbewahrung -----------------------------------------------------------------------------

@router.get("/privacy")
def privacy_overview(request: Request):
    check_auth(request, require_admin=True)
    return {
        "settings": {"retention_audit_days": get_int_setting("retention_audit_days", retention.DEFAULT_AUDIT_DAYS, ttl=0),
                     "retention_ex_member_years": get_int_setting("retention_ex_member_years", retention.DEFAULT_EX_MEMBER_YEARS, ttl=0)},
        "candidates": retention.ex_member_candidates(),
    }


class PrivacySettings(BaseModel):
    retention_audit_days: int = 365
    retention_ex_member_years: int = 5


@router.put("/privacy-settings")
def save_privacy_settings(data: PrivacySettings, request: Request):
    user = check_auth(request, require_admin=True)
    if not (0 <= data.retention_audit_days <= 3650 and 0 <= data.retention_ex_member_years <= 50):
        raise HTTPException(status_code=400, detail="Ungültiger Wert.")
    if 0 < data.retention_audit_days < 30:
        raise HTTPException(status_code=400, detail="Das Protokoll sollte mindestens 30 Tage aufbewahrt werden.")
    set_int_setting("retention_audit_days", data.retention_audit_days)
    set_int_setting("retention_ex_member_years", data.retention_ex_member_years)
    log_audit_action(user["username"], "AUFBEWAHRUNG_GEAENDERT",
                     f"Protokoll: {data.retention_audit_days} Tage (0 = unbegrenzt), Vorschlag ausgeschiedene Mitglieder: nach {data.retention_ex_member_years} Jahren")
    return {"status": "success"}


@router.post("/privacy/anonymize/{personnel_id}")
def anonymize(personnel_id: int, request: Request):
    user = check_auth(request, require_admin=True)
    try:
        placeholder = retention.anonymize_person(personnel_id)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    log_audit_action(user["username"], "PERSON_ANONYMISIERT", f"Personenbezug von Personal-ID {personnel_id} entfernt (jetzt '{placeholder}').")
    return {"status": "success", "name": placeholder}


@router.post("/privacy/run-now")
def run_retention_now(request: Request):
    user = check_auth(request, require_admin=True)
    out = retention.run_retention(force=True)
    log_audit_action(user["username"], "AUFBEWAHRUNG_MANUELL", f"Bereinigung ausgeführt: {out}")
    return {"deleted": out}

from datetime import datetime

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from core.security import get_int_setting, set_int_setting
from core.utils import check_auth, log_audit_action
from database import get_db_connection

router = APIRouter(prefix="/api/admin", tags=["security"])

_FAILS = ("LOGIN_FEHLVERSUCH", "LOGIN_BENUTZER_UNBEKANNT", "LOGIN_2FA_FEHLGESCHLAGEN")


def _fmt(dt):
    return dt.strftime("%d.%m.%Y %H:%M") if dt else None


@router.get("/security")
def security_overview(request: Request):
    check_auth(request, require_admin=True)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    marks = ", ".join(["%s"] * len(_FAILS))
    cur.execute(f"SELECT COUNT(*) AS c FROM audit_log WHERE action IN ({marks}) AND created_at > NOW() - INTERVAL 1 DAY", _FAILS)
    failed_24h = cur.fetchone()["c"]
    cur.execute(f"SELECT COUNT(*) AS c FROM audit_log WHERE action IN ({marks}) AND created_at > NOW() - INTERVAL 7 DAY", _FAILS)
    failed_7d = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) AS c FROM audit_log WHERE action = 'LOGIN_IP_GESPERRT' AND created_at > NOW() - INTERVAL 7 DAY")
    ip_blocked = cur.fetchone()["c"]
    cur.execute("SELECT username, lockout_until FROM users WHERE lockout_until > NOW() ORDER BY username")
    locked = [{"username": r["username"], "until": _fmt(r["lockout_until"])} for r in cur.fetchall()]
    cur.execute("SELECT username, role FROM users WHERE role IN ('admin', 'leitung') AND (totp_enabled IS NULL OR totp_enabled = 0) ORDER BY role, username")
    no_2fa = cur.fetchall()
    cur.execute("SELECT username FROM users WHERE is_first_login = 1 ORDER BY username")
    pending = [r["username"] for r in cur.fetchall()]
    cur.execute("SELECT username, role, last_login FROM users WHERE last_login IS NOT NULL ORDER BY last_login DESC LIMIT 8")
    last_logins = [{"username": r["username"], "role": r["role"], "at": _fmt(r["last_login"])} for r in cur.fetchall()]
    cur.execute("""
        SELECT created_at, username, action, details FROM audit_log
        WHERE action LIKE 'PASSWORT%%' OR action LIKE 'KONTO%%' OR action LIKE 'ROLLE%%' OR action LIKE '2FA%%'
           OR action IN ('NUTZER_ANLEGEN', 'LOGIN_IP_GESPERRT', 'EINLADUNG_GESENDET')
        ORDER BY id DESC LIMIT 10
    """)
    events = [{"at": _fmt(r["created_at"]), "who": r["username"], "action": r["action"], "details": (r["details"] or "")[:140]} for r in cur.fetchall()]
    cur.close(); conn.close()
    return {
        "settings": {"require_2fa": get_int_setting("require_2fa", 0), "pwned_check": get_int_setting("pwned_check", 0)},
        "failed_24h": failed_24h, "failed_7d": failed_7d, "ip_blocked_7d": ip_blocked,
        "locked": locked, "no_2fa": no_2fa, "pending_invites": pending, "last_logins": last_logins, "events": events,
    }


class SecuritySettings(BaseModel):
    require_2fa: int = 0
    pwned_check: int = 0


@router.put("/security-settings")
def save_security_settings(data: SecuritySettings, request: Request):
    user = check_auth(request, require_admin=True)
    if data.require_2fa not in (0, 1, 2) or data.pwned_check not in (0, 1):
        raise HTTPException(status_code=400, detail="Ungültige Einstellung.")
    if data.require_2fa >= 1 and not user.get("totp"):
        # Sonst sperrt sich der Admin mit der eigenen Einstellung selbst aus (nur noch Lesen und die Einrichtung selbst bleiben erlaubt)
        raise HTTPException(status_code=400, detail="Richte bitte zuerst für dein eigenes Konto die Zwei-Faktor-Anmeldung ein (Mein Profil), bevor du sie zur Pflicht machst.")
    set_int_setting("require_2fa", data.require_2fa)
    set_int_setting("pwned_check", data.pwned_check)
    log_audit_action(user["username"], "SICHERHEIT_EINSTELLUNG",
                     f"2FA-Pflicht: Stufe {data.require_2fa}, Leak-Prüfung: {'an' if data.pwned_check else 'aus'}")
    return {"status": "success"}

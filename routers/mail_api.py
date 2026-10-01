import hashlib
import secrets
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from core.mailer import encrypt_secret, get_mail_settings, is_mail_configured, send_mail, send_mail_background
from core.utils import check_auth, hash_password, log_audit_action
from database import get_db_connection

router = APIRouter(tags=["Mail"])

_RESET_VALID_MINUTES = 30
_RESET_COOLDOWN_MINUTES = 5


class MailSettingsIn(BaseModel):
    enabled: bool = False
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_security: str = "starttls"
    smtp_user: str = ""
    smtp_password: Optional[str] = None  # leer = bisheriges Passwort behalten
    sender_address: str = ""
    sender_name: str = ""
    app_url: str = ""
    alarm_mail: bool = False
    reminder_mail: bool = True


@router.get("/api/admin/mail-settings")
def get_settings(request: Request):
    check_auth(request, require_admin=True)
    s = get_mail_settings()
    return {
        "enabled": bool(s.get("enabled")),
        "smtp_host": s.get("smtp_host") or "",
        "smtp_port": s.get("smtp_port") or 587,
        "smtp_security": s.get("smtp_security") or "starttls",
        "smtp_user": s.get("smtp_user") or "",
        "has_password": bool(s.get("smtp_password_enc")),
        "sender_address": s.get("sender_address") or "",
        "sender_name": s.get("sender_name") or "",
        "app_url": s.get("app_url") or "",
        "alarm_mail": bool(s.get("alarm_mail")),
        "reminder_mail": bool(s.get("reminder_mail")) if s else True,
    }


@router.put("/api/admin/mail-settings")
def save_settings(data: MailSettingsIn, request: Request):
    user = check_auth(request, require_admin=True)
    if data.smtp_security not in ("starttls", "ssl", "none"):
        raise HTTPException(status_code=400, detail="Ungültige Verschlüsselungsart.")
    if not 1 <= data.smtp_port <= 65535:
        raise HTTPException(status_code=400, detail="Ungültiger Port.")
    app_url = data.app_url.strip().rstrip("/")
    if app_url and not app_url.startswith(("https://", "http://")):
        raise HTTPException(status_code=400, detail="Die Adresse des Dienstbuchs muss mit https:// beginnen.")

    fields = {
        "enabled": data.enabled,
        "smtp_host": data.smtp_host.strip(),
        "smtp_port": data.smtp_port,
        "smtp_security": data.smtp_security,
        "smtp_user": data.smtp_user.strip(),
        "sender_address": data.sender_address.strip(),
        "sender_name": data.sender_name.strip(),
        "app_url": app_url,
        "alarm_mail": data.alarm_mail,
        "reminder_mail": data.reminder_mail,
    }
    if data.smtp_password:
        fields["smtp_password_enc"] = encrypt_secret(data.smtp_password)

    cols = ", ".join(fields)
    placeholders = ", ".join(["%s"] * len(fields))
    updates = ", ".join(f"{c} = VALUES({c})" for c in fields)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        f"INSERT INTO mail_settings (id, {cols}) VALUES (1, {placeholders}) ON DUPLICATE KEY UPDATE {updates}",
        tuple(fields.values()),
    )
    conn.commit()
    cur.close()
    conn.close()
    log_audit_action(user["username"], "MAIL_EINSTELLUNGEN", f"E-Mail-Versand {'aktiviert' if data.enabled else 'deaktiviert'} (Server {fields['smtp_host']}).")
    return {"status": "success"}


@router.post("/api/admin/mail-settings/test")
async def send_test_mail(request: Request):
    check_auth(request, require_admin=True)
    data = await request.json()
    to = (data.get("to") or "").strip()
    if "@" not in to:
        raise HTTPException(status_code=400, detail="Bitte eine gültige Empfängeradresse angeben.")
    try:
        send_mail([to], "Test-E-Mail vom Dienstbuch", "Der E-Mail-Versand des Dienstbuchs ist korrekt eingerichtet.")
    except Exception as e:
        # SMTP-Fehlermeldung bewusst durchreichen: nur Admins sehen sie, und sie ist zur Fehlersuche nötig
        raise HTTPException(status_code=400, detail=f"Versand fehlgeschlagen: {e}")
    return {"status": "success"}


@router.get("/api/auth/mail-enabled")
def mail_enabled():
    try:
        s = get_mail_settings()
        return {"enabled": is_mail_configured(s) and bool(s.get("app_url"))}
    except Exception:
        return {"enabled": False}


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


@router.post("/api/auth/forgot-password")
async def forgot_password(request: Request):
    data = await request.json()
    identifier = (data.get("identifier") or "").strip()[:255]
    # Immer dieselbe Antwort - sonst ließe sich ausprobieren, welche Benutzernamen/Adressen existieren.
    generic = {"status": "ok", "message": "Falls ein Konto mit dieser Angabe existiert und eine E-Mail-Adresse hinterlegt ist, wurde ein Link zum Zurücksetzen verschickt."}

    settings = get_mail_settings()
    # Link-Adresse kommt bewusst aus den Einstellungen, nicht aus dem Host-Header der Anfrage -
    # sonst könnte ein Angreifer Reset-Links auf eine fremde Domain umbiegen.
    app_url = (settings.get("app_url") or "").rstrip("/")
    if not identifier or not is_mail_configured(settings) or not app_url:
        return generic

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT u.id, u.username, p.email FROM users u
        JOIN personnel p ON p.id = u.personnel_id
        WHERE (u.username = %s OR LOWER(p.email) = LOWER(%s)) AND p.email IS NOT NULL AND p.email != ''
    """, (identifier, identifier))
    accounts = cur.fetchall()

    for acc in accounts:
        cur.execute(
            "SELECT id FROM password_resets WHERE user_id = %s AND created_at > NOW() - INTERVAL %s MINUTE",
            (acc["id"], _RESET_COOLDOWN_MINUTES),
        )
        if cur.fetchone():
            continue
        token = secrets.token_urlsafe(32)
        cur.execute(
            "INSERT INTO password_resets (user_id, token_hash, expires_at) VALUES (%s, %s, NOW() + INTERVAL %s MINUTE)",
            (acc["id"], _token_hash(token), _RESET_VALID_MINUTES),
        )
        conn.commit()
        body = (
            f"Hallo,\n\nfür das Konto \"{acc['username']}\" wurde ein neues Passwort angefordert.\n\n"
            f"Über diesen Link kannst du innerhalb von {_RESET_VALID_MINUTES} Minuten ein neues Passwort festlegen:\n"
            f"{app_url}/reset-password?token={token}\n\n"
            "Falls du das nicht warst, kannst du diese E-Mail ignorieren - dein Passwort bleibt unverändert."
        )
        send_mail_background([acc["email"]], "Passwort zurücksetzen - Dienstbuch", body)
        log_audit_action(acc["username"], "PASSWORT_RESET_ANGEFORDERT", "Link zum Zurücksetzen per E-Mail verschickt.")

    cur.close()
    conn.close()
    return generic


@router.post("/api/auth/reset-password")
async def reset_password(request: Request):
    data = await request.json()
    token = (data.get("token") or "").strip()
    password = data.get("password") or ""
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Passwort muss mindestens 8 Zeichen lang sein!")

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT r.user_id, u.username FROM password_resets r JOIN users u ON u.id = r.user_id "
        "WHERE r.token_hash = %s AND r.used = 0 AND r.expires_at > NOW()",
        (_token_hash(token),),
    )
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        raise HTTPException(status_code=400, detail="Der Link ist ungültig oder abgelaufen. Bitte neu anfordern.")

    cur.execute(
        "UPDATE users SET password_hash = %s, failed_logins = 0, lockout_until = NULL, is_first_login = 0 WHERE id = %s",
        (hash_password(password), row["user_id"]),
    )
    cur.execute("UPDATE password_resets SET used = 1 WHERE user_id = %s", (row["user_id"],))
    conn.commit()
    cur.close()
    conn.close()
    log_audit_action(row["username"], "PASSWORT_ZURUECKGESETZT", "Passwort über E-Mail-Link neu gesetzt.")
    return {"status": "success"}

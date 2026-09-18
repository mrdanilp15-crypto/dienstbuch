import time
import hashlib
import json
import secrets
from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel
from datetime import datetime, timedelta

from database import get_db_connection
from core.utils import (log_audit_action, verify_password, hash_password, needs_password_rehash,
                        create_session_token, get_current_user, invalidate_role_cache,
                        get_session_max_days, create_pending_2fa_token, verify_pending_2fa_token)

router = APIRouter()

# Für die Timing-Angriff-Prävention bei unbekannten Benutzernamen (siehe api_login unten) -
# ein fest vorberechneter Argon2id-Hash eines beliebigen Platzhalter-Passworts, NICHT das
# Passwort eines echten Kontos.
_DUMMY_ARGON2_HASH = "$argon2id$v=19$m=65536,t=3,p=4$WywStsVMq8DmjLcR1ka19w$pyMguHHW9K6BLyMGqvyrfUlbaOYRkV/A7llm6hECBX8"

class LoginRequest(BaseModel):
    username: str
    password: str

def _issue_session(user: dict, response: Response, request: Request) -> dict:
    """Setzt das Session-Cookie und liefert die Login-Antwort - gemeinsam genutzt vom
    direkten Login (kein 2FA) und von /api/login/2fa (nach erfolgreichem zweiten Faktor),
    damit beide Wege exakt dieselbe Session erzeugen."""
    token = create_session_token(user['username'], user['role'])
    is_https = request.url.scheme == "https" or request.headers.get("x-forwarded-proto", "").lower() == "https"
    response.set_cookie(key="session_token", value=token, httponly=True, max_age=86400 * get_session_max_days(), samesite="lax", secure=is_https)
    log_audit_action(user['username'], "LOGIN", "Erfolgreich eingeloggt.")
    return {"status": "success", "username": user['username'], "role": user['role'], "is_first_login": bool(user['is_first_login']), "redirect": "/dashboard"}

# Die bestehende Konto-Sperre (5 Fehlversuche -> 15 Min) schützt nur PRO BENUTZERNAME - jemand,
# der von einer Adresse aus viele VERSCHIEDENE Benutzernamen durchprobiert (Enumeration/verteilter
# Brute-Force), würde davon nie erfasst. Zusätzliche, IP-basierte Bremse dagegen. Bewusst ein
# einfacher In-Memory-Zähler statt einer neuen Abhängigkeit (Redis/slowapi) - reicht für diese
# Single-Instance-Installation, setzt sich bei jedem Neustart zurück (kein Problem, da es nur ein
# zusätzliches Bremsschild ist, kein alleiniger Schutzmechanismus).
_ip_login_attempts = {}  # ip -> [Zeitstempel fehlgeschlagener Versuche]
_IP_RATE_LIMIT_WINDOW = 900  # 15 Minuten
_IP_RATE_LIMIT_MAX = 20  # Fehlversuche pro IP in diesem Zeitfenster

def _get_client_ip(request: Request) -> str:
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else "unknown"

def _ip_rate_limited(ip: str) -> bool:
    now = time.time()
    attempts = [t for t in _ip_login_attempts.get(ip, []) if now - t < _IP_RATE_LIMIT_WINDOW]
    _ip_login_attempts[ip] = attempts
    return len(attempts) >= _IP_RATE_LIMIT_MAX

def _record_ip_failure(ip: str):
    _ip_login_attempts.setdefault(ip, []).append(time.time())

@router.post("/api/login")
def api_login(data: LoginRequest, response: Response, request: Request):
    client_ip = _get_client_ip(request)
    if _ip_rate_limited(client_ip):
        log_audit_action("SYSTEM", "LOGIN_IP_GESPERRT", f"Zu viele Fehlversuche von IP '{client_ip}' - vorübergehend blockiert.")
        raise HTTPException(status_code=429, detail="Zu viele Anmeldeversuche von dieser Adresse. Bitte später erneut versuchen.")
    username_clean = data.username.strip()
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE LOWER(username) = LOWER(%s)", (username_clean,))
    user = cur.fetchone()
    
    if user:
        if user["lockout_until"] and datetime.now() < user["lockout_until"]:
            cur.close(); conn.close()
            remaining = (user["lockout_until"] - datetime.now()).seconds // 60 + 1
            log_audit_action("SYSTEM", "LOGIN_VERSUCH_GESPERRT", f"Anmeldeversuch auf gesperrtes Konto '{username_clean}'.")
            raise HTTPException(status_code=423, detail=f"Konto gesperrt. Bitte in {remaining} Min. versuchen.")
            
        if verify_password(user['password_hash'], data.password):
            # Stilles Umstellen auf Argon2id, sobald das Konto sich das nächste Mal erfolgreich
            # anmeldet - der alte PBKDF2-Hash bleibt bis dahin über verify_password() gültig,
            # niemand muss sein Passwort deswegen ändern.
            if needs_password_rehash(user['password_hash']):
                cur.execute("UPDATE users SET password_hash = %s, failed_logins = 0, lockout_until = NULL, last_login = NOW() WHERE id = %s",
                           (hash_password(data.password), user["id"]))
            else:
                cur.execute("UPDATE users SET failed_logins = 0, lockout_until = NULL, last_login = NOW() WHERE id = %s", (user["id"],))
            conn.commit(); cur.close(); conn.close()

            if user.get("totp_enabled"):
                # Passwort war richtig, aber der zweite Faktor fehlt noch - bewusst NOCH KEIN
                # Session-Cookie setzen (siehe create_pending_2fa_token: das Zwischen-Token ist
                # ausdrücklich NICHT als Session verwendbar).
                log_audit_action(user['username'], "LOGIN_2FA_AUSSTEHEND", "Passwort korrekt, warte auf TOTP-Code.")
                return {"status": "2fa_required", "temp_token": create_pending_2fa_token(user['username'])}

            return _issue_session(user, response, request)
        else:
            _record_ip_failure(client_ip)
            failed = user["failed_logins"] + 1
            lockout = datetime.now() + timedelta(minutes=15) if failed >= 5 else None
            cur.execute("UPDATE users SET failed_logins = %s, lockout_until = %s WHERE id = %s", (failed, lockout, user["id"],))
            conn.commit(); cur.close(); conn.close()
            if failed >= 5:
                log_audit_action("SYSTEM", "KONTO_GESPERRT", f"Konto '{username_clean}' wegen zu vieler Fehllogins für 15 Min. gesperrt.")
                raise HTTPException(status_code=423, detail="Konto wegen zu vieler Fehllogins für 15 Min. gesperrt.")
            log_audit_action("SYSTEM", "LOGIN_FEHLVERSUCH", f"Falsches Passwort für Benutzer '{username_clean}' ({failed}/5).")
            # Bewusst dieselbe generische Meldung wie im "Benutzername existiert nicht"-Zweig
            # unten (siehe dort für die Begründung) - nur die Zähler-Info bleibt drin, die
            # verrät nichts über die Existenz eines Kontos.
            raise HTTPException(status_code=401, detail=f"Benutzername oder Passwort falsch! ({failed}/5)")
    else:
        cur.close(); conn.close()
        # Absichtlich dieselbe Meldung wie "falsches Passwort" oben (statt z.B. "Benutzername
        # existiert nicht") UND dieselbe PBKDF2-Rechenzeit wie eine echte Passwortprüfung
        # (siehe verify_password-Dummy-Aufruf) - sonst ließe sich über Fehlertext und/oder
        # Antwortzeit-Unterschied durchprobieren, welche Benutzernamen im System existieren
        # (OWASP Authentication Cheat Sheet: User Enumeration Prevention).
        # Fest vorberechneter Argon2id-Hash statt eines PBKDF2-Platzhalters: seit hash_password()
        # standardmäßig Argon2id verwendet (siehe core/utils.py), hätte ein PBKDF2-Dummy hier
        # eine ANDERE Rechenzeit als eine echte Prüfung gegen ein modernes Konto - genau der
        # Timing-Unterschied, den dieser Aufruf eigentlich verhindern soll.
        verify_password(_DUMMY_ARGON2_HASH, data.password)
        _record_ip_failure(client_ip)
        log_audit_action("SYSTEM", "LOGIN_BENUTZER_UNBEKANNT", f"Anmeldeversuch mit nicht existierendem Namen '{username_clean}'.")
        raise HTTPException(status_code=401, detail="Benutzername oder Passwort falsch!")

def _hash_recovery_code(code: str) -> str:
    return hashlib.sha256(code.strip().upper().replace("-", "").replace(" ", "").encode()).hexdigest()

def _generate_recovery_codes(n: int = 8):
    """8-stellige Einmal-Codes für den Fall, dass das Zweitgerät verloren geht oder kaputt
    ist. Rückgabe: (Klartext-Liste zum einmaligen Anzeigen, gehashte Liste zum Speichern)."""
    raw = [secrets.token_hex(4).upper() for _ in range(n)]
    formatted = [f"{c[:4]}-{c[4:]}" for c in raw]
    return formatted, [_hash_recovery_code(c) for c in formatted]

@router.post("/api/login/2fa")
def api_login_2fa(data: dict, response: Response, request: Request):
    """Zweiter Anmeldeschritt: Benutzername+Passwort waren in /api/login bereits korrekt
    (siehe temp_token), hier kommt nur noch der TOTP- oder ein Wiederherstellungscode."""
    client_ip = _get_client_ip(request)
    if _ip_rate_limited(client_ip):
        raise HTTPException(status_code=429, detail="Zu viele Versuche von dieser Adresse. Bitte später erneut versuchen.")

    username = verify_pending_2fa_token(data.get("temp_token") or "")
    code = (data.get("code") or "").strip()
    if not username:
        _record_ip_failure(client_ip)
        raise HTTPException(status_code=401, detail="Anmeldevorgang abgelaufen. Bitte erneut mit Benutzername und Passwort anmelden.")

    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE username = %s", (username,))
    user = cur.fetchone()
    if not user or not user.get("totp_enabled") or not user.get("totp_secret"):
        cur.close(); conn.close()
        raise HTTPException(status_code=400, detail="Zwei-Faktor-Authentifizierung ist für dieses Konto nicht aktiv.")

    import pyotp
    # valid_window=1: akzeptiert auch den vorherigen/nächsten 30s-Schritt, sonst scheitert
    # der Code regelmäßig an einer leicht abweichenden Geräteuhr oder normaler Tippzeit.
    ok = bool(code) and pyotp.TOTP(user["totp_secret"]).verify(code, valid_window=1)

    if not ok and code:
        codes = json.loads(user.get("totp_recovery_codes") or "[]")
        code_hash = _hash_recovery_code(code)
        if code_hash in codes:
            ok = True
            codes.remove(code_hash)
            cur.execute("UPDATE users SET totp_recovery_codes = %s WHERE username = %s", (json.dumps(codes), username))
            conn.commit()
            log_audit_action(username, "2FA_RECOVERY_CODE_VERWENDET", f"Wiederherstellungscode verbraucht, {len(codes)} verbleibend.")

    if not ok:
        _record_ip_failure(client_ip)
        cur.close(); conn.close()
        log_audit_action(username, "LOGIN_2FA_FEHLGESCHLAGEN", "Falscher TOTP-/Wiederherstellungscode.")
        raise HTTPException(status_code=401, detail="Code falsch oder abgelaufen.")

    cur.execute("UPDATE users SET failed_logins = 0, lockout_until = NULL, last_login = NOW() WHERE username = %s", (username,))
    conn.commit(); cur.close(); conn.close()
    return _issue_session(user, response, request)

@router.get("/api/auth/2fa/status")
def get_2fa_status(request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT totp_enabled FROM users WHERE username = %s", (user["username"],))
    row = cur.fetchone(); cur.close(); conn.close()
    return {"enabled": bool(row and row["totp_enabled"])}

@router.post("/api/auth/2fa/setup")
def setup_2fa(request: Request):
    """Erzeugt ein neues, noch NICHT aktives Geheimnis - erst ein gültiger Code in /confirm
    schaltet es scharf. Ohne diesen Zwischenschritt könnte ein Tippfehler beim Einscannen des
    QR-Codes (z.B. falsche Zeitzone der Authenticator-App) das eigene Konto beim nächsten
    Login aussperren, ohne dass man es vorher merkt."""
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    import pyotp
    secret = pyotp.random_base32()
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("UPDATE users SET totp_secret = %s, totp_enabled = 0 WHERE username = %s", (secret, user["username"]))
    conn.commit(); cur.close(); conn.close()
    from core.utils import get_station_name
    otpauth_url = pyotp.TOTP(secret).provisioning_uri(name=user["username"], issuer_name=get_station_name())
    log_audit_action(user["username"], "2FA_SETUP_GESTARTET", "Neues TOTP-Geheimnis erzeugt (noch nicht bestätigt).")
    return {"secret": secret, "otpauth_url": otpauth_url}

@router.post("/api/auth/2fa/confirm")
def confirm_2fa(data: dict, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    code = (data.get("code") or "").strip()
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT totp_secret FROM users WHERE username = %s", (user["username"],))
    row = cur.fetchone()
    if not row or not row["totp_secret"]:
        cur.close(); conn.close()
        raise HTTPException(status_code=400, detail="Zuerst die Einrichtung starten.")
    import pyotp
    if not code or not pyotp.TOTP(row["totp_secret"]).verify(code, valid_window=1):
        cur.close(); conn.close()
        raise HTTPException(status_code=400, detail="Code falsch. Bitte erneut versuchen.")
    codes, hashed = _generate_recovery_codes()
    cur.execute("UPDATE users SET totp_enabled = 1, totp_recovery_codes = %s WHERE username = %s",
               (json.dumps(hashed), user["username"]))
    conn.commit(); cur.close(); conn.close()
    invalidate_role_cache(user["username"])
    log_audit_action(user["username"], "2FA_AKTIVIERT", "Zwei-Faktor-Authentifizierung aktiviert.")
    return {"status": "success", "recovery_codes": codes}

@router.post("/api/auth/2fa/disable")
def disable_2fa(data: dict, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT password_hash FROM users WHERE username = %s", (user["username"],))
    row = cur.fetchone()
    if not row or not verify_password(row["password_hash"], data.get("password") or ""):
        cur.close(); conn.close()
        raise HTTPException(status_code=400, detail="Passwort nicht korrekt.")
    cur.execute("UPDATE users SET totp_enabled = 0, totp_secret = NULL, totp_recovery_codes = NULL WHERE username = %s", (user["username"],))
    conn.commit(); cur.close(); conn.close()
    invalidate_role_cache(user["username"])
    log_audit_action(user["username"], "2FA_DEAKTIVIERT", "Zwei-Faktor-Authentifizierung deaktiviert.")
    return {"status": "success"}

@router.get("/api/auth/me")
def api_auth_me(request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
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

@router.post("/api/logout")
def api_logout(response: Response):
    response.delete_cookie("session_token", path="/")
    return {"status": "success"}

@router.put("/api/auth/change-password")
def user_change_self_password(data: dict, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    old_pw = data.get("old_password")
    new_pw = data.get("new_password")
    
    if not old_pw or not new_pw or len(new_pw.strip()) < 8:
        raise HTTPException(status_code=400, detail="Eingaben ungültig oder Passwort zu kurz (mind. 8 Zeichen)!")
        
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT password_hash FROM users WHERE username = %s", (user["username"],))
    db_user = cur.fetchone()
    
    if not db_user or not verify_password(db_user['password_hash'], old_pw):
        cur.close(); conn.close()
        raise HTTPException(status_code=400, detail="Das aktuelle Passwort ist nicht korrekt!")
        
    new_hash = hash_password(new_pw.strip())
    cur.execute("UPDATE users SET password_hash = %s, is_first_login = 0 WHERE username = %s", (new_hash, user["username"]))
    conn.commit(); cur.close(); conn.close()
    invalidate_role_cache(user["username"])
    log_audit_action(user["username"], "PASSWORT_ÄNDERUNG", "Eigenes Passwort erfolgreich aktualisiert.")
    return {"status": "success"}

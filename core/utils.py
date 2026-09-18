import hashlib
import secrets
import hmac
import base64
import json
import time
import os
from fastapi import Request, HTTPException
from typing import Optional
from database import get_db_connection

def _get_or_create_secret_key() -> str:
    """
    Liefert den Signier-Schlüssel für Session-Tokens.
    Wird SECRET_KEY nicht per Umgebungsvariable gesetzt, generiert und
    persistiert das System automatisch einen zufälligen Schlüssel (statt eines
    im Quellcode sichtbaren, für jede Installation identischen Fallback-Werts).
    So bleiben Sessions über Neustarts hinweg gültig, aber der Schlüssel ist
    nicht mehr öffentlich bekannt/rätbar.
    """
    env_key = os.getenv("SECRET_KEY")
    if env_key:
        return env_key

    # Im persistenten Docker-Volume ablegen (/app/data, siehe docker-compose.yml),
    # sonst würde jeder Redeploy den Schlüssel neu würfeln und alle Logins invalidieren.
    data_dir = "/app/data" if os.path.exists("/app/data") else os.getcwd()
    key_path = os.path.join(data_dir, "secret.key")
    try:
        if os.path.exists(key_path):
            with open(key_path, "r") as f:
                existing = f.read().strip()
                if existing:
                    return existing
        new_key = secrets.token_hex(32)
        with open(key_path, "w") as f:
            f.write(new_key)
        return new_key
    except Exception as e:
        print(f"WARNUNG: Konnte secret.key nicht lesen/schreiben ({e}). Nutze einen nur für diesen Prozesslauf gültigen Schlüssel.")
        return secrets.token_hex(32)


SECRET_KEY = _get_or_create_secret_key()

def get_station_name() -> str:
    """
    Liefert den Wehr-/Ortsnamen aus der in der Software gepflegten
    Standortverwaltung (station_settings), nicht aus der TOWN_NAME-Umgebungsvariable.
    So landet überall (Dashboard, Hallenmonitor, Dienstberichte, PDFs,
    Arbeitgeberbescheinigung) derselbe Name, den ein Admin einmal in den
    Einstellungen einträgt - TOWN_NAME wird dann nur noch als Startwert für die
    allererste Installation gebraucht, nicht mehr für den laufenden Betrieb.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT station_name FROM station_settings ORDER BY id ASC LIMIT 1")
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row and row[0]:
            return row[0]
    except Exception as e:
        print(f"Konnte station_name nicht laden: {e}")
    return os.getenv("TOWN_NAME", "Deine Feuerwehr")

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

_LEGACY_PBKDF2_ITERATIONS = 100_000  # Format alter Hashes ("salt:hash"), implizit diese Rundenzahl
_PBKDF2_ITERATIONS = 600_000  # OWASP Password Storage Cheat Sheet (2023): PBKDF2-HMAC-SHA256 >= 600.000

def _get_argon2_hasher():
    from argon2 import PasswordHasher
    # Parametrisierung entspricht den OWASP-Empfehlungen (2023) für Argon2id auf einem
    # gewöhnlichen Server ohne dedizierte GPU-Härtung: 19 MiB waren der alte PasswordHasher-
    # Standard, das hier folgt stattdessen den aktuell empfohlenen Mindestwerten.
    return PasswordHasher(time_cost=3, memory_cost=65536, parallelism=4)

def hash_password(password: str) -> str:
    # Argon2id ist seit 2023 der von OWASP empfohlene Standard für Passwort-Hashing (gewinnt
    # gegen GPU-/ASIC-gestützte Angriffe, wo PBKDF2 rein rechenzeitbasiert bleibt). Neue Hashes
    # verwenden deshalb Argon2id; alte PBKDF2-Hashes bleiben über verify_password() weiterhin
    # gültig und werden beim nächsten erfolgreichen Login automatisch umgestellt (siehe
    # needs_password_rehash() + deren Verwendung in routers/auth_mgr.py).
    return _get_argon2_hasher().hash(password)

def needs_password_rehash(stored_password: str) -> bool:
    """True, wenn stored_password noch im alten PBKDF2-Format vorliegt und beim nächsten
    erfolgreichen Login durch einen frischen Argon2id-Hash ersetzt werden sollte."""
    return not stored_password.startswith("$argon2id$")

def verify_password(stored_password: str, provided_password: str) -> bool:
    try:
        if stored_password.startswith("$argon2id$"):
            from argon2 import PasswordHasher
            from argon2.exceptions import VerifyMismatchError, InvalidHash
            try:
                return _get_argon2_hasher().verify(stored_password, provided_password)
            except (VerifyMismatchError, InvalidHash):
                return False

        parts = stored_password.split(":")
        if len(parts) == 3:
            iterations, salt, stored_hash = int(parts[0]), parts[1], parts[2]
        else:
            iterations, salt, stored_hash = _LEGACY_PBKDF2_ITERATIONS, parts[0], parts[1]
        hash_value = hashlib.pbkdf2_hmac('sha256', provided_password.encode(), salt.encode(), iterations)
        # hmac.compare_digest statt "==": zeitkonstanter Vergleich, verhindert Timing-Angriffe
        # (bei "==" bricht der String-Vergleich beim ersten abweichenden Zeichen ab, wodurch die
        # Antwortzeit theoretisch Rückschlüsse auf korrekte Hash-Präfixe zulassen könnte).
        return hmac.compare_digest(hash_value.hex(), stored_hash)
    except Exception:
        return False

def create_session_token(username: str, role: str) -> str:
    payload = {"username": username, "role": role, "ts": time.time()}
    payload_b64 = base64.b64encode(json.dumps(payload).encode()).decode()
    signature = hmac.new(SECRET_KEY.encode(), payload_b64.encode(), hashlib.sha256).hexdigest()
    return f"{payload_b64}.{signature}"

_PENDING_2FA_TTL_SECONDS = 300  # 5 Minuten, um den zweiten Faktor einzugeben

def create_pending_2fa_token(username: str) -> str:
    """Zwischen-Token für den zweiten Anmeldeschritt (TOTP-Code): bestätigt, dass Benutzername
    UND Passwort bereits korrekt waren, aber NOCH KEINE Session - dieses Token landet nie in
    einem Cookie und wird von get_current_user() nicht akzeptiert (eigenes 'purpose'-Feld),
    kann also nicht mit einer echten Session verwechselt werden. Kurze Gültigkeit, damit ein
    abgefangenes Zwischen-Token nicht beliebig lange für Brute-Force gegen den TOTP-Code
    nutzbar ist."""
    payload = {"purpose": "2fa_pending", "username": username, "ts": time.time()}
    payload_b64 = base64.b64encode(json.dumps(payload).encode()).decode()
    signature = hmac.new(SECRET_KEY.encode(), payload_b64.encode(), hashlib.sha256).hexdigest()
    return f"{payload_b64}.{signature}"

def verify_pending_2fa_token(token: str) -> Optional[str]:
    """Prüft ein Zwischen-Token aus create_pending_2fa_token() und liefert den Benutzernamen
    zurück, oder None bei ungültiger Signatur, falschem 'purpose' oder Ablauf."""
    try:
        payload_b64, signature = token.split(".", 1)
        expected = hmac.new(SECRET_KEY.encode(), payload_b64.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected):
            return None
        payload = json.loads(base64.b64decode(payload_b64))
        if payload.get("purpose") != "2fa_pending":
            return None
        if time.time() - payload.get("ts", 0) > _PENDING_2FA_TTL_SECONDS:
            return None
        return payload.get("username")
    except Exception:
        return None

_role_cache = {}  # username -> ((role, is_first_login) | None, cached_at) - None = Konto existiert nicht (mehr)
_ROLE_CACHE_TTL = 15  # Sekunden

_session_max_days_cache = None  # (value, cached_at) - vermeidet einen DB-Query pro Request
_SESSION_MAX_DAYS_CACHE_TTL = 300  # Sekunden

def get_session_max_days() -> int:
    """Liest die konfigurierbare Sitzungsdauer (Einstellungen -> 'session_max_days') mit
    kurzem Cache, da get_current_user() das bei praktisch jedem Request aufruft. Wird sowohl
    beim Login (Cookie max_age) als auch bei jeder Token-Prüfung (Ablauf-Check) verwendet -
    beide MÜSSEN denselben Wert nutzen, sonst könnte das Cookie länger leben als der Token
    gültig ist (oder umgekehrt vorzeitig ablaufen, obwohl das Cookie noch da ist)."""
    global _session_max_days_cache
    now = time.time()
    if _session_max_days_cache and now - _session_max_days_cache[1] < _SESSION_MAX_DAYS_CACHE_TTL:
        return _session_max_days_cache[0]
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT setting_value FROM settings WHERE setting_key = 'session_max_days'")
        row = cur.fetchone()
        cur.close()
        conn.close()
        value = row[0] if row and row[0] else 30
    except Exception:
        value = _session_max_days_cache[0] if _session_max_days_cache else 30
    _session_max_days_cache = (value, now)
    return value

def invalidate_role_cache(username: str):
    """Vom Admin-Bereich aufrufen, wenn Rolle/Passwort geändert oder Konto gelöscht wird,
    damit das sofort greift statt bis zu _ROLE_CACHE_TTL Sekunden zu warten."""
    _role_cache.pop(username, None)

def _get_live_user_info(username: str):
    """
    Fragt Rolle + is_first_login direkt aus der DB ab (kurz gecacht, damit nicht
    jeder der vielen parallelen Fetches pro Seitenaufruf einen eigenen DB-Query
    auslöst). Gibt None zurück, wenn der Account nicht mehr existiert.
    """
    now = time.time()
    cached = _role_cache.get(username)
    if cached and now - cached[1] < _ROLE_CACHE_TTL:
        return cached[0]
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT role, is_first_login FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        info = (row[0], bool(row[1])) if row else None
        _role_cache[username] = (info, now)
        return info
    except Exception as e:
        print(f"Konnte aktuelle Rolle nicht prüfen (Session bleibt vorerst gültig): {e}")
        # DB gerade nicht erreichbar: nicht jeden aussperren, altem Token vertrauen.
        return cached[0] if cached else "__db_unavailable__"

# Diese API-Pfade bleiben während eines erzwungenen Passwortwechsels (is_first_login)
# erlaubt - alles andere wird für schreibende Requests blockiert (siehe unten).
_FIRST_LOGIN_ALLOWED_PATHS = {"/api/auth/change-password", "/api/auth/me", "/api/logout"}

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
        if time.time() - data.get("ts", 0) > 86400 * get_session_max_days():
            return None

        # WICHTIG: Rolle live gegen die DB prüfen statt dem (bis zu 30 Tage alten)
        # Cookie-Inhalt blind zu vertrauen. Sonst würden gelöschte Accounts oder
        # entzogene/geänderte Rollen erst nach dem nächsten Login wirksam - bis
        # dahin hätte die Person mit ihrem alten Cookie weiter vollen Zugriff.
        info = _get_live_user_info(data.get("username", ""))
        if info is None:
            return None  # Account existiert nicht mehr -> Session ungültig
        if info != "__db_unavailable__":
            live_role, is_first_login = info
            data["role"] = live_role
            # Der erzwungene Passwortwechsel bei Erstanmeldung/-Reset war bisher nur eine
            # UI-Sperre im Frontend - wer den (oft bekannten) Default wie admin/admin123
            # kannte, konnte an der Oberfläche vorbei direkt per API arbeiten, ohne je das
            # Passwort ändern zu müssen. Deshalb hier zusätzlich serverseitig durchsetzen:
            # schreibende Requests (POST/PUT/DELETE) außerhalb weniger Ausnahmen werden
            # blockiert, bis das Passwort geändert wurde. Lesende Requests bleiben erlaubt,
            # damit das Dashboard hinter dem blockierenden Modal normal weiterlädt.
            if is_first_login and request.method != "GET" and request.url.path not in _FIRST_LOGIN_ALLOWED_PATHS:
                return None
        return data
    except Exception:
        return None

def check_auth(request: Request, require_admin: bool = False, allowed_roles: tuple = None) -> dict:
    """Gemeinsamer Auth+Rollen-Prüfhelfer - war zuvor identisch in material_mgr.py, mission_mgr.py
    und personnel_mgr.py dupliziert (3 unabhängige Kopien, Risiko dass ein künftiger Fix nicht
    überall ankommt). Jetzt eine gemeinsame Stelle, die drei Router importieren nur noch von hier."""
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht angemeldet")
    if require_admin and user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Keine Berechtigung (Admin erforderlich)")
    if allowed_roles and user["role"] not in allowed_roles:
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    return user

# Zwei Geräte-/Feed-Zugänge müssen OHNE Login funktionieren (Hallen-Display-Kiosk, externe
# Kalender-Apps können keine Session-Cookies senden), lieferten ihre Daten deshalb bisher komplett
# offen für jeden im Internet aus (Einsatz-Rückmeldungen mit Klarnamen, Fahrzeugstatus, geplante
# Dienste). Statt echtem Login ein geheimer, zufälliger Token pro Zweck - wie beim "geheimen
# iCal-Link" von Google Kalender: wer den Link/Token nicht kennt, kommt nicht rein; wer angemeldet
# ist, braucht ihn gar nicht erst (siehe check_display_access).
_TOKEN_COLUMNS = {"display_token", "calendar_token"}

def get_or_create_token(token_name: str) -> str:
    if token_name not in _TOKEN_COLUMNS:
        raise ValueError(f"Unbekannter Token-Name: {token_name}")
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(f"SELECT id, {token_name} FROM station_settings ORDER BY id ASC LIMIT 1")
    except Exception:
        conn.rollback()
        cur.execute(f"ALTER TABLE station_settings ADD COLUMN {token_name} VARCHAR(64) NULL")
        conn.commit()
        cur.execute(f"SELECT id, {token_name} FROM station_settings ORDER BY id ASC LIMIT 1")
    row = cur.fetchone()
    token = row.get(token_name) if row else None
    if not token:
        token = secrets.token_urlsafe(32)
        if row:
            cur.execute(f"UPDATE station_settings SET {token_name} = %s WHERE id = %s", (token, row["id"]))
        else:
            cur.execute(f"INSERT INTO station_settings (station_name, lat, lng, zoom, {token_name}) VALUES (%s, 50.1109, 8.6821, 14, %s)", ("Feuerwehr", token))
        conn.commit()
    cur.close(); conn.close()
    return token

def regenerate_token(token_name: str) -> str:
    """Für den 'Link/Token neu erzeugen'-Button in der Verwaltung - z.B. falls ein Hallen-Display-
    Link mal in falsche Hände geraten ist. Macht alle bisher ausgegebenen Links für diesen Zweck
    sofort ungültig."""
    if token_name not in _TOKEN_COLUMNS:
        raise ValueError(f"Unbekannter Token-Name: {token_name}")
    new_token = secrets.token_urlsafe(32)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f"UPDATE station_settings SET {token_name} = %s ORDER BY id ASC LIMIT 1", (new_token,))
    conn.commit(); cur.close(); conn.close()
    return new_token

def check_display_access(request: Request) -> None:
    """Für Endpunkte, die sowohl vom (angemeldeten) Dashboard als auch vom nicht angemeldeten
    Hallen-Display-Kiosk aufgerufen werden: lässt entweder eine gültige Session ODER einen
    gültigen ?token=... Query-Parameter durch. Wirft 401, wenn keins von beidem passt."""
    if get_current_user(request):
        return
    token = request.query_params.get("token")
    if token and hmac.compare_digest(token, get_or_create_token("display_token")):
        return
    raise HTTPException(status_code=401, detail="Nicht angemeldet und kein gültiger Display-Token.")

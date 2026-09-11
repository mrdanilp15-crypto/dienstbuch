import hashlib
import secrets
import hmac
import base64
import json
import time
import os
from fastapi import Request
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

def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    hash_value = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
    return f"{salt}:{hash_value.hex()}"

def verify_password(stored_password: str, provided_password: str) -> bool:
    try:
        salt, stored_hash = stored_password.split(":")
        hash_value = hashlib.pbkdf2_hmac('sha256', provided_password.encode(), salt.encode(), 100000)
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

_role_cache = {}  # username -> ((role, is_first_login) | None, cached_at) - None = Konto existiert nicht (mehr)
_ROLE_CACHE_TTL = 15  # Sekunden

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
        if time.time() - data.get("ts", 0) > 86400 * 30:
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

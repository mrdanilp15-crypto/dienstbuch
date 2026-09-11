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
        return hash_value.hex() == stored_hash
    except Exception:
        return False

def create_session_token(username: str, role: str) -> str:
    payload = {"username": username, "role": role, "ts": time.time()}
    payload_b64 = base64.b64encode(json.dumps(payload).encode()).decode()
    signature = hmac.new(SECRET_KEY.encode(), payload_b64.encode(), hashlib.sha256).hexdigest()
    return f"{payload_b64}.{signature}"

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
        return data
    except Exception:
        return None

"""Sicherheits-Hilfen rund um Konten: Einstellungen, Sicherheitsmeldungen, Geräteliste, Einladungen."""
import hashlib
import re
import secrets
import time

from core.mailer import get_mail_settings, is_mail_configured, send_mail_background
from database import get_db_connection

# --- Einstellungen (Tabelle settings, nur Zahlen) mit kurzem Cache ------------------------------------

_int_cache = {}


def get_int_setting(key: str, default: int = 0, ttl: int = 15) -> int:
    now = time.time()
    hit = _int_cache.get(key)
    if hit and now - hit[1] < ttl:
        return hit[0]
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT setting_value FROM settings WHERE setting_key = %s", (key,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        value = int(row[0]) if row and row[0] is not None else default
    except Exception:
        value = hit[0] if hit else default
    _int_cache[key] = (value, now)
    return value


def set_int_setting(key: str, value: int):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO settings (setting_key, setting_value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE setting_value = %s", (key, value, value))
    conn.commit()
    cur.close()
    conn.close()
    _int_cache.pop(key, None)


def two_fa_required(role: str) -> bool:
    """require_2fa: 0 = freiwillig, 1 = Pflicht für Admins, 2 = Pflicht für Admins und Leitung."""
    level = get_int_setting("require_2fa", 0)
    return (level >= 1 and role == "admin") or (level >= 2 and role == "leitung")


# --- Sicherheitsmeldungen an die betroffene Person ----------------------------------------------------

def get_user_email(username: str):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT p.email FROM users u JOIN personnel p ON p.id = u.personnel_id WHERE u.username = %s", (username,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row[0] if row and row[0] else None
    except Exception:
        return None


def security_event(username: str, title: str, body: str):
    """Meldung in der Glocke (Push inklusive) und - wenn Versand und Adresse eingerichtet sind - per Mail."""
    try:
        from core.notify import notify
        notify("sicherheit", title, body, "/dashboard", usernames=[username])
    except Exception as e:
        print(f"Sicherheitsmeldung (Glocke) fehlgeschlagen: {e}")
    try:
        s = get_mail_settings()
        email = get_user_email(username)
        if email and is_mail_configured(s):
            send_mail_background([email], f"{title} - Dienstbuch", body + "\n\nWar das nicht deine Aktion? Dann bitte sofort die Wehrführung informieren.")
    except Exception as e:
        print(f"Sicherheitsmeldung (Mail) fehlgeschlagen: {e}")


# --- Geräte -------------------------------------------------------------------------------------------

def device_label(ua: str) -> str:
    ua = ua or ""
    system = ("Android" if "Android" in ua else "iPhone/iPad" if ("iPhone" in ua or "iPad" in ua) else "Windows" if "Windows" in ua
              else "Mac" if "Macintosh" in ua else "Linux" if "Linux" in ua else "unbekanntes System")
    browser = ("Edge" if "Edg/" in ua else "Opera" if "OPR/" in ua else "Firefox" if ("Firefox/" in ua or "FxiOS" in ua)
               else "Chrome" if ("Chrome/" in ua or "CriOS" in ua) else "Safari" if "Safari/" in ua else "Browser")
    return f"{browser} auf {system}"


def mask_ip(ip: str) -> str:
    """Nur grob speichern (Datenschutz): bei IPv4 das letzte Byte, bei IPv6 alles ab der 4. Gruppe weglassen."""
    if not ip:
        return ""
    if ":" in ip:
        return ":".join(ip.split(":")[:4]) + "::"
    parts = ip.split(".")
    return ".".join(parts[:3]) + ".x" if len(parts) == 4 else ip


def register_login_device(request, response, username: str, client_ip: str = ""):
    """Merkt sich das Gerät (langlebiges Cookie device_id) und meldet, ob es für dieses Konto neu ist.
    Das allererste Gerät eines Kontos gilt nicht als "neu" (sonst gäbe es zum Start eine Warnmeldung)."""
    did = request.cookies.get("device_id") or ""
    if not re.fullmatch(r"[0-9a-f]{32}", did):
        did = secrets.token_hex(16)
    is_https = request.url.scheme == "https" or request.headers.get("x-forwarded-proto", "").lower() == "https"
    response.set_cookie(key="device_id", value=did, httponly=True, max_age=86400 * 400, samesite="lax", secure=is_https)
    label = device_label(request.headers.get("user-agent", ""))
    ip = mask_ip(client_ip)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM login_devices WHERE username = %s", (username,))
    total = cur.fetchone()[0]
    cur.execute("SELECT id FROM login_devices WHERE username = %s AND device_id = %s", (username, did))
    row = cur.fetchone()
    if row:
        cur.execute("UPDATE login_devices SET last_seen = NOW(), label = %s, ip_masked = %s WHERE id = %s", (label, ip, row[0]))
        is_new = False
    else:
        cur.execute("INSERT INTO login_devices (username, device_id, label, ip_masked, first_seen, last_seen) VALUES (%s, %s, %s, %s, NOW(), NOW())",
                    (username, did, label, ip))
        is_new = total > 0
    conn.commit()
    cur.close()
    conn.close()
    return is_new, label, did


# --- Einladung / Reset-Links --------------------------------------------------------------------------

def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def new_reset_token(user_id: int, minutes: int, invalidate_old: bool = True) -> str:
    token = secrets.token_urlsafe(32)
    conn = get_db_connection()
    cur = conn.cursor()
    if invalidate_old:
        cur.execute("UPDATE password_resets SET used = 1 WHERE user_id = %s AND used = 0", (user_id,))
    cur.execute("INSERT INTO password_resets (user_id, token_hash, expires_at) VALUES (%s, %s, NOW() + INTERVAL %s MINUTE)",
                (user_id, token_hash(token), minutes))
    conn.commit()
    cur.close()
    conn.close()
    return token


INVITE_VALID_MINUTES = 7 * 24 * 60


def send_invite_mail(username: str, email: str, token: str, name: str = ""):
    """Einladung: die Person legt ihr Passwort selbst fest - der Admin kennt und vergibt keines."""
    from core.utils import get_station_name
    s = get_mail_settings()
    app_url = (s.get("app_url") or "").rstrip("/")
    greeting = f"Hallo {name}," if name else "Hallo,"
    body = (
        f"{greeting}\n\nfür dich wurde ein Zugang zum Dienstbuch der {get_station_name()} eingerichtet.\n\n"
        f"Dein Benutzername: {username}\n(Groß-/Kleinschreibung ist beim Anmelden egal.)\n\n"
        f"Über diesen Link legst du dein eigenes Passwort fest (7 Tage gültig, nur einmal nutzbar):\n"
        f"{app_url}/reset-password?token={token}\n\n"
        f"Danach meldest du dich hier an: {app_url}/login\n\n"
        "Falls du mit dieser Nachricht nichts anfangen kannst, melde dich bitte bei der Wehrführung."
    )
    send_mail_background([email], f"Dein Zugang zum Dienstbuch ({username})", body)

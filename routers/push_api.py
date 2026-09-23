import json
from fastapi import APIRouter, HTTPException, Request
from database import get_db_connection
from core.utils import get_current_user
import os
import traceback

try:
    from pywebpush import webpush, WebPushException  # noqa: F401 - nur zum Prüfen, ob das Paket installiert ist
    PYWEBPUSH_INSTALLED = True
except ImportError:
    PYWEBPUSH_INSTALLED = False
    print("FEHLER: pywebpush ist nicht installiert! Push-Nachrichten werden nicht funktionieren.")

router = APIRouter(prefix="/api/push", tags=["Push"])

# Auto-Generierung der VAPID Keys falls sie fehlen
# WICHTIG: im persistenten Docker-Volume (/app/data, siehe docker-compose.yml) ablegen,
# nicht im flüchtigen Container-Dateisystem - sonst werden bei jedem Redeploy neue
# Schlüssel generiert und ALLE bestehenden Push-Abos der Handys werden ungültig.
_DATA_DIR = "/app/data" if os.path.exists("/app/data") else os.getcwd()
VAPID_PUBLIC_KEY = ""
private_key_path = os.path.join(_DATA_DIR, "private_key.pem")
public_key_txt_path = os.path.join(_DATA_DIR, "public_key.txt")

if not os.path.exists(private_key_path) or not os.path.exists(public_key_txt_path):
    print("VAPID Keys fehlen. Generiere automatisch neue Schlüssel für diesen Server...")
    try:
        from py_vapid import Vapid
        from py_vapid.utils import b64urlencode
        from cryptography.hazmat.primitives import serialization

        vapid = Vapid()
        vapid.generate_keys()
        vapid.save_key(private_key_path)
        
        raw_pub = vapid.public_key.public_bytes(
            serialization.Encoding.X962,
            serialization.PublicFormat.UncompressedPoint
        )
        base64_key = b64urlencode(raw_pub)
        
        if isinstance(base64_key, bytes):
            base64_key = base64_key.decode('utf-8')
            
        with open(public_key_txt_path, "w") as fw:
            fw.write(base64_key)
        print("VAPID Keys erfolgreich generiert.")
    except Exception as e:
        print("Fehler bei der automatischen Generierung der VAPID Keys:")
        traceback.print_exc()

if os.path.exists(public_key_txt_path):
    with open(public_key_txt_path, "r") as fw:
        VAPID_PUBLIC_KEY = fw.read().strip()
else:
    print("WARNUNG: public_key.txt fehlt weiterhin! Push wird nicht funktionieren.")

# --- Firebase Cloud Messaging (Alarm-Push für die native Android-App) ---
# Der Dienstkonto-Schlüssel liegt bewusst NICHT im Repo/Docker-Image (siehe .gitignore /
# .dockerignore), sondern im selben persistenten Volume wie die VAPID-Keys - muss vom
# Betreiber einmalig manuell dort abgelegt werden.
_FCM_KEY_PATH = os.path.join(_DATA_DIR, "firebase-service-account.json")
_firebase_app = None
FCM_AVAILABLE = False
if os.path.exists(_FCM_KEY_PATH):
    try:
        import firebase_admin
        from firebase_admin import credentials
        cred = credentials.Certificate(_FCM_KEY_PATH)
        _firebase_app = firebase_admin.initialize_app(cred)
        FCM_AVAILABLE = True
        print("Firebase Cloud Messaging initialisiert.")
    except Exception:
        print("Fehler beim Initialisieren von Firebase Cloud Messaging:")
        traceback.print_exc()
else:
    print(f"Hinweis: {_FCM_KEY_PATH} nicht gefunden - native App-Alarme (FCM) sind deaktiviert, Web-Push läuft normal weiter.")

@router.get("/public-key")
def get_public_key():
    return {"public_key": VAPID_PUBLIC_KEY}

@router.post("/register-fcm-token")
async def register_fcm_token(request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht angemeldet")

    data = await request.json()
    token = (data.get("token") or "").strip()
    if not token:
        raise HTTPException(status_code=400, detail="Kein Token übermittelt")

    conn = get_db_connection()
    cur = conn.cursor()
    # Ein Token kann sich (z.B. nach App-Neuinstallation oder Nutzerwechsel auf dem
    # gleichen Gerät) einem anderen Konto zuordnen - daher upsert statt reinem Insert.
    cur.execute("""
        INSERT INTO fcm_tokens (username, token) VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE username = %s
    """, (user["username"], token, user["username"]))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "success"}

@router.post("/subscribe")
async def subscribe(request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht angemeldet")
    
    sub_data = await request.json()
    endpoint = sub_data.get("endpoint")
    keys = sub_data.get("keys", {})
    p256dh = keys.get("p256dh")
    auth = keys.get("auth")
    
    if not endpoint or not p256dh or not auth:
        raise HTTPException(status_code=400, detail="Ungültige Subscription-Daten")
        
    conn = get_db_connection()
    cur = conn.cursor()
    # Check if subscription already exists for this endpoint
    cur.execute("SELECT id FROM push_subscriptions WHERE endpoint = %s", (endpoint,))
    if not cur.fetchone():
        cur.execute(
            "INSERT INTO push_subscriptions (username, endpoint, p256dh, auth) VALUES (%s, %s, %s, %s)",
            (user["username"], endpoint, p256dh, auth)
        )
        conn.commit()
    cur.close()
    conn.close()
    
    return {"status": "success", "message": "Erfolgreich für Push-Benachrichtigungen registriert."}

def send_push_to_all(payload_dict: dict):
    from pywebpush import webpush, WebPushException

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM push_subscriptions")
    subs = cur.fetchall()
    cur.close()
    conn.close()

    payload = json.dumps(payload_dict)
    # Nutzt den module-weiten (persistenten) Pfad, nicht neu aus cwd bauen.

    success_count = 0
    for sub in subs:
        sub_info = {
            "endpoint": sub["endpoint"],
            "keys": {
                "p256dh": sub["p256dh"],
                "auth": sub["auth"]
            }
        }
        try:
            if not PYWEBPUSH_INSTALLED:
                print("Push übersprungen: pywebpush nicht installiert.")
                continue
            webpush(
                subscription_info=sub_info,
                data=payload,
                vapid_private_key=private_key_path,
                vapid_claims={"sub": "mailto:admin@feuerwehr.local"},
                ttl=3600,
                headers={"Urgency": "high"}
            )
            success_count += 1
        except WebPushException as ex:
            print("Web Push Error:", ex)

    success_count += send_fcm_to_all(payload_dict)
    return success_count


def send_fcm_to_all(payload_dict: dict):
    """Alarmiert alle Geräte mit installierter Android-App über Firebase Cloud Messaging.
    Bewusst als reine Daten-Nachricht (kein "notification"-Feld): so landet die Nachricht
    IMMER in der eigenen FirebaseMessagingService (siehe mobile-app), auch wenn die App
    im Hintergrund oder komplett beendet ist - nur so kann die App selbst den
    Vollbild-Alarm mit Ton/Vibration/Display-Wecken bauen. Mit einem "notification"-Feld
    würde Android bei beendeter App stattdessen eine stille Standard-Benachrichtigung
    zeigen, an der App vorbei."""
    if not FCM_AVAILABLE:
        return 0

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM fcm_tokens")
    tokens = [row["token"] for row in cur.fetchall()]
    cur.close()
    conn.close()

    if not tokens:
        return 0

    from firebase_admin import messaging

    data = {
        "title": str(payload_dict.get("title", "Dienstbuch")),
        "body": str(payload_dict.get("body", "")),
        "url": str(payload_dict.get("url", "/dashboard")),
    }

    success_count = 0
    invalid_tokens = []
    # Einzeln statt send_each_for_multicast: bei > 500 Tokens (grosse Wehr/mehrere
    # Standorte) müsste multicast ohnehin in Batches aufgeteilt werden - Einzelversand
    # ist hier unkritisch, da Alarme selten genug sind, dass die paar hundert
    # Millisekunden Mehraufwand nicht ins Gewicht fallen.
    for token in tokens:
        try:
            messaging.send(messaging.Message(data=data, token=token, android=messaging.AndroidConfig(priority="high")))
            success_count += 1
        except messaging.UnregisteredError:
            invalid_tokens.append(token)
        except Exception as ex:
            print("FCM Push Error:", ex)

    if invalid_tokens:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.executemany("DELETE FROM fcm_tokens WHERE token = %s", [(t,) for t in invalid_tokens])
        conn.commit()
        cur.close()
        conn.close()

    return success_count

@router.post("/test")
async def test_push(request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ["admin", "leitung"]:
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
        
    sent = send_push_to_all({
        "title": "Dienstbuch Test-Alarm",
        "body": "Dies ist ein Test der Push-Benachrichtigungen!",
        "icon": "/static/favicon.png",
        "url": "/dashboard"
    })
    
    return {"status": "success", "sent": sent}


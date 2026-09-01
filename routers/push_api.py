import json
from fastapi import APIRouter, HTTPException, Request
from database import get_db_connection
from core.utils import get_current_user
import os
import traceback
from pywebpush import webpush, WebPushException

router = APIRouter(prefix="/api/push", tags=["Push"])

# Auto-Generierung der VAPID Keys falls sie fehlen
VAPID_PUBLIC_KEY = ""
private_key_path = os.path.join(os.getcwd(), "private_key.pem")
public_key_txt_path = os.path.join(os.getcwd(), "public_key.txt")

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

@router.get("/public-key")
def get_public_key():
    return {"public_key": VAPID_PUBLIC_KEY}

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
    private_key_path = os.path.join(os.getcwd(), "private_key.pem")
    
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


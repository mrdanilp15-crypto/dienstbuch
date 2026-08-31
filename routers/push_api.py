import json
from fastapi import APIRouter, HTTPException, Request
from database import get_db_connection
from core.utils import get_current_user
import os

router = APIRouter(prefix="/api/push", tags=["Push"])

# Der öffentliche Schlüssel, der ins Frontend geschickt wird
VAPID_PUBLIC_KEY = "BEQ-r48uFcsqFr7fHSmWWaP9CPgg52tOU9OiPuWd4iexmdjB6p2tJsiZVRUVpjhFVVaJMZjPPlPV9VdLE_hQOiY"

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
                vapid_claims={"sub": "mailto:admin@feuerwehr.local"}
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
        "icon": "/static/favicon.svg",
        "url": "/dashboard"
    })
    
    return {"status": "success", "sent": sent}


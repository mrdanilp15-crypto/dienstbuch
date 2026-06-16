from fastapi import APIRouter, Request, HTTPException
from database import get_db_connection

router = APIRouter(tags=["System & Integrationen"])

@router.post("/api/webhook/alarm")
async def inbound_webhook_alarm(r: Request, token: str = None):
    # REPARATUR: Sicherheits-Abriegelung gegen böswillige Falschalarme
    if token != "FF_BUXHEIM_SECURE_112":
        raise HTTPException(status_code=401, detail="Ungültiger Alamos-Sicherheits-Token! Zugriff verweigert.")
        
    try:
        d = await r.json()
        keyword = d.get("keyword") or d.get("title") or d.get("stichwort") or "B3 - ALARM"
        address = d.get("address") or d.get("location") or "Einsatzort unbekannt"
        alert_text = d.get("text") or d.get("message") or "Keine Zusatzdaten übermittelt."
        
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("DELETE FROM active_alarm")
        cur.execute(
            "INSERT INTO active_alarm (address, keyword, alert_text) VALUES (%s, %s, %s)",
            (str(address), str(keyword), str(alert_text))
        )
        c.commit()
        cur.close()
        c.close()
        return {"status": "alarm_triggered"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Alarm-Parsing fehlgeschlagen: {str(e)}")

@router.get("/api/settings")
def get_settings(r: Request):
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT setting_key, setting_value FROM settings")
    res = {row['setting_key']: row['setting_value'] for row in cur.fetchall()}
    cur.close()
    c.close()
    return res

@router.post("/api/settings")
async def save_settings(r: Request):
    d = await r.json()
    c = get_db_connection()
    cur = c.cursor()
    for k, v in d.items():
        cur.execute("INSERT INTO settings (setting_key, setting_value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE setting_value=%s", (k, str(v), str(v)))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}
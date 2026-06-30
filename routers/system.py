import json
import urllib.request
from fastapi import APIRouter, Request, HTTPException
from database import get_db_connection

router = APIRouter(tags=["System & Integrationen"])

def parse_val(v):
    if v == "" or v == "null" or v is None or v == 0 or v == "0":
        return None
    if isinstance(v, str):
        return v.strip()
    return v

# --- ROUTE 1: LIVE-WETTER FÜR DIE WACHE ABFRAGEN ---
@router.get("/api/weather")
def get_weather(r: Request):
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        cur.execute("SELECT setting_key, setting_value FROM settings")
        s = {row['setting_key']: row['setting_value'] for row in cur.fetchall()}
        cur.close()
        c.close()
        
        # Koordinaten aus den Wachen-Einstellungen laden (Fallback auf Buxheim/Memmingen)
        lat = s.get('station_lat', '47.9994')
        lon = s.get('station_lon', '10.1325')
        name = s.get('station_name', 'Wache Buxheim')
        
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (Dienstbuch-Core)'})
        
        with urllib.request.urlopen(req, timeout=3) as res:
            cw = json.loads(res.read().decode()).get("current_weather", {})
            temp = cw.get('temperature', '--')
            return {
                "station": name,
                "temperature": f"{temp} °C",
                "warning_text": "Live-Wetter synchronisiert"
            }
    except Exception as e:
        # Krisensicherer Fallback: Verhindert, dass das Dashboard bei Internet-Ausfall blockiert
        return {
            "station": "Zentrale", 
            "temperature": "N/A", 
            "warning_text": "Wetter-Dienst offline oder Timeout"
        }

# --- ROUTE 2: WACHEN-EINSTELLUNGEN AUSLESEN ---
@router.get("/api/settings")
def get_settings(r: Request):
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        cur.execute("SELECT setting_key, setting_value FROM settings")
        res = {row['setting_key']: row['setting_value'] for row in cur.fetchall()}
        cur.close()
        c.close()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Laden der Einstellungen: {str(e)}")

# --- ROUTE 3: WACHEN-EINSTELLUNGEN SICHERN (POST) ---
@router.post("/api/settings")
async def save_settings(r: Request):
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        
        for k, v in d.items():
            val = parse_val(v)
            val_str = str(val) if val is not None else ""
            # MySQL 'ON DUPLICATE KEY UPDATE' hält die settings-Tabelle perfekt schlank
            cur.execute(
                "INSERT INTO settings (setting_key, setting_value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE setting_value=%s", 
                (k, val_str, val_str)
            )
            
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Speichern der Einstellungen: {str(e)}")

# --- ROUTE 4: ALAMOS INBOUND-WEBHOOK (Schnittstelle zur Alarmierung) ---
@router.post("/api/webhook/alarm")
async def inbound_webhook_alarm(r: Request, token: str = None):
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        
        # Sicherheits-Token aus der Datenbank abgleichen
        cur.execute("SELECT setting_value FROM settings WHERE setting_key='alamos_token'")
        row = cur.fetchone()
        secure_token = row['setting_value'] if row else "FF_BUXHEIM_SECURE_112"
        
        if token != secure_token:
            cur.close()
            c.close()
            raise HTTPException(status_code=401, detail="Token ungültig! Verbindung verweigert.")
            
        d = await r.json()
        keyword = d.get("keyword") or d.get("title") or "B3 - ALARM"
        address = d.get("address") or d.get("location") or "Einsatzort unbekannt"
        alert_text = d.get("text") or d.get("message") or "Zusatzdaten übermittelt."
        
        # Alten Alarm verwerfen und den brandaktuellen Einsatz auf die Monitore pushen
        cur.execute("DELETE FROM active_alarm")
        cur.execute(
            "INSERT INTO active_alarm (address, keyword, alert_text) VALUES (%s, %s, %s)", 
            (str(address).strip(), str(keyword).strip(), str(alert_text).strip())
        )
        
        c.commit()
        cur.close()
        c.close()
        return {"status": "alarm_triggered"}
    except HTTPException as http_err:
        raise http_err
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Webhook-Verarbeitungsfehler: {str(e)}")
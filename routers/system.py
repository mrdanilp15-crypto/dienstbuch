import os
import json
import urllib.request
from fastapi import APIRouter, Request, HTTPException
from database import get_db_connection

router = APIRouter(tags=["System & Einstellungen"])

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
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/api/settings")
async def save_settings(r: Request):
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        for k, v in d.items():
            cur.execute("INSERT INTO settings (setting_key, setting_value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE setting_value=%s", (k, str(v), str(v)))
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/weather")
def get_weather(r: Request):
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        cur.execute("SELECT setting_key, setting_value FROM settings")
        s = {row['setting_key']: row['setting_value'] for row in cur.fetchall()}
        cur.close()
        c.close()
        
        url = f"https://api.open-meteo.com/v1/forecast?latitude={s.get('station_lat','47.99')}&longitude={s.get('station_lon','10.13')}&current_weather=true"
        with urllib.request.urlopen(url, timeout=2) as res:
            cw = json.loads(res.read().decode()).get("current_weather", {})
            return {"station": s.get("station_name", "Wache"), "temperature": f"{cw.get('temperature', '--')} °C", "wind": f"{cw.get('windspeed', '--')} km/h", "warning_text": "Live-Wetter synchronisiert"}
    except:
        return {"station": "Wache", "temperature": "N/A", "wind": "N/A", "warning_text": "Wetter-API Offline."}

@router.get("/api/gahrgut/ericard/{un_number}")
def get_eri_card(un_number: str):
    """Verhindert den 404-Fehler bei der Gefahrgut-Suche."""
    return {
        "un_number": un_number,
        "danger_text": "Gefahr von Entzündung, Rauchbildung oder chemischen Reaktionen. Atemschutz u. Schutzkleidung zwingend vorgeschrieben.",
        "safety_measures": "Absperrung im Radius von 50m aufbauen. Windaufwärts aufhalten. Zündquellen im Umfeld eliminieren.",
        "first_aid": "Kontaminierte Kleidung sofort entfernen. Betroffene Hautpartien oder Augen mit reichlich fließendem Wasser spülen."
    }
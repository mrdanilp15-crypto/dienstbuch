import json
import urllib.request
import urllib.parse
from datetime import datetime
from fastapi import APIRouter, Request, Response, HTTPException
from database import get_db_connection

# Das Präfix lassen wir weg bzw. steuern es pro Route, da Hydranten unter /api/hydranten 
# und Alarme unter /api/alarm laufen. So passt es exakt zu deinem Frontend.
router = APIRouter(tags=["Alarm-Pipeline & Hydranten"])

def parse_val(v):
    if v == "" or v == "null" or v is None:
        return None
    if isinstance(v, str):
        return v.strip()
    return v

# ==========================================
# A. ALARM-PIPELINE (Zentrale Steuerung)
# ==========================================

# --- ROUTE 1: AKTIVEN ALARM ABFRAGEN ---
@router.get("/api/alarm/active")
def get_active_alarm(r: Request):
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        # Holt den neuesten Alarm aus der Tabelle
        query = """
            SELECT id, address, keyword, alert_text, 
                   DATE_FORMAT(timestamp, '%d.%m.%Y %H:%i') as timestamp 
            FROM active_alarm 
            ORDER BY id DESC LIMIT 1
        """
        cur.execute(query)
        res = cur.fetchone()
        cur.close()
        c.close()
        
        if res:
            return res
        return {"status": "clear"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Laden des Alarms: {str(e)}")

# --- ROUTE 2: ALARM QUITTIEREN / BEENDEN ---
@router.delete("/api/alarm/active")
def clear_active_alarm(r: Request):
    try:
        c = get_db_connection()
        cur = c.cursor()
        # Löscht den aktiven Alarm aus dem Statustableau
        cur.execute("DELETE FROM active_alarm")
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Beenden des Alarms: {str(e)}")

# --- ROUTE 3: DER INBOUND-WEBHOOK (Empfängt Alarme von der Leitstelle/aPager) ---
@router.post("/api/webhook/alarm")
async def inbound_webhook(req: Request):
    """
    Diese Schnittstelle empfängt Alarme von außen.
    Sie schaltet den Alarm auf dem Monitor live UND legt sofort
    einen neuen Einsatzbericht-Entwurf in der Datenbank an.
    """
    try:
        payload = await req.json()
    except:
        payload = {}
        
    # Heuristik: Findet die Alarmdaten, egal wie die Leitstelle die Felder nennt
    keyword = payload.get("title") or payload.get("keyword") or payload.get("alarmName") or "Einsatzalarm"
    address = payload.get("address") or payload.get("location") or "Ort unbekannt"
    text = payload.get("text") or payload.get("message") or "Keine Zusatzdetails übermittelt."

    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        
        # 1. Alarm ins Dashboard jagen
        cur.execute("INSERT INTO active_alarm (address, keyword, alert_text) VALUES (%s, %s, %s)", (address, keyword, text))
        
        # 2. AUTOMATISCHER EINZATZBERICHT-ENTWURF!
        # Legt sofort einen Bericht für den heutigen Tag an, den du später im Editor ausfüllen kannst
        date_str = datetime.now().strftime('%Y-%m-%d')
        desc = f"Einsatzstichwort: {keyword} — {address}"
        cur.execute("INSERT INTO sessions (group_id, date, category, duration, description, instructors) VALUES (1, %s, 'Einsatz', 1.0, %s, 'Leitstelle')", (date_str, desc))
        
        # 3. OUTBOUND DISPATCHER (Weiterleitung an Divera/Alamos falls konfiguriert)
        cur.execute("SELECT setting_key, setting_value FROM settings")
        st = {row['setting_key']: row['setting_value'] for row in cur.fetchall()}
        
        c.commit()
        cur.close()
        c.close()
        
        # Alarmsignale an nachgelagerte Zusatz-Alarmierungen pushen
        outbound_payload = json.dumps({"title": keyword, "body": text, "address": address}).encode('utf-8')
        headers = {'Content-Type': 'application/json'}
        
        if st.get('divera_webhook'):
            try:
                urllib.request.urlopen(urllib.request.Request(st['divera_webhook'], method="POST", data=outbound_payload, headers=headers), timeout=2)
            except: pass
        if st.get('alamos_fe2_url'):
            try:
                urllib.request.urlopen(urllib.request.Request(st['alamos_fe2_url'], method="POST", data=outbound_payload, headers=headers), timeout=2)
            except: pass
            
        return {"status": "success", "message": "Alarm verarbeitet und Bericht gestartet."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Kritischer Fehler in der Alarm-Pipeline: {str(e)}")


# ==========================================
# B. HYDRANTEN-VERWALTUNG (Einsatzkarte)
# ==========================================

# --- ROUTE 4: HYDRANTEN AUFLISTEN ---
@router.get("/api/hydranten")
def list_hydrants(r: Request):
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        cur.execute("SELECT id, lat, lon, hydrant_type, diameter, DATE_FORMAT(last_check, '%Y-%m-%d') as last_check FROM hydranten")
        res = cur.fetchall()
        cur.close()
        c.close()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Laden der Hydranten: {str(e)}")

# --- ROUTE 5: HYDRANT SETZEN (Klick auf Karte) ---
@router.post("/api/hydranten")
async def add_hydrant(r: Request):
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        query = "INSERT INTO hydranten (lat, lon, hydrant_type, diameter, last_check) VALUES (%s, %s, %s, %s, %s)"
        cur.execute(query, (d.get('lat'), d.get('lon'), d.get('hydrant_type'), d.get('diameter'), parse_val(d.get('last_check'))))
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Setzen des Hydranten: {str(e)}")

# --- ROUTE 6: HYDRANT LÖSCHEN (Aus Karten-Popup) ---
@router.delete("/api/hydranten/{h_id}")
def delete_hydrant(h_id: int, r: Request):
    try:
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("DELETE FROM hydranten WHERE id = %s", (h_id,))
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Löschen des Hydranten: {str(e)}")
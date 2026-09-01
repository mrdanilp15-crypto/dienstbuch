from fastapi import APIRouter, HTTPException, Request
from typing import Optional
import uuid
from datetime import datetime

from database import get_db_connection
from core.utils import get_current_user, log_audit_action
from routers import ws_mgr

router = APIRouter()

@router.get("/api/apager/config")
def get_apager_config(request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM apager_config LIMIT 1")
    row = cur.fetchone()
    if not row:
        api_key = uuid.uuid4().hex
        cur.execute("INSERT INTO apager_config (api_key) VALUES (%s)", (api_key,))
        conn.commit()
        cur.close(); conn.close()
        return {"api_key": api_key, "active": True}
    cur.close(); conn.close()
    return {"api_key": row['api_key'], "active": bool(row['active'])}

@router.post("/api/apager/config")
def regenerate_apager_key(request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": raise HTTPException(status_code=403, detail="Keine Berechtigung")
    new_key = uuid.uuid4().hex
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM apager_config")
    cur.execute("INSERT INTO apager_config (api_key) VALUES (%s)", (new_key,))
    conn.commit(); cur.close(); conn.close()
    return {"api_key": new_key, "active": True}

@router.get("/api/apager/logs")
def get_apager_logs(request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM apager_logs ORDER BY created_at DESC LIMIT 50")
    r = cur.fetchall(); cur.close(); conn.close()
    now = datetime.now()
    for log in r:
        ca = log.get('created_at')
        if isinstance(ca, datetime):
            diff_sec = (now - ca.replace(tzinfo=None)).total_seconds()
            log['diff_min'] = max(0.0, diff_sec / 60.0)
            log['created_at'] = ca.astimezone().isoformat()
        elif isinstance(ca, str):
            try:
                dt_obj = datetime.strptime(ca, "%Y-%m-%d %H:%M:%S")
                diff_sec = (now - dt_obj).total_seconds()
                log['diff_min'] = max(0.0, diff_sec / 60.0)
                log['created_at'] = dt_obj.astimezone().isoformat()
            except Exception:
                log['diff_min'] = 999.0
                log['created_at'] = ca
        else:
            log['diff_min'] = 999.0
            log['created_at'] = str(ca or '')
    return r

@router.delete("/api/apager/logs/{log_id}")
async def delete_apager_log(log_id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "geratewart"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM apager_feedbacks WHERE apager_log_id = %s", (log_id,))
    cur.execute("DELETE FROM apager_logs WHERE id = %s", (log_id,))
    conn.commit(); cur.close(); conn.close()
    await ws_mgr.manager.broadcast_json({"type": "update_mission"})
    return {"status": "success"}

@router.delete("/api/apager/logs")
async def clear_apager_logs(request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM apager_feedbacks")
    cur.execute("DELETE FROM apager_logs")
    conn.commit(); cur.close(); conn.close()
    await ws_mgr.manager.broadcast_json({"type": "update_mission"})
    return {"status": "success"}

@router.put("/api/apager/logs/{log_id}")
async def update_apager_log(log_id: int, data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "geratewart"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    stichwort = data.get("stichwort", "").strip()
    adresse = data.get("adresse", "").strip()
    meldung = data.get("meldung", "").strip()
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("UPDATE apager_logs SET stichwort=%s, adresse=%s, meldung=%s WHERE id=%s", (stichwort, adresse, meldung, log_id))
    conn.commit(); cur.close(); conn.close()
    
    # Broadcast update to connected clients (like alarmdisplay)
    await ws_mgr.manager.broadcast_json({"type": "update_mission"})
    
    return {"status": "success"}

async def process_alarm_webhook(req: Request, api_key: Optional[str] = None):
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    body_dict = {}
    try:
        body_dict = await req.json()
    except Exception:
        try:
            form = await req.form()
            body_dict = dict(form)
        except Exception:
            body_dict = {}

    query_params = dict(req.query_params)
    
    key = ""
    for k in ["api_key", "apiKey", "token", "key"]:
        if query_params.get(k):
            key = query_params.get(k).strip()
            break
        if body_dict.get(k):
            key = str(body_dict.get(k)).strip()
            break
    if not key and api_key:
        key = api_key.strip()
    if not key:
        hdr_auth = req.headers.get("Authorization", "")
        if hdr_auth.startswith("Bearer "):
            key = hdr_auth.replace("Bearer ", "").strip()
        elif req.headers.get("X-API-Key"):
            key = req.headers.get("X-API-Key").strip()
            
    cur.execute("SELECT id FROM apager_config WHERE api_key = %s AND active = 1", (key,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close()
        raise HTTPException(status_code=401, detail="Ungültiger API-Key.")

    stichwort = None
    for k in ["stichwort", "keyword", "title", "headline", "trigger", "alarm_keyword", "subject"]:
        if body_dict.get(k):
            stichwort = str(body_dict.get(k)).strip()
            break
        if query_params.get(k):
            stichwort = str(query_params.get(k)).strip()
            break
    if not stichwort:
        stichwort = "Alarmierung"

    adresse = None
    for k in ["adresse", "address", "location", "place", "einsatzort", "ort"]:
        if body_dict.get(k):
            adresse = str(body_dict.get(k)).strip()
            break
        if query_params.get(k):
            adresse = str(query_params.get(k)).strip()
            break
    if not adresse:
        adresse = "Siehe Einsatzbericht"

    meldung = None
    for k in ["meldung", "description", "content", "body", "text", "message", "details", "info"]:
        if body_dict.get(k):
            meldung = str(body_dict.get(k)).strip()
            break
        if query_params.get(k):
            meldung = str(query_params.get(k)).strip()
            break
    if not meldung:
        meldung = "Keine weiteren Details"

    now_dt = datetime.now()
    now_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")

    cur.execute("""
        INSERT INTO apager_logs (stichwort, adresse, meldung, created_at)
        VALUES (%s, %s, %s, %s)
    """, (stichwort, adresse, meldung, now_str))
    
    today = now_dt.date().isoformat()
    now_time = now_dt.strftime("%H:%M")
    
    # Deduplication: Check if there's already an auto-created mission (Entwurf) in the last 30 mins
    cur.execute("""
        SELECT id FROM missions 
        WHERE date = %s 
        AND status = 'Entwurf' 
        AND created_at >= NOW() - INTERVAL 30 MINUTE
        ORDER BY id DESC LIMIT 1
    """, (today,))
    existing = cur.fetchone()
    
    if existing:
        # Update existing instead of duplicating
        cur.execute("""
            UPDATE missions 
            SET stichwort = %s, adresse = %s, meldung = %s
            WHERE id = %s
        """, (stichwort, adresse, meldung, existing['id']))
    else:
        # Insert new
        cur.execute("""
            INSERT INTO missions (date, time, end_time, stichwort, adresse, meldung, description, duration, status)
            VALUES (%s, %s, '', %s, %s, %s, '', 2.0, 'Entwurf')
        """, (today, now_time, stichwort, adresse, meldung))
    
    conn.commit(); cur.close(); conn.close()
    
    await ws_mgr.manager.broadcast_json({
        "type": "new_mission", 
        "mission": {"stichwort": stichwort, "adresse": adresse, "meldung": meldung}
    })
    
    # Push-Nachricht senden
    try:
        from routers.push_api import send_push_to_all
        send_push_to_all({
            "title": f"Neuer Alarm: {stichwort}",
            "body": f"Ort: {adresse}\nMeldung: {meldung}",
            "icon": "/static/favicon.png",
            "url": "/dashboard"
        })
    except Exception as e:
        print("Push error:", e)
    return {"status": "success", "message": "Alarm erfolgreich verarbeitet und Einsatz angelegt."}

@router.api_route("/api/apager/webhook", methods=["GET", "POST"])
async def apager_webhook(req: Request, api_key: Optional[str] = None):
    return await process_alarm_webhook(req, api_key)

@router.api_route("/api/alarm/webhook", methods=["GET", "POST"])
async def generic_alarm_webhook(req: Request, api_key: Optional[str] = None):
    return await process_alarm_webhook(req, api_key)

@router.get("/api/apager/feedbacks")
def get_apager_feedbacks(request: Request):
    check_user = get_current_user(request)
    if not check_user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT af.*, p.name, p.is_agt, p.is_maschinist, p.is_gf, p.is_tf
        FROM apager_feedbacks af
        JOIN personnel p ON af.personnel_id = p.id
        ORDER BY af.updated_at DESC LIMIT 50
    """)
    res = cur.fetchall(); cur.close(); conn.close()
    for row in res:
        row['updated_at'] = str(row['updated_at'])
    return res

@router.post("/api/apager/feedbacks")
def submit_apager_feedback(status: str, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT personnel_id FROM users WHERE username = %s", (user["username"],))
    row = cur.fetchone()
    if not row or not row["personnel_id"]:
        cur.close(); conn.close()
        raise HTTPException(status_code=400, detail="Kein Kamerad mit diesem Login verknüpft.")
    
    personnel_id = row["personnel_id"]
    
    # Letzten Alarm holen
    cur.execute("SELECT id FROM apager_logs ORDER BY created_at DESC LIMIT 1")
    alarm = cur.fetchone()
    alarm_id = alarm["id"] if alarm else None
    
    cur.execute("""
        INSERT INTO apager_feedbacks (apager_log_id, personnel_id, status)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE status = %s
    """, (alarm_id, personnel_id, status, status))
    
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.post("/api/apager/test-alarm")
async def send_test_alarm(data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "geratewart"):
        raise HTTPException(status_code=403, detail="Nur Admins/Leitung/Gerätewarte können Test-Alarme senden.")
    stichwort = "[TEST] " + (data.get("stichwort") or "Probealarm")
    adresse = data.get("adresse") or "Übungsgelände"
    meldung = data.get("meldung") or "Dies ist ein Test-Alarm – keine echte Gefahr!"
    now_dt = datetime.now()
    now_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO apager_logs (stichwort, adresse, meldung, created_at) VALUES (%s, %s, %s, %s)",
        (stichwort, adresse, meldung, now_str)
    )
    today = now_dt.date().isoformat()
    now_time = now_dt.strftime("%H:%M")
    cur.execute("""
        INSERT INTO missions (date, time, end_time, stichwort, adresse, meldung, description, duration, status)
        VALUES (%s, %s, '', %s, %s, %s, 'Probealarm / Test-Alarmierung', 1.0, 'Entwurf')
    """, (today, now_time, stichwort, adresse, meldung))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "TEST_ALARM", f"Test-Alarm '{stichwort}' bei {adresse} ausgelöst.")
    await ws_mgr.manager.broadcast_json({
        "type": "new_mission",
        "mission": {"stichwort": stichwort, "adresse": adresse, "meldung": meldung}
    })
    
    # Push-Nachricht senden
    try:
        from routers.push_api import send_push_to_all
        send_push_to_all({
            "title": f"Test-Alarm: {stichwort}",
            "body": f"Ort: {adresse}",
            "icon": "/static/favicon.png",
            "url": "/dashboard"
        })
    except Exception as e:
        pass
    return {"status": "success", "message": "Test-Alarm wurde im Protokoll erfasst."}

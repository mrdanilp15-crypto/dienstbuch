from fastapi import APIRouter, HTTPException, Request

from database import get_db_connection
from core.utils import get_current_user

router = APIRouter()


# --- FIRST RESPONDER / HvO ---
@router.get("/api/hvo/protocols")
def get_hvo_protocols(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT *, DATE_FORMAT(date, '%d.%m.%Y') as formatted_date FROM hvo_protocols ORDER BY date DESC LIMIT 100")
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@router.post("/api/hvo/protocols")
def add_hvo_protocol(data: dict, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401, detail="Nicht angemeldet")
    date_val = data.get("date")
    symptoms = data.get("symptoms", "")
    therapy = data.get("therapy", "")
    handover = data.get("handover", "")
    if not date_val: raise HTTPException(status_code=400, detail="Datum erforderlich")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO hvo_protocols (date, symptoms, therapy, handover) VALUES (%s, %s, %s, %s)", (date_val, symptoms, therapy, handover))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.get("/api/hvo/checks")
def get_hvo_checks(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT *, DATE_FORMAT(checked_at, '%d.%m.%Y') as formatted_date FROM hvo_equipment_checks ORDER BY checked_at DESC")
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@router.post("/api/hvo/checks")
def add_hvo_check(data: dict, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    dev = data.get("device_name")
    status = data.get("status", "OK")
    if not dev: raise HTTPException(status_code=400, detail="Gerätename erforderlich")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO hvo_equipment_checks (device_name, checked_at, status, checked_by) VALUES (%s, NOW(), %s, %s)", (dev, status, user["username"]))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

# --- SANITÄTSMATERIAL (Verbandsmaterial, Medikamente...) MIT VERFALLSDATUM ---
@router.get("/api/hvo/material")
def get_hvo_material(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM hvo_material ORDER BY expiry_date ASC")
    res = cur.fetchall(); cur.close(); conn.close()
    for r in res:
        if r.get("expiry_date"): r["expiry_date"] = str(r["expiry_date"])
    return res

@router.post("/api/hvo/material")
def add_hvo_material(data: dict, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401, detail="Nicht angemeldet")
    name = (data.get("name") or "").strip()
    if not name: raise HTTPException(status_code=400, detail="Bezeichnung erforderlich")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO hvo_material (name, quantity, expiry_date, note) VALUES (%s, %s, %s, %s)",
        (name, int(data.get("quantity") or 1), data.get("expiry_date") or None, data.get("note") or "")
    )
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.delete("/api/hvo/material/{material_id}")
def delete_hvo_material(material_id: int, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM hvo_material WHERE id = %s", (material_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}


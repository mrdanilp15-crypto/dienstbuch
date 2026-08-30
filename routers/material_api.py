from fastapi import APIRouter, HTTPException, Request, Response, UploadFile, File
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Optional, List, Union
from datetime import datetime, date
import json

from database import get_db_connection
from core.utils import get_current_user, log_audit_action

router = APIRouter()


# --- MÄNGELMELDER (GERÄTEWART - ERWEITERT) ---
@router.post("/api/material/defect-reports")
def create_defect_report(data: dict, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    eq_id = data.get("equipment_id")
    desc = data.get("description", "").strip()
    severity = data.get("severity", "Mittel")
    image_url = data.get("image_url")
    assigned_to = data.get("assigned_to")
    priority = data.get("priority", "Mittel")
    if not eq_id or not desc:
        raise HTTPException(status_code=400, detail="Gerät und Beschreibung erforderlich.")
    reporter = data.get("reporter_name") or user["username"]
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO equipment_defect_reports (equipment_id, reporter_name, description, severity, image_url, assigned_to, priority) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (eq_id, reporter, desc, severity, image_url, assigned_to, priority)
    )
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "MANGEL_MELDEN", f"Mangel für Gerät-ID {eq_id} gemeldet: {desc[:60]}")
    return {"status": "success"}

@router.get("/api/material/defect-reports")
def get_defect_reports(request: Request, status: str = "Offen"):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    if status == "alle":
        cur.execute("""
            SELECT dr.*, e.name as equipment_name, e.barcode
            FROM equipment_defect_reports dr
            LEFT JOIN equipment e ON dr.equipment_id = e.id
            ORDER BY dr.created_at DESC LIMIT 100
        """)
    else:
        cur.execute("""
            SELECT dr.*, e.name as equipment_name, e.barcode
            FROM equipment_defect_reports dr
            LEFT JOIN equipment e ON dr.equipment_id = e.id
            WHERE dr.status = %s
            ORDER BY dr.created_at DESC LIMIT 100
        """, (status,))
    rows = cur.fetchall(); cur.close(); conn.close()
    for r in rows:
        for k in ["created_at", "resolved_at"]:
            if r.get(k): r[k] = str(r[k])
    return rows

@router.put("/api/material/defect-reports/{report_id}")
def resolve_defect_report(report_id: int, data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "geratewart"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    new_status = data.get("status", "Erledigt")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute(
        "UPDATE equipment_defect_reports SET status = %s, resolved_by = %s, resolved_at = NOW() WHERE id = %s",
        (new_status, user["username"], report_id)
    )
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "MANGEL_ERLEDIGT", f"Mangel-Meldung ID {report_id} als '{new_status}' markiert.")
    return {"status": "success"}

@router.delete("/api/material/defect-reports/{report_id}")
def delete_defect_report(report_id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "geratewart"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM equipment_defect_reports WHERE id = %s", (report_id,))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "MANGEL_GELOESCHT", f"Mangel-Meldung ID {report_id} gelöscht.")
    return {"status": "success"}

# --- DRONE MAP IMAGES ---
@router.get("/api/material/drone-images")
def get_drone_images(request: Request):
    check_auth = get_current_user(request)
    if not check_auth: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM drone_images ORDER BY id DESC")
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@router.post("/api/material/drone-images")
def add_drone_image(data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "geratewart"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    url = data.get("url")
    if not url: raise HTTPException(status_code=400, detail="URL erforderlich")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO drone_images (url) VALUES (%s)", (url,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.delete("/api/material/drone-images/{img_id}")
def delete_drone_image(img_id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "geratewart"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM drone_images WHERE id = %s", (img_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}


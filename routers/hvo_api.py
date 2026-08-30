from fastapi import APIRouter, HTTPException, Request, Response, UploadFile, File
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Optional, List, Union
from datetime import datetime, date
import json

from database import get_db_connection
from core.utils import get_current_user, log_audit_action

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


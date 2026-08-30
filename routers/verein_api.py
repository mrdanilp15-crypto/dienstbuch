from fastapi import APIRouter, HTTPException, Request, Response, UploadFile, File
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Optional, List, Union
from datetime import datetime, date
import json

from database import get_db_connection
from core.utils import get_current_user, log_audit_action

router = APIRouter()



@router.get("/api/verein/inventory")
def get_club_inventory(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM club_inventory ORDER BY item_name ASC")
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@router.post("/api/verein/inventory")
def add_club_inventory(data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    item = data.get("item_name")
    qty = int(data.get("quantity", 1))
    status = data.get("status", "OK")
    if not item: raise HTTPException(status_code=400, detail="Bezeichnung erforderlich")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO club_inventory (item_name, quantity, status) VALUES (%s, %s, %s)", (item, qty, status))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.delete("/api/verein/inventory/{i_id}")
def delete_club_inventory(i_id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM club_inventory WHERE id = %s", (i_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.get("/api/verein/donations")
def get_donations(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT *, DATE_FORMAT(date, '%d.%m.%Y') as formatted_date FROM club_donations ORDER BY date DESC")
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@router.post("/api/verein/donations")
def add_donation(data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    donor = data.get("donor")
    amount = float(data.get("amount", 0))
    date_val = data.get("date")
    if not donor or not date_val: raise HTTPException(status_code=400, detail="Spender und Datum erforderlich")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO club_donations (donor, amount, date) VALUES (%s, %s, %s)", (donor, amount, date_val))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}


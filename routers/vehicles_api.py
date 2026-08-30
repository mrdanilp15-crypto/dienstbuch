from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from typing import Optional

from database import get_db_connection
from core.utils import get_current_user, log_audit_action

router = APIRouter()

class VehicleData(BaseModel): 
    name: str
    radio_name: Optional[str] = ""
    status: Optional[int] = 2
    tuv_date: Optional[str] = None
    sp_date: Optional[str] = None
    milage: Optional[int] = 0
    next_service: Optional[str] = None

@router.get("/api/vehicles")
def get_vehicles(request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, name, radio_name, status, tuv_date, sp_date, milage, next_service FROM vehicles ORDER BY name")
    r = cur.fetchall(); c.close()
    for v in r:
        if v['tuv_date']: v['tuv_date'] = str(v['tuv_date'])
        if v['sp_date']: v['sp_date'] = str(v['sp_date'])
        if v['next_service']: v['next_service'] = str(v['next_service'])
    return r

@router.post("/api/vehicles")
def create_vehicle(v: VehicleData, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("""
        INSERT INTO vehicles (name, radio_name, status, tuv_date, sp_date, milage, next_service) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (v.name, v.radio_name, v.status or 2, v.tuv_date or None, v.sp_date or None, v.milage or 0, v.next_service or None))
    c.commit(); c.close()
    log_audit_action(user["username"], "FAHRZEUG_ANLEGEN", f"Fahrzeug '{v.name}' in Flotte aufgenommen.")
    return {"status": "created"}

@router.put("/api/vehicles/{id}")
def update_vehicle(id: int, v: VehicleData, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("""
        UPDATE vehicles 
        SET name=%s, radio_name=%s, status=%s, tuv_date=%s, sp_date=%s, milage=%s, next_service=%s 
        WHERE id=%s
    """, (v.name, v.radio_name, v.status or 2, v.tuv_date or None, v.sp_date or None, v.milage or 0, v.next_service or None, id))
    c.commit(); c.close()
    return {"status": "updated"}

@router.put("/api/vehicles/{id}/status")
def update_vehicle_status(id: int, data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt")
    new_status = data.get("status", 2)
    c = get_db_connection(); cur = c.cursor()
    cur.execute("UPDATE vehicles SET status=%s WHERE id=%s", (new_status, id))
    c.commit(); c.close()
    log_audit_action(user["username"], "FUNKSTATUS", f"Fahrzeug ID {id} auf BOS Status {new_status} gesetzt.")
    return {"status": "status updated"}

@router.delete("/api/vehicles/{id}")
def delete_vehicle(id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM vehicles WHERE id=%s", (id,))
    c.commit(); c.close()
    return {"status": "deleted"}

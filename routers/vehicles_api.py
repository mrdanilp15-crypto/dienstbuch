from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from typing import Optional

from database import get_db_connection
from core.utils import get_current_user, log_audit_action, check_display_access

router = APIRouter()

class VehicleData(BaseModel):
    name: str
    radio_name: Optional[str] = ""
    status: Optional[int] = 2
    tuv_date: Optional[str] = None
    sp_date: Optional[str] = None
    milage: Optional[int] = 0
    next_service: Optional[str] = None
    required_license: Optional[str] = None
    purchase_value: Optional[float] = None
    insurance_policy: Optional[str] = ""

class VehicleReservationCreate(BaseModel):
    purpose: str
    start_datetime: str
    end_datetime: str

_VEHICLE_PUBLIC_FIELDS = {"id", "name", "radio_name", "status"}

@router.get("/api/vehicles")
def get_vehicles(request: Request):
    # Kein Login-Zwang: wird auch vom Hallenmonitor (alarmdisplay.html) ohne Session gelesen -
    # braucht dafür aber einen gültigen Display-Token (siehe check_display_access). Zusätzlich
    # bekommen nicht angemeldete Aufrufer nur die für die Anzeige nötigen Felder zurück, nicht
    # Anschaffungswert/Versicherungs-Policennummer.
    check_display_access(request)
    is_authenticated = bool(get_current_user(request))
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, name, radio_name, status, tuv_date, sp_date, milage, next_service, required_license, purchase_value, insurance_policy FROM vehicles ORDER BY name")
    r = cur.fetchall(); c.close()
    for v in r:
        if v['tuv_date']: v['tuv_date'] = str(v['tuv_date'])
        if v['sp_date']: v['sp_date'] = str(v['sp_date'])
        if v['next_service']: v['next_service'] = str(v['next_service'])
    if not is_authenticated:
        r = [{k: v[k] for k in _VEHICLE_PUBLIC_FIELDS} for v in r]
    return r

@router.post("/api/vehicles")
def create_vehicle(v: VehicleData, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "geratewart"): raise HTTPException(status_code=403, detail="Schreibgeschützt")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("""
        INSERT INTO vehicles (name, radio_name, status, tuv_date, sp_date, milage, next_service, required_license, purchase_value, insurance_policy)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (v.name, v.radio_name, v.status or 2, v.tuv_date or None, v.sp_date or None, v.milage or 0, v.next_service or None, v.required_license or None, v.purchase_value, v.insurance_policy or ""))
    c.commit(); c.close()
    log_audit_action(user["username"], "FAHRZEUG_ANLEGEN", f"Fahrzeug '{v.name}' in Flotte aufgenommen.")
    return {"status": "created"}

@router.put("/api/vehicles/{id}")
def update_vehicle(id: int, v: VehicleData, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "geratewart"): raise HTTPException(status_code=403, detail="Schreibgeschützt")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("""
        UPDATE vehicles
        SET name=%s, radio_name=%s, status=%s, tuv_date=%s, sp_date=%s, milage=%s, next_service=%s, required_license=%s, purchase_value=%s, insurance_policy=%s
        WHERE id=%s
    """, (v.name, v.radio_name, v.status or 2, v.tuv_date or None, v.sp_date or None, v.milage or 0, v.next_service or None, v.required_license or None, v.purchase_value, v.insurance_policy or "", id))
    c.commit(); c.close()
    log_audit_action(user["username"], "FAHRZEUG_BEARBEITEN", f"Fahrzeug ID {id} ('{v.name}') aktualisiert.")
    return {"status": "updated"}

@router.put("/api/vehicles/{id}/status")
def update_vehicle_status(id: int, data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "geratewart"): raise HTTPException(status_code=403, detail="Schreibgeschützt")
    new_status = data.get("status", 2)
    c = get_db_connection(); cur = c.cursor()
    cur.execute("UPDATE vehicles SET status=%s WHERE id=%s", (new_status, id))
    c.commit(); c.close()
    log_audit_action(user["username"], "FUNKSTATUS", f"Fahrzeug ID {id} auf BOS Status {new_status} gesetzt.")
    return {"status": "status updated"}

@router.delete("/api/vehicles/{id}")
def delete_vehicle(id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "geratewart"): raise HTTPException(status_code=403, detail="Schreibgeschützt")
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT name FROM vehicles WHERE id=%s", (id,))
    row = cur.fetchone()
    cur.execute("DELETE FROM vehicles WHERE id=%s", (id,))
    c.commit(); c.close()
    log_audit_action(user["username"], "FAHRZEUG_LOESCHEN", f"Fahrzeug ID {id} ('{row['name'] if row else '?'}') gelöscht.")
    return {"status": "deleted"}

# --- BELEGUNGSPLAN (Reservierungen, um Doppelbelegungen bei geteilten Fahrzeugen zu vermeiden) ---
@router.get("/api/vehicles/{id}/reservations")
def list_vehicle_reservations(id: int, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute(
        "SELECT * FROM vehicle_reservations WHERE vehicle_id = %s AND end_datetime >= NOW() ORDER BY start_datetime ASC",
        (id,)
    )
    res = cur.fetchall(); c.close()
    for r in res:
        r["start_datetime"] = str(r["start_datetime"])
        r["end_datetime"] = str(r["end_datetime"])
    return res

@router.post("/api/vehicles/{id}/reservations")
def create_vehicle_reservation(id: int, r: VehicleReservationCreate, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    if not r.purpose or not r.purpose.strip():
        raise HTTPException(status_code=400, detail="Zweck der Reservierung erforderlich")
    if r.end_datetime <= r.start_datetime:
        raise HTTPException(status_code=400, detail="Ende muss nach dem Start liegen")
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    # Überschneidungsprüfung: zwei Zeiträume überlappen sich, wenn Start1 < Ende2 UND Start2 < Ende1.
    cur.execute(
        "SELECT id, purpose, reserved_by FROM vehicle_reservations "
        "WHERE vehicle_id = %s AND start_datetime < %s AND %s < end_datetime",
        (id, r.end_datetime, r.start_datetime)
    )
    conflict = cur.fetchone()
    if conflict:
        c.close()
        raise HTTPException(status_code=400, detail=f"Fahrzeug ist in diesem Zeitraum bereits reserviert ({conflict['purpose']} - {conflict['reserved_by']})")
    cur.execute(
        "INSERT INTO vehicle_reservations (vehicle_id, purpose, reserved_by, start_datetime, end_datetime) VALUES (%s, %s, %s, %s, %s)",
        (id, r.purpose.strip(), user["username"], r.start_datetime, r.end_datetime)
    )
    c.commit(); c.close()
    return {"status": "success"}

@router.delete("/api/vehicles/reservations/{reservation_id}")
def delete_vehicle_reservation(reservation_id: int, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT reserved_by FROM vehicle_reservations WHERE id = %s", (reservation_id,))
    row = cur.fetchone()
    if not row:
        c.close()
        raise HTTPException(status_code=404, detail="Reservierung nicht gefunden")
    if row["reserved_by"] != user["username"] and user["role"] not in ("admin", "leitung", "geratewart"):
        c.close()
        raise HTTPException(status_code=403, detail="Nur die eigene Reservierung oder Admin/Leitung/Gerätewart können stornieren")
    cur.execute("DELETE FROM vehicle_reservations WHERE id = %s", (reservation_id,))
    c.commit(); c.close()
    return {"status": "success"}

# --- WARTUNGSKOSTEN-HISTORIE ---
class MaintenanceCreate(BaseModel):
    date: str
    description: str
    workshop: Optional[str] = ""
    cost: Optional[float] = 0

@router.get("/api/vehicles/{id}/maintenance")
def list_vehicle_maintenance(id: int, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT * FROM vehicle_maintenance WHERE vehicle_id = %s ORDER BY date DESC", (id,))
    res = cur.fetchall(); c.close()
    for r in res:
        r["date"] = str(r["date"])
        r["cost"] = float(r["cost"])
    return res

@router.post("/api/vehicles/{id}/maintenance")
def add_vehicle_maintenance(id: int, m: MaintenanceCreate, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "geratewart"): raise HTTPException(status_code=403, detail="Schreibgeschützt")
    if not m.description or not m.description.strip():
        raise HTTPException(status_code=400, detail="Beschreibung erforderlich")
    c = get_db_connection(); cur = c.cursor()
    cur.execute(
        "INSERT INTO vehicle_maintenance (vehicle_id, date, description, workshop, cost) VALUES (%s, %s, %s, %s, %s)",
        (id, m.date, m.description.strip(), m.workshop or "", m.cost or 0)
    )
    c.commit(); c.close()
    return {"status": "success"}

@router.delete("/api/vehicles/maintenance/{entry_id}")
def delete_vehicle_maintenance(entry_id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "geratewart"): raise HTTPException(status_code=403, detail="Schreibgeschützt")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM vehicle_maintenance WHERE id = %s", (entry_id,))
    c.commit(); c.close()
    return {"status": "success"}

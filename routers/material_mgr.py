from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from typing import List, Optional
import mysql.connector
import os
from datetime import date

router = APIRouter(prefix="/api/material", tags=["Material"])
def get_db_connection():
    host = os.getenv("DB_HOST", os.getenv("MYSQL_HOST", "db"))
    user = os.getenv("DB_USER", os.getenv("MYSQL_USER", "app_user"))
    password = os.getenv("DB_PASSWORD") or os.getenv("DB_PASS") or os.getenv("MYSQL_PASSWORD") or "dein_app_passwort"
    database = os.getenv("DB_NAME", os.getenv("MYSQL_DATABASE", "attendance_system"))
    port = int(os.getenv("DB_PORT", os.getenv("MYSQL_PORT", "3306")))
    return mysql.connector.connect(
        host=host, user=user, password=password, database=database, port=port
    )

def check_auth(request: Request, require_admin: bool = False) -> dict:
    from main import get_current_user
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht angemeldet")
    if require_admin and user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Keine Berechtigung (Admin erforderlich)")
    return user

class EquipmentCreate(BaseModel):
    name: str
    barcode: str
    category: str
    image_url: Optional[str] = ""
    manual_url: Optional[str] = ""
    interval_months: Optional[int] = 12
    last_inspection: Optional[str] = None
    next_inspection: Optional[str] = None

class BatchInspectRequest(BaseModel):
    barcodes: List[str]
    inspector: str
    status: str # 'Bestanden', 'Mangel', 'Defekt'
    note: Optional[str] = ""

class InspectionCreate(BaseModel):
    date: str
    inspector: str
    status: str
    note: Optional[str] = ""

class InventarCreate(BaseModel):
    item_name: str
    size: str
    issue_date: str

class CourseCreate(BaseModel):
    course_name: str
    date: str
    certificate_url: Optional[str] = ""

class BmaCreate(BaseModel):
    object_name: str
    address: str
    bma_number: str
    key_depot: Optional[bool] = False
    map_url: Optional[str] = ""
    lat: Optional[float] = None
    lng: Optional[float] = None

class HydrantCreate(BaseModel):
    lat: float
    lng: float
    type: str # 'Unterflur', 'Überflur', 'Zisterne'
    label: str

class VehicleLogCreate(BaseModel):
    date: str
    mileage_start: int
    mileage_end: int
    driver_name: str
    purpose: str

# --- 🔧 GERÄTE & MATERIAL ENDPUNKTE ---
@router.get("/equipment")
def list_equipment(request: Request):
    check_auth(request)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT e.*,
               (SELECT status FROM equipment_inspections WHERE equipment_id = e.id ORDER BY date DESC, id DESC LIMIT 1) as current_status
        FROM equipment e
        ORDER BY e.next_inspection ASC
    """)
    res = cur.fetchall(); cur.close(); conn.close()
    for row in res:
        if isinstance(row["last_inspection"], date):
            row["last_inspection"] = str(row["last_inspection"])
        if isinstance(row["next_inspection"], date):
            row["next_inspection"] = str(row["next_inspection"])
        if not row.get("current_status"):
            row["current_status"] = "Bestanden"
    return res

@router.post("/equipment")
def create_equipment(eq: EquipmentCreate, request: Request):
    user = check_auth(request)
    if user["role"] == "mannschaft":
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    last_i = eq.last_inspection if eq.last_inspection else None
    next_i = eq.next_inspection if eq.next_inspection else None
    cur.execute("""
        INSERT INTO equipment (name, barcode, category, image_url, manual_url, interval_months, last_inspection, next_inspection)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (eq.name.strip(), eq.barcode.strip(), eq.category, eq.image_url, eq.manual_url, eq.interval_months, last_i, next_i))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.put("/equipment/{eq_id}")
def update_equipment(eq_id: int, eq: EquipmentCreate, request: Request):
    user = check_auth(request)
    if user["role"] == "mannschaft":
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    last_i = eq.last_inspection if eq.last_inspection else None
    next_i = eq.next_inspection if eq.next_inspection else None
    cur.execute("""
        UPDATE equipment 
        SET name=%s, barcode=%s, category=%s, image_url=%s, manual_url=%s, interval_months=%s, last_inspection=%s, next_inspection=%s
        WHERE id=%s
    """, (eq.name.strip(), eq.barcode.strip(), eq.category, eq.image_url, eq.manual_url, eq.interval_months, last_i, next_i, eq_id))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.delete("/equipment/{eq_id}")
def delete_equipment(eq_id: int, request: Request):
    user = check_auth(request, require_admin=True)
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM equipment WHERE id = %s", (eq_id,))
    cur.execute("DELETE FROM equipment_inspections WHERE equipment_id = %s", (eq_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.get("/equipment/{eq_id}/inspections")
def list_inspections(eq_id: int, request: Request):
    check_auth(request)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM equipment_inspections WHERE equipment_id = %s ORDER BY date DESC", (eq_id,))
    res = cur.fetchall(); cur.close(); conn.close()
    for row in res:
        if isinstance(row["date"], date):
            row["date"] = str(row["date"])
    return res

@router.post("/equipment/{eq_id}/inspections")
def create_inspection(eq_id: int, insp: InspectionCreate, request: Request):
    user = check_auth(request)
    if user["role"] == "mannschaft":
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO equipment_inspections (equipment_id, date, inspector, status, note)
        VALUES (%s, %s, %s, %s, %s)
    """, (eq_id, insp.date, insp.inspector, insp.status, insp.note))
    cur.execute("""
        UPDATE equipment 
        SET last_inspection = %s, next_inspection = DATE_ADD(%s, INTERVAL interval_months MONTH)
        WHERE id = %s
    """, (insp.date, insp.date, eq_id))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.post("/equipment/batch-inspect")
def batch_inspect(b: BatchInspectRequest, request: Request):
    user = check_auth(request)
    if user["role"] == "mannschaft":
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    today = date.today().isoformat()
    for bc in b.barcodes:
        cur.execute("SELECT id, interval_months FROM equipment WHERE barcode = %s", (bc.strip(),))
        eq = cur.fetchone()
        if eq:
            eq_id, interval = eq
            cur.execute("""
                INSERT INTO equipment_inspections (equipment_id, date, inspector, status, note)
                VALUES (%s, %s, %s, %s, %s)
            """, (eq_id, today, b.inspector, b.status, b.note))
            cur.execute("""
                UPDATE equipment 
                SET last_inspection = %s, next_inspection = DATE_ADD(%s, INTERVAL interval_months MONTH)
                WHERE id = %s
            """, (today, today, eq_id))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

# --- 👕 PERSONAL-INVENTAR (BEKLEIDUNG) ---
@router.get("/personnel/{p_id}/inventar")
def list_personal_inventar(p_id: int, request: Request):
    check_auth(request)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM personal_inventar WHERE personnel_id = %s ORDER BY issue_date DESC", (p_id,))
    res = cur.fetchall(); cur.close(); conn.close()
    for row in res:
        if isinstance(row["issue_date"], date):
            row["issue_date"] = str(row["issue_date"])
        if isinstance(row["return_date"], date):
            row["return_date"] = str(row["return_date"])
    return res

@router.post("/personnel/{p_id}/inventar")
def add_inventar_item(p_id: int, item: InventarCreate, request: Request):
    user = check_auth(request)
    if user["role"] == "mannschaft":
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO personal_inventar (personnel_id, item_name, size, issue_date)
        VALUES (%s, %s, %s, %s)
    """, (p_id, item.item_name.strip(), item.size.strip(), item.issue_date))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.delete("/personnel/inventar/{item_id}")
def delete_inventar_item(item_id: int, request: Request):
    user = check_auth(request)
    if user["role"] == "mannschaft":
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM personal_inventar WHERE id = %s", (item_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

# --- 🎓 LEHRGÄNGE & AUSBILDUNG ---
@router.get("/personnel/{p_id}/lehrgaenge")
def list_lehrgaenge(p_id: int, request: Request):
    check_auth(request)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM lehrgaenge WHERE personnel_id = %s ORDER BY date DESC", (p_id,))
    res = cur.fetchall(); cur.close(); conn.close()
    for row in res:
        if isinstance(row["date"], date):
            row["date"] = str(row["date"])
    return res

@router.post("/personnel/{p_id}/lehrgaenge")
def add_lehrgang(p_id: int, course: CourseCreate, request: Request):
    user = check_auth(request)
    if user["role"] == "mannschaft":
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO lehrgaenge (personnel_id, course_name, date, certificate_url)
        VALUES (%s, %s, %s, %s)
    """, (p_id, course.course_name.strip(), course.date, course.certificate_url))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.delete("/personnel/lehrgaenge/{course_id}")
def delete_lehrgang(course_id: int, request: Request):
    user = check_auth(request)
    if user["role"] == "mannschaft":
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM lehrgaenge WHERE id = %s", (course_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

# --- 🏢 OBJEKTE & BRANDMELDEANLAGEN (BMA) ---
@router.get("/bma")
def list_bmas(request: Request):
    check_auth(request)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM bma ORDER BY object_name ASC")
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@router.post("/bma")
def create_bma(b: BmaCreate, request: Request):
    user = check_auth(request)
    if user["role"] == "mannschaft":
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO bma (object_name, address, bma_number, key_depot, map_url, lat, lng)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (b.object_name.strip(), b.address.strip(), b.bma_number.strip(), int(b.key_depot), b.map_url, b.lat, b.lng))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.put("/bma/{b_id}")
def update_bma(b_id: int, b: BmaCreate, request: Request):
    user = check_auth(request)
    if user["role"] == "mannschaft":
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        UPDATE bma 
        SET object_name=%s, address=%s, bma_number=%s, key_depot=%s, map_url=%s, lat=%s, lng=%s
        WHERE id=%s
    """, (b.object_name.strip(), b.address.strip(), b.bma_number.strip(), int(b.key_depot), b.map_url, b.lat, b.lng, b_id))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.delete("/bma/{b_id}")
def delete_bma(b_id: int, request: Request):
    user = check_auth(request, require_admin=True)
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM bma WHERE id = %s", (b_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

# --- 🗺️ HYDRANTEN & WATER POINTS ---
@router.get("/hydrants")
def list_hydrants(request: Request):
    check_auth(request)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM hydrants")
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@router.post("/hydrants")
def create_hydrant(h: HydrantCreate, request: Request):
    user = check_auth(request)
    if user["role"] == "mannschaft":
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO hydrants (lat, lng, type, label)
        VALUES (%s, %s, %s, %s)
    """, (h.lat, h.lng, h.type, h.label.strip()))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.delete("/hydrants/{h_id}")
def delete_hydrant(h_id: int, request: Request):
    user = check_auth(request)
    if user["role"] == "mannschaft":
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM hydrants WHERE id = %s", (h_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

# --- 📖 FAHRTENBUCH ---
@router.get("/vehicles/{veh_id}/log")
def list_vehicle_logs(veh_id: int, request: Request):
    check_auth(request)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM vehicle_log WHERE vehicle_id = %s ORDER BY date DESC, id DESC", (veh_id,))
    res = cur.fetchall(); cur.close(); conn.close()
    for row in res:
        if isinstance(row["date"], date):
            row["date"] = str(row["date"])
    return res

@router.post("/vehicles/{veh_id}/log")
def add_vehicle_log(veh_id: int, log: VehicleLogCreate, request: Request):
    user = check_auth(request)
    if user["role"] == "mannschaft":
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO vehicle_log (vehicle_id, date, mileage_start, mileage_end, driver_name, purpose)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (veh_id, log.date, log.mileage_start, log.mileage_end, log.driver_name.strip(), log.purpose.strip()))
    
    # Automatisch Kilometerstand des Fahrzeugs aktualisieren
    cur.execute("UPDATE vehicles SET milage = %s WHERE id = %s", (log.mileage_end, veh_id))
    
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

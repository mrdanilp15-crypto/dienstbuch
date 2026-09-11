from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel
from typing import List, Optional
from datetime import date
import mysql.connector

router = APIRouter(prefix="/api/material", tags=["Material"])
from database import get_db_connection

def check_auth(request: Request, require_admin: bool = False, allowed_roles: tuple = None) -> dict:
    from core.utils import get_current_user
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht angemeldet")
    if require_admin and user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Keine Berechtigung (Admin erforderlich)")
    if allowed_roles and user["role"] not in allowed_roles:
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
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
    purchase_value: Optional[float] = None
    insurance_policy: Optional[str] = ""

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
    valid_until: Optional[str] = None

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

class VehicleCheckCreate(BaseModel):
    date: str
    checker_name: str
    status: str
    items_checked: dict
    notes: str = ""

class LoanCreate(BaseModel):
    borrower_name: str
    note: Optional[str] = ""

# --- 🔧 GERÄTE & MATERIAL ENDPUNKTE ---
@router.get("/equipment")
def list_equipment(request: Request):
    check_auth(request)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT e.*,
               (SELECT status FROM equipment_inspections WHERE equipment_id = e.id ORDER BY date DESC, id DESC LIMIT 1) as current_status,
               (SELECT borrower_name FROM equipment_loans WHERE equipment_id = e.id AND returned_at IS NULL ORDER BY checked_out_at DESC LIMIT 1) as loaned_to,
               (SELECT checked_out_at FROM equipment_loans WHERE equipment_id = e.id AND returned_at IS NULL ORDER BY checked_out_at DESC LIMIT 1) as loaned_since
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
        if row.get("loaned_since"):
            row["loaned_since"] = str(row["loaned_since"])
    return res

@router.post("/equipment")
def create_equipment(eq: EquipmentCreate, request: Request):
    user = check_auth(request, allowed_roles=("admin", "leitung", "geratewart"))
    conn = get_db_connection(); cur = conn.cursor()
    last_i = eq.last_inspection if eq.last_inspection else None
    next_i = eq.next_inspection if eq.next_inspection else None
    try:
        cur.execute("""
            INSERT INTO equipment (name, barcode, category, image_url, manual_url, interval_months, last_inspection, next_inspection, purchase_value, insurance_policy)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (eq.name.strip(), eq.barcode.strip(), eq.category, eq.image_url, eq.manual_url, eq.interval_months, last_i, next_i, eq.purchase_value, eq.insurance_policy or ""))
        conn.commit()
    except mysql.connector.Error as err:
        # barcode ist UNIQUE NOT NULL - ohne diese Behandlung schlug das Anlegen bei einem
        # bereits vergebenen Barcode mit einem unbehandelten 500-Fehler fehl. Das Frontend
        # zeigte dabei keinerlei Meldung an, das Modal blieb einfach offen - für den Nutzer
        # sah es so aus, als wäre das neu angelegte Gerät "verschwunden", obwohl es nie
        # gespeichert wurde.
        if err.errno == 1062:
            raise HTTPException(status_code=400, detail=f"Barcode '{eq.barcode.strip()}' ist bereits einem anderen Gerät zugeordnet!")
        raise HTTPException(status_code=500, detail=str(err))
    finally:
        cur.close(); conn.close()
    return {"status": "success"}

@router.put("/equipment/{eq_id}")
def update_equipment(eq_id: int, eq: EquipmentCreate, request: Request):
    user = check_auth(request, allowed_roles=("admin", "leitung", "geratewart"))
    conn = get_db_connection(); cur = conn.cursor()
    last_i = eq.last_inspection if eq.last_inspection else None
    next_i = eq.next_inspection if eq.next_inspection else None
    try:
        cur.execute("""
            UPDATE equipment
            SET name=%s, barcode=%s, category=%s, image_url=%s, manual_url=%s, interval_months=%s, last_inspection=%s, next_inspection=%s, purchase_value=%s, insurance_policy=%s
            WHERE id=%s
        """, (eq.name.strip(), eq.barcode.strip(), eq.category, eq.image_url, eq.manual_url, eq.interval_months, last_i, next_i, eq.purchase_value, eq.insurance_policy or "", eq_id))
        conn.commit()
    except mysql.connector.Error as err:
        if err.errno == 1062:
            raise HTTPException(status_code=400, detail=f"Barcode '{eq.barcode.strip()}' ist bereits einem anderen Gerät zugeordnet!")
        raise HTTPException(status_code=500, detail=str(err))
    finally:
        cur.close(); conn.close()
    return {"status": "success"}

@router.delete("/equipment/{eq_id}")
def delete_equipment(eq_id: int, request: Request):
    user = check_auth(request, allowed_roles=("admin", "leitung", "geratewart"))
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM equipment_inspections WHERE equipment_id = %s", (eq_id,))
    cur.execute("DELETE FROM equipment_defect_reports WHERE equipment_id = %s", (eq_id,))
    cur.execute("DELETE FROM equipment WHERE id = %s", (eq_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.get("/equipment/{eq_id}/loans")
def list_loans(eq_id: int, request: Request):
    check_auth(request)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM equipment_loans WHERE equipment_id = %s ORDER BY checked_out_at DESC", (eq_id,))
    res = cur.fetchall(); cur.close(); conn.close()
    for row in res:
        row["checked_out_at"] = str(row["checked_out_at"])
        row["returned_at"] = str(row["returned_at"]) if row.get("returned_at") else None
    return res

@router.post("/equipment/{eq_id}/loans")
def checkout_equipment(eq_id: int, loan: LoanCreate, request: Request):
    check_auth(request)
    if not loan.borrower_name or not loan.borrower_name.strip():
        raise HTTPException(status_code=400, detail="Name des Ausleihenden erforderlich")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id FROM equipment_loans WHERE equipment_id = %s AND returned_at IS NULL", (eq_id,))
    if cur.fetchone():
        cur.close(); conn.close()
        raise HTTPException(status_code=400, detail="Gerät ist bereits ausgeliehen. Erst zurückgeben, dann erneut ausleihen.")
    cur.execute(
        "INSERT INTO equipment_loans (equipment_id, borrower_name, note) VALUES (%s, %s, %s)",
        (eq_id, loan.borrower_name.strip(), (loan.note or "").strip())
    )
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.put("/equipment/loans/{loan_id}/return")
def return_equipment(loan_id: int, request: Request):
    check_auth(request)
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("UPDATE equipment_loans SET returned_at = NOW() WHERE id = %s AND returned_at IS NULL", (loan_id,))
    conn.commit()
    affected = cur.rowcount
    cur.close(); conn.close()
    if affected == 0:
        raise HTTPException(status_code=404, detail="Keine offene Ausleihe mit dieser ID gefunden")
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

@router.get("/equipment/{eq_id}/report/pdf")
def get_equipment_report_pdf(eq_id: int, request: Request):
    check_auth(request)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM equipment WHERE id = %s", (eq_id,))
    eq = cur.fetchone()
    if not eq:
        cur.close(); conn.close()
        raise HTTPException(status_code=404, detail="Gerät nicht gefunden")

    cur.execute("SELECT * FROM equipment_inspections WHERE equipment_id = %s ORDER BY date DESC, id DESC", (eq_id,))
    inspections = cur.fetchall()
    cur.execute("SELECT * FROM equipment_defect_reports WHERE equipment_id = %s ORDER BY created_at DESC", (eq_id,))
    defects = cur.fetchall()
    cur.close(); conn.close()

    from core.utils import get_station_name
    station_name = get_station_name()
    today_fmt = date.today().strftime("%d.%m.%Y")

    insp_rows = ""
    for i in inspections:
        d = i["date"].strftime("%d.%m.%Y") if isinstance(i["date"], date) else str(i["date"])
        insp_rows += f"<tr><td>{d}</td><td>{i['inspector']}</td><td>{i['status']}</td><td>{i.get('note') or ''}</td></tr>"
    if not insp_rows:
        insp_rows = "<tr><td colspan='4' style='text-align:center; color:#6b7280;'>Keine Prüfungen erfasst.</td></tr>"

    defect_rows = ""
    for d in defects:
        created = d["created_at"].strftime("%d.%m.%Y") if hasattr(d["created_at"], "strftime") else str(d["created_at"])
        resolved = d["resolved_at"].strftime("%d.%m.%Y") if d.get("resolved_at") and hasattr(d["resolved_at"], "strftime") else "-"
        defect_rows += f"<tr><td>{created}</td><td>{d['reporter_name']}</td><td>{d['severity']}</td><td>{d['description']}</td><td>{d['status']}</td><td>{resolved}</td></tr>"
    if not defect_rows:
        defect_rows = "<tr><td colspan='6' style='text-align:center; color:#6b7280;'>Keine Mängel gemeldet.</td></tr>"

    # WICHTIG: kein CSS-Flexbox (xhtml2pdf unterstützt das nicht, siehe reports.py) -
    # ausschließlich Tabellen für nebeneinander liegende Elemente.
    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        @page {{ size: a4 portrait; margin: 2cm; }}
        body {{ font-family: Helvetica, Arial, sans-serif; font-size: 10.5pt; color: #1f2937; }}
        .header-table {{ width: 100%; border-bottom: 2px solid #b91c1c; padding-bottom: 12px; margin-bottom: 20px; }}
        .station-title {{ font-size: 16pt; font-weight: bold; color: #b91c1c; margin: 0; }}
        .doc-title {{ font-size: 14pt; font-weight: bold; text-transform: uppercase; margin-top: 15px; margin-bottom: 15px; color: #111827; }}
        .data-table {{ width: 100%; border-collapse: collapse; margin: 10px 0 25px 0; }}
        .data-table td {{ padding: 6px 8px; vertical-align: top; }}
        .data-table td.label {{ width: 30%; font-weight: bold; color: #374151; border-bottom: 1px solid #f3f4f6; }}
        .data-table td.value {{ width: 70%; border-bottom: 1px solid #f3f4f6; }}
        h3 {{ font-size: 12pt; color: #b91c1c; margin-top: 25px; margin-bottom: 8px; }}
        table.list-table {{ width: 100%; border-collapse: collapse; }}
        table.list-table th {{ background: #f8f9fa; border: 1px solid #ddd; padding: 6px 8px; text-align: left; font-size: 8.5pt; text-transform: uppercase; }}
        table.list-table td {{ border: 1px solid #ddd; padding: 6px 8px; font-size: 9.5pt; }}
    </style>
</head>
<body>
    <table class="header-table">
        <tr>
            <td><div class="station-title">{station_name}</div></td>
            <td style="text-align: right; vertical-align: bottom; font-size: 9pt; color: #4b5563;">Erstellt am: {today_fmt}</td>
        </tr>
    </table>
    <div class="doc-title">Prüf- und Mängelnachweis</div>
    <table class="data-table">
        <tr><td class="label">Gerätebezeichnung:</td><td class="value"><strong>{eq['name']}</strong></td></tr>
        <tr><td class="label">Barcode / ID:</td><td class="value">{eq['barcode']}</td></tr>
        <tr><td class="label">Kategorie:</td><td class="value">{eq['category']}</td></tr>
        <tr><td class="label">Prüfintervall:</td><td class="value">{eq.get('interval_months') or '-'} Monate</td></tr>
    </table>
    <h3>Prüfhistorie</h3>
    <table class="list-table">
        <thead><tr><th>Datum</th><th>Prüfer</th><th>Status</th><th>Notiz</th></tr></thead>
        <tbody>{insp_rows}</tbody>
    </table>
    <h3>Gemeldete Mängel</h3>
    <table class="list-table">
        <thead><tr><th>Gemeldet am</th><th>Melder</th><th>Schwere</th><th>Beschreibung</th><th>Status</th><th>Erledigt am</th></tr></thead>
        <tbody>{defect_rows}</tbody>
    </table>
</body>
</html>"""

    import xhtml2pdf.pisa as pisa
    import io
    pdf_buf = io.BytesIO()
    pisa.CreatePDF(html_content, dest=pdf_buf)
    pdf_bytes = pdf_buf.getvalue()

    safe_name = eq['name'].replace(' ', '_').replace('/', '_')
    filename = f"Pruefnachweis_{safe_name}.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{filename}"'}
    )

@router.post("/equipment/{eq_id}/inspections")
def create_inspection(eq_id: int, insp: InspectionCreate, request: Request):
    user = check_auth(request, allowed_roles=("admin", "leitung", "geratewart"))
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
    user = check_auth(request, allowed_roles=("admin", "leitung", "geratewart"))
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
    user = check_auth(request, allowed_roles=("admin", "leitung", "geratewart"))
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO personal_inventar (personnel_id, item_name, size, issue_date)
        VALUES (%s, %s, %s, %s)
    """, (p_id, item.item_name.strip(), item.size.strip(), item.issue_date))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.delete("/personnel/inventar/{item_id}")
def delete_inventar_item(item_id: int, request: Request):
    user = check_auth(request, allowed_roles=("admin", "leitung", "geratewart"))
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
        if isinstance(row.get("valid_until"), date):
            row["valid_until"] = str(row["valid_until"])
    return res

@router.post("/personnel/{p_id}/lehrgaenge")
def add_lehrgang(p_id: int, course: CourseCreate, request: Request):
    user = check_auth(request, allowed_roles=("admin", "leitung"))
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO lehrgaenge (personnel_id, course_name, date, certificate_url, valid_until)
        VALUES (%s, %s, %s, %s, %s)
    """, (p_id, course.course_name.strip(), course.date, course.certificate_url, course.valid_until or None))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.delete("/personnel/lehrgaenge/{course_id}")
def delete_lehrgang(course_id: int, request: Request):
    user = check_auth(request, allowed_roles=("admin", "leitung"))
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM lehrgaenge WHERE id = %s", (course_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

# --- 🏢 OBJEKTE & BRANDMELDEANLAGEN (BMA) ---
@router.get("/bma")
def list_bmas(request: Request):
    # Kein check_auth: wird auch vom Hallenmonitor (alarmdisplay.html) ohne Session gelesen.
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM bma ORDER BY object_name ASC")
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@router.post("/bma")
def create_bma(b: BmaCreate, request: Request):
    user = check_auth(request, allowed_roles=("admin", "leitung"))
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO bma (object_name, address, bma_number, key_depot, map_url, lat, lng)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (b.object_name.strip(), b.address.strip(), b.bma_number.strip(), int(b.key_depot), b.map_url, b.lat, b.lng))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.put("/bma/{b_id}")
def update_bma(b_id: int, b: BmaCreate, request: Request):
    user = check_auth(request, allowed_roles=("admin", "leitung"))
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
    user = check_auth(request, allowed_roles=("admin", "leitung"))
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM bma WHERE id = %s", (b_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

# --- 🗺️ HYDRANTEN & WATER POINTS ---
@router.get("/hydrants")
def list_hydrants(request: Request):
    # Kein check_auth: wird auch vom Hallenmonitor (alarmdisplay.html) ohne Session gelesen.
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM hydrants")
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@router.post("/hydrants")
def create_hydrant(h: HydrantCreate, request: Request):
    user = check_auth(request, allowed_roles=("admin", "leitung", "geratewart"))
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO hydrants (lat, lng, type, label)
        VALUES (%s, %s, %s, %s)
    """, (h.lat, h.lng, h.type, h.label.strip()))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.delete("/hydrants/{h_id}")
def delete_hydrant(h_id: int, request: Request):
    user = check_auth(request, allowed_roles=("admin", "leitung", "geratewart"))
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
    user = check_auth(request, allowed_roles=("admin", "leitung", "geratewart", "gruppenfuehrer"))
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO vehicle_log (vehicle_id, date, mileage_start, mileage_end, driver_name, purpose)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (veh_id, log.date, log.mileage_start, log.mileage_end, log.driver_name.strip(), log.purpose.strip()))
    
    # Automatisch Kilometerstand des Fahrzeugs aktualisieren
    cur.execute("UPDATE vehicles SET milage = %s WHERE id = %s", (log.mileage_end, veh_id))
    
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.get("/vehicles/{veh_id}/checks")
def list_vehicle_checks(veh_id: int, request: Request):
    check_auth(request)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM vehicle_checks WHERE vehicle_id = %s ORDER BY date DESC, id DESC", (veh_id,))
    res = cur.fetchall()
    cur.close(); conn.close()
    import json
    for r in res:
        if r["items_checked"] and isinstance(r["items_checked"], str):
            r["items_checked"] = json.loads(r["items_checked"])
    return res

@router.post("/vehicles/{veh_id}/checks")
def add_vehicle_check(veh_id: int, check: VehicleCheckCreate, request: Request):
    user = check_auth(request, allowed_roles=("admin", "leitung", "geratewart", "gruppenfuehrer"))
    import json
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO vehicle_checks (vehicle_id, date, checker_name, status, items_checked, notes)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (veh_id, check.date, check.checker_name, check.status, json.dumps(check.items_checked), check.notes))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

# --- VERBRAUCHSMATERIAL / BESTANDSALARM ---
@router.get("/consumables")
def list_consumables(request: Request):
    check_auth(request)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM consumables ORDER BY name ASC")
    res = cur.fetchall(); cur.close(); conn.close()
    for r in res:
        r["current_stock"] = float(r["current_stock"])
        r["min_stock"] = float(r["min_stock"])
    return res

@router.post("/consumables")
def add_consumable(data: dict, request: Request):
    check_auth(request, allowed_roles=("admin", "leitung", "geratewart"))
    name = (data.get("name") or "").strip()
    if not name: raise HTTPException(status_code=400, detail="Bezeichnung erforderlich")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO consumables (name, unit, current_stock, min_stock, note) VALUES (%s, %s, %s, %s, %s)",
        (name, data.get("unit") or "Stk", float(data.get("current_stock") or 0), float(data.get("min_stock") or 0), data.get("note") or "")
    )
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.put("/consumables/{item_id}")
def update_consumable(item_id: int, data: dict, request: Request):
    check_auth(request, allowed_roles=("admin", "leitung", "geratewart"))
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute(
        "UPDATE consumables SET name=%s, unit=%s, current_stock=%s, min_stock=%s, note=%s WHERE id=%s",
        (data.get("name"), data.get("unit") or "Stk", float(data.get("current_stock") or 0), float(data.get("min_stock") or 0), data.get("note") or "", item_id)
    )
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.delete("/consumables/{item_id}")
def delete_consumable(item_id: int, request: Request):
    check_auth(request, allowed_roles=("admin", "leitung", "geratewart"))
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM consumables WHERE id = %s", (item_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

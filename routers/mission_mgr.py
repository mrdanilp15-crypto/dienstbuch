from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel
from typing import List, Optional
import mysql.connector
import os
import base64
from datetime import date

router = APIRouter(prefix="/api/missions", tags=["Missions"])
from database import get_db_connection

def safe_decode(val):
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray)):
        try:
            return val.decode("utf-8")
        except Exception:
            return str(val)
    return str(val)

def check_auth(request: Request, require_admin: bool = False) -> dict:
    from main import get_current_user
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht angemeldet")
    if require_admin and user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Keine Berechtigung (Admin erforderlich)")
    return user

class MissionCreate(BaseModel):
    date: str
    time: str
    end_time: Optional[str] = ""
    stichwort: str
    adresse: str
    meldung: str
    description: Optional[str] = ""
    duration: Optional[float] = 2.0
    status: Optional[str] = "Entwurf"
    media_files: Optional[str] = ""
    group_id: Optional[int] = None
    attendance: Optional[List[MissionAttendanceEntry]] = []

class MissionAttendanceEntry(BaseModel):
    personnel_id: int
    is_present: str # 'Abgerückt', 'Bereitstellung', 'Nein'
    vehicle: Optional[str] = ""

class MissionUpdate(BaseModel):
    date: str
    time: str
    end_time: Optional[str] = ""
    stichwort: str
    adresse: str
    meldung: str
    description: str
    duration: float
    status: str
    media_files: Optional[str] = ""
    group_id: Optional[int] = None
    attendance: List[MissionAttendanceEntry]

class RespirationEntry(BaseModel):
    personnel_id: int
    druck_start: int
    druck_10: int
    druck_20: int
    druck_ende: int
    dauer: int
    fit_ok: Optional[bool] = True

class BillingCreate(BaseModel):
    recipient_name: str
    address: str
    amount: float
    details: str

class ScheduleCreate(BaseModel):
    title: str
    date: str
    time: str
    description: Optional[str] = ""
    type: str # 'Übung', 'Schulung', 'Sonstiges'
    group_id: Optional[int] = None

class ScheduleAttendanceEntry(BaseModel):
    personnel_id: int
    status: str # 'Anwesend', 'Entschuldigt', 'Unentschuldigt'

# --- 🚨 EINSÄTZE ENDPUNKTE ---
@router.get("")
def list_missions(request: Request):
    check_auth(request)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, date, time, end_time, stichwort, adresse, meldung, status, duration, group_id, leader_signature FROM missions ORDER BY date DESC, time DESC")
    res = cur.fetchall()
    cur.close()
    conn.close()
    for row in res:
        if isinstance(row["date"], date):
            row["date"] = str(row["date"])
        sig = row.get("leader_signature")
        if sig:
            decoded_sig = safe_decode(sig)
            row["leader_signature"] = decoded_sig
            if decoded_sig and len(str(decoded_sig).strip()) > 10:
                row["status"] = "Freigegeben"
        if row.get("status") == "Freigegeben":
            row["status"] = "Freigegeben"
    return res

@router.get("/{mission_id}")
def get_mission(mission_id: int, request: Request):
    check_auth(request)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM missions WHERE id = %s", (mission_id,))
    m = cur.fetchone()
    if not m:
        cur.close(); conn.close()
        raise HTTPException(status_code=404, detail="Einsatz nicht gefunden")
    
    cur.execute("SELECT personnel_id, is_present, vehicle FROM mission_attendance WHERE mission_id = %s", (mission_id,))
    att = cur.fetchall()
    cur.close(); conn.close()
    
    if isinstance(m["date"], date):
        m["date"] = str(m["date"])
    sig = m.get("leader_signature")
    if sig:
        decoded_sig = safe_decode(sig)
        m["leader_signature"] = decoded_sig
        if decoded_sig and len(str(decoded_sig).strip()) > 10:
            m["status"] = "Freigegeben"
    if m.get("status") == "Freigegeben":
        m["status"] = "Freigegeben"
    m["attendance"] = att
    return m

@router.post("")
def create_mission(m: MissionCreate, request: Request):
    user = check_auth(request)
    if user["role"] == "mannschaft":
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO missions (date, time, end_time, stichwort, adresse, meldung, description, duration, status, media_files, group_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (m.date, m.time, m.end_time or "", m.stichwort, m.adresse, m.meldung, m.description, m.duration, m.status, m.media_files, m.group_id))
    mission_id = cur.lastrowid
    
    if m.attendance:
        for entry in m.attendance:
            cur.execute("""
                INSERT INTO mission_attendance (mission_id, personnel_id, is_present, vehicle)
                VALUES (%s, %s, %s, %s)
            """, (mission_id, entry.personnel_id, entry.is_present, entry.vehicle))
        
    conn.commit(); cur.close(); conn.close()
    from main import log_audit_action
    log_audit_action(user["username"], "EINSATZ_ERSTELLT", f"Einsatz '{m.stichwort}' anlegen.")
    return {"status": "success", "id": mission_id}

@router.put("/{mission_id}")
def update_mission(mission_id: int, m: MissionCreate, request: Request):
    user = check_auth(request)
    if user["role"] == "mannschaft":
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT status, leader_signature FROM missions WHERE id = %s", (mission_id,))
    existing = cur.fetchone()
    if not existing:
        cur.close(); conn.close()
        raise HTTPException(status_code=404, detail="Einsatz nicht gefunden")
    
    if existing["status"] == "Freigegeben" and user["role"] != "admin":
        cur.close(); conn.close()
        raise HTTPException(status_code=403, detail="Freigegebene Einsätze können nur von Admins editiert werden!")

    final_status = m.status
    if existing.get("leader_signature") and final_status == "Entwurf":
        final_status = "Freigegeben"

    # 1. Update Stammdaten
    cur.execute("""
        UPDATE missions 
        SET date=%s, time=%s, end_time=%s, stichwort=%s, adresse=%s, meldung=%s, description=%s, duration=%s, status=%s, media_files=%s, group_id=%s
        WHERE id=%s
    """, (m.date, m.time, m.end_time or "", m.stichwort, m.adresse, m.meldung, m.description, m.duration, final_status, m.media_files, m.group_id, mission_id))
    
    # 2. Update Personnel/Vehicles Attendance
    cur.execute("DELETE FROM mission_attendance WHERE mission_id = %s", (mission_id,))
    for entry in m.attendance:
        cur.execute("""
            INSERT INTO mission_attendance (mission_id, personnel_id, is_present, vehicle)
            VALUES (%s, %s, %s, %s)
        """, (mission_id, entry.personnel_id, entry.is_present, entry.vehicle))
        
    conn.commit()
    cur.close(); conn.close()
    from main import log_audit_action
    log_audit_action(user["username"], "EINSATZ_GEAENDERT", f"Einsatz ID {mission_id} geändert (Status: {final_status}).")
    return {"status": "success"}

@router.delete("/{mission_id}")
def delete_mission(mission_id: int, request: Request):
    user = check_auth(request, require_admin=True)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM missions WHERE id = %s", (mission_id,))
    cur.execute("DELETE FROM mission_attendance WHERE mission_id = %s", (mission_id,))
    cur.execute("DELETE FROM respiration_log WHERE mission_id = %s", (mission_id,))
    conn.commit(); cur.close(); conn.close()
    from main import log_audit_action
    log_audit_action(user["username"], "EINSATZ_GELOESCHT", f"Einsatz ID {mission_id} unwiderruflich gelöscht.")
    return {"status": "success"}

@router.post("/{mission_id}/signature")
def save_mission_signature(mission_id: int, data: dict, request: Request):
    user = check_auth(request)
    if user["role"] == "mannschaft":
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("UPDATE missions SET leader_signature = %s, status = 'Freigegeben' WHERE id = %s", (data.get("signature"), mission_id))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

# --- 💨 ATEMSCHUTZ OVERVIEW & LOGGER ---
@router.get("/{mission_id}/respiration")
def list_respiration_log(mission_id: int, request: Request):
    check_auth(request)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT r.*, p.name 
        FROM respiration_log r
        JOIN personnel p ON r.personnel_id = p.id
        WHERE r.mission_id = %s
    """, (mission_id,))
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@router.post("/{mission_id}/respiration")
def add_respiration_entry(mission_id: int, r: RespirationEntry, request: Request):
    user = check_auth(request)
    if user["role"] == "mannschaft":
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO respiration_log (mission_id, personnel_id, druck_start, druck_10, druck_20, druck_ende, dauer, fit_ok)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (mission_id, r.personnel_id, r.druck_start, r.druck_10, r.druck_20, r.druck_ende, r.dauer, int(r.fit_ok)))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.delete("/respiration/{entry_id}")
def delete_respiration_entry(entry_id: int, request: Request):
    user = check_auth(request)
    if user["role"] == "mannschaft":
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM respiration_log WHERE id = %s", (entry_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

# --- 💶 ABRECHNUNG / KOSTENBESCHEIDE & EXPORTE ---
@router.get("/billing/list")
def list_bills(request: Request):
    check_auth(request)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT b.*, 
               COALESCE(m.stichwort, 'Einsatz') as stichwort, 
               COALESCE(m.date, DATE(b.sent_at)) as date,
               COALESCE(m.adresse, '') as adresse
        FROM billing_verursacher b
        LEFT JOIN missions m ON b.mission_id = m.id
        ORDER BY b.id DESC
    """)
    res = cur.fetchall(); cur.close(); conn.close()
    for row in res:
        if row.get("date") and isinstance(row["date"], date):
            row["date"] = str(row["date"])
        if row["sent_at"]:
            row["sent_at"] = str(row["sent_at"])
        if row["paid_at"]:
            row["paid_at"] = str(row["paid_at"])
    return res

@router.post("/billing/{mission_id}")
def create_bill(mission_id: int, b: BillingCreate, request: Request):
    user = check_auth(request)
    if user["role"] not in ("admin", "leitung", "geratewart"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO billing_verursacher (mission_id, recipient_name, address, amount, details)
        VALUES (%s, %s, %s, %s, %s)
    """, (mission_id, b.recipient_name, b.address, b.amount, b.details))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.put("/billing/{bill_id}")
def update_bill(bill_id: int, b: BillingCreate, request: Request):
    user = check_auth(request)
    if user["role"] not in ("admin", "leitung", "geratewart"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        UPDATE billing_verursacher
        SET recipient_name=%s, address=%s, amount=%s, details=%s
        WHERE id=%s
    """, (b.recipient_name, b.address, b.amount, b.details, bill_id))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.delete("/billing/{bill_id}")
def delete_bill(bill_id: int, request: Request):
    user = check_auth(request)
    if user["role"] not in ("admin", "leitung", "geratewart"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM billing_verursacher WHERE id = %s", (bill_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.post("/billing/{bill_id}/pay")
def mark_bill_paid(bill_id: int, request: Request):
    user = check_auth(request)
    if user["role"] not in ("admin", "leitung", "geratewart"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("UPDATE billing_verursacher SET paid_at = CURRENT_TIMESTAMP WHERE id = %s", (bill_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

# --- 💶 STUNDEN-ENTSCHÄDIGUNG & SEPA EXPORT HELFER ---
@router.get("/billing/compensations/list")
def calculate_compensations(year: int, hourly_rate: float, request: Request):
    check_auth(request)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    
    # 1. Berechne alle Dienst- und Einsatzstunden pro Kamerad
    query = """
        SELECT p.id, p.name, p.email,
               COALESCE((
                   SELECT SUM(s.duration) 
                   FROM attendance a 
                   JOIN sessions s ON a.session_id = s.id 
                   JOIN persons prs ON a.person_id = prs.id
                   WHERE (LOWER(TRIM(prs.name)) = LOWER(TRIM(p.name)) OR prs.name LIKE CONCAT('%%', p.name, '%%') OR p.name LIKE CONCAT('%%', prs.name, '%%')) 
                     AND a.is_present = 1 
                     AND YEAR(s.date) = %s
               ), 0) as session_hours,
               COALESCE((
                   SELECT SUM(m.duration) 
                   FROM mission_attendance ma 
                   JOIN missions m ON ma.mission_id = m.id 
                   WHERE (ma.personnel_id = p.id OR LOWER(TRIM((SELECT pl.name FROM personnel pl WHERE pl.id = ma.personnel_id))) = LOWER(TRIM(p.name)))
                     AND ma.is_present NOT IN ('Nein', '0', 'false', 'False', '') 
                     AND ma.is_present IS NOT NULL 
                     AND YEAR(m.date) = %s
               ), 0) as mission_hours
        FROM personnel p
        WHERE p.membership_status = 'Aktiv'
        ORDER BY p.name ASC
    """
    cur.execute(query, (year, year))
    members = cur.fetchall(); cur.close(); conn.close()
    
    result = []
    for m in members:
        total_hours = float(m["session_hours"]) + float(m["mission_hours"])
        compensation = round(total_hours * hourly_rate, 2)
        result.append({
            "id": m["id"],
            "name": m["name"],
            "email": m["email"],
            "session_hours": float(m["session_hours"]),
            "mission_hours": float(m["mission_hours"]),
            "total_hours": total_hours,
            "compensation": compensation
        })
    return result

@router.get("/billing/export/sepa")
def export_sepa_xml(year: int, hourly_rate: float, sender_iban: str, sender_bic: str, request: Request):
    check_auth(request, require_admin=True)
    comps = calculate_compensations(year, hourly_rate, request)
    
    # SEPA XML Template generieren
    import datetime
    today = datetime.date.today().isoformat()
    msg_id = f"MSG{int(datetime.datetime.now().timestamp())}"
    pmt_id = f"PMT{int(datetime.datetime.now().timestamp())}"
    
    total_amount = sum(c["compensation"] for c in comps)
    num_tx = len([c for c in comps if c["compensation"] > 0])
    
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pain.001.001.03" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <CstmrCdtTrfInitn>
    <GrpHdr>
      <MsgId>{msg_id}</MsgId>
      <CreDtTm>{today}T12:00:00Z</CreDtTm>
      <NbOfTxs>{num_tx}</NbOfTxs>
      <CtrlSum>{total_amount:.2f}</CtrlSum>
      <InitgPty>
        <Nm>Freiwillige Feuerwehr</Nm>
      </InitgPty>
    </GrpHdr>
    <PmtInf>
      <PmtInfId>{pmt_id}</PmtInfId>
      <PmtMtd>TRF</PmtMtd>
      <NbOfTxs>{num_tx}</NbOfTxs>
      <CtrlSum>{total_amount:.2f}</CtrlSum>
      <Dbtr>
        <Nm>Freiwillige Feuerwehr</Nm>
      </Dbtr>
      <DbtrAcct>
        <Id>
          <IBAN>{sender_iban.strip()}</IBAN>
        </Id>
      </DbtrAcct>
      <DbtrAgt>
        <FinInstnId>
          <BIC>{sender_bic.strip()}</BIC>
        </FinInstnId>
      </DbtrAgt>
      <ChrgBr>SLEV</ChrgBr>
"""
    
    for idx, c in enumerate(comps):
        if c["compensation"] <= 0:
            continue
        # Da wir keine IBANs der Kameraden speichern, nutzen wir Dummy-Daten zum Befüllen, die die Bank ablehnen / korrigieren lassen kann, oder der User anpasst.
        xml += f"""      <CdtTrfTxInf>
        <PmtId>
          <EndToEndId>COMP{idx}</EndToEndId>
        </PmtId>
        <Amt>
          <InstdAmt Ccy="EUR">{c["compensation"]:.2f}</InstdAmt>
        </Amt>
        <Cdtr>
          <Nm>{c["name"]}</Nm>
        </Cdtr>
        <CdtrAcct>
          <Id>
            <IBAN>DE89370400440532013000</IBAN>
          </Id>
        </CdtrAcct>
        <RmtInf>
          <Ustrd>Aufwandsentschaedigung Feuerwehr {year} - {c["total_hours"]} Std</Ustrd>
        </RmtInf>
      </CdtTrfTxInf>
"""
    
    xml += """    </PmtInf>
  </CstmrCdtTrfInitn>
</Document>"""

    return Response(
        content=xml,
        media_type="application/xml",
        headers={"Content-Disposition": f"attachment; filename=sepa_compensations_{year}.xml"}
    )

# --- 📅 ÜBUNGS- & DIENSTPLANUNG ---
@router.get("/schedules/list")
def list_schedules(request: Request):
    check_auth(request)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM schedules ORDER BY date ASC, time ASC")
    res = cur.fetchall(); cur.close(); conn.close()
    for row in res:
        if isinstance(row["date"], date):
            row["date"] = str(row["date"])
    return res

@router.post("/schedules")
def create_schedule(s: ScheduleCreate, request: Request):
    user = check_auth(request)
    if user["role"] == "mannschaft":
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO schedules (title, date, time, description, type, group_id)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (s.title.strip(), s.date, s.time, s.description, s.type, s.group_id))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.put("/schedules/{sch_id}")
def update_schedule(sch_id: int, s: ScheduleCreate, request: Request):
    user = check_auth(request)
    if user["role"] == "mannschaft":
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        UPDATE schedules 
        SET title=%s, date=%s, time=%s, description=%s, type=%s, group_id=%s
        WHERE id=%s
    """, (s.title.strip(), s.date, s.time, s.description, s.type, s.group_id, sch_id))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.delete("/schedules/{sch_id}")
def delete_schedule(sch_id: int, request: Request):
    user = check_auth(request)
    if user["role"] == "mannschaft":
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM schedules WHERE id = %s", (sch_id,))
    cur.execute("DELETE FROM schedule_attendance WHERE schedule_id = %s", (sch_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.get("/schedules/{sch_id}/attendance")
def get_schedule_attendance(sch_id: int, request: Request):
    check_auth(request)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT sa.personnel_id, sa.status, p.name 
        FROM schedule_attendance sa
        JOIN personnel p ON sa.personnel_id = p.id
        WHERE sa.schedule_id = %s
    """, (sch_id,))
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@router.post("/schedules/{sch_id}/attendance")
def save_schedule_attendance(sch_id: int, data: List[ScheduleAttendanceEntry], request: Request):
    user = check_auth(request)
    if user["role"] == "mannschaft":
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM schedule_attendance WHERE schedule_id = %s", (sch_id,))
    for entry in data:
        cur.execute("""
            INSERT INTO schedule_attendance (schedule_id, personnel_id, status)
            VALUES (%s, %s, %s)
        """, (sch_id, entry.personnel_id, entry.status))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

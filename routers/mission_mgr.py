from fastapi import APIRouter, HTTPException, Request, Response, BackgroundTasks
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import List, Optional
from datetime import date

router = APIRouter(prefix="/api/missions", tags=["Missions"])
from database import get_db_connection
from core.utils import check_auth

def safe_decode(val):
    if not val: return None
    if isinstance(val, (bytes, bytearray)):
        return val.decode('utf-8', errors='ignore')
    return str(val)

def format_time_val(t_val):
    if not t_val: return ""
    if hasattr(t_val, "total_seconds"):
        h, r = divmod(t_val.seconds, 3600)
        m, _ = divmod(r, 60)
        return f"{h:02d}:{m:02d}"
    if isinstance(t_val, str):
        return t_val[:5] if len(t_val) >= 5 else t_val
    return str(t_val)[:5]

class MissionAttendanceEntry(BaseModel):
    personnel_id: int
    is_present: str # 'Abgerückt', 'Bereitstellung', 'Nein'
    vehicle: Optional[str] = ""

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

class EmployerCertCreate(BaseModel):
    signature: Optional[str] = None  # Base64 Data-URL aus dem SignaturePad, optional

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
        if "time" in row:
            row["time"] = format_time_val(row["time"])
        if "end_time" in row:
            row["end_time"] = format_time_val(row["end_time"])
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
    if "time" in m:
        m["time"] = format_time_val(m["time"])
    if "end_time" in m:
        m["end_time"] = format_time_val(m["end_time"])
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

def _prepare_mission_report_data(mission_id: int):
    """Lädt einen Einsatz + Anwesenheit und bringt sie in die Form, die
    generate_single_report() erwartet. Wird sowohl für die PDF- als auch
    für die HTML-Berichtsansicht genutzt, damit beide identisch aussehen."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM missions WHERE id = %s", (mission_id,))
    m = cur.fetchone()
    if not m:
        cur.close(); conn.close()
        raise HTTPException(status_code=404, detail="Einsatz nicht gefunden")

    cur.execute("""
        SELECT ma.is_present, ma.vehicle, p.name
        FROM mission_attendance ma
        JOIN personnel p ON ma.personnel_id = p.id
        WHERE ma.mission_id = %s
    """, (mission_id,))
    att = cur.fetchall()
    cur.close(); conn.close()

    if isinstance(m["date"], date):
        m["date"] = str(m["date"])

    # We need to adapt `m` to fit `generate_single_report` which expects specific keys
    m['gname'] = "Feuerwehr"
    m['instructors'] = "Einsatzleiter"
    m['category'] = "Einsatz"
    if not m.get('description'):
        m['description'] = m.get('stichwort', 'Einsatz') + " - " + m.get('meldung', '')

    sig = m.get("leader_signature")
    if sig:
        m["leader_signature"] = safe_decode(sig)

    # Adapt persons - Einsatz-Teilnehmer haben (anders als normale Dienste) keine
    # eigene Unterschrift pro Person in der DB, nur der Einsatzleiter unterschreibt
    # (missions.leader_signature oben).
    persons = []
    for a in att:
        is_p = 1 if a['is_present'] in ('Abgerückt', 'Bereitstellung') else 0
        persons.append({
            'name': a['name'],
            'is_present': is_p,
            'vehicle': a['vehicle'],
            'signature': None
        })

    from core.utils import get_station_name
    town_name = get_station_name()
    return m, persons, town_name

@router.get("/{mission_id}/report", response_class=HTMLResponse)
def get_mission_report(mission_id: int, request: Request):
    check_auth(request)
    m, persons, town_name = _prepare_mission_report_data(mission_id)
    from routers.reports import generate_single_report, get_report_styles
    html_content = generate_single_report(m, persons, town_name)
    return f"<html><head><meta charset='utf-8'><style>{get_report_styles()}</style></head><body>{html_content}</body></html>"

@router.get("/{mission_id}/pdf")
def get_mission_pdf(mission_id: int, request: Request):
    check_auth(request)
    m, persons, town_name = _prepare_mission_report_data(mission_id)

    from routers.reports import generate_single_report, get_report_styles
    html_content = generate_single_report(m, persons, town_name)

    full_html = f"<html><head><meta charset='utf-8'><style>{get_report_styles()}</style></head><body>{html_content}</body></html>"

    import xhtml2pdf.pisa as pisa
    import io
    pdf_buf = io.BytesIO()
    pisa.CreatePDF(full_html, dest=pdf_buf)
    pdf_bytes = pdf_buf.getvalue()

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        # "inline" statt "attachment": öffnet das PDF im Browser-Tab/-Fenster
        # (window.open im Frontend), statt es nur stumm herunterzuladen und eine
        # Download-Benachrichtigung anzuzeigen.
        headers={"Content-Disposition": f"inline; filename=Einsatzbericht_{mission_id}.pdf"}
    )

@router.get("/employer-certificates")
def list_employer_certificates(request: Request):
    check_auth(request)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT ec.id, ec.mission_id, ec.personnel_id, ec.created_by, ec.created_at,
               (ec.signature IS NOT NULL) as has_signature,
               p.name as personnel_name, m.date as mission_date, m.stichwort as mission_stichwort
        FROM employer_certificates ec
        JOIN personnel p ON ec.personnel_id = p.id
        JOIN missions m ON ec.mission_id = m.id
        ORDER BY ec.created_at DESC
    """)
    res = cur.fetchall(); cur.close(); conn.close()
    for r in res:
        r["created_at"] = str(r["created_at"])
        r["mission_date"] = str(r["mission_date"])
        r["has_signature"] = bool(r["has_signature"])
    return res

@router.post("/{mission_id}/employer-certificate/{personnel_id}")
def create_employer_certificate(mission_id: int, personnel_id: int, cert: EmployerCertCreate, request: Request):
    user = check_auth(request)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id FROM missions WHERE id = %s", (mission_id,))
    if not cur.fetchone():
        cur.close(); conn.close()
        raise HTTPException(status_code=404, detail="Einsatz nicht gefunden")
    cur.execute("SELECT is_present FROM mission_attendance WHERE mission_id = %s AND personnel_id = %s", (mission_id, personnel_id))
    att = cur.fetchone()
    if not att or att["is_present"] in (None, "", "Nein", "0", "false", "False"):
        cur.close(); conn.close()
        raise HTTPException(status_code=400, detail="Keine erfasste Anwesenheit dieser Person bei diesem Einsatz - Bescheinigung kann nicht erstellt werden.")
    cur2 = conn.cursor()
    cur2.execute(
        "INSERT INTO employer_certificates (mission_id, personnel_id, signature, created_by) VALUES (%s, %s, %s, %s)",
        (mission_id, personnel_id, cert.signature, user["username"])
    )
    new_id = cur2.lastrowid
    conn.commit(); cur.close(); cur2.close(); conn.close()
    return {"status": "success", "id": new_id}

@router.delete("/employer-certificates/{cert_id}")
def delete_employer_certificate(cert_id: int, request: Request):
    check_auth(request, allowed_roles=("admin", "leitung"))
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM employer_certificates WHERE id = %s", (cert_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.get("/{mission_id}/employer-certificate/{personnel_id}")
def get_employer_certificate(mission_id: int, personnel_id: int, request: Request, cert_id: Optional[int] = None):
    check_auth(request)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)

    cur.execute("SELECT * FROM missions WHERE id = %s", (mission_id,))
    m = cur.fetchone()
    if not m:
        cur.close(); conn.close()
        raise HTTPException(status_code=404, detail="Einsatz nicht gefunden")

    cur.execute("SELECT * FROM personnel WHERE id = %s", (personnel_id,))
    p = cur.fetchone()
    if not p:
        cur.close(); conn.close()
        raise HTTPException(status_code=404, detail="Kamerad nicht gefunden")

    cur.execute("SELECT is_present, vehicle FROM mission_attendance WHERE mission_id = %s AND personnel_id = %s", (mission_id, personnel_id))
    att = cur.fetchone()

    # Gespeicherte Unterschrift (falls über das neue "Arbeitsbescheinigungen"-Menü bereits
    # erstellt) sowie das einmal hinterlegte Dienstsiegel/Stempel-Bild laden, um sie direkt
    # in die PDF einzubetten statt einer leeren Signaturzeile/gestrichelten Box.
    signature_data_url = None
    if cert_id:
        cur.execute("SELECT signature FROM employer_certificates WHERE id = %s AND mission_id = %s AND personnel_id = %s", (cert_id, mission_id, personnel_id))
    else:
        cur.execute("SELECT signature FROM employer_certificates WHERE mission_id = %s AND personnel_id = %s ORDER BY created_at DESC LIMIT 1", (mission_id, personnel_id))
    sig_row = cur.fetchone()
    if sig_row and sig_row.get("signature"):
        signature_data_url = sig_row["signature"]

    try:
        cur.execute("SELECT stamp_image FROM station_settings ORDER BY id ASC LIMIT 1")
        stamp_row = cur.fetchone()
        stamp_image = stamp_row["stamp_image"] if stamp_row else None
    except Exception:
        stamp_image = None

    cur.close(); conn.close()

    # Serverseitig erzwingen, dass die Person auch tatsächlich als anwesend erfasst wurde -
    # der "Bescheinigung"-Button im Frontend blendet zwar nur bei is_present != 'Nein' ein,
    # das war aber rein kosmetisch: ohne diese Prüfung könnte jeder eingeloggte Nutzer für
    # jede Person/jeden Einsatz eine offizielle Arbeitgeber-Bescheinigung erzeugen, auch ohne
    # erfasste oder mit verneinter Teilnahme.
    if not att or att["is_present"] in (None, "", "Nein", "0", "false", "False"):
        raise HTTPException(status_code=400, detail="Keine erfasste Anwesenheit dieser Person bei diesem Einsatz - Bescheinigung kann nicht erstellt werden.")

    from core.utils import get_station_name
    station_name = get_station_name()

    if stamp_image:
        stamp_html = f'<img src="{stamp_image}" style="height:60px; margin-top:5px;">'
    else:
        stamp_html = '<div style="height: 60px; border: 1px dashed #d1d5db; border-radius: 4px; margin-top: 5px;"></div>'
    if signature_data_url:
        # Das Bild wird jetzt bereits im Browser (SignaturePad-Export) auf den tatsächlich
        # gezeichneten Bereich zugeschnitten (siehe trimSignatureDataUrl in dashboard.js/editor.html),
        # statt der kompletten, größtenteils leeren Zeichenfläche - dadurch reicht hier eine feste,
        # moderate Höhe ohne negativen Rand, die Unterschrift sitzt direkt über der Linie.
        signature_html = (
            f'<img src="{signature_data_url}" style="height:40px; max-width:180px;">'
            '<div class="sig-line">(Unterschrift Feuerwehrkommandant / Einsatzleiter)</div>'
        )
    else:
        signature_html = '<div class="sig-line">(Unterschrift Feuerwehrkommandant / Einsatzleiter)</div>'

    try:
        if isinstance(m["date"], date):
            e_date_fmt = m["date"].strftime("%d.%m.%Y")
        else:
            parts = str(m["date"]).split("-")
            e_date_fmt = f"{parts[2]}.{parts[1]}.{parts[0]}"
    except Exception:
        e_date_fmt = str(m["date"])

    start_time = format_time_val(m.get("time") or "00:00")
    end_time = format_time_val(m.get("end_time") or "")
    if not end_time:
        end_time = "Gemäß Einsatzdauer"

    birth_str = ""
    if p.get("birth_date"):
        try:
            if isinstance(p["birth_date"], date):
                birth_str = f", geb. am {p['birth_date'].strftime('%d.%m.%Y')}"
            else:
                b_parts = str(p["birth_date"]).split("-")
                birth_str = f", geb. am {b_parts[2]}.{b_parts[1]}.{b_parts[0]}"
        except Exception:
            birth_str = f", geb. am {p['birth_date']}"

    addr_str = f" ({p['address']})" if p.get("address") else ""
    today_fmt = date.today().strftime("%d.%m.%Y")
    duration_str = f"{m.get('duration', 2.0)} Std."

    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        @page {{
            size: a4 portrait;
            margin: 2cm 2cm 1.5cm 2cm;
        }}
        body {{
            font-family: Helvetica, Arial, sans-serif;
            font-size: 10.5pt;
            line-height: 1.5;
            color: #1f2937;
        }}
        .header-table {{
            width: 100%;
            border-bottom: 2px solid #b91c1c;
            padding-bottom: 12px;
            margin-bottom: 14px;
        }}
        .station-title {{
            font-size: 16pt;
            font-weight: bold;
            color: #b91c1c;
            margin: 0;
        }}
        .station-sub {{
            font-size: 9pt;
            color: #4b5563;
            margin: 0;
        }}
        .doc-title {{
            font-size: 14pt;
            font-weight: bold;
            text-align: center;
            text-transform: uppercase;
            margin-top: 15px;
            margin-bottom: 5px;
            color: #111827;
        }}
        .doc-subtitle {{
            font-size: 10pt;
            text-align: center;
            color: #4b5563;
            margin-bottom: 14px;
        }}
        .box {{
            background-color: #f9fafb;
            border: 1px solid #e5e7eb;
            border-radius: 4px;
            padding: 12px 16px;
            margin-bottom: 12px;
        }}
        .data-table {{
            width: 100%;
            border-collapse: collapse;
            margin: 10px 0;
        }}
        .data-table td {{
            padding: 6px 8px;
            vertical-align: top;
        }}
        .data-table td.label {{
            width: 32%;
            font-weight: bold;
            color: #374151;
            border-bottom: 1px solid #f3f4f6;
        }}
        .data-table td.value {{
            width: 68%;
            border-bottom: 1px solid #f3f4f6;
        }}
        .legal-box {{
            background-color: #fef2f2;
            border-left: 4px solid #b91c1c;
            padding: 10px 14px;
            font-size: 8.5pt;
            color: #4b5563;
            margin-top: 12px;
            margin-bottom: 14px;
            line-height: 1.4;
        }}
        .signature-table {{
            width: 100%;
            margin-top: 20px;
        }}
        .signature-table td {{
            vertical-align: top;
            width: 50%;
        }}
        .sig-line {{
            border-top: 1px solid #374151;
            margin-top: 25px;
            padding-top: 5px;
            font-size: 8.5pt;
            color: #4b5563;
            text-align: center;
        }}
    </style>
</head>
<body>
    <table class="header-table">
        <tr>
            <td>
                <div class="station-title">{station_name}</div>
                <div class="station-sub">Freiwillige Feuerwehr &bull; Gesetzliche Gefahrenabwehr</div>
            </td>
            <td style="text-align: right; vertical-align: bottom;">
                <div style="font-size: 9pt; color: #4b5563;">Datum: {today_fmt}</div>
            </td>
        </tr>
    </table>

    <div class="doc-title">Bescheinigung</div>
    <div class="doc-subtitle">über die Teilnahme an einem Feuerwehreinsatz zur Vorlage beim Arbeitgeber<br>(Anspruch auf Lohnfortzahlung / Erstattung von Verdienstausfall)</div>

    <p>Hiermit wird zur Vorlage bei der zuständigen Personalabteilung / dem Lohnbüro amtlich bescheinigt, dass</p>

    <div class="box">
        <span style="font-size: 12pt; font-weight: bold; color: #111827;">Herr / Frau {p['name']}</span>{birth_str}{addr_str}
    </div>

    <p>als ehrenamtliche/r Angehörige/r der <strong>{station_name}</strong> im Rahmen der gesetzlichen Pflichtaufgaben an folgendem Einsatz teilgenommen hat:</p>

    <table class="data-table">
        <tr>
            <td class="label">Einsatzdatum:</td>
            <td class="value"><strong>{e_date_fmt}</strong></td>
        </tr>
        <tr>
            <td class="label">Einsatzzeit (von – bis):</td>
            <td class="value"><strong>{start_time} Uhr bis {end_time} Uhr</strong></td>
        </tr>
        <tr>
            <td class="label">Gesamtdauer:</td>
            <td class="value"><strong>{duration_str}</strong> (inkl. Nachbereitung / Wiederherstellung der Einsatzbereitschaft)</td>
        </tr>
        <tr>
            <td class="label">Einsatzstichwort:</td>
            <td class="value">{m.get('stichwort') or 'Einsatz'}</td>
        </tr>
        <tr>
            <td class="label">Einsatzort / Adresse:</td>
            <td class="value">{m.get('adresse') or 'Einsatzgebiet'}</td>
        </tr>
        <tr>
            <td class="label">Einsatz-Aktenzeichen:</td>
            <td class="value">Einsatz #{mission_id}</td>
        </tr>
    </table>

    <div class="legal-box">
        <strong>Rechtliche Hinweise für den Arbeitgeber:</strong><br>
        Feuerwehrangehörige sind während der Dauer des Einsatzes sowie für einen zur Wiederherstellung der Arbeits- und Leistungsfähigkeit notwendigen Zeitraum nach den Feuerwehrgesetzen der Länder von der Arbeitsleistung freigestellt. Dem Arbeitnehmer darf durch den ehrenamtlichen Feuerwehrdienst kein Nachteil im Arbeitsverhältnis entstehen. Das Arbeitsentgelt (inklusive aller Nebenleistungen) ist für den Zeitraum der Freistellung fortzuzahlen.<br><br>
        <strong>Erstattungsanspruch:</strong><br>
        Die zuständige Gemeinde bzw. Stadtverwaltung erstattet privaten Arbeitgebern auf Antrag das weitergewährte Arbeitsentgelt einschließlich der vom Arbeitgeber zu tragenden Sozialversicherungsbeiträge. Bitte reichen Sie hierfür den Erstattungsantrag mit Angabe der Brutto-Lohnkosten bei der zuständigen Stadt-/Gemeindeverwaltung ein.
    </div>

    <table class="signature-table">
        <tr>
            <td style="padding-right: 25px;">
                <div style="font-size: 9pt; color: #4b5563;">Dienstsiegel / Stempel:</div>
                {stamp_html}
            </td>
            <td style="padding-left: 25px;">
                {signature_html}
            </td>
        </tr>
    </table>
</body>
</html>"""

    import xhtml2pdf.pisa as pisa
    import io
    pdf_buf = io.BytesIO()
    pisa.CreatePDF(html_content, dest=pdf_buf)
    pdf_bytes = pdf_buf.getvalue()

    safe_p_name = p['name'].replace(' ', '_').replace('/', '_')
    filename = f"Arbeitgeberbescheinigung_{safe_p_name}_Einsatz_{mission_id}.pdf"

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{filename}"'}
    )

@router.post("")
def create_mission(m: MissionCreate, request: Request, background_tasks: BackgroundTasks):
    user = check_auth(request)
    if user["role"] not in ("admin", "leitung", "gruppenfuehrer"):
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
    from core.utils import log_audit_action
    log_audit_action(user["username"], "EINSATZ_ERSTELLT", f"Einsatz '{m.stichwort}' anlegen.")
    
    from routers import ws_mgr
    background_tasks.add_task(ws_mgr.manager.broadcast_json, {"type": "mission_created", "mission_id": mission_id})
    return {"status": "success", "id": mission_id}

@router.put("/{mission_id}")
def update_mission(mission_id: int, m: MissionCreate, request: Request, background_tasks: BackgroundTasks):
    user = check_auth(request)
    if user["role"] not in ("admin", "leitung", "gruppenfuehrer"):
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
    from core.utils import log_audit_action
    log_audit_action(user["username"], "EINSATZ_GEAENDERT", f"Einsatz ID {mission_id} geändert (Status: {final_status}).")
    
    from routers import ws_mgr
    background_tasks.add_task(ws_mgr.manager.broadcast_json, {"type": "mission_updated", "mission_id": mission_id})
    return {"status": "success"}

@router.delete("/{mission_id}")
def delete_mission(mission_id: int, request: Request):
    user = check_auth(request, allowed_roles=("admin", "leitung"))
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM mission_attendance WHERE mission_id = %s", (mission_id,))
    cur.execute("DELETE FROM respiration_log WHERE mission_id = %s", (mission_id,))
    cur.execute("DELETE FROM missions WHERE id = %s", (mission_id,))
    conn.commit(); cur.close(); conn.close()
    from core.utils import log_audit_action
    log_audit_action(user["username"], "EINSATZ_GELOESCHT", f"Einsatz ID {mission_id} unwiderruflich gelöscht.")
    return {"status": "success"}

@router.post("/{mission_id}/signature")
def save_mission_signature(mission_id: int, data: dict, request: Request):
    user = check_auth(request)
    if user["role"] not in ("admin", "leitung", "gruppenfuehrer"):
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
    if user["role"] not in ("admin", "leitung", "gruppenfuehrer"):
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
    if user["role"] not in ("admin", "leitung", "gruppenfuehrer"):
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
    # WICHTIG: exakter Namensabgleich, kein LIKE '%Name%'-Fallback - das hier ist die
    # Grundlage für die Aufwandsentschädigung (SEPA-Export), ein "Max" != "Maximilian"
    # Fehlmatch würde direkt zu falschen Auszahlungen führen.
    query = """
        SELECT p.id, p.name, p.email,
               COALESCE((
                   SELECT SUM(s.duration) 
                   FROM attendance a 
                   JOIN sessions s ON a.session_id = s.id 
                   JOIN persons prs ON a.person_id = prs.id
                   WHERE LOWER(TRIM(prs.name)) = LOWER(TRIM(p.name))
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
        # round(): die Addition zweier aus Decimal konvertierter floats (session_hours +
        # mission_hours) kann durch Binärgleitkomma-Rundung hässliche Werte wie
        # 7.300000000000001 erzeugen, die ohne Rundung 1:1 in der Abrechnungstabelle
        # angezeigt wurden.
        total_hours = round(float(m["session_hours"]) + float(m["mission_hours"]), 2)
        compensation = round(total_hours * hourly_rate, 2)
        result.append({
            "id": m["id"],
            "name": m["name"],
            "email": m["email"],
            "session_hours": round(float(m["session_hours"]), 2),
            "mission_hours": round(float(m["mission_hours"]), 2),
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
    # Kein check_auth: wird auch vom nicht eingeloggten Hallenmonitor (alarmdisplay.html) gelesen.
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM schedules ORDER BY date ASC, time ASC")
    res = cur.fetchall(); cur.close(); conn.close()
    for row in res:
        if isinstance(row["date"], date):
            row["date"] = str(row["date"])
        if "time" in row:
            row["time"] = format_time_val(row["time"])
    return res

@router.post("/schedules")
def create_schedule(s: ScheduleCreate, request: Request):
    user = check_auth(request)
    if user["role"] not in ("admin", "leitung", "gruppenfuehrer"):
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
    if user["role"] not in ("admin", "leitung", "gruppenfuehrer"):
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
    if user["role"] not in ("admin", "leitung", "gruppenfuehrer"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM schedule_attendance WHERE schedule_id = %s", (sch_id,))
    cur.execute("DELETE FROM schedules WHERE id = %s", (sch_id,))
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
    if user["role"] not in ("admin", "leitung", "gruppenfuehrer"):
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

# --- VORAB-RÜCKMELDUNG (RSVP) FÜR GEPLANTE TERMINE ---
def _resolve_own_personnel_id(cur, username):
    # Gleicher Verknüpfungs-/Fallback-Abgleich wie in users_mgr.py get_my_global_fire_stats()
    # und personnel_mgr.py get_my_licenses().
    cur.execute("SELECT personnel_id FROM users WHERE username = %s", (username,))
    row = cur.fetchone()
    if row and row.get("personnel_id"):
        return row["personnel_id"]
    cur.execute("SELECT id FROM personnel WHERE LOWER(name) LIKE %s", (f"%{username.lower()}%",))
    fallback = cur.fetchone()
    if fallback:
        cur.execute("UPDATE users SET personnel_id = %s WHERE username = %s", (fallback["id"], username))
        return fallback["id"]
    return None

@router.get("/schedules/{sch_id}/rsvp")
def get_schedule_rsvp(sch_id: int, request: Request):
    user = check_auth(request)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT r.status, p.name FROM schedule_rsvp r JOIN personnel p ON r.personnel_id = p.id
        WHERE r.schedule_id = %s ORDER BY p.name ASC
    """, (sch_id,))
    all_responses = cur.fetchall()

    personnel_id = _resolve_own_personnel_id(cur, user["username"])
    my_status = None
    if personnel_id:
        cur.execute("SELECT status FROM schedule_rsvp WHERE schedule_id = %s AND personnel_id = %s", (sch_id, personnel_id))
        row = cur.fetchone()
        my_status = row["status"] if row else None
    conn.commit(); cur.close(); conn.close()
    return {"my_status": my_status, "unlinked": personnel_id is None, "responses": all_responses}

@router.post("/schedules/{sch_id}/rsvp")
def set_schedule_rsvp(sch_id: int, data: dict, request: Request):
    user = check_auth(request)
    status = data.get("status")
    if status not in ("Ja", "Nein", "Vielleicht"):
        raise HTTPException(status_code=400, detail="Ungültiger Status")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    personnel_id = _resolve_own_personnel_id(cur, user["username"])
    if not personnel_id:
        cur.close(); conn.close()
        raise HTTPException(status_code=400, detail="Kein Kamerad mit diesem Login verknüpft.")
    cur.execute(
        "INSERT INTO schedule_rsvp (schedule_id, personnel_id, status) VALUES (%s, %s, %s) "
        "ON DUPLICATE KEY UPDATE status = VALUES(status)",
        (sch_id, personnel_id, status)
    )
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

from fastapi import APIRouter, HTTPException, Response, Request, BackgroundTasks
from pydantic import BaseModel
from typing import Optional
import mysql.connector
import os
import base64
from datetime import date

router = APIRouter(prefix="/api/personnel", tags=["personnel"])
from database import get_db_connection
from core.utils import check_auth

class PersonnelMember(BaseModel):
    name: str
    rank: Optional[str] = ""
    membership_status: Optional[str] = "Aktiv"
    phone: Optional[str] = ""
    email: Optional[str] = ""
    address: Optional[str] = ""
    badge_number: Optional[str] = ""
    birth_date: Optional[str] = None
    entry_date: Optional[str] = None
    honors: Optional[str] = ""
    profile_picture: Optional[str] = None
    is_truppmann: int = 0
    is_funk: int = 0
    is_agt: int = 0
    is_maschinist: int = 0
    is_tf: int = 0
    is_gf: int = 0
    lic_b: int = 0
    lic_be: int = 0
    lic_c: int = 0
    lic_ce: int = 0
    g26_3_date: Optional[str] = None
    belastungslauf_date: Optional[str] = None
    unterweisung_date: Optional[str] = None
    dsgvo_ack_date: Optional[str] = None
    dienstordnung_ack_date: Optional[str] = None
    emergency_contact_name: Optional[str] = ""
    emergency_contact_phone: Optional[str] = ""

class GlobalSettings(BaseModel):
    int_g26: int
    int_belastung: int
    int_unterweisung: int
    reminder_window_days: Optional[int] = 14
    session_max_days: Optional[int] = 30

class AvailabilityCreate(BaseModel):
    start_date: str
    end_date: str
    reason: Optional[str] = ""

# --- AUTOMATISCHE HINTERGRUND-SYNCHRONISATION ---
def internal_sync_personnel_to_groups():
    """ Gleicht alle globalen Kameraden mit den einzelnen Editor-Gruppenlisten ab """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        sync_query = """
            INSERT INTO persons (group_id, name)
            SELECT g.id, TRIM(p.name)
            FROM groups_table g
            CROSS JOIN personnel p
            WHERE NOT EXISTS (
                SELECT 1 FROM persons src 
                WHERE src.group_id = g.id AND TRIM(src.name) = TRIM(p.name)
            ) AND p.name IS NOT NULL AND LENGTH(TRIM(p.name)) > 0;
        """
        cur.execute(sync_query)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Hintergrund-Synchronisationsfehler: {e}")

# --- SCHNELLE ÜBERSICHTSLISTE (OHNE BILDER UND NOTIZEN) ---
@router.get("/list")
def get_all_personnel(request: Request, background_tasks: BackgroundTasks):
    check_auth(request)
    # Sicherheits-Trigger: Vor dem Ausgeben der Liste kurz synchronisieren (im Hintergrund!)
    background_tasks.add_task(internal_sync_personnel_to_groups)
    
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    sql = """SELECT id, name, rank, membership_status, phone, email, address, 
                    badge_number, birth_date, entry_date, profile_picture, is_truppmann, is_funk, 
                    is_agt, is_maschinist, is_tf, is_gf, lic_b, lic_be, lic_c, lic_ce, 
                    g26_3_date, belastungslauf_date, unterweisung_date,
                    CASE WHEN profile_picture IS NOT NULL AND LENGTH(profile_picture) > 0 THEN 1 ELSE 0 END AS has_picture
             FROM personnel ORDER BY name ASC"""
    cur.execute(sql)
    res = cur.fetchall()

    # G26.3-Ablauf berechnen (wie in groups_api.py get_attendance()) - dieser Endpunkt ist die
    # gemeinsame Datenquelle für den Kameraden-Pool in editor.html UND für die Teilnehmerliste
    # im Einsatzbericht-Editor (dashboard.js initActiveMissionAttendance/editMission). Ohne
    # g26_expired hier zeigt die "G26 abgelaufen"-Sicherheitswarnung im Einsatz-Editor nie an.
    cur.execute("SELECT setting_value FROM settings WHERE setting_key = 'int_g26'")
    g26_row = cur.fetchone()
    g26_allowed_months = g26_row['setting_value'] if g26_row else 36
    cur.close()
    conn.close()

    for row in res:
        row["g26_expired"] = False
        if row.get("is_agt") and row.get("g26_3_date"):
            g26_date = row["g26_3_date"]
            if isinstance(g26_date, date):
                diff_days = (date.today() - g26_date).days
                if diff_days > (g26_allowed_months * 30.44):
                    row["g26_expired"] = True
        for key, value in row.items():
            if isinstance(value, date):
                row[key] = str(value)
            if key.startswith("is_") or key.startswith("lic_"):
                row[key] = bool(value)
        row["has_picture"] = bool(row["has_picture"])
    return res

# --- EINZELNES MITGLIED VOLLSTÄNDIG LADEN (FÜR MODAL) ---
@router.get("/get/{member_id}")
def get_single_member(member_id: int, request: Request):
    check_auth(request)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM personnel WHERE id = %s", (member_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    
    if not row:
        raise HTTPException(status_code=404, detail="Mitglied nicht gefunden")
        
    for key, value in row.items():
        if isinstance(value, date):
            row[key] = str(value)
        if key.startswith("is_") or key.startswith("lic_"):
            row[key] = bool(value)
    return row

# --- VERFÜGBARKEIT / ABWESENHEIT (Urlaub, Krankheit, ...) ---
@router.get("/{member_id}/availability")
def list_availability(member_id: int, request: Request):
    check_auth(request)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM personnel_availability WHERE personnel_id = %s ORDER BY start_date DESC", (member_id,))
    res = cur.fetchall(); cur.close(); conn.close()
    for row in res:
        if isinstance(row["start_date"], date): row["start_date"] = str(row["start_date"])
        if isinstance(row["end_date"], date): row["end_date"] = str(row["end_date"])
    return res

@router.post("/{member_id}/availability")
def add_availability(member_id: int, a: AvailabilityCreate, request: Request):
    # Jeder darf sich selbst eintragen; für andere braucht es Personalverwaltungs-Rechte.
    # Da der Login nicht zwingend mit einer personnel_id verknüpft ist, reicht hier "eingeloggt" -
    # Admin/Leitung können ohnehin jeden eintragen, Missbrauch durch Mannschaft wäre nur eine
    # falsche Abwesenheitsnotiz für einen Kollegen, kein Sicherheitsrisiko.
    check_auth(request)
    if not a.start_date or not a.end_date:
        raise HTTPException(status_code=400, detail="Zeitraum erforderlich")
    if a.end_date < a.start_date:
        raise HTTPException(status_code=400, detail="Enddatum darf nicht vor dem Startdatum liegen")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO personnel_availability (personnel_id, start_date, end_date, reason) VALUES (%s, %s, %s, %s)",
        (member_id, a.start_date, a.end_date, (a.reason or "").strip())
    )
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.delete("/availability/{entry_id}")
def delete_availability(entry_id: int, request: Request):
    check_auth(request)
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM personnel_availability WHERE id = %s", (entry_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

DEFAULT_AVATAR_SVG = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="#9ca3af"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm0 4c1.93 0 3.5 1.57 3.5 3.5S13.93 13 12 13s-3.5-1.57-3.5-3.5S10.07 6 12 6zm0 14c-2.03 0-3.8-1.04-4.83-2.61.03-1.6 3.23-2.48 4.83-2.48s4.79.87 4.83 2.48C15.8 18.96 14.03 20 12 20z"/></svg>"""

# --- BILDER DIREKT ALS BINÄRDATEI STREAMEN ---
# Cache-Control lang setzen: das Frontend hängt an jede Avatar-URL bereits einen
# cacheBuster-Query-Parameter an, der bei jedem Speichern eines Mitglieds hochgezählt wird
# - Invalidierung passiert also schon über die URL selbst. Ohne diesen Header lädt der
# Browser bei jedem Aufruf der Mitgliederliste (dashboard/personnel/editor) für JEDE
# angezeigte Person erneut Bild+DB-Abfrage, statt aus dem Cache zu bedienen.
AVATAR_CACHE_HEADERS = {"Cache-Control": "public, max-age=604800, immutable"}

@router.get("/avatar/{member_id}")
def get_avatar(member_id: int, request: Request):
    check_auth(request)
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT profile_picture FROM personnel WHERE id = %s", (member_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()

        if row and row[0]:
            data_str = row[0].strip()
            if data_str.startswith("data:"):
                header, encoded = data_str.split(",", 1)
                mime = header.split(";")[0].split(":")[1]
                image_bytes = base64.b64decode(encoded)
                return Response(content=image_bytes, media_type=mime, headers=AVATAR_CACHE_HEADERS)
            elif data_str.startswith("/static/") or data_str.startswith("static/"):
                filepath = data_str.lstrip("/")
                if os.path.exists(filepath):
                    mime = "image/jpeg"
                    if filepath.endswith(".png"): mime = "image/png"
                    elif filepath.endswith(".gif"): mime = "image/gif"
                    with open(filepath, "rb") as f:
                        return Response(content=f.read(), media_type=mime, headers=AVATAR_CACHE_HEADERS)
    except Exception:
        pass
    return Response(content=DEFAULT_AVATAR_SVG, media_type="image/svg+xml", headers=AVATAR_CACHE_HEADERS)

# --- KORREKTUR: MITGLIED NEU ANLEGEN UND SOFORT ALLERWEGS FREISCHALTEN ---
@router.post("/add")
def add_member(m: PersonnelMember, request: Request):
    user = check_auth(request, allowed_roles=("admin", "leitung"))
    if not m.name or len(m.name.strip()) == 0:
        raise HTTPException(status_code=400, detail="Name darf nicht leer sein!")
        
    clean_name = m.name.strip()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # 1. In globale Akte schmeißen (nur Name+Status, damit der UNIQUE-Name-Konflikt bei
        # bereits existierenden Kameraden sauber ignoriert statt als Fehler geworfen wird)
        cur.execute("INSERT IGNORE INTO personnel (name, membership_status) VALUES (%s, %s)", (clean_name, m.membership_status))
        conn.commit()

        # 2. Direkt für alle Gruppen in persons eintragen, damit er im Editor wählbar wird
        cur.execute("SELECT id FROM groups_table")
        groups = cur.fetchall()
        for (group_id,) in groups:
            cur.execute("INSERT IGNORE INTO persons (group_id, name) VALUES (%s, %s)", (group_id, clean_name))
        conn.commit()

        # 3. Alle restlichen Formularfelder nachtragen (Rang, Kontakt, Lizenzen, Quals, ...).
        # Schritt 1 legt bewusst nur Name+Status an; ohne dieses UPDATE würden alle übrigen
        # Angaben aus dem "Neues Mitglied"-Formular (Telefon, Adresse, Führerscheine,
        # Qualifikationen, Geburtsdatum, ...) beim Anlegen stillschweigend verloren gehen.
        cur.execute("""UPDATE personnel SET
                 rank=%s, phone=%s, email=%s, address=%s,
                 badge_number=%s, birth_date=%s, entry_date=%s, honors=%s, profile_picture=%s,
                 is_truppmann=%s, is_funk=%s, is_agt=%s, is_maschinist=%s, is_tf=%s, is_gf=%s,
                 lic_b=%s, lic_be=%s, lic_c=%s, lic_ce=%s,
                 g26_3_date=%s, belastungslauf_date=%s, unterweisung_date=%s,
                 dsgvo_ack_date=%s, dienstordnung_ack_date=%s,
                 emergency_contact_name=%s, emergency_contact_phone=%s
                 WHERE name=%s""",
                    (m.rank, m.phone, m.email, m.address,
                     m.badge_number, m.birth_date or None, m.entry_date or None, m.honors, m.profile_picture,
                     int(m.is_truppmann), int(m.is_funk), int(m.is_agt), int(m.is_maschinist), int(m.is_tf), int(m.is_gf),
                     int(m.lic_b), int(m.lic_be), int(m.lic_c), int(m.lic_ce),
                     m.g26_3_date or None, m.belastungslauf_date or None, m.unterweisung_date or None,
                     m.dsgvo_ack_date or None, m.dienstordnung_ack_date or None,
                     m.emergency_contact_name, m.emergency_contact_phone,
                     clean_name))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Fehler beim Hinzufügen: {e}")
    finally:
        cur.close()
        conn.close()

    # Sicherheits-Zweitprüfung anstoßen
    internal_sync_personnel_to_groups()
    from core.utils import log_audit_action
    log_audit_action(user["username"], "PERSONAL_ANLEGEN", f"Mitglied '{clean_name}' neu angelegt.")
    return {"status": "success"}

@router.post("/update/{member_id}")
def update_member(member_id: int, m: PersonnelMember, request: Request):
    user = check_auth(request, allowed_roles=("admin", "leitung"))
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT name FROM personnel WHERE id=%s", (member_id,))
    old_name_row = cur.fetchone()
    old_name = old_name_row[0] if old_name_row else None
    
    b_date = m.birth_date if m.birth_date else None
    e_date = m.entry_date if m.entry_date else None
    g26 = m.g26_3_date if m.g26_3_date else None
    bel = m.belastungslauf_date if m.belastungslauf_date else None
    unt = m.unterweisung_date if m.unterweisung_date else None
    dsgvo = m.dsgvo_ack_date if m.dsgvo_ack_date else None
    dienstordnung = m.dienstordnung_ack_date if m.dienstordnung_ack_date else None

    sql = """UPDATE personnel SET
             name=%s, rank=%s, membership_status=%s, phone=%s, email=%s, address=%s,
             badge_number=%s, birth_date=%s, entry_date=%s, honors=%s, profile_picture=%s,
             is_truppmann=%s, is_funk=%s, is_agt=%s, is_maschinist=%s, is_tf=%s, is_gf=%s,
             lic_b=%s, lic_be=%s, lic_c=%s, lic_ce=%s,
             g26_3_date=%s, belastungslauf_date=%s, unterweisung_date=%s,
             dsgvo_ack_date=%s, dienstordnung_ack_date=%s,
             emergency_contact_name=%s, emergency_contact_phone=%s
             WHERE id=%s"""

    vals = (m.name.strip(), m.rank, m.membership_status, m.phone, m.email, m.address,
            m.badge_number, b_date, e_date, m.honors, m.profile_picture,
            int(m.is_truppmann), int(m.is_funk), int(m.is_agt), int(m.is_maschinist), int(m.is_tf), int(m.is_gf),
            int(m.lic_b), int(m.lic_be), int(m.lic_c), int(m.lic_ce),
            g26, bel, unt, dsgvo, dienstordnung, m.emergency_contact_name, m.emergency_contact_phone, member_id)
    
    cur.execute(sql, vals)

    if old_name and old_name.strip() != m.name.strip():
        cur.execute("UPDATE persons SET name=%s WHERE name=%s", (m.name.strip(), old_name.strip()))
    
    conn.commit()
    cur.close()
    conn.close()

    internal_sync_personnel_to_groups()
    from core.utils import log_audit_action
    log_audit_action(user["username"], "PERSONAL_BEARBEITEN", f"Mitglied ID {member_id} ('{m.name.strip()}') aktualisiert.")
    return {"status": "updated"}

@router.delete("/delete/{member_id}")
def delete_member(member_id: int, request: Request):
    user = check_auth(request, allowed_roles=("admin", "leitung"))
    conn = get_db_connection()
    cur = conn.cursor()

    # Vor dem Löschen den Namen holen, um ihn auch aus persons zu fegen
    cur.execute("SELECT name FROM personnel WHERE id = %s", (member_id,))
    name_row = cur.fetchone()
    
    cur.execute("DELETE FROM mission_attendance WHERE personnel_id = %s", (member_id,))
    cur.execute("DELETE FROM respiration_log WHERE personnel_id = %s", (member_id,))
    cur.execute("DELETE FROM schedule_attendance WHERE personnel_id = %s", (member_id,))
    
    cur.execute("DELETE FROM personnel WHERE id = %s", (member_id,))
    if name_row:
        cur.execute("DELETE FROM persons WHERE name = %s", (name_row[0],))
        
    conn.commit()
    cur.close()
    conn.close()
    from core.utils import log_audit_action
    log_audit_action(user["username"], "PERSONAL_LOESCHEN", f"Mitglied ID {member_id} ('{name_row[0] if name_row else '?'}') gelöscht.")
    return {"status": "deleted"}

@router.get("/settings")
def get_settings(request: Request):
    check_auth(request)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT setting_key, setting_value FROM settings")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    res = {row['setting_key']: row['setting_value'] for row in rows}
    if not res:
        return {"int_g26": 36, "int_belastung": 12, "int_unterweisung": 12, "reminder_window_days": 14, "session_max_days": 30}
    res.setdefault("reminder_window_days", 14)
    res.setdefault("session_max_days", 30)
    return res

@router.post("/settings")
def save_settings(s: GlobalSettings, request: Request):
    check_auth(request, allowed_roles=("admin", "leitung"))
    conn = get_db_connection()
    cur = conn.cursor()
    settings = [
        ('int_g26', s.int_g26),
        ('int_belastung', s.int_belastung),
        ('int_unterweisung', s.int_unterweisung),
        ('reminder_window_days', s.reminder_window_days or 14),
        ('session_max_days', s.session_max_days or 30)
    ]
    for key, val in settings:
        cur.execute("INSERT INTO settings (setting_key, setting_value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE setting_value=%s", (key, val, val))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "settings updated"}

def _compute_anniversaries(target_year: int):
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT id, name, rank, membership_status, entry_date, birth_date, honors
        FROM personnel
        WHERE (entry_date IS NOT NULL AND entry_date != '') 
           OR (birth_date IS NOT NULL AND birth_date != '')
        ORDER BY name ASC
    """)
    members = cur.fetchall()
    cur.close()
    conn.close()

    service_honors_map = {
        10: "10 Jahre Dienstzeit (z. B. Ehrenurkunde / Treueabzeichen)",
        20: "20 Jahre Dienstzeit",
        25: "25 Jahre Dienstzeit (Feuerwehr-Ehrenzeichen in Silber)",
        30: "30 Jahre Dienstzeit",
        40: "40 Jahre Dienstzeit (Feuerwehr-Ehrenzeichen in Gold)",
        50: "50 Jahre Dienstzeit (Großes Feuerwehr-Ehrenzeichen)",
        60: "60 Jahre Dienstzeit (Große Ehrenurkunde LFV)",
        70: "70 Jahre Dienstzeit (Große Ehrenurkunde LFV)"
    }
    # Liste begann vorher erst bei 50 - jüngere runde Geburtstage (18., 20., 30., 40.) wurden
    # dadurch nie erkannt, egal welches Geburtsdatum eingetragen war.
    round_birthdays = [18, 20, 30, 40, 50, 60, 65, 70, 75, 80, 85, 90, 95, 100]

    service_anniversaries = []
    birthday_anniversaries = []

    for m in members:
        # Dienstjubiläen
        if m.get("entry_date"):
            try:
                e_val = m["entry_date"]
                if isinstance(e_val, date):
                    e_year = e_val.year
                    e_month = e_val.month
                    e_day = e_val.day
                    e_str = str(e_val)
                else:
                    parts = str(e_val).split("-")
                    e_year = int(parts[0])
                    e_month = int(parts[1]) if len(parts) > 1 else 1
                    e_day = int(parts[2]) if len(parts) > 2 else 1
                    e_str = str(e_val)

                years_diff = target_year - e_year
                if years_diff > 0 and years_diff in service_honors_map:
                    service_anniversaries.append({
                        "personnel_id": m["id"],
                        "name": m["name"],
                        "rank": m.get("rank") or "Kamerad/in",
                        "status": m.get("membership_status") or "Aktiv",
                        "entry_date": e_str,
                        "years": years_diff,
                        "badge": service_honors_map[years_diff],
                        "anniversary_date": f"{target_year}-{e_month:02d}-{e_day:02d}"
                    })
            except Exception:
                pass

        # Runde Geburtstage
        if m.get("birth_date"):
            try:
                b_val = m["birth_date"]
                if isinstance(b_val, date):
                    b_year = b_val.year
                    b_month = b_val.month
                    b_day = b_val.day
                    b_str = str(b_val)
                else:
                    parts = str(b_val).split("-")
                    b_year = int(parts[0])
                    b_month = int(parts[1]) if len(parts) > 1 else 1
                    b_day = int(parts[2]) if len(parts) > 2 else 1
                    b_str = str(b_val)

                age = target_year - b_year
                if age in round_birthdays:
                    birthday_anniversaries.append({
                        "personnel_id": m["id"],
                        "name": m["name"],
                        "rank": m.get("rank") or "Kamerad/in",
                        "status": m.get("membership_status") or "Aktiv",
                        "birth_date": b_str,
                        "age": age,
                        "birthday_date": f"{target_year}-{b_month:02d}-{b_day:02d}"
                    })
            except Exception:
                pass

    service_anniversaries.sort(key=lambda x: (x["years"], x["anniversary_date"]))
    birthday_anniversaries.sort(key=lambda x: (x["age"], x["birthday_date"]))

    return {
        "year": target_year,
        "service_anniversaries": service_anniversaries,
        "birthday_anniversaries": birthday_anniversaries
    }

@router.get("/anniversaries")
def get_anniversaries(request: Request, year: Optional[int] = None):
    check_auth(request)
    target_year = year if year else date.today().year
    return _compute_anniversaries(target_year)

@router.get("/anniversaries/pdf")
def get_anniversaries_pdf(request: Request, year: Optional[int] = None):
    check_auth(request)
    from core.utils import get_station_name
    target_year = year if year else date.today().year
    data = _compute_anniversaries(target_year)

    service_rows = ""
    for a in data["service_anniversaries"]:
        service_rows += (f"<tr><td>{a['name']}</td><td>{a['rank']}</td><td>{a['years']} Jahre</td>"
                          f"<td>{a['badge']}</td><td>{a['anniversary_date']}</td></tr>")
    if not service_rows:
        service_rows = "<tr><td colspan='5' style='text-align:center; color:#6b7280;'>Keine Dienstjubiläen in diesem Jahr.</td></tr>"

    birthday_rows = ""
    for a in data["birthday_anniversaries"]:
        birthday_rows += f"<tr><td>{a['name']}</td><td>{a['rank']}</td><td>{a['age']} Jahre</td><td>{a['birthday_date']}</td></tr>"
    if not birthday_rows:
        birthday_rows = "<tr><td colspan='4' style='text-align:center; color:#6b7280;'>Keine runden Geburtstage in diesem Jahr.</td></tr>"

    today_fmt = date.today().strftime("%d.%m.%Y")
    # WICHTIG: kein CSS-Flexbox (xhtml2pdf unterstützt das nicht, siehe reports.py). Anders als
    # bei der Mitgliederliste (siehe get_personnel_roster_pdf) sind hier alle Zellwerte kurz
    # und enthalten Leerzeichen (Name, Ehrungstext) - eine flache Tabelle ohne explizite
    # Spaltenbreiten ist deshalb hier unproblematisch.
    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        @page {{ size: a4 portrait; margin: 2cm; }}
        body {{ font-family: Helvetica, Arial, sans-serif; font-size: 10.5pt; color: #1f2937; }}
        .header-table {{ width: 100%; border-bottom: 2px solid #b91c1c; padding-bottom: 12px; margin-bottom: 20px; }}
        .station-title {{ font-size: 16pt; font-weight: bold; color: #b91c1c; margin: 0; }}
        .doc-title {{ font-size: 14pt; font-weight: bold; text-transform: uppercase; margin-bottom: 15px; color: #111827; }}
        h3 {{ font-size: 12pt; color: #b91c1c; margin-top: 25px; margin-bottom: 8px; }}
        table.list-table {{ width: 100%; border-collapse: collapse; }}
        table.list-table th {{ background: #f8f9fa; border: 1px solid #ddd; padding: 6px 8px; text-align: left; font-size: 8.5pt; text-transform: uppercase; }}
        table.list-table td {{ border: 1px solid #ddd; padding: 6px 8px; font-size: 9.5pt; }}
    </style>
</head>
<body>
    <table class="header-table">
        <tr>
            <td><div class="station-title">{get_station_name()}</div></td>
            <td style="text-align: right; vertical-align: bottom; font-size: 9pt; color: #4b5563;">Erstellt am: {today_fmt}</td>
        </tr>
    </table>
    <div class="doc-title">Jubiläen &amp; runde Geburtstage {target_year}</div>

    <h3>Dienstjubiläen</h3>
    <table class="list-table">
        <thead><tr><th>Name</th><th>Dienstgrad</th><th>Jahre</th><th>Ehrung</th><th>Datum</th></tr></thead>
        <tbody>{service_rows}</tbody>
    </table>

    <h3>Runde Geburtstage</h3>
    <table class="list-table">
        <thead><tr><th>Name</th><th>Dienstgrad</th><th>Alter</th><th>Datum</th></tr></thead>
        <tbody>{birthday_rows}</tbody>
    </table>
</body>
</html>"""

    import xhtml2pdf.pisa as pisa
    import io
    pdf_buf = io.BytesIO()
    pisa.CreatePDF(html_content, dest=pdf_buf)
    pdf_bytes = pdf_buf.getvalue()

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="Jubilaeen_{target_year}.pdf"'}
    )

# --- NACHWEISE / BESCHEINIGUNGEN ---------------------------------------------------------
# Gemeinsame Dokumenthülle für die personenbezogenen Nachweise. Bewusst eine Stelle statt
# dreimal dasselbe CSS: xhtml2pdf ist bei Rändern und Tabellen empfindlich, und genau daran
# ist die Arbeitgeberbescheinigung schon einmal über zwei Seiten gerutscht.
def _certificate_document(doc_title: str, person: dict, meta_rows: str, body_html: str) -> str:
    from core.utils import get_station_name
    from html import escape
    today_fmt = date.today().strftime("%d.%m.%Y")
    return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        @page {{ size: a4 portrait; margin: 1.8cm; }}
        body {{ font-family: Helvetica, Arial, sans-serif; font-size: 10.5pt; color: #1f2937; }}
        .header-table {{ width: 100%; border-bottom: 2px solid #b91c1c; padding-bottom: 10px; margin-bottom: 16px; }}
        .station-title {{ font-size: 16pt; font-weight: bold; color: #b91c1c; margin: 0; }}
        .doc-title {{ font-size: 14pt; font-weight: bold; text-transform: uppercase; margin-bottom: 4px; color: #111827; }}
        .doc-subtitle {{ font-size: 9.5pt; color: #4b5563; margin-bottom: 14px; }}
        table.meta {{ width: 100%; margin-bottom: 14px; }}
        table.meta td {{ font-size: 10pt; padding: 2px 0; }}
        table.list-table {{ width: 100%; border-collapse: collapse; }}
        table.list-table th {{ background: #f8f9fa; border: 1px solid #ddd; padding: 5px 7px; text-align: left; font-size: 8.5pt; text-transform: uppercase; }}
        table.list-table td {{ border: 1px solid #ddd; padding: 5px 7px; font-size: 9.5pt; }}
        .summary {{ margin-top: 14px; padding: 8px 10px; background: #f8f9fa; border: 1px solid #e5e7eb; font-size: 10pt; }}
        .sig-table {{ width: 100%; margin-top: 34px; }}
        .sig-line {{ border-top: 1px solid #6b7280; padding-top: 4px; font-size: 8.5pt; color: #4b5563; }}
    </style>
</head>
<body>
    <table class="header-table">
        <tr>
            <td><div class="station-title">{escape(get_station_name())}</div></td>
            <td style="text-align: right; vertical-align: bottom; font-size: 9pt; color: #4b5563;">Erstellt am: {today_fmt}</td>
        </tr>
    </table>
    <div class="doc-title">{escape(doc_title)}</div>
    <div class="doc-subtitle">Hiermit wird bescheinigt, dass die nachfolgend aufgef&uuml;hrten Angaben
        dem im Digitalen Dienstbuch dokumentierten Stand entsprechen.</div>

    <table class="meta">
        <tr><td style="width:32%;"><b>Name</b></td><td>{escape(str(person.get('name') or '-'))}</td></tr>
        <tr><td><b>Dienstgrad</b></td><td>{escape(str(person.get('rank') or '-'))}</td></tr>
        {meta_rows}
    </table>

    {body_html}

    <table class="sig-table">
        <tr>
            <td style="width:48%;"><div class="sig-line">Ort, Datum</div></td>
            <td style="width:4%;"></td>
            <td style="width:48%;"><div class="sig-line">Unterschrift Wehrf&uuml;hrung / Kommandant</div></td>
        </tr>
    </table>
</body>
</html>"""


def _certificate_response(html_content: str, filename: str) -> Response:
    import xhtml2pdf.pisa as pisa
    import io
    pdf_buf = io.BytesIO()
    pisa.CreatePDF(html_content, dest=pdf_buf)
    return Response(
        content=pdf_buf.getvalue(),
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{filename}"'}
    )


def _load_person(cur, member_id: int) -> dict:
    cur.execute("SELECT * FROM personnel WHERE id = %s", (member_id,))
    person = cur.fetchone()
    if not person:
        raise HTTPException(status_code=404, detail="Kamerad nicht gefunden")
    return person


def _safe_filename(value: str) -> str:
    return "".join(c for c in (value or "Unbekannt") if c.isalnum() or c in ("-", "_")) or "Unbekannt"


@router.get("/{member_id}/certificate/dienstzeit/pdf")
def get_service_time_certificate(member_id: int, request: Request, year: Optional[int] = None):
    """Dienstzeit-/Taetigkeitsnachweis: alle Dienste und Einsaetze einer Person in einem Jahr."""
    check_auth(request, allowed_roles=("admin", "leitung"))
    from html import escape
    target_year = year or date.today().year

    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    person = _load_person(cur, member_id)

    # Dienste haengen (historisch) ueber persons.name am Namen, Einsaetze direkt an der
    # personnel_id - deshalb zwei Zweige. Exakter Namensabgleich, kein LIKE: sonst wuerden
    # "Max" und "Maximilian" zusammengezaehlt (gleiche Begruendung wie in users_mgr.py).
    cur.execute("""
        SELECT d, art, bezeichnung, dauer FROM (
            SELECT s.date AS d, 'Dienst' AS art,
                   COALESCE(NULLIF(s.description, ''), s.category, 'Dienst') AS bezeichnung,
                   s.duration AS dauer
            FROM attendance a
            JOIN sessions s ON a.session_id = s.id
            JOIN persons pe ON a.person_id = pe.id
            WHERE pe.name = %s AND YEAR(s.date) = %s AND a.is_present = 1
            UNION ALL
            SELECT m.date AS d, 'Einsatz' AS art,
                   COALESCE(NULLIF(m.stichwort, ''), 'Einsatz') AS bezeichnung,
                   m.duration AS dauer
            FROM mission_attendance ma
            JOIN missions m ON ma.mission_id = m.id
            WHERE ma.personnel_id = %s AND YEAR(m.date) = %s
              AND ma.is_present IS NOT NULL
              AND ma.is_present NOT IN ('Nein', '0', 'false', 'False', '')
        ) AS eintraege
        ORDER BY d ASC
    """, (person["name"], target_year, member_id, target_year))
    entries = cur.fetchall()
    cur.close(); conn.close()

    rows, total_hours, count_dienst, count_einsatz = "", 0.0, 0, 0
    for e in entries:
        hours = float(e["dauer"] or 0)
        total_hours += hours
        if e["art"] == "Einsatz":
            count_einsatz += 1
        else:
            count_dienst += 1
        d = e["d"].strftime("%d.%m.%Y") if hasattr(e["d"], "strftime") else str(e["d"])
        rows += (f"<tr><td>{d}</td><td>{escape(str(e['art']))}</td>"
                 f"<td>{escape(str(e['bezeichnung'] or '-'))}</td>"
                 f"<td style='text-align:right;'>{hours:.2f} h</td></tr>")
    if not rows:
        rows = ("<tr><td colspan='4' style='text-align:center; color:#6b7280;'>"
                "Keine Eintr&auml;ge in diesem Jahr.</td></tr>")

    body = f"""
    <table class="list-table">
        <thead><tr><th style="width:18%;">Datum</th><th style="width:16%;">Art</th>
        <th>Bezeichnung</th><th style="width:16%; text-align:right;">Dauer</th></tr></thead>
        <tbody>{rows}</tbody>
    </table>
    <div class="summary">
        Gesamt: <b>{total_hours:.2f} Stunden</b> &nbsp;&bull;&nbsp;
        {count_dienst} Dienste &nbsp;&bull;&nbsp; {count_einsatz} Eins&auml;tze
    </div>"""

    html_content = _certificate_document(
        f"Dienstzeitnachweis {target_year}", person,
        f"<tr><td><b>Berichtszeitraum</b></td><td>01.01.{target_year} &ndash; 31.12.{target_year}</td></tr>",
        body)
    return _certificate_response(
        html_content, f"Dienstzeitnachweis_{_safe_filename(person['name'])}_{target_year}.pdf")


@router.get("/{member_id}/certificate/lehrgaenge/pdf")
def get_courses_certificate(member_id: int, request: Request):
    """Lehrgangs- und Qualifikationsuebersicht einer Person (vollstaendige Historie)."""
    check_auth(request, allowed_roles=("admin", "leitung"))
    from html import escape

    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    person = _load_person(cur, member_id)
    cur.execute("""SELECT course_name, date FROM lehrgaenge
                   WHERE personnel_id = %s ORDER BY date ASC""", (member_id,))
    courses = cur.fetchall()
    cur.close(); conn.close()

    rows = ""
    for c in courses:
        d = c["date"].strftime("%d.%m.%Y") if hasattr(c["date"], "strftime") else str(c["date"] or "-")
        rows += f"<tr><td>{escape(str(c['course_name'] or '-'))}</td><td style='width:22%;'>{d}</td></tr>"
    if not rows:
        rows = ("<tr><td colspan='2' style='text-align:center; color:#6b7280;'>"
                "Keine Lehrg&auml;nge erfasst.</td></tr>")

    body = f"""
    <table class="list-table">
        <thead><tr><th>Lehrgang / Qualifikation</th><th>Abgeschlossen am</th></tr></thead>
        <tbody>{rows}</tbody>
    </table>
    <div class="summary">Erfasste Lehrg&auml;nge: <b>{len(courses)}</b></div>"""

    html_content = _certificate_document("Lehrgangs&uuml;bersicht", person, "", body)
    return _certificate_response(
        html_content, f"Lehrgaenge_{_safe_filename(person['name'])}.pdf")


@router.get("/{member_id}/certificate/atemschutz/pdf")
def get_respiratory_certificate(member_id: int, request: Request, year: Optional[int] = None):
    """Atemschutz-Nachweis: dokumentierte Einsaetze/Uebungen unter Atemschutz je Jahr."""
    check_auth(request, allowed_roles=("admin", "leitung"))
    from html import escape
    target_year = year or date.today().year

    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    person = _load_person(cur, member_id)
    cur.execute("""
        SELECT m.date AS d, m.stichwort, r.dauer, r.druck_start, r.druck_ende, r.fit_ok
        FROM respiration_log r
        JOIN missions m ON r.mission_id = m.id
        WHERE r.personnel_id = %s AND YEAR(m.date) = %s
        ORDER BY m.date ASC
    """, (member_id, target_year))
    entries = cur.fetchall()
    cur.close(); conn.close()

    rows = ""
    for e in entries:
        d = e["d"].strftime("%d.%m.%Y") if hasattr(e["d"], "strftime") else str(e["d"])
        fit = "ja" if e["fit_ok"] else "nein"
        rows += (f"<tr><td>{d}</td><td>{escape(str(e['stichwort'] or '-'))}</td>"
                 f"<td style='text-align:right;'>{escape(str(e['dauer'] or '-'))}</td>"
                 f"<td style='text-align:right;'>{escape(str(e['druck_start'] or '-'))}</td>"
                 f"<td style='text-align:right;'>{escape(str(e['druck_ende'] or '-'))}</td>"
                 f"<td style='text-align:center;'>{fit}</td></tr>")
    if not rows:
        rows = ("<tr><td colspan='6' style='text-align:center; color:#6b7280;'>"
                "Keine Atemschutz-Eins&auml;tze in diesem Jahr dokumentiert.</td></tr>")

    body = f"""
    <table class="list-table">
        <thead><tr><th style="width:16%;">Datum</th><th>Anlass</th>
        <th style="width:12%; text-align:right;">Dauer</th>
        <th style="width:13%; text-align:right;">Druck Start</th>
        <th style="width:13%; text-align:right;">Druck Ende</th>
        <th style="width:10%; text-align:center;">Fit</th></tr></thead>
        <tbody>{rows}</tbody>
    </table>
    <div class="summary">
        Dokumentierte Eins&auml;tze unter Atemschutz: <b>{len(entries)}</b>
        <br><span style="font-size:8.5pt; color:#4b5563;">Hinweis: Dieser Nachweis ersetzt weder
        die arbeitsmedizinische Vorsorge (G26) noch den Nachweis der j&auml;hrlichen
        Belastungs&uuml;bung.</span>
    </div>"""

    html_content = _certificate_document(
        f"Atemschutznachweis {target_year}", person,
        f"<tr><td><b>Berichtszeitraum</b></td><td>01.01.{target_year} &ndash; 31.12.{target_year}</td></tr>",
        body)
    return _certificate_response(
        html_content, f"Atemschutznachweis_{_safe_filename(person['name'])}_{target_year}.pdf")


@router.get("/me/licenses")
def get_my_licenses(request: Request):
    user = check_auth(request)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT p.lic_b, p.lic_be, p.lic_c, p.lic_ce, u.personnel_id FROM users u "
                "LEFT JOIN personnel p ON u.personnel_id = p.id WHERE u.username = %s", (user["username"],))
    res = cur.fetchone()
    if not res or not res["personnel_id"]:
        # Gleicher Fallback-Abgleich wie in users_mgr.py get_my_global_fire_stats(): Konto noch
        # nicht mit einem Personal-Datensatz verknüpft, versuche über den Benutzernamen zu finden.
        cur.execute("SELECT id, lic_b, lic_be, lic_c, lic_ce FROM personnel WHERE LOWER(name) LIKE %s", (f"%{user['username'].lower()}%",))
        fallback = cur.fetchone()
        if fallback:
            cur.execute("UPDATE users SET personnel_id = %s WHERE username = %s", (fallback["id"], user["username"]))
            conn.commit()
            res = fallback
    cur.close(); conn.close()
    if not res:
        return {"unlinked": True, "lic_b": 0, "lic_be": 0, "lic_c": 0, "lic_ce": 0}
    return {"unlinked": False, "lic_b": res["lic_b"], "lic_be": res["lic_be"], "lic_c": res["lic_c"], "lic_ce": res["lic_ce"]}

@router.get("/roster/pdf")
def get_personnel_roster_pdf(request: Request):
    check_auth(request, allowed_roles=("admin", "leitung"))
    from core.utils import get_station_name
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT name, rank, phone, email, address FROM personnel WHERE membership_status = 'Aktiv' ORDER BY name ASC")
    members = cur.fetchall(); cur.close(); conn.close()

    # WICHTIG: KEINE flache mehrspaltige Tabelle (Name/Dienstgrad/Telefon/E-Mail/Adresse
    # nebeneinander) - xhtml2pdf/reportlab berechnet Spaltenbreiten bei langen, nicht
    # umbrechbaren Werten (E-Mail-Adressen!) unzuverlässig: mit expliziten Breiten (per
    # <col>/<colgroup> oder CSS width auf <th>) stürzt die PDF-Erzeugung mit einem
    # reportlab-internen "negative availWidth"-Fehler ab; ganz ohne Breitenvorgabe quetscht
    # es die E-Mail-Spalte auf wenige Pixel zusammen und der Text überlappt die Adress-Spalte
    # (siehe Nutzer-Screenshot). Stattdessen: pro Mitglied eine kompakte Label/Wert-Tabelle
    # mit nur 2 Spalten (label ~28%, value ~72%) - dasselbe bewährte Muster wie die
    # Kopf-Datentabelle im Geräte-Prüfnachweis (siehe material_mgr.py .data-table), das dort
    # nie zu diesem Problem geführt hat.
    cards = ""
    for m in members:
        cards += f"""
        <table class="member-card">
            <tr><td class="label">Name</td><td class="value"><strong>{m['name']}</strong></td></tr>
            <tr><td class="label">Dienstgrad</td><td class="value">{m.get('rank') or '-'}</td></tr>
            <tr><td class="label">Telefon</td><td class="value">{m.get('phone') or '-'}</td></tr>
            <tr><td class="label">E-Mail</td><td class="value">{m.get('email') or '-'}</td></tr>
            <tr><td class="label">Adresse</td><td class="value">{m.get('address') or '-'}</td></tr>
        </table>"""
    if not cards:
        cards = "<p style='text-align:center; color:#6b7280;'>Keine aktiven Mitglieder gefunden.</p>"

    today_fmt = date.today().strftime("%d.%m.%Y")
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
        .doc-title {{ font-size: 14pt; font-weight: bold; text-transform: uppercase; margin-bottom: 15px; color: #111827; }}
        table.member-card {{ width: 100%; border-collapse: collapse; margin-bottom: 12px; }}
        table.member-card td {{ border: 1px solid #ddd; padding: 5px 8px; font-size: 9.5pt; }}
        table.member-card td.label {{ width: 28%; background: #f8f9fa; font-weight: bold; color: #4b5563; font-size: 8.5pt; text-transform: uppercase; }}
        table.member-card td.value {{ width: 72%; }}
    </style>
</head>
<body>
    <table class="header-table">
        <tr>
            <td><div class="station-title">{get_station_name()}</div></td>
            <td style="text-align: right; vertical-align: bottom; font-size: 9pt; color: #4b5563;">Erstellt am: {today_fmt}</td>
        </tr>
    </table>
    <div class="doc-title">Mitgliederliste (Aktive Mitglieder)</div>
    {cards}
</body>
</html>"""

    import xhtml2pdf.pisa as pisa
    import io
    pdf_buf = io.BytesIO()
    pisa.CreatePDF(html_content, dest=pdf_buf)
    pdf_bytes = pdf_buf.getvalue()

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": 'inline; filename="Mitgliederliste.pdf"'}
    )

def init_personnel_db():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS personnel (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL UNIQUE
            ) ENGINE=InnoDB;
        """)

        extended_columns = [
            ("rank", "VARCHAR(100) DEFAULT ''"),
            ("membership_status", "VARCHAR(50) DEFAULT 'Aktiv'"),
            ("phone", "VARCHAR(100) DEFAULT ''"),
            ("email", "VARCHAR(255) DEFAULT ''"),
            ("address", "TEXT NULL"),
            ("badge_number", "VARCHAR(100) DEFAULT ''"),
            ("birth_date", "DATE NULL"),
            ("entry_date", "DATE NULL"),
            ("honors", "TEXT NULL"),
            ("profile_picture", "LONGTEXT NULL"),
            ("is_truppmann", "BOOLEAN DEFAULT FALSE"),
            ("is_funk", "BOOLEAN DEFAULT FALSE"),
            ("is_agt", "BOOLEAN DEFAULT FALSE"),
            ("is_maschinist", "BOOLEAN DEFAULT FALSE"),
            ("is_tf", "BOOLEAN DEFAULT FALSE"),
            ("is_gf", "BOOLEAN DEFAULT FALSE"),
            ("lic_b", "BOOLEAN DEFAULT FALSE"),
            ("lic_be", "BOOLEAN DEFAULT FALSE"),
            ("lic_c", "BOOLEAN DEFAULT FALSE"),
            ("lic_ce", "BOOLEAN DEFAULT FALSE"),
            ("g26_3_date", "DATE NULL"),
            ("belastungslauf_date", "DATE NULL"),
            ("unterweisung_date", "DATE NULL"),
            ("dsgvo_ack_date", "DATE NULL"),
            ("dienstordnung_ack_date", "DATE NULL"),
            ("notes", "TEXT NULL"),
            ("skills", "TEXT NULL"),
            ("parent_contact", "VARCHAR(255) NULL"),
            ("lic_am", "BOOLEAN DEFAULT FALSE"),
            ("lic_a1", "BOOLEAN DEFAULT FALSE"),
            ("lic_l", "BOOLEAN DEFAULT FALSE"),
            ("lic_t", "BOOLEAN DEFAULT FALSE"),
            ("has_jf1", "BOOLEAN DEFAULT FALSE"),
            ("has_jf2", "BOOLEAN DEFAULT FALSE"),
            ("has_jf3", "BOOLEAN DEFAULT FALSE"),
            ("has_wissentest", "BOOLEAN DEFAULT FALSE"),
            ("has_leistungsspange", "BOOLEAN DEFAULT FALSE"),
            ("has_jugendabzeichen", "BOOLEAN DEFAULT FALSE"),
            ("has_mta_basis", "BOOLEAN DEFAULT FALSE"),
            ("has_erste_hilfe", "BOOLEAN DEFAULT FALSE"),
            ("has_funk", "BOOLEAN DEFAULT FALSE"),
            ("emergency_contact_name", "VARCHAR(255) DEFAULT ''"),
            ("emergency_contact_phone", "VARCHAR(100) DEFAULT ''")
        ]

        for col_name, col_type in extended_columns:
            try:
                cur.execute(f"ALTER TABLE personnel ADD COLUMN {col_name} {col_type}")
            except mysql.connector.Error as err:
                if err.errno == 1060: pass
                if err.errno == 1060: pass

        # Verfügbarkeits-/Abwesenheitszeiträume (Urlaub, Krankheit, ...)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS personnel_availability (
                id INT AUTO_INCREMENT PRIMARY KEY,
                personnel_id INT NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                reason VARCHAR(255) DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (personnel_id) REFERENCES personnel(id) ON DELETE CASCADE
            ) ENGINE=InnoDB;
        """)
        conn.commit()

        # Start Youth Migration
        try:
            cur.execute("SHOW TABLES LIKE 'youth_members'")
            if cur.fetchone():
                print("youth_members table found. Starting migration to personnel...")
                youth_cols = [
                    ("skills", "TEXT NULL"), ("parent_contact", "VARCHAR(255) NULL"), ("notes", "TEXT NULL"),
                    ("lic_am", "TINYINT(1) DEFAULT 0"), ("lic_a1", "TINYINT(1) DEFAULT 0"), ("lic_b", "TINYINT(1) DEFAULT 0"),
                    ("lic_l", "TINYINT(1) DEFAULT 0"), ("lic_t", "TINYINT(1) DEFAULT 0"),
                    ("has_jf1", "TINYINT(1) DEFAULT 0"), ("has_jf2", "TINYINT(1) DEFAULT 0"), ("has_jf3", "TINYINT(1) DEFAULT 0"),
                    ("has_wissentest", "TINYINT(1) DEFAULT 0"), ("has_leistungsspange", "TINYINT(1) DEFAULT 0"),
                    ("has_jugendabzeichen", "TINYINT(1) DEFAULT 0"), ("has_mta_basis", "TINYINT(1) DEFAULT 0"),
                    ("has_erste_hilfe", "TINYINT(1) DEFAULT 0"), ("has_funk", "TINYINT(1) DEFAULT 0")
                ]
                for c_name, c_type in youth_cols:
                    try:
                        cur.execute(f"SHOW COLUMNS FROM personnel LIKE '{c_name}'")
                        if not cur.fetchone():
                            cur.execute(f"ALTER TABLE personnel ADD COLUMN {c_name} {c_type}")
                    except: pass
                
                # Fetch as dict manually to prevent tuple index error
                cur.execute("SELECT * FROM youth_members")
                columns = [col[0] for col in cur.description]
                youths_tuples = cur.fetchall()
                youths = [dict(zip(columns, row)) for row in youths_tuples]
                
                id_map = {}
                for y in youths:
                    cur.execute("SELECT id FROM personnel WHERE name = %s", (y["name"],))
                    p = cur.fetchone()
                    if p:
                        new_id = p[0] # tuple index
                        cur.execute("""
                            UPDATE personnel SET
                            parent_contact = %s, skills = %s, membership_status = 'Jugend',
                            lic_am = %s, lic_a1 = %s, lic_b = %s, lic_l = %s, lic_t = %s,
                            has_jf1 = %s, has_jf2 = %s, has_jf3 = %s, has_wissentest = %s,
                            has_leistungsspange = %s, has_jugendabzeichen = %s, has_mta_basis = %s,
                            has_erste_hilfe = %s, has_funk = %s
                            WHERE id = %s
                        """, (
                            y.get("parent_contact"), y.get("skills"),
                            y.get("lic_am", 0), y.get("lic_a1", 0), y.get("lic_b", 0), y.get("lic_l", 0), y.get("lic_t", 0),
                            y.get("has_jf1", 0), y.get("has_jf2", 0), y.get("has_jf3", 0), y.get("has_wissentest", 0),
                            y.get("has_leistungsspange", 0), y.get("has_jugendabzeichen", 0), y.get("has_mta_basis", 0),
                            y.get("has_erste_hilfe", 0), y.get("has_funk", 0), new_id
                        ))
                    else:
                        cur.execute("""
                            INSERT INTO personnel (
                                name, membership_status, phone, email, address, notes, birth_date, entry_date,
                                parent_contact, skills, lic_am, lic_a1, lic_b, lic_l, lic_t,
                                has_jf1, has_jf2, has_jf3, has_wissentest, has_leistungsspange, has_jugendabzeichen,
                                has_mta_basis, has_erste_hilfe, has_funk
                            ) VALUES (
                                %s, 'Jugend', %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s,
                                %s, %s, %s
                            )
                        """, (
                            y["name"], y.get("phone"), y.get("email"), y.get("address"), y.get("notes"), y.get("birth_date"), y.get("entry_date"),
                            y.get("parent_contact"), y.get("skills"), y.get("lic_am", 0), y.get("lic_a1", 0), y.get("lic_b", 0), y.get("lic_l", 0), y.get("lic_t", 0),
                            y.get("has_jf1", 0), y.get("has_jf2", 0), y.get("has_jf3", 0), y.get("has_wissentest", 0), y.get("has_leistungsspange", 0), y.get("has_jugendabzeichen", 0),
                            y.get("has_mta_basis", 0), y.get("has_erste_hilfe", 0), y.get("has_funk", 0)
                        ))
                        new_id = cur.lastrowid
                    id_map[y["id"]] = new_id

                try:
                    cur.execute("""
                        SELECT CONSTRAINT_NAME FROM information_schema.KEY_COLUMN_USAGE 
                        WHERE TABLE_NAME = 'youth_attendance' AND COLUMN_NAME = 'member_id' AND TABLE_SCHEMA = DATABASE()
                    """)
                    fks = cur.fetchall()
                    for fk in fks:
                        cur.execute(f"ALTER TABLE youth_attendance DROP FOREIGN KEY {fk[0]}")
                except Exception as e: 
                    print("Error dropping FK:", e)

                for old_id, new_id in id_map.items():
                    cur.execute("UPDATE youth_attendance SET member_id = %s WHERE member_id = %s", (-new_id, old_id))
                cur.execute("UPDATE youth_attendance SET member_id = -member_id WHERE member_id < 0")
                
                try: cur.execute("ALTER TABLE youth_attendance ADD CONSTRAINT fk_ya_personnel FOREIGN KEY (member_id) REFERENCES personnel(id) ON DELETE CASCADE")
                except: pass
                
                try: cur.execute("DROP TABLE youth_members")
                except: pass
        except Exception as e:
            print(f"Error during youth migration: {e}")
        # End Youth Migration

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Fehler bei init_personnel_db: {e}")

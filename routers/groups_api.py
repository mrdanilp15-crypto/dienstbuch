from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from typing import Optional
from datetime import datetime

from database import get_db_connection
from core.utils import get_current_user, log_audit_action, get_station_name

router = APIRouter()
from core.models import safe_decode, AttendanceUpload, GroupData
from routers import reports

# --- GRUPPEN & DIENST-STRUKTUREN ---
@router.get("/groups")
def get_groups(request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    c=get_db_connection(); cur=c.cursor(dictionary=True)
    cur.execute("SELECT * FROM groups_table ORDER BY name")
    r=cur.fetchall(); c.close(); return r

@router.put("/groups/{id}")
def update_group(id: int, g: GroupData, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung"): raise HTTPException(status_code=403, detail="Schreibgeschützt")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("UPDATE groups_table SET name=%s WHERE id=%s", (g.name, id))
    c.commit(); cur.close(); c.close()
    return {"status": "updated"}

@router.post("/groups")
def create_group(g: GroupData, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung"): raise HTTPException(status_code=403, detail="Schreibgeschützt")
    c=get_db_connection(); cur=c.cursor()
    cur.execute("INSERT INTO groups_table (name) VALUES (%s)", (g.name,))
    c.commit(); c.close(); return {"status": "created"}

@router.delete("/groups/{id}")
def delete_group(id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung"): raise HTTPException(status_code=403, detail="Schreibgeschützt")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM groups_table WHERE id=%s", (id,))
    c.commit(); c.close(); return {"status": "deleted"}

@router.get("/groups/{group_id}/persons")
def get_group_persons(group_id: int, request: Request):
    # Liefert die Gruppen-Dienstliste (persons.id, NICHT personnel.id!) - wird vom
    # Dienst-Editor benutzt, um neu über den "Kameraden hinzufügen"-Dialog ausgewählte
    # Personen mit der korrekten persons.id zu verknüpfen, bevor sie gespeichert werden.
    # attendance.person_id referenziert per Fremdschlüssel persons.id, nicht personnel.id -
    # ohne diese Zuordnung würde die Anwesenheit beim Speichern fälschlich der personnel.id
    # zugeordnet, was auf eine komplett andere (oder gar keine) persons-Zeile zeigen kann.
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, name FROM persons WHERE group_id = %s", (group_id,))
    r = cur.fetchall(); c.close()
    return r

@router.get("/groups/{id}/sessions")
def get_sessions(id: int, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    
    # 1. Reguläre Dienste
    cur.execute("SELECT id, date, time, end_time, category, description, duration, leader_signature FROM sessions WHERE group_id=%s ORDER BY date DESC, id DESC", (id,))
    sessions = cur.fetchall()
    for x in sessions: 
        x['date'] = str(x['date'])
        x['time'] = str(x.get('time') or '')
        x['end_time'] = str(x.get('end_time') or '')
        sig = x.get('leader_signature')
        if sig:
            x['leader_signature'] = safe_decode(sig)
        x['is_signed'] = bool(sig and len(str(sig).strip()) > 10)
        x['is_mission'] = False
        
    # 2. Einsatzberichte mit einbinden
    try:
        cur.execute("SELECT id, date, time, end_time, stichwort, meldung, adresse, description, duration, leader_signature, status FROM missions ORDER BY date DESC, id DESC")
        missions = cur.fetchall()
        for m in missions:
            desc = f"{m['stichwort']}: {m['meldung']} ({m['adresse']})"
            if m.get('description'):
                desc += f" - {m['description']}"
            sig = m.get('leader_signature')
            decoded_sig = safe_decode(sig) if sig else None
            sessions.append({
                'id': f"m_{m['id']}",
                'real_mission_id': m['id'],
                'date': str(m['date']),
                'time': str(m.get('time') or ''),
                'end_time': str(m.get('end_time') or ''),
                'category': 'Einsatz',
                'description': desc,
                'duration': float(m['duration'] or 2.0),
                'leader_signature': decoded_sig,
                'status': 'Freigegeben' if (decoded_sig or m.get('status') == 'Freigegeben') else 'Entwurf',
                'is_signed': bool(decoded_sig and len(str(decoded_sig).strip()) > 10),
                'is_mission': True
            })
    except Exception as e:
        print(f"Mission fetch warning: {e}")
        
    sessions.sort(key=lambda x: str(x['date']), reverse=True)
    c.close()
    return sessions

@router.get("/groups/{id}/stats")
def get_stats(id: int, year: int, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT COUNT(*) as total FROM sessions WHERE group_id=%s AND YEAR(date)=%s", (id, year))
    max_s = cur.fetchone()['total'] or 0
    # WICHTIG: nur exakter (getrimmter, groß-/kleinschreibungsunabhängiger) Namensabgleich.
    # Ein LIKE '%Name%'-Fallback wurde hier bewusst entfernt: er hätte z.B. "Max" auch auf
    # "Maximilian" gematcht und so Einsatzstunden der falschen Person zugerechnet/doppelt
    # gezählt. persons.name wird durch sync_personnel_to_editor_groups() ohnehin laufend
    # exakt mit personnel.name synchronisiert, ein Fallback ist dafür nicht nötig.
    sql = """
        SELECT p.id as person_id, p.name,
               COALESCE(SUM(CASE WHEN a.is_present=1 AND s.id IS NOT NULL THEN 1 ELSE 0 END), 0) as present_count, 
               COALESCE(SUM(CASE WHEN a.is_present=1 AND s.id IS NOT NULL THEN s.duration ELSE 0 END), 0) as session_hours,
               COALESCE((
                   SELECT SUM(m.duration)
                   FROM mission_attendance ma
                   JOIN missions m ON ma.mission_id = m.id
                   JOIN personnel pl ON ma.personnel_id = pl.id
                   WHERE LOWER(TRIM(pl.name)) = LOWER(TRIM(p.name))
                     AND ma.is_present NOT IN ('Nein', '0', 'false', 'False', '') 
                     AND ma.is_present IS NOT NULL
                     AND YEAR(m.date) = %s
               ), 0) as mission_hours
        FROM persons p 
        LEFT JOIN attendance a ON p.id = a.person_id 
        LEFT JOIN sessions s ON a.session_id = s.id AND YEAR(s.date) = %s AND s.group_id = %s 
        WHERE p.group_id = %s 
        GROUP BY p.id, p.name
    """
    cur.execute(sql, (year, year, id, id))
    persons = cur.fetchall()
    c.close()

    for p in persons:
        s_h = float(p.get("session_hours") or 0.0)
        m_h = float(p.get("mission_hours") or 0.0)
        p["total_hours"] = round(s_h + m_h, 1)

    persons.sort(key=lambda x: x["total_hours"], reverse=True)
    return {"persons": persons, "total_sessions": max_s}

@router.get("/groups/{group_id}/attendance")
def get_attendance(group_id: int, request: Request, session_id: Optional[str] = None):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    try:
        session_data = {"session_id": session_id, "description": "", "duration": 2.0, "time": "", "end_time": "", "category": "Übung", "date": datetime.now().strftime("%Y-%m-%d"), "leader_signature": None, "instructors": ""}
        is_mission_session = False
        real_m_id = None
        if session_id:
            s_str = str(session_id).strip()
            if s_str.startswith("m_"):
                is_mission_session = True
                real_m_id = int(s_str.replace("m_", ""))
                cur.execute("SELECT id as session_id, stichwort, meldung, adresse, description, duration, date, time, end_time, leader_signature FROM missions WHERE id = %s", (real_m_id,))
                mrow = cur.fetchone()
                if mrow:
                    session_data['session_id'] = session_id
                    session_data['category'] = 'Einsatz'
                    desc = f"{mrow['stichwort']}: {mrow['meldung']} ({mrow['adresse']})"
                    if mrow.get('description'): desc += f" - {mrow['description']}"
                    session_data['description'] = desc
                    session_data['duration'] = float(mrow.get('duration') or 2.0)
                    session_data['date'] = str(mrow['date'])
                    session_data['time'] = str(mrow.get('time') or '')
                    session_data['end_time'] = str(mrow.get('end_time') or '')
                    if mrow.get('leader_signature'): session_data['leader_signature'] = safe_decode(mrow['leader_signature'])
            else:
                cur.execute("SELECT id as session_id, description, duration, date, time, end_time, category, leader_signature, instructors FROM sessions WHERE id = %s", (int(session_id),))
                row = cur.fetchone()
                if row:
                    session_data = row
                    session_data['date'] = str(session_data['date'])
                    session_data['time'] = str(session_data.get('time') or '')
                    session_data['end_time'] = str(session_data.get('end_time') or '')
                    if session_data.get('leader_signature'): session_data['leader_signature'] = safe_decode(session_data['leader_signature'])

        cur.execute("SELECT setting_value FROM settings WHERE setting_key = 'int_g26'")
        g26_row = cur.fetchone()
        g26_allowed_months = g26_row['setting_value'] if g26_row else 36

        if is_mission_session and real_m_id:
            query = """SELECT p.id, p.name, COALESCE(ma.is_present, 'Nein') as is_present_str, COALESCE(ma.vehicle, '') as vehicle, 
                              pl.id AS personnel_id, 
                              CASE WHEN pl.profile_picture IS NOT NULL AND LENGTH(pl.profile_picture) > 0 THEN 1 ELSE 0 END AS has_picture,
                              pl.g26_3_date, pl.is_agt
                       FROM persons p 
                       LEFT JOIN personnel pl ON p.name = pl.name 
                       LEFT JOIN mission_attendance ma ON pl.id = ma.personnel_id AND ma.mission_id = %s 
                       WHERE p.group_id = %s ORDER BY p.name"""
            cur.execute(query, (real_m_id, group_id))
            persons = cur.fetchall()
            for p in persons:
                p['signature'] = None
                p['note'] = ""
                p['is_present'] = bool(p.get('is_present_str') and p['is_present_str'] != 'Nein')
                p['has_picture'] = bool(p.get('has_picture', 0))
                p['g26_expired'] = False
                if p.get('is_agt') and p.get('g26_3_date'):
                    g26_date = p['g26_3_date']
                    if g26_date:
                        diff_days = (datetime.now().date() - g26_date).days
                        if diff_days > (g26_allowed_months * 30.44):
                            p['g26_expired'] = True
                if p.get('g26_3_date'):
                    p['g26_3_date'] = str(p['g26_3_date'])
        else:
            query = """SELECT p.id, p.name, COALESCE(a.is_present, 0) as is_present, COALESCE(a.note, '') as note, 
                              COALESCE(a.vehicle, '') as vehicle, a.signature, pl.id AS personnel_id, 
                              CASE WHEN pl.profile_picture IS NOT NULL AND LENGTH(pl.profile_picture) > 0 THEN 1 ELSE 0 END AS has_picture,
                              pl.g26_3_date, pl.is_agt
                       FROM persons p 
                       LEFT JOIN attendance a ON p.id = a.person_id AND a.session_id = %s 
                       LEFT JOIN personnel pl ON p.name = pl.name 
                       WHERE p.group_id = %s ORDER BY p.name"""
            cur.execute(query, (int(session_id) if (session_id and str(session_id).isdigit()) else 0, group_id))
            persons = cur.fetchall()
            
            for p in persons:
                p['signature'] = safe_decode(p['signature'])
                p['is_present'] = bool(p['is_present'])
                p['has_picture'] = bool(p.get('has_picture', 0))
                p['g26_expired'] = False
                if p.get('is_agt') and p.get('g26_3_date'):
                    g26_date = p['g26_3_date']
                    if g26_date:
                        diff_days = (datetime.now().date() - g26_date).days
                        if diff_days > (g26_allowed_months * 30.44):
                            p['g26_expired'] = True
                if p.get('g26_3_date'):
                    p['g26_3_date'] = str(p['g26_3_date'])

        return {**session_data, "persons": persons}
    finally: cur.close(); conn.close()

@router.post("/attendance")
def save_attendance(payload: AttendanceUpload, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "gruppenfuehrer"): raise HTTPException(status_code=403, detail="Schreibgeschützt")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    try:
        if payload.session_id:
            s_str = str(payload.session_id).strip()
            if s_str.startswith("m_"):
                real_m_id = int(s_str.replace("m_", ""))
                cur.execute("""UPDATE missions SET date=%s, time=%s, end_time=%s, duration=%s, leader_signature=%s WHERE id=%s""",
                            (payload.date, payload.time or "", payload.end_time or "", payload.duration, payload.leader_signature, real_m_id))
                session_id = payload.session_id
            else:
                s_int = int(payload.session_id)
                cur.execute("""UPDATE sessions SET date=%s, time=%s, end_time=%s, description=%s, duration=%s, category=%s, instructors=%s, leader_signature=%s WHERE id=%s""",(payload.date, payload.time or "", payload.end_time or "", payload.description, payload.duration, payload.category, payload.instructors, payload.leader_signature, s_int))
                session_id = s_int
                cur.execute("DELETE FROM attendance WHERE session_id = %s", (session_id,))
                for entry in payload.entries:
                    cur.execute("INSERT INTO attendance (session_id, person_id, is_present, note, vehicle, signature) VALUES (%s, %s, %s, %s, %s, %s)",(session_id, entry.person_id, 1 if entry.is_present else 0, entry.note or "", entry.vehicle or "", entry.signature))
        else:
            cur.execute("""INSERT INTO sessions (group_id, date, time, end_time, description, duration, category, instructors, leader_signature) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",(payload.group_id, payload.date, payload.time or "", payload.end_time or "", payload.description, payload.duration, payload.category, payload.instructors, payload.leader_signature))
            session_id = cur.lastrowid
            cur.execute("DELETE FROM attendance WHERE session_id = %s", (session_id,))
            for entry in payload.entries:
                cur.execute("INSERT INTO attendance (session_id, person_id, is_present, note, vehicle, signature) VALUES (%s, %s, %s, %s, %s, %s)",(session_id, entry.person_id, 1 if entry.is_present else 0, entry.note or "", entry.vehicle or "", entry.signature))
        conn.commit(); return {"status": "success", "session_id": session_id}
    except Exception as e: conn.rollback(); raise HTTPException(status_code=500, detail=str(e))
    finally: cur.close(); conn.close()

@router.get("/groups/{group_id}/topics")
def get_topics(group_id: int, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("SELECT DISTINCT description FROM sessions WHERE group_id=%s AND description IS NOT NULL LIMIT 50", (group_id,))
    r = [row[0] for row in cur.fetchall()]; c.close(); return r

@router.get("/groups/{group_id}/instructors")
def get_instructors(group_id: int, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("SELECT DISTINCT instructors FROM sessions WHERE group_id=%s AND instructors IS NOT NULL LIMIT 50", (group_id,))
    r = [row[0] for row in cur.fetchall()]; c.close(); return r

@router.post("/sessions/{session_id}/leader_signature")
def save_leader_sig(session_id: str, data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "gruppenfuehrer"): raise HTTPException(status_code=403, detail="Schreibgeschützt")
    c = get_db_connection(); cur = c.cursor()
    sid_str = str(session_id)
    sig = data.get("signature")
    if sid_str.startswith("m_"):
        real_id = int(sid_str.replace("m_", ""))
        cur.execute("UPDATE missions SET leader_signature = %s, status = 'Freigegeben' WHERE id = %s", (sig, real_id))
    else:
        real_id = int(sid_str)
        cur.execute("UPDATE sessions SET leader_signature = %s WHERE id = %s", (sig, real_id))
    c.commit(); c.close(); return {"status": "success"}

# --- EINTRÄGE / DIENSTE PERMANENT LÖSCHEN ---
@router.delete("/sessions/{session_id}")
def delete_session(session_id: str, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "gruppenfuehrer"): 
        raise HTTPException(status_code=403, detail="Schreibgeschützt")
    conn = get_db_connection(); cur = conn.cursor()
    sid_str = str(session_id)
    if sid_str.startswith("m_"):
        real_id = int(sid_str.replace("m_", ""))
        cur.execute("DELETE FROM mission_attendance WHERE mission_id = %s", (real_id,))
        cur.execute("DELETE FROM respiration_log WHERE mission_id = %s", (real_id,))
        cur.execute("DELETE FROM missions WHERE id = %s", (real_id,))
    else:
        real_id = int(sid_str)
        cur.execute("DELETE FROM attendance WHERE session_id = %s", (real_id,))
        cur.execute("DELETE FROM sessions WHERE id = %s", (real_id,))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "EINTRAG_LOESCHEN", f"Diensteintrag / Einsatz ID {session_id} wurde unwiderruflich gelöscht.")
    return {"status": "success"}

# --- BERICHTE & JAHRESBERICHTE SYSTEM ---
@router.get("/sessions/{session_id}/report", response_class=HTMLResponse)
def single_report(session_id: int, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT s.*, g.name as gname FROM sessions s JOIN groups_table g ON s.group_id = g.id WHERE s.id=%s", (session_id,))
    s = cur.fetchone()
    if s and s['leader_signature']: s['leader_signature'] = safe_decode(s['leader_signature'])
    cur.execute("SELECT p.name, a.is_present, a.note, a.vehicle, a.signature FROM attendance a JOIN persons p ON a.person_id = p.id WHERE a.session_id=%s ORDER BY p.name", (session_id,))
    persons = cur.fetchall(); c.close()
    for p in persons: p['signature'] = safe_decode(p['signature'])
    return f"<html><head><meta charset='UTF-8'><style>{reports.get_report_styles()}</style></head><body>{reports.generate_single_report(s, persons, get_station_name())}</body></html>"

@router.get("/groups/{group_id}/print_view", response_class=HTMLResponse)
def year_report(group_id: int, year: int, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT name FROM groups_table WHERE id=%s", (group_id,))
    gname_res = cur.fetchone(); gname = gname_res['name'] if gname_res else "Unbekannt"
    cur.execute("SELECT COUNT(*) as total FROM sessions WHERE group_id=%s AND YEAR(date)=%s", (group_id, year))
    max_s = cur.fetchone()['total'] or 0
    cur.execute("SELECT s.*, g.name as gname FROM sessions s JOIN groups_table g ON s.group_id = g.id WHERE s.group_id=%s AND YEAR(s.date)=%s ORDER BY s.date ASC, s.id ASC", (group_id, year))
    sessions_list = cur.fetchall()

    # Anwesenheit für ALLE Sitzungen des Jahres in einer Abfrage statt einer Query pro
    # Sitzung (N+1) holen und in Python nach session_id gruppieren.
    attendance_by_session = {}
    if sessions_list:
        session_ids = [s['id'] for s in sessions_list]
        placeholders = ", ".join(["%s"] * len(session_ids))
        cur.execute(f"""
            SELECT a.session_id, p.name, a.is_present, a.note, a.vehicle, a.signature
            FROM attendance a JOIN persons p ON a.person_id = p.id
            WHERE a.session_id IN ({placeholders})
            ORDER BY a.session_id, p.name
        """, tuple(session_ids))
        for row in cur.fetchall():
            attendance_by_session.setdefault(row['session_id'], []).append(row)

    html_body = ""; p_stats = {}; cat_sums = {"Übung": 0.0, "Einsatz": 0.0, "Sonstiges": 0.0}
    for s in sessions_list:
        if s['leader_signature']: s['leader_signature'] = safe_decode(s['leader_signature'])
        persons = attendance_by_session.get(s['id'], [])
        for p in persons: p['signature'] = safe_decode(p['signature'])
        html_body += reports.generate_single_report(s, persons, get_station_name())
        cat = s['category'] if s['category'] in cat_sums else "Sonstiges"
        cat_sums[cat] += float(s['duration'])
        for p in persons:
            if p['name'] not in p_stats: p_stats[p['name']] = {"Übung": 0.0, "Einsatz": 0.0, "Sonstiges": 0.0, "total_h": 0.0, "p": 0}
            if p['is_present']: p_stats[p['name']]["p"] += 1; p_stats[p['name']][cat] += float(s['duration']); p_stats[p['name']]["total_h"] += float(s['duration'])
    for n in p_stats: p_stats[n]['q'] = round((p_stats[n]['p'] / max_s) * 100) if max_s > 0 else 0
    html_body += reports.generate_year_report(gname, year, p_stats, cat_sums, get_station_name())
    c.close()
    return f"<html><head><meta charset='UTF-8'><style>{reports.get_report_styles()}</style></head><body>{html_body}</body></html>"

@router.get("/annual-report", response_class=HTMLResponse)
def year_report_all_groups(year: int, request: Request):
    # Gesamt-Jahresbericht über ALLE Gruppen hinweg statt nur eine einzelne - gleiche Logik
    # wie year_report() oben, nur über jede Gruppe hinweg aggregiert. Da jede Person laut
    # internal_sync_personnel_to_groups() in JEDER Gruppe im Roster steht, ist die Quote hier
    # eine Näherung (Nenner = Summe der Diensttermine aller Gruppen) - für den schnellen
    # Gesamtüberblick der Wehrführung ausreichend genau.
    # WICHTIG: bewusst NICHT als /groups/all/print_view angelegt - das hätte mit
    # /groups/{group_id}/print_view kollidiert (group_id würde "all" als String annehmen und
    # dann an der int-Validierung scheitern, statt zu diesem Endpunkt durchzufallen).
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, name FROM groups_table ORDER BY name ASC")
    groups = cur.fetchall()

    html_body = ""
    p_stats = {}
    cat_sums = {"Übung": 0.0, "Einsatz": 0.0, "Sonstiges": 0.0}
    max_s_total = 0

    for g in groups:
        group_id = g['id']
        cur.execute("SELECT COUNT(*) as total FROM sessions WHERE group_id=%s AND YEAR(date)=%s", (group_id, year))
        max_s = cur.fetchone()['total'] or 0
        max_s_total += max_s

        cur.execute("SELECT s.*, g.name as gname FROM sessions s JOIN groups_table g ON s.group_id = g.id WHERE s.group_id=%s AND YEAR(s.date)=%s ORDER BY s.date ASC, s.id ASC", (group_id, year))
        sessions_list = cur.fetchall()
        if not sessions_list:
            continue

        attendance_by_session = {}
        session_ids = [s['id'] for s in sessions_list]
        placeholders = ", ".join(["%s"] * len(session_ids))
        cur.execute(f"""
            SELECT a.session_id, p.name, a.is_present, a.note, a.vehicle, a.signature
            FROM attendance a JOIN persons p ON a.person_id = p.id
            WHERE a.session_id IN ({placeholders})
            ORDER BY a.session_id, p.name
        """, tuple(session_ids))
        for row in cur.fetchall():
            attendance_by_session.setdefault(row['session_id'], []).append(row)

        for s in sessions_list:
            if s['leader_signature']: s['leader_signature'] = safe_decode(s['leader_signature'])
            persons = attendance_by_session.get(s['id'], [])
            for p in persons: p['signature'] = safe_decode(p['signature'])
            html_body += reports.generate_single_report(s, persons, get_station_name())
            cat = s['category'] if s['category'] in cat_sums else "Sonstiges"
            cat_sums[cat] += float(s['duration'])
            for p in persons:
                if p['name'] not in p_stats: p_stats[p['name']] = {"Übung": 0.0, "Einsatz": 0.0, "Sonstiges": 0.0, "total_h": 0.0, "p": 0}
                if p['is_present']:
                    p_stats[p['name']]["p"] += 1
                    p_stats[p['name']][cat] += float(s['duration'])
                    p_stats[p['name']]["total_h"] += float(s['duration'])

    for n in p_stats:
        p_stats[n]['q'] = round((p_stats[n]['p'] / max_s_total) * 100) if max_s_total > 0 else 0
    html_body += reports.generate_year_report("Gesamte Feuerwehr", year, p_stats, cat_sums, get_station_name())
    c.close()
    return f"<html><head><meta charset='UTF-8'><style>{reports.get_report_styles()}</style></head><body>{html_body}</body></html>"

# --- GLOBALER NUTZERSTUNDEN-ABGLEICH ---
# Routes moved to routers/users_mgr.py

# --- ALARMIERUNG (APAGER PRO WEBHOOK & CONFIG) ---
# Routes moved to routers/apager_api.py

# --- VORLAGEN FÜR WIEDERKEHRENDE DIENSTEINTRÄGE ---
@router.get("/groups/{group_id}/templates")
def list_session_templates(group_id: int, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401, detail="Nicht angemeldet")
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    cur.execute("SELECT * FROM session_templates WHERE group_id = %s ORDER BY name ASC", (group_id,))
    res = cur.fetchall(); c.close()
    for r in res:
        if r.get("duration") is not None: r["duration"] = float(r["duration"])
    return res

@router.post("/groups/{group_id}/templates")
def create_session_template(group_id: int, data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "gruppenfuehrer"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    name = (data.get("name") or "").strip()
    if not name: raise HTTPException(status_code=400, detail="Name erforderlich")
    c = get_db_connection(); cur = c.cursor()
    cur.execute(
        "INSERT INTO session_templates (group_id, name, category, duration, description, instructors, time, end_time) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (group_id, name, data.get("category") or "Übung", float(data.get("duration") or 2.0),
         data.get("description") or "", data.get("instructors") or "", data.get("time") or "", data.get("end_time") or "")
    )
    c.commit(); cur.close(); c.close()
    return {"status": "success"}

@router.delete("/templates/{template_id}")
def delete_session_template(template_id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "gruppenfuehrer"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM session_templates WHERE id = %s", (template_id,))
    c.commit(); cur.close(); c.close()
    return {"status": "success"}

# --- GEMEINSAME KALENDERANSICHT ---
@router.get("/api/calendar")
def get_calendar_events(request: Request, year: int, month: int):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    c = get_db_connection(); cur = c.cursor(dictionary=True)
    events = []

    cur.execute("""
        SELECT s.id, s.date, s.time, s.category, s.description, g.name as gname
        FROM sessions s JOIN groups_table g ON s.group_id = g.id
        WHERE YEAR(s.date) = %s AND MONTH(s.date) = %s
    """, (year, month))
    for row in cur.fetchall():
        events.append({
            "date": str(row["date"]), "type": "dienst",
            "title": row["description"] or row["category"] or "Dienst",
            "subtitle": row["gname"], "time": str(row["time"]) if row["time"] else None
        })

    cur.execute("""
        SELECT id, date, time, stichwort, adresse, status FROM missions
        WHERE YEAR(date) = %s AND MONTH(date) = %s
    """, (year, month))
    for row in cur.fetchall():
        events.append({
            "date": str(row["date"]), "type": "einsatz",
            "title": row["stichwort"] or "Einsatz",
            "subtitle": row["adresse"], "time": str(row["time"]) if row["time"] else None
        })

    cur.execute("""
        SELECT r.id, r.start_datetime, r.end_datetime, r.purpose, v.name as vname
        FROM vehicle_reservations r JOIN vehicles v ON r.vehicle_id = v.id
        WHERE YEAR(r.start_datetime) = %s AND MONTH(r.start_datetime) = %s
    """, (year, month))
    for row in cur.fetchall():
        events.append({
            "date": str(row["start_datetime"])[:10], "type": "reservierung",
            "title": f"{row['vname']}: {row['purpose']}",
            "subtitle": None, "time": str(row["start_datetime"])[11:16]
        })

    # Geplante Termine (Dienst- & Übungsplaner) - bisher fehlte diese Abfrage komplett, ein
    # dort eingetragener Termin (z.B. über den Kalender-Tagesklick angelegt) tauchte im
    # gemeinsamen Kalender nie auf.
    cur.execute("""
        SELECT id, date, time, title, type FROM schedules
        WHERE YEAR(date) = %s AND MONTH(date) = %s
    """, (year, month))
    for row in cur.fetchall():
        events.append({
            "date": str(row["date"]), "type": "termin", "schedule_id": row["id"],
            "title": row["title"], "subtitle": row["type"], "time": str(row["time"]) if row["time"] else None
        })
    c.close()

    from routers import personnel_mgr
    anniversaries = personnel_mgr.get_anniversaries(request, year)
    for b in anniversaries.get("birthday_anniversaries", []):
        if b["birthday_date"][5:7] == f"{month:02d}":
            events.append({"date": b["birthday_date"], "type": "jubilaeum", "title": f"{b['name']} wird {b['age']}", "subtitle": "Runder Geburtstag", "time": None})
    for sv in anniversaries.get("service_anniversaries", []):
        if sv["anniversary_date"][5:7] == f"{month:02d}":
            events.append({"date": sv["anniversary_date"], "type": "jubilaeum", "title": f"{sv['name']}: {sv['years']} Jahre Dienst", "subtitle": "Dienstjubiläum", "time": None})

    events.sort(key=lambda e: (e["date"], e["time"] or ""))
    return events


from fastapi import APIRouter, HTTPException, Request, Response, UploadFile, File
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Optional, List, Union
from datetime import datetime, date
import json

from database import get_db_connection
from core.utils import get_current_user, log_audit_action

router = APIRouter()
import main
from core.models import safe_decode, PersonData, VehicleData, EntryDto, AttendanceUpload, GroupData
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
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("UPDATE groups_table SET name=%s WHERE id=%s", (g.name, id))
    c.commit(); cur.close(); c.close()
    return {"status": "updated"}

@router.post("/groups")
def create_group(g: GroupData, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt")
    c=get_db_connection(); cur=c.cursor()
    cur.execute("INSERT INTO groups_table (name) VALUES (%s)", (g.name,))
    c.commit(); c.close(); return {"status": "created"}

@router.delete("/groups/{id}")
def delete_group(id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] == "mannschaft": raise HTTPException(status_code=403, detail="Schreibgeschützt")
    c = get_db_connection(); cur = c.cursor()
    cur.execute("DELETE FROM groups_table WHERE id=%s", (id,))
    c.commit(); c.close(); return {"status": "deleted"}

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
    sql = """
        SELECT p.id as person_id, p.name, 
               COALESCE(SUM(CASE WHEN a.is_present=1 AND s.id IS NOT NULL THEN 1 ELSE 0 END), 0) as present_count, 
               COALESCE(SUM(CASE WHEN a.is_present=1 AND s.id IS NOT NULL THEN s.duration ELSE 0 END), 0) as session_hours,
               COALESCE((
                   SELECT SUM(m.duration)
                   FROM mission_attendance ma
                   JOIN missions m ON ma.mission_id = m.id
                   JOIN personnel pl ON ma.personnel_id = pl.id
                   WHERE (LOWER(TRIM(pl.name)) = LOWER(TRIM(p.name)) OR pl.name LIKE CONCAT('%%', p.name, '%%') OR p.name LIKE CONCAT('%%', pl.name, '%%'))
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
    if not user or user["role"] in ("mannschaft", "geratewart"): raise HTTPException(status_code=403, detail="Schreibgeschützt")
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
    if not user or user["role"] in ("mannschaft", "geratewart"): raise HTTPException(status_code=403, detail="Schreibgeschützt")
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
    if not user or user["role"] in ("mannschaft", "geratewart"): 
        raise HTTPException(status_code=403, detail="Schreibgeschützt")
    conn = get_db_connection(); cur = conn.cursor()
    sid_str = str(session_id)
    if sid_str.startswith("m_"):
        real_id = int(sid_str.replace("m_", ""))
        cur.execute("DELETE FROM mission_attendance WHERE mission_id = %s", (real_id,))
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
    return f"<html><head><meta charset='UTF-8'><style>{reports.get_report_styles()}</style></head><body>{reports.generate_single_report(s, persons, main.TOWN_NAME)}</body></html>"

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
    html_body = ""; p_stats = {}; cat_sums = {"Übung": 0.0, "Einsatz": 0.0, "Sonstiges": 0.0}
    for s in sessions_list:
        if s['leader_signature']: s['leader_signature'] = safe_decode(s['leader_signature'])
        cur.execute("SELECT p.name, a.is_present, a.note, a.vehicle, a.signature FROM attendance a JOIN persons p ON a.person_id = p.id WHERE a.session_id=%s ORDER BY p.name", (s['id'],))
        persons = cur.fetchall()
        for p in persons: p['signature'] = safe_decode(p['signature'])
        html_body += reports.generate_single_report(s, persons, main.TOWN_NAME)
        cat = s['category'] if s['category'] in cat_sums else "Sonstiges"
        cat_sums[cat] += float(s['duration'])
        for p in persons:
            if p['name'] not in p_stats: p_stats[p['name']] = {"Übung": 0.0, "Einsatz": 0.0, "Sonstiges": 0.0, "total_h": 0.0, "p": 0}
            if p['is_present']: p_stats[p['name']]["p"] += 1; p_stats[p['name']][cat] += float(s['duration']); p_stats[p['name']]["total_h"] += float(s['duration'])
    for n in p_stats: p_stats[n]['q'] = round((p_stats[n]['p'] / max_s) * 100) if max_s > 0 else 0
    html_body += reports.generate_year_report(gname, year, p_stats, cat_sums, main.TOWN_NAME)
    c.close()
    return f"<html><head><meta charset='UTF-8'><style>{reports.get_report_styles()}</style></head><body>{html_body}</body></html>"

# --- GLOBALER NUTZERSTUNDEN-ABGLEICH ---
# Routes moved to routers/users_mgr.py

# --- ALARMIERUNG (APAGER PRO WEBHOOK & CONFIG) ---
# Routes moved to routers/apager_api.py


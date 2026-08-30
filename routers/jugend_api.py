from fastapi import APIRouter, HTTPException, Request, Response, UploadFile, File
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Optional, List, Union
from datetime import datetime, date
import json

from database import get_db_connection
from core.utils import get_current_user, log_audit_action

router = APIRouter()


# --- JUGENDFEUERWEHR & MITGLIEDER ---
@router.get("/api/jugend/members")
def get_youth_members(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM youth_members ORDER BY name ASC")
    res = cur.fetchall(); cur.close(); conn.close()
    for r in res:
        for k in ["birth_date", "entry_date"]:
            if r.get(k): r[k] = str(r[k])
        for k in ["lic_am", "lic_a1", "lic_b", "lic_l", "lic_t", "has_jf1", "has_jf2", "has_jf3", "has_wissentest", "has_leistungsspange", "has_jugendabzeichen", "has_mta_basis", "has_erste_hilfe", "has_funk"]:
            if k in r: r[k] = bool(r[k])
        if r.get("profile_picture"):
            r["profile_picture"] = safe_decode(r["profile_picture"])
    return res

@router.post("/api/jugend/members")
def add_youth_member(data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "jugendwarte"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    name = (data.get("name") or "").strip()
    if not name: raise HTTPException(status_code=400, detail="Name erforderlich")
    
    parent = data.get("parent_contact", "")
    badges = data.get("badges", "")
    skills = data.get("skills", "")
    birth_date = data.get("birth_date") or None
    entry_date = data.get("entry_date") or None
    phone = data.get("phone", "")
    email = data.get("email", "")
    address = data.get("address", "")
    notes = data.get("notes", "")
    profile_picture = data.get("profile_picture", "")

    lic_am = 1 if data.get("lic_am") else 0
    lic_a1 = 1 if data.get("lic_a1") else 0
    lic_b = 1 if data.get("lic_b") else 0
    lic_l = 1 if data.get("lic_l") else 0
    lic_t = 1 if data.get("lic_t") else 0

    has_jf1 = 1 if data.get("has_jf1") else 0
    has_jf2 = 1 if data.get("has_jf2") else 0
    has_jf3 = 1 if data.get("has_jf3") else 0
    has_wissentest = 1 if data.get("has_wissentest") else 0
    has_leistungsspange = 1 if data.get("has_leistungsspange") else 0
    has_jugendabzeichen = 1 if data.get("has_jugendabzeichen") else 0
    has_mta_basis = 1 if data.get("has_mta_basis") else 0
    has_erste_hilfe = 1 if data.get("has_erste_hilfe") else 0
    has_funk = 1 if data.get("has_funk") else 0

    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO youth_members 
        (name, parent_contact, badges, skills, birth_date, entry_date, phone, email, address, notes, profile_picture,
         lic_am, lic_a1, lic_b, lic_l, lic_t, has_jf1, has_jf2, has_jf3, has_wissentest, has_leistungsspange, has_jugendabzeichen, has_mta_basis, has_erste_hilfe, has_funk)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (name, parent, badges, skills, birth_date, entry_date, phone, email, address, notes, profile_picture,
          lic_am, lic_a1, lic_b, lic_l, lic_t, has_jf1, has_jf2, has_jf3, has_wissentest, has_leistungsspange, has_jugendabzeichen, has_mta_basis, has_erste_hilfe, has_funk))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "JUGEND_ANLEGEN", f"Jugendmitglied '{name}' neu angelegt.")
    return {"status": "success"}

@router.put("/api/jugend/members/{m_id}")
def update_youth_member(m_id: int, data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "jugendwarte"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    name = (data.get("name") or "").strip()
    if not name: raise HTTPException(status_code=400, detail="Name erforderlich")
    
    parent = data.get("parent_contact", "")
    badges = data.get("badges", "")
    skills = data.get("skills", "")
    birth_date = data.get("birth_date") or None
    entry_date = data.get("entry_date") or None
    phone = data.get("phone", "")
    email = data.get("email", "")
    address = data.get("address", "")
    notes = data.get("notes", "")
    profile_picture = data.get("profile_picture", "")

    lic_am = 1 if data.get("lic_am") else 0
    lic_a1 = 1 if data.get("lic_a1") else 0
    lic_b = 1 if data.get("lic_b") else 0
    lic_l = 1 if data.get("lic_l") else 0
    lic_t = 1 if data.get("lic_t") else 0

    has_jf1 = 1 if data.get("has_jf1") else 0
    has_jf2 = 1 if data.get("has_jf2") else 0
    has_jf3 = 1 if data.get("has_jf3") else 0
    has_wissentest = 1 if data.get("has_wissentest") else 0
    has_leistungsspange = 1 if data.get("has_leistungsspange") else 0
    has_jugendabzeichen = 1 if data.get("has_jugendabzeichen") else 0
    has_mta_basis = 1 if data.get("has_mta_basis") else 0
    has_erste_hilfe = 1 if data.get("has_erste_hilfe") else 0
    has_funk = 1 if data.get("has_funk") else 0

    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        UPDATE youth_members 
        SET name=%s, parent_contact=%s, badges=%s, skills=%s, birth_date=%s, entry_date=%s, phone=%s, email=%s, address=%s, notes=%s, profile_picture=%s,
            lic_am=%s, lic_a1=%s, lic_b=%s, lic_l=%s, lic_t=%s,
            has_jf1=%s, has_jf2=%s, has_jf3=%s, has_wissentest=%s, has_leistungsspange=%s, has_jugendabzeichen=%s, has_mta_basis=%s, has_erste_hilfe=%s, has_funk=%s
        WHERE id=%s
    """, (name, parent, badges, skills, birth_date, entry_date, phone, email, address, notes, profile_picture,
          lic_am, lic_a1, lic_b, lic_l, lic_t,
          has_jf1, has_jf2, has_jf3, has_wissentest, has_leistungsspange, has_jugendabzeichen, has_mta_basis, has_erste_hilfe, has_funk, m_id))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "JUGEND_BEARBEITEN", f"Jugendmitglied ID {m_id} ('{name}') aktualisiert.")
    return {"status": "success"}

@router.delete("/api/jugend/members/{m_id}")
def delete_youth_member(m_id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "jugendwarte"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM youth_members WHERE id = %s", (m_id,))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "JUGEND_GELOESCHT", f"Jugendmitglied ID {m_id} gelöscht.")
    return {"status": "success"}

# --- JUGEND-DIENSTBERICHTE ---
@router.get("/api/jugend/sessions")
def get_youth_sessions(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM youth_sessions ORDER BY date DESC, id DESC")
    sessions = cur.fetchall()
    for s in sessions:
        if isinstance(s["date"], date):
            s["date"] = str(s["date"])
        cur.execute("""
            SELECT ya.member_id, ya.is_present, ym.name
            FROM youth_attendance ya
            JOIN youth_members ym ON ya.member_id = ym.id
            WHERE ya.session_id = %s
        """, (s["id"],))
        s["attendance"] = cur.fetchall()
    cur.close(); conn.close()
    return sessions

@router.post("/api/jugend/sessions")
def add_youth_session(data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "jugendwarte"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    sess_date = data.get("date")
    topic = data.get("topic")
    duration = float(data.get("duration", 2.0))
    instructors = data.get("instructors", "")
    description = data.get("description", "")
    attendance = data.get("attendance", {})
    if not sess_date or not topic:
        raise HTTPException(status_code=400, detail="Datum und Thema erforderlich")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO youth_sessions (date, topic, duration, instructors, description)
        VALUES (%s, %s, %s, %s, %s)
    """, (sess_date, topic, duration, instructors, description))
    session_id = cur.lastrowid
    cur.execute("SELECT id FROM youth_members")
    member_ids = [row[0] for row in cur.fetchall()]
    for m_id in member_ids:
        is_pres = attendance.get(str(m_id)) or attendance.get(m_id) or False
        cur.execute("""
            INSERT INTO youth_attendance (session_id, member_id, is_present)
            VALUES (%s, %s, %s)
        """, (session_id, m_id, 1 if is_pres else 0))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success", "session_id": session_id}

@router.put("/api/jugend/sessions/{s_id}")
def update_youth_session(s_id: int, data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "jugendwarte"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    sess_date = data.get("date")
    topic = data.get("topic")
    duration = float(data.get("duration", 2.0))
    instructors = data.get("instructors", "")
    description = data.get("description", "")
    attendance = data.get("attendance", {})
    if not sess_date or not topic:
        raise HTTPException(status_code=400, detail="Datum und Thema erforderlich")
    
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("""
        UPDATE youth_sessions
        SET date = %s, topic = %s, duration = %s, instructors = %s, description = %s
        WHERE id = %s
    """, (sess_date, topic, duration, instructors, description, s_id))
    
    cur.execute("DELETE FROM youth_attendance WHERE session_id = %s", (s_id,))
    cur.execute("SELECT id FROM youth_members")
    member_ids = [row[0] for row in cur.fetchall()]
    for m_id in member_ids:
        is_pres = attendance.get(str(m_id)) or attendance.get(m_id) or False
        cur.execute("""
            INSERT INTO youth_attendance (session_id, member_id, is_present)
            VALUES (%s, %s, %s)
        """, (s_id, m_id, 1 if is_pres else 0))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}


@router.delete("/api/jugend/sessions/{s_id}")
def delete_youth_session(s_id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung", "jugendwarte"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM youth_sessions WHERE id = %s", (s_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

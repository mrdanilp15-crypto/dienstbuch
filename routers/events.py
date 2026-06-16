from fastapi import APIRouter, Request, HTTPException
from typing import Optional
from datetime import datetime
from database import get_db_connection

router = APIRouter(tags=["Dienstplan, Events & Editor"])

def parse_val(v):
    if v == "" or v == "null" or v is None:
        return None
    if isinstance(v, str):
        return v.strip()
    return v

# --- KALENDER: TERMINE ANZEIGEN ---
@router.get("/api/events")
def list_events(r: Request):
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        cur.execute("SELECT id, DATE_FORMAT(date, '%d.%m.%Y') as date_formatted, DATE_FORMAT(date, '%Y-%m-%d') as date, title, responsible FROM events ORDER BY date ASC")
        res = cur.fetchall()
        cur.close()
        c.close()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- KALENDER: TERMIN SPEICHERN ---
@router.post("/api/events")
async def save_event(r: Request):
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        ed = parse_val(d.get('date'))
        e_id = d.get('id')
        if e_id:
            cur.execute("UPDATE events SET date=%s, title=%s, responsible=%s WHERE id=%s", (ed, d.get('title'), d.get('responsible'), e_id))
        else:
            cur.execute("INSERT INTO events (date, title, responsible) VALUES (%s,%s,%s)", (ed, d.get('title'), d.get('responsible')))
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- KALENDER: TERMIN LÖSCHEN ---
@router.delete("/api/events/{e_id}")
def del_event(e_id: int, r: Request):
    try:
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("DELETE FROM events WHERE id = %s", (e_id,))
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- EDITOR: BERICHTE/SESSIONS AUFLISTEN ---
@router.get("/groups/{group_id}/sessions")
def list_sessions(group_id: int, r: Request):
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        cur.execute("SELECT id, description, duration, DATE_FORMAT(date, '%d.%m.%Y') as date, category, instructors FROM sessions WHERE group_id = %s ORDER BY date DESC", (group_id,))
        res = cur.fetchall()
        cur.close()
        c.close()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- EDITOR: ANWESENHEITSLISTE LADEN (Der 404-Fix) ---
@router.get("/groups/{group_id}/attendance")
def get_attendance(group_id: int, r: Request, session_id: Optional[int] = None):
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        sd = {"session_id": session_id, "description": "", "duration": 2.0, "category": "Übung", "date": datetime.now().strftime("%Y-%m-%d"), "instructors": ""}
        
        if session_id and int(session_id) != 0:
            cur.execute("SELECT id as session_id, description, duration, DATE_FORMAT(date, '%Y-%m-%d') as date, category, instructors FROM sessions WHERE id = %s", (session_id,))
            row = cur.fetchone()
            if row:
                sd = row
                
        cur.execute("SELECT p.id as personnel_id, p.name, p.rank, CASE WHEN a.is_present IS NOT NULL THEN a.is_present ELSE 0 END as is_present, COALESCE(a.vehicle, '') as vehicle FROM personnel p LEFT JOIN attendance a ON p.id = a.person_id AND a.session_id = %s ORDER BY p.name ASC", (session_id,))
        persons = cur.fetchall()
        for p in persons:
            p['is_present'] = bool(p['is_present'])
            
        cur.execute("SELECT DISTINCT description FROM sessions ORDER BY id DESC LIMIT 5")
        pt = [row_t['description'] for row_t in cur.fetchall()]
        cur.execute("SELECT DISTINCT instructors FROM sessions ORDER BY id DESC LIMIT 5")
        pl = [row_l['instructors'] for row_l in cur.fetchall()]
        cur.close()
        c.close()
        return {**sd, "persons": persons, "presets": {"topics": pt, "leaders": pl}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- EDITOR: BERICHT SPEICHERN ---
@router.post("/attendance")
async def save_attendance(r: Request):
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        s_id = d.get('session_id')
        
        if s_id and int(s_id) != 0:
            cur.execute("UPDATE sessions SET date=%s, duration=%s, description=%s, instructors=%s, category=%s WHERE id=%s", (d.get('date'), d.get('duration'), d.get('description'), d.get('instructors'), d.get('category'), s_id))
            cur.execute("DELETE FROM attendance WHERE session_id = %s", (s_id,))
        else:
            cur.execute("INSERT INTO sessions (group_id, date, category, duration, description, instructors) VALUES (%s,%s,%s,%s,%s,%s)", (d.get('group_id', 1), d.get('date'), d.get('category'), d.get('duration'), d.get('description'), d.get('instructors')))
            s_id = cur.lastrowid
        
        for e in d.get('entries', []):
            cur.execute("INSERT INTO attendance (session_id, person_id, is_present, vehicle) VALUES (%s,%s,%s,%s)", (s_id, e.get('person_id'), 1 if e.get('is_present') else 0, e.get('vehicle') or ""))
            
        c.commit()
        cur.close()
        c.close()
        return {"status": "success", "session_id": s_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
import os
import mysql.connector
import urllib.request
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timedelta
import reports

ADMIN_PIN = os.getenv("ADMIN_PIN")
USER_PIN = os.getenv("USER_PIN")
DB_PASSWORD = os.getenv("DB_PASSWORD")
UPDATE_BASE_URL = os.getenv("UPDATE_BASE_URL", "https://raw.githubusercontent.com/mrdanilp15-crypto/dienstbuch/main/")

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

# FAVICON ROUTE: Serviert das Icon direkt
@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse("static/favicon.svg")

def get_db_connection():
    return mysql.connector.connect(host="db", user="app_user", password=DB_PASSWORD, database="attendance_system")

def safe_decode(value):
    if isinstance(value, bytes): return value.decode('utf-8')
    return value

class PinCheck(BaseModel): pin: str
class EntryDto(BaseModel): person_id: int; is_present: bool; note: Optional[str] = ""; vehicle: Optional[str] = ""; signature: Optional[str] = None
class AttendanceUpload(BaseModel): 
    session_id: Optional[int] = None; date: str; group_id: int; category: str = "Übung"; duration: float = 0.0; 
    description: str; address: Optional[str] = ""; instructors: Optional[str] = ""; 
    leader_signature: Optional[str] = None; entries: List[EntryDto]

@app.get("/", response_class=FileResponse)
def get_login(): return "static/login.html"
@app.get("/dashboard", response_class=FileResponse)
def get_dash(): return "static/dashboard.html"
@app.get("/editor", response_class=FileResponse)
def get_edit(): return "static/editor.html"

@app.post("/api/login")
async def login(data: dict, request: Request):
    p = data.get("pin", "").strip()
    role = "admin" if (ADMIN_PIN and p == ADMIN_PIN) else "user" if (USER_PIN and p == USER_PIN) else None
    if role: return {"status": "success", "role": role, "redirect": "/dashboard"}
    raise HTTPException(401, detail="Falsch")

@app.get("/api/system/update")
async def run_update(pin: str):
    if pin != ADMIN_PIN: raise HTTPException(401, detail="Nicht autorisiert")
    # Liste inklusive favicon.svg
    files = [
        ("main.py", "main.py"), ("reports.py", "reports.py"), 
        ("static/dashboard.html", "static/dashboard.html"), 
        ("static/editor.html", "static/editor.html"),
        ("static/login.html", "static/login.html"),
        ("static/favicon.svg", "static/favicon.svg")
    ]
    try:
        for remote_name, local_path in files:
            url = UPDATE_BASE_URL + remote_name
            with urllib.request.urlopen(url) as response:
                content = response.read()
                with open(local_path, "wb") as f: f.write(content)
        os._exit(0)
    except Exception as e: raise HTTPException(500, detail=str(e))

# ... (Rest der API-Methoden wie gehabt)
@app.get("/groups")
def groups():
    c=get_db_connection(); cur=c.cursor(dictionary=True); cur.execute("SELECT * FROM groups_table ORDER BY name"); r=cur.fetchall(); c.close(); return r

@app.get("/groups/{id}/attendance")
def attendance(id:int, session_id:Optional[int]=None):
    c=get_db_connection(); cur=c.cursor(dictionary=True); row=None
    if session_id: cur.execute("SELECT * FROM sessions WHERE id=%s",(session_id,)); row=cur.fetchone()
    sid = row['id'] if row else 0
    l_sig = safe_decode(row['leader_signature']) if row else None
    sql = "SELECT p.id, p.name, IFNULL(a.is_present,0) as is_present, IFNULL(a.note,'') as note, IFNULL(a.vehicle,'') as vehicle, a.signature FROM persons p LEFT JOIN attendance a ON p.id=a.person_id AND a.session_id=%s WHERE p.group_id=%s ORDER BY p.name"
    cur.execute(sql, (sid, id)); persons = cur.fetchall()
    for p in persons: p['is_present']=bool(p['is_present']); p['signature']=safe_decode(p['signature'])
    c.close()
    return {"session_id":sid, "date":str(row['date']) if row else str(datetime.now().date()), "category":row['category'] if row else "Übung", "duration":float(row['duration']) if row else 2.0, "description":row['description'] if row else "", "instructors":row['instructors'] if row else "", "leader_signature": l_sig, "persons":persons}

@app.post("/attendance")
def save_attendance(d: AttendanceUpload):
    c=get_db_connection(); cur=c.cursor()
    if d.session_id:
        cur.execute("UPDATE sessions SET date=%s, category=%s, duration=%s, description=%s, instructors=%s, leader_signature=%s WHERE id=%s", (d.date, d.category, d.duration, d.description, d.instructors, d.leader_signature, d.session_id)); sid = d.session_id
        cur.execute("DELETE FROM attendance WHERE session_id=%s", (sid,))
    else:
        cur.execute("INSERT INTO sessions (date, group_id, category, duration, description, instructors, leader_signature) VALUES (%s,%s,%s,%s,%s,%s,%s)", (d.date, d.group_id, d.category, d.duration, d.description, d.instructors, d.leader_signature)); sid = cur.lastrowid
    for e in d.entries: cur.execute("INSERT INTO attendance (session_id, person_id, is_present, note, vehicle, signature) VALUES (%s,%s,%s,%s,%s,%s)", (sid, e.person_id, e.is_present, e.note, e.vehicle, e.signature))
    c.commit(); c.close(); return {"session_id":sid}

@app.get("/groups/{id}/sessions")
def sessions(id:int):
    c=get_db_connection(); cur=c.cursor(dictionary=True); cur.execute("SELECT id, date, category, description, duration, leader_signature FROM sessions WHERE group_id=%s ORDER BY date DESC, id DESC",(id,)); r=cur.fetchall(); c.close()
    for x in r: x['date']=str(x['date']); x['is_signed']=bool(x['leader_signature'] and len(str(x['leader_signature'])) > 100); del x['leader_signature']
    return r

@app.get("/groups/{id}/stats")
def stats(id:int, year:int):
    c=get_db_connection(); cur=c.cursor(dictionary=True)
    cur.execute("SELECT COUNT(*) as total FROM sessions WHERE group_id=%s AND YEAR(date)=%s", (id,year)); max_s = cur.fetchone()['total'] or 0
    sql = "SELECT p.name, SUM(CASE WHEN a.is_present=1 AND s.id IS NOT NULL THEN 1 ELSE 0 END) as present_count, SUM(CASE WHEN a.is_present=1 AND s.id IS NOT NULL THEN s.duration ELSE 0 END) as total_hours, %s as total_sessions FROM persons p LEFT JOIN attendance a ON p.id=a.person_id LEFT JOIN sessions s ON a.session_id=s.id AND YEAR(s.date)=%s AND s.group_id=%s WHERE p.group_id=%s GROUP BY p.id, p.name ORDER BY total_hours DESC"
    cur.execute(sql, (max_s, year, id, id)); p = cur.fetchall(); c.close(); return {"persons":p}

@app.get("/sessions/{session_id}/report", response_class=HTMLResponse)
def single_report(session_id: int):
    c=get_db_connection(); cur=c.cursor(dictionary=True)
    cur.execute("SELECT s.*, g.name as gname FROM sessions s JOIN groups_table g ON s.group_id = g.id WHERE s.id=%s", (session_id,))
    s = cur.fetchone()
    if s: s['leader_signature'] = safe_decode(s['leader_signature'])
    cur.execute("SELECT p.name, a.is_present, a.note, a.vehicle, a.signature FROM attendance a JOIN persons p ON a.person_id = p.id WHERE a.session_id=%s ORDER BY p.name", (session_id,))
    persons = cur.fetchall(); c.close()
    for p in persons: p['signature'] = safe_decode(p['signature'])
    return f"<html><head><style>{reports.get_report_styles()}</style></head><body>{reports.generate_single_report(s, persons)}</body></html>"

@app.post("/sessions/{id}/leader_signature")
def update_leader_sig(id: int, data: LeaderSigUpdate):
    c = get_db_connection(); cur = c.cursor(); cur.execute("UPDATE sessions SET leader_signature=%s WHERE id=%s", (data.signature, id)); c.commit(); c.close(); return {"status": "updated"}

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
UPDATE_BASE_URL = "https://raw.githubusercontent.com/mrdanilp15-crypto/dienstbuch/main/"

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

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
class LeaderSigUpdate(BaseModel): signature: Optional[str] = None
class PersonData(BaseModel): name: str
class GroupName(BaseModel): name: str

@app.get("/", response_class=FileResponse)
def get_login(): return "static/login.html"
@app.get("/dashboard", response_class=FileResponse)
def get_dash(): return "static/dashboard.html"
@app.get("/editor", response_class=FileResponse)
def get_edit(): return "static/editor.html"

@app.post("/api/verify_admin")
async def verify_admin(data: PinCheck): return {"success": (data.pin == ADMIN_PIN)}

@app.post("/api/login")
async def login(data: dict, request: Request):
    p = data.get("pin", "").strip()
    role = "admin" if (ADMIN_PIN and p == ADMIN_PIN) else "user" if (USER_PIN and p == USER_PIN) else None
    if role: return {"status": "success", "role": role, "redirect": "/dashboard"}
    raise HTTPException(401, detail="Falsch")

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
    return {"session_id":sid, "date":str(row['date']) if row else str(datetime.now().date()), "category":row['category'] if row else "Übung", "duration":float(row['duration']) if row else 2.0, "description":row['description'] if row else "", "address":row.get('address','') if row else "", "instructors":row['instructors'] if row else "", "leader_signature": l_sig, "persons":persons}

@app.post("/attendance")
def save_attendance(d: AttendanceUpload):
    c=get_db_connection(); cur=c.cursor()
    if d.session_id:
        cur.execute("UPDATE sessions SET date=%s, category=%s, duration=%s, description=%s, address=%s, instructors=%s, leader_signature=%s WHERE id=%s", (d.date, d.category, d.duration, d.description, d.address, d.instructors, d.leader_signature, d.session_id)); sid = d.session_id
        cur.execute("DELETE FROM attendance WHERE session_id=%s", (sid,))
    else:
        cur.execute("INSERT INTO sessions (date, group_id, category, duration, description, address, instructors, leader_signature) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", (d.date, d.group_id, d.category, d.duration, d.description, d.address, d.instructors, d.leader_signature)); sid = cur.lastrowid
    for e in d.entries: cur.execute("INSERT INTO attendance (session_id, person_id, is_present, note, vehicle, signature) VALUES (%s,%s,%s,%s,%s,%s)", (sid, e.person_id, e.is_present, e.note, e.vehicle, e.signature))
    c.commit(); c.close(); return {"session_id":sid}

@app.post("/sessions/{id}/leader_signature")
def update_leader_sig(id: int, data: LeaderSigUpdate):
    c = get_db_connection(); cur = c.cursor(); cur.execute("UPDATE sessions SET leader_signature=%s WHERE id=%s", (data.signature, id)); c.commit(); c.close(); return {"status": "updated"}

@app.delete("/sessions/{id}")
def delete_session(id: int):
    c = get_db_connection(); cur = c.cursor(); cur.execute("DELETE FROM attendance WHERE session_id=%s", (id,)); cur.execute("DELETE FROM sessions WHERE id=%s", (id,)); c.commit(); c.close(); return {"status": "deleted"}

@app.get("/groups/{id}/sessions")
def sessions(id:int):
    c=get_db_connection(); cur=c.cursor(dictionary=True); cur.execute("SELECT id, date, category, description, address, duration, leader_signature FROM sessions WHERE group_id=%s ORDER BY date DESC, id DESC",(id,)); r=cur.fetchall(); c.close()
    for x in r: x['date']=str(x['date']); x['is_signed']=bool(x['leader_signature'] and len(str(x['leader_signature'])) > 100); del x['leader_signature']
    return r

@app.get("/groups/{id}/topics")
def get_topics(id: int):
    c = get_db_connection(); cur = c.cursor(); cur.execute("SELECT DISTINCT description FROM sessions WHERE group_id=%s AND description != '' ORDER BY description", (id,)); r = cur.fetchall(); c.close(); return [x[0] for x in r]

@app.get("/groups/{id}/instructors")
def get_instructors(id: int):
    c = get_db_connection(); cur = c.cursor(); cur.execute("SELECT DISTINCT instructors FROM sessions WHERE group_id=%s AND instructors != '' ORDER BY instructors", (id,)); r = cur.fetchall(); c.close(); return [x[0] for x in r]

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

@app.get("/groups/{group_id}/print_view", response_class=HTMLResponse)
def print_view(group_id: int, year: int):
    c=get_db_connection(); cur=c.cursor(dictionary=True)
    cur.execute("SELECT name FROM groups_table WHERE id=%s",(group_id,)); gname=cur.fetchone()['name']
    cur.execute("SELECT COUNT(*) as total FROM sessions WHERE group_id=%s AND YEAR(date)=%s", (group_id, year)); max_s = cur.fetchone()['total'] or 0
    cur.execute("SELECT s.*, g.name as gname FROM sessions s JOIN groups_table g ON s.group_id = g.id WHERE s.group_id=%s AND YEAR(s.date)=%s ORDER BY s.date ASC, s.id ASC", (group_id, year))
    sessions_list = cur.fetchall()
    html_body = ""; p_stats = {}; cat_sums = {"Übung":0.0, "Einsatz":0.0, "Sonstiges":0.0}
    for s in sessions_list:
        if s['leader_signature']: s['leader_signature'] = safe_decode(s['leader_signature'])
        cur.execute("SELECT p.name, a.is_present, a.note, a.vehicle, a.signature FROM attendance a JOIN persons p ON a.person_id = p.id WHERE a.session_id=%s ORDER BY p.name", (s['id'],))
        persons = cur.fetchall()
        for p in persons: p['signature'] = safe_decode(p['signature'])
        html_body += reports.generate_single_report(s, persons)
        cat = s['category'] if s['category'] in cat_sums else "Sonstiges"
        cat_sums[cat] += float(s['duration'])
        for p in persons:
            if p['name'] not in p_stats: p_stats[p['name']] = {"Übung":0.0,"Einsatz":0.0,"Sonstiges":0.0,"total_h":0.0,"p":0}
            if p['is_present']: p_stats[p['name']]["p"] += 1; p_stats[p['name']][cat] += float(s['duration']); p_stats[p['name']]["total_h"] += float(s['duration'])
    for n in p_stats: p_stats[n]['q'] = round((p_stats[n]['p']/max_s)*100) if max_s>0 else 0
    html_body += reports.generate_year_report(gname, year, p_stats, cat_sums)
    c.close(); return f"<html><head><style>{reports.get_report_styles()}</style></head><body>{html_body}</body></html>"

@app.post("/groups/{id}/persons")
def add_person(id: int, p: PersonData):
    c = get_db_connection(); cur = c.cursor(); cur.execute("INSERT INTO persons (name, group_id) VALUES (%s, %s)", (p.name, id)); c.commit(); c.close(); return {"status": "created"}

@app.put("/persons/{id}")
def update_person(id: int, p: PersonData):
    c = get_db_connection(); cur = c.cursor(); cur.execute("UPDATE persons SET name=%s WHERE id=%s", (p.name, id)); c.commit(); c.close(); return {"status": "updated"}

@app.get("/api/system/update")
async def run_update(pin: str):
    if pin != ADMIN_PIN: raise HTTPException(401, detail="Nicht autorisiert")
    files = [("main.py", "main.py"), ("reports.py", "reports.py"), ("static/dashboard.html", "static/dashboard.html"), ("static/editor.html", "static/editor.html")]
    try:
        for remote_name, local_path in files:
            url = UPDATE_BASE_URL + remote_name
            with urllib.request.urlopen(url) as response:
                content = response.read().decode('utf-8')
                with open(local_path, "w") as f: f.write(content)
        os._exit(0)
    except Exception as e: raise HTTPException(500, detail=str(e))

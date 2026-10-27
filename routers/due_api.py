"""Eigene Fristarten (z.B. "Drohnenpilot Landkreis") und die einzelnen Fristen dazu.

Fristarten legen Admin/Leitung an. Die Fristen selbst tragen Admin/Leitung und die jeweils Zuständigen
(Rolle und/oder Person der Fristart) ein und haken sie ab. Wer sie sieht und wer erinnert wird, regelt
core/reminders.py (get_due_items / visible_items)."""
import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from core.utils import check_auth, log_audit_action
from database import get_db_connection
from routers.building_api import _add_months, _parse

router = APIRouter(prefix="/api/due", tags=["Eigene Fristen"])

_MANAGERS = ("admin", "leitung")
SUBJECTS = ("person", "vehicle", "equipment", "general")
RESPONSIBLE_ROLES = ("", "geratewart", "atemschutzwart", "gruppenfuehrer", "jugendwarte")
_SUBJECT_COLUMN = {"person": "personnel_id", "vehicle": "vehicle_id", "equipment": "equipment_id"}


class TypeIn(BaseModel):
    name: str
    subject: str = "person"
    interval_months: Optional[int] = None
    lead_days: Optional[int] = None
    responsible_role: Optional[str] = ""
    responsible_pid: Optional[int] = None
    logbook_id: Optional[int] = None  # Frist ergibt sich aus den Einträgen dieses Logbuchs


class EntryIn(BaseModel):
    type_id: int
    subject_id: Optional[int] = None
    due_date: Optional[str] = None
    note: Optional[str] = ""


class DoneIn(BaseModel):
    date: Optional[str] = None


def _me(request):
    user = check_auth(request)
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("SELECT personnel_id FROM users WHERE username = %s", (user["username"],))
    row = cur.fetchone(); cur.close(); conn.close()
    return user, (row[0] if row else None)


def _manages(user, pid, t) -> bool:
    return (user["role"] in _MANAGERS or (bool(t["responsible_role"]) and t["responsible_role"] == user["role"])
            or (pid is not None and t["responsible_pid"] == pid))


def _types(cur):
    cur.execute("""
        SELECT t.*, p.name AS responsible_name FROM due_types t LEFT JOIN personnel p ON p.id = t.responsible_pid
        ORDER BY t.name ASC
    """)
    return cur.fetchall()


def _validate_type(data: TypeIn, cur):
    name = data.name.strip()
    if not name or len(name) > 120:
        raise HTTPException(status_code=400, detail="Bitte einen Namen angeben (höchstens 120 Zeichen).")
    if data.subject not in SUBJECTS:
        raise HTTPException(status_code=400, detail="Unbekannter Bezug.")
    if data.interval_months is not None and not 1 <= data.interval_months <= 240:
        raise HTTPException(status_code=400, detail="Wiederholung: 1 bis 240 Monate (oder leer lassen).")
    if data.lead_days is not None and not 1 <= data.lead_days <= 365:
        raise HTTPException(status_code=400, detail="Vorlauf: 1 bis 365 Tage (oder leer lassen).")
    role = data.responsible_role or ""
    if role not in RESPONSIBLE_ROLES:
        raise HTTPException(status_code=400, detail="Diese Rolle kann nicht als zuständig gewählt werden.")
    if data.responsible_pid is not None:
        cur.execute("SELECT 1 FROM personnel WHERE id = %s", (data.responsible_pid,))
        if not cur.fetchone():
            raise HTTPException(status_code=400, detail="Die zuständige Person gibt es nicht.")
    if data.logbook_id is not None:
        cur.execute("SELECT 1 FROM logbooks WHERE id = %s", (data.logbook_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=400, detail="Das Logbuch gibt es nicht.")
        if data.subject != "person" or not data.interval_months:
            raise HTTPException(status_code=400, detail="Eine Frist aus einem Logbuch braucht den Bezug „Person“ und eine Wiederholung in Monaten.")
    return name, role


@router.get("/types")
def list_types(request: Request):
    """Fristarten, die man verwalten darf (Admin/Leitung: alle)."""
    user, pid = _me(request)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    rows = [dict(t, can_manage=True) for t in _types(cur) if _manages(user, pid, t)]
    cur.close(); conn.close()
    return {"types": rows, "can_create": user["role"] in _MANAGERS}


@router.post("/types")
def create_type(data: TypeIn, request: Request):
    user = check_auth(request, allowed_roles=_MANAGERS)
    conn = get_db_connection(); cur = conn.cursor()
    name, role = _validate_type(data, cur)
    cur.execute(
        "INSERT INTO due_types (name, subject, interval_months, lead_days, responsible_role, responsible_pid, logbook_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (name, data.subject, data.interval_months, data.lead_days, role, data.responsible_pid, data.logbook_id),
    )
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "FRISTART_ANGELEGT", name)
    return {"status": "success"}


@router.put("/types/{type_id}")
def update_type(type_id: int, data: TypeIn, request: Request):
    user = check_auth(request, allowed_roles=_MANAGERS)
    conn = get_db_connection(); cur = conn.cursor()
    name, role = _validate_type(data, cur)
    cur.execute(
        "UPDATE due_types SET name=%s, subject=%s, interval_months=%s, lead_days=%s, responsible_role=%s, responsible_pid=%s, logbook_id=%s WHERE id=%s",
        (name, data.subject, data.interval_months, data.lead_days, role, data.responsible_pid, data.logbook_id, type_id),
    )
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "FRISTART_GEAENDERT", name)
    return {"status": "success"}


@router.delete("/types/{type_id}")
def delete_type(type_id: int, request: Request):
    user = check_auth(request, allowed_roles=_MANAGERS)
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM due_entries WHERE type_id = %s", (type_id,))
    cur.execute("DELETE FROM due_types WHERE id = %s", (type_id,))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "FRISTART_GELOESCHT", f"Fristart {type_id} samt Fristen")
    return {"status": "success"}


@router.get("/entries")
def list_entries(request: Request):
    """Die Fristen der Fristarten, die man verwalten darf - mit Bezeichnung des Bezugs und letztem Erledigt-Datum."""
    user, pid = _me(request)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    allowed = {t["id"] for t in _types(cur) if _manages(user, pid, t)}
    cur.execute("""
        SELECT e.id, e.type_id, e.personnel_id, e.vehicle_id, e.equipment_id, e.due_date, e.last_done, e.note,
               t.name AS type_name, t.subject, t.interval_months,
               COALESCE(p.name, v.name, q.name) AS subject_name
        FROM due_entries e JOIN due_types t ON t.id = e.type_id
        LEFT JOIN personnel p ON p.id = e.personnel_id
        LEFT JOIN vehicles v ON v.id = e.vehicle_id
        LEFT JOIN equipment q ON q.id = e.equipment_id
        ORDER BY e.due_date IS NULL, e.due_date ASC, t.name ASC
    """)
    rows = [r for r in cur.fetchall() if r["type_id"] in allowed]
    cur.close(); conn.close()
    for r in rows:
        for k in ("due_date", "last_done"):
            r[k] = r[k].isoformat() if r[k] else None
    return rows


@router.get("/subjects")
def subjects(request: Request, kind: str):
    """Auswahl für das Eintragen: Personen, Fahrzeuge oder Geräte - nur für Verwalter von Fristarten."""
    user, pid = _me(request)
    if kind not in _SUBJECT_COLUMN:
        raise HTTPException(status_code=400, detail="Unbekannter Bezug.")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    if user["role"] not in _MANAGERS and not any(_manages(user, pid, t) and t["subject"] == kind for t in _types(cur)):
        cur.close(); conn.close()
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    if kind == "person":
        cur.execute("SELECT id, name FROM personnel WHERE exit_date IS NULL OR exit_date > CURDATE() ORDER BY name")
    elif kind == "vehicle":
        cur.execute("SELECT id, name FROM vehicles ORDER BY name")
    else:
        cur.execute("SELECT id, CONCAT(name, IF(barcode IS NULL OR barcode = '', '', CONCAT(' (', barcode, ')'))) AS name FROM equipment ORDER BY name")
    rows = cur.fetchall(); cur.close(); conn.close()
    return rows


def _load_type(cur, type_id):
    cur.execute("SELECT * FROM due_types WHERE id = %s", (type_id,))
    t = cur.fetchone()
    if not t:
        raise HTTPException(status_code=404, detail="Fristart nicht gefunden.")
    return t


def _entry_values(data: EntryIn, t, cur):
    col = _SUBJECT_COLUMN.get(t["subject"])
    if col:
        if not data.subject_id:
            raise HTTPException(status_code=400, detail="Bitte auswählen, für wen oder was die Frist gilt.")
        table = {"personnel_id": "personnel", "vehicle_id": "vehicles", "equipment_id": "equipment"}[col]
        cur.execute(f"SELECT 1 FROM {table} WHERE id = %s", (data.subject_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=400, detail="Die Auswahl gibt es nicht mehr.")
    due = _parse(data.due_date)
    if not due:
        raise HTTPException(status_code=400, detail="Bitte ein Datum angeben, an dem die Frist abläuft.")
    return col, due, (data.note or "").strip()[:300]


@router.post("/entries")
def create_entry(data: EntryIn, request: Request):
    user, pid = _me(request)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    t = _load_type(cur, data.type_id)
    if not _manages(user, pid, t):
        cur.close(); conn.close()
        raise HTTPException(status_code=403, detail="Keine Berechtigung für diese Fristart")
    if t["logbook_id"]:
        cur.close(); conn.close()
        raise HTTPException(status_code=400, detail="Diese Frist ergibt sich aus dem Logbuch - dort einen Eintrag machen.")
    col, due, note = _entry_values(data, t, cur)
    cols, vals = ["type_id", "due_date", "note"], [t["id"], due, note]
    if col:
        cols.append(col); vals.append(data.subject_id)
    cur.execute(f"INSERT INTO due_entries ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(vals))})", tuple(vals))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "FRIST_ANGELEGT", f"{t['name']} bis {due.isoformat()}")
    return {"status": "success"}


def _load_entry_and_check(cur, entry_id, user, pid):
    cur.execute("SELECT * FROM due_entries WHERE id = %s", (entry_id,))
    e = cur.fetchone()
    if not e:
        raise HTTPException(status_code=404, detail="Frist nicht gefunden.")
    t = _load_type(cur, e["type_id"])
    if not _manages(user, pid, t):
        raise HTTPException(status_code=403, detail="Keine Berechtigung für diese Fristart")
    return e, t


@router.put("/entries/{entry_id}")
def update_entry(entry_id: int, data: EntryIn, request: Request):
    user, pid = _me(request)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    e, t = _load_entry_and_check(cur, entry_id, user, pid)
    col, due, note = _entry_values(data, t, cur)
    if col:
        cur.execute(f"UPDATE due_entries SET due_date=%s, note=%s, {col}=%s WHERE id=%s", (due, note, data.subject_id, entry_id))
    else:
        cur.execute("UPDATE due_entries SET due_date=%s, note=%s WHERE id=%s", (due, note, entry_id))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "FRIST_GEAENDERT", f"{t['name']} bis {due.isoformat()}")
    return {"status": "success"}


@router.post("/entries/{entry_id}/done")
def done_entry(entry_id: int, data: DoneIn, request: Request):
    """Abhaken: mit Wiederholungsintervall rückt das Datum weiter (vom späteren von Fälligkeit und Erledigung aus),
    ohne Intervall ist die Frist erledigt und verschwindet aus den Erinnerungen."""
    user, pid = _me(request)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    e, t = _load_entry_and_check(cur, entry_id, user, pid)
    done = _parse(data.date) or datetime.date.today()
    nxt = _add_months(max(e["due_date"], done) if e["due_date"] else done, t["interval_months"]) if t["interval_months"] else None
    cur.execute("UPDATE due_entries SET last_done=%s, due_date=%s WHERE id=%s", (done, nxt, entry_id))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "FRIST_ERLEDIGT", f"{t['name']} am {done.isoformat()}" + (f", nächste {nxt.isoformat()}" if nxt else ""))
    return {"status": "success", "next_due": nxt.isoformat() if nxt else None}


@router.delete("/entries/{entry_id}")
def delete_entry(entry_id: int, request: Request):
    user, pid = _me(request)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    e, t = _load_entry_and_check(cur, entry_id, user, pid)
    cur.execute("DELETE FROM due_entries WHERE id = %s", (entry_id,))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "FRIST_GELOESCHT", t["name"])
    return {"status": "success"}

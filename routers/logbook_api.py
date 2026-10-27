"""Frei definierbare Logbücher (z.B. Flugbuch).

Admin/Leitung legen Logbücher samt Feldern an. Eintragen dürfen die ausgewählten Personen (Mitglieder) - oder alle, wenn
das Logbuch "für alle offen" ist - und sehen dann ihre eigenen Einträge. Die Zuständigen (Rolle und/oder Person) sowie
Admin/Leitung sehen und bearbeiten alles und bekommen die Auswertung und den Export."""
import datetime
import json
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel

from core import logbook as lb
from core.utils import check_auth, log_audit_action
from database import get_db_connection

router = APIRouter(prefix="/api/logbooks", tags=["Logbücher"])

_MANAGERS = ("admin", "leitung")
SUBJECTS = ("none", "equipment", "vehicle")
RESPONSIBLE_ROLES = ("", "geratewart", "atemschutzwart", "gruppenfuehrer", "jugendwarte")
_SUBJECT_LABEL = {"equipment": "Gerät", "vehicle": "Fahrzeug", "none": ""}


class FieldIn(BaseModel):
    uid: str
    id: Optional[int] = None
    label: str
    ftype: str = "text"
    unit: Optional[str] = ""
    options: Optional[List[str]] = None
    required: bool = False
    evaluate: str = "none"
    config: Optional[dict] = None


class LogbookIn(BaseModel):
    name: str
    subject: str = "none"
    responsible_role: Optional[str] = ""
    responsible_pid: Optional[int] = None
    open_to_all: bool = False
    member_ids: List[int] = []
    fields: List[FieldIn]


class EntryIn(BaseModel):
    entry_date: str
    personnel_id: Optional[int] = None
    subject_id: Optional[int] = None
    values: dict = {}


def _me(request):
    user = check_auth(request)
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("SELECT personnel_id FROM users WHERE username = %s", (user["username"],))
    row = cur.fetchone(); cur.close(); conn.close()
    return user, (row[0] if row else None)


def _fields(cur, lb_id):
    cur.execute("SELECT * FROM logbook_fields WHERE logbook_id = %s ORDER BY sort_order, id", (lb_id,))
    return [lb.public_field(r) for r in cur.fetchall()]


def _members(cur, lb_id):
    cur.execute("SELECT personnel_id FROM logbook_members WHERE logbook_id = %s", (lb_id,))
    return [r["personnel_id"] for r in cur.fetchall()]


def _access(user, pid, book, members) -> dict:
    manage = user["role"] in _MANAGERS
    sees_all = manage or (bool(book["responsible_role"]) and book["responsible_role"] == user["role"]) \
        or (pid is not None and book["responsible_pid"] == pid)
    can_write = sees_all or bool(book["open_to_all"]) or (pid is not None and pid in members)
    return {"manage": manage, "sees_all": sees_all, "can_write": can_write}


def _load(cur, lb_id):
    cur.execute("SELECT * FROM logbooks WHERE id = %s", (lb_id,))
    book = cur.fetchone()
    if not book:
        raise HTTPException(status_code=404, detail="Logbuch nicht gefunden.")
    return book


def _load_access(request, lb_id, need="can_write"):
    """Lädt Logbuch, Felder und Zugriff - 403, wenn die benötigte Berechtigung fehlt."""
    user, pid = _me(request)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    book = _load(cur, lb_id)
    members = _members(cur, lb_id)
    acc = _access(user, pid, book, members)
    if not acc[need]:
        cur.close(); conn.close()
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    return user, pid, conn, cur, book, _fields(cur, lb_id), acc


def _subject_names(cur, subject):
    if subject == "equipment":
        cur.execute("SELECT id, name FROM equipment")
    elif subject == "vehicle":
        cur.execute("SELECT id, name FROM vehicles")
    else:
        return {}
    return {r["id"]: r["name"] for r in cur.fetchall()}


def _year_clause(year):
    return ("", ()) if not year else (" AND entry_date >= %s AND entry_date <= %s", (datetime.date(year, 1, 1), datetime.date(year, 12, 31)))


def _entries(cur, book, fields, user_pid, acc, year):
    """Einträge, die diese Person sehen darf (Zuständige/Leitung: alle, sonst die eigenen)."""
    clause, params = _year_clause(year)
    sql = ("SELECT e.id, e.entry_date, e.personnel_id, e.subject_id, e.data, p.name AS person FROM logbook_entries e "
           "LEFT JOIN personnel p ON p.id = e.personnel_id WHERE e.logbook_id = %s" + clause.replace("entry_date", "e.entry_date"))
    args = [book["id"], *params]
    if not acc["sees_all"]:
        sql += " AND e.personnel_id = %s"
        args.append(user_pid if user_pid is not None else -1)
    cur.execute(sql + " ORDER BY e.entry_date DESC, e.id DESC", tuple(args))
    rows = cur.fetchall()  # erst lesen, dann die nächste Abfrage (sonst "Unread result found")
    names = _subject_names(cur, book["subject"])
    out = []
    for r in rows:
        values = lb.parse_json(r["data"], {})
        out.append({"id": r["id"], "entry_date": r["entry_date"].isoformat(), "personnel_id": r["personnel_id"], "person": r["person"] or "",
                    "subject_id": r["subject_id"], "subject": names.get(r["subject_id"], "") if r["subject_id"] else "", "values": values,
                    "durations": {str(f["id"]): lb.duration_minutes(f, values) for f in fields if f["ftype"] == "duration"}})
    return out


def _check_year(year):
    if year and not 2000 <= year <= 2100:
        raise HTTPException(status_code=400, detail="Ungültiges Jahr.")


def _validate_book(data: LogbookIn, cur):
    name = data.name.strip()
    if not name or len(name) > 120:
        raise HTTPException(status_code=400, detail="Bitte einen Namen angeben (höchstens 120 Zeichen).")
    if data.subject not in SUBJECTS:
        raise HTTPException(status_code=400, detail="Unbekannter Bezug.")
    if (data.responsible_role or "") not in RESPONSIBLE_ROLES:
        raise HTTPException(status_code=400, detail="Diese Rolle kann nicht als zuständig gewählt werden.")
    for pid in [data.responsible_pid, *data.member_ids]:
        if pid is not None:
            cur.execute("SELECT 1 FROM personnel WHERE id = %s", (pid,))
            if not cur.fetchone():
                raise HTTPException(status_code=400, detail="Eine ausgewählte Person gibt es nicht.")
    defs = [f.model_dump() for f in data.fields]
    try:
        lb.check_field_defs(defs)
    except lb.FieldError as err:
        raise HTTPException(status_code=400, detail=str(err))
    by_uid = {f["uid"]: f for f in defs}
    for f in defs:
        if f["ftype"] == "duration":
            for k in ("start", "end"):
                ref = by_uid.get((f.get("config") or {}).get(k))
                if not ref or ref["ftype"] != "time":
                    raise HTTPException(status_code=400, detail=f"Bei „{f['label'].strip()}“ (Dauer) müssen Start und Ende Uhrzeit-Felder sein.")
    if len(by_uid) != len(defs):
        raise HTTPException(status_code=400, detail="Felder sind doppelt vorhanden.")
    return name, defs


def _save_fields(conn, cur, lb_id, defs):
    """Felder anlegen/ändern, nicht mehr gesendete entfernen; Start/Ende der Dauer-Felder auf echte IDs umschreiben."""
    cur.execute("SELECT id FROM logbook_fields WHERE logbook_id = %s", (lb_id,))
    existing = {r["id"] for r in cur.fetchall()}
    uid_to_id = {}
    for order, f in enumerate(defs):
        fid = f.get("id") if f.get("id") in existing else None
        opts = json.dumps([o.strip() for o in (f.get("options") or []) if o.strip()], ensure_ascii=False) if f["ftype"] == "select" else None
        args = (f["label"].strip(), f["ftype"], (f.get("unit") or "").strip()[:20], opts, 1 if f.get("required") else 0, f.get("evaluate") or "none", order)
        if fid:
            cur.execute("UPDATE logbook_fields SET label=%s, ftype=%s, unit=%s, options=%s, required=%s, evaluate=%s, sort_order=%s WHERE id=%s", (*args, fid))
        else:
            cur.execute("INSERT INTO logbook_fields (label, ftype, unit, options, required, evaluate, sort_order, logbook_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (*args, lb_id))
            fid = cur.lastrowid
        uid_to_id[f["uid"]] = fid
    for f in defs:
        cfg = None
        if f["ftype"] == "duration":
            cfg = json.dumps({"start": uid_to_id[f["config"]["start"]], "end": uid_to_id[f["config"]["end"]]})
        cur.execute("UPDATE logbook_fields SET config = %s WHERE id = %s", (cfg, uid_to_id[f["uid"]]))
    keep = set(uid_to_id.values())
    for fid in existing - keep:
        cur.execute("DELETE FROM logbook_fields WHERE id = %s", (fid,))
    conn.commit()


def _save_members(conn, cur, lb_id, ids):
    cur.execute("DELETE FROM logbook_members WHERE logbook_id = %s", (lb_id,))
    for pid in dict.fromkeys(ids):
        cur.execute("INSERT INTO logbook_members (logbook_id, personnel_id) VALUES (%s, %s)", (lb_id, pid))
    conn.commit()


@router.get("")
def list_logbooks(request: Request):
    """Die Logbücher, in denen man etwas darf - mit Feldern und Berechtigung (Mitglieder nur für Admin/Leitung)."""
    user, pid = _me(request)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM logbooks ORDER BY name")
    out = []
    for book in cur.fetchall():
        members = _members(cur, book["id"])
        acc = _access(user, pid, book, members)
        if not acc["can_write"]:
            continue
        out.append({"id": book["id"], "name": book["name"], "subject": book["subject"], "subject_label": _SUBJECT_LABEL[book["subject"]],
                    "responsible_role": book["responsible_role"], "responsible_pid": book["responsible_pid"], "open_to_all": bool(book["open_to_all"]),
                    "member_ids": members if acc["manage"] else [], "fields": _fields(cur, book["id"]), **acc})
    cur.close(); conn.close()
    return out


@router.post("")
def create_logbook(data: LogbookIn, request: Request):
    user = check_auth(request, allowed_roles=_MANAGERS)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    name, defs = _validate_book(data, cur)
    cur.execute("INSERT INTO logbooks (name, subject, responsible_role, responsible_pid, open_to_all) VALUES (%s, %s, %s, %s, %s)",
                (name, data.subject, data.responsible_role or "", data.responsible_pid, 1 if data.open_to_all else 0))
    lb_id = cur.lastrowid
    _save_fields(conn, cur, lb_id, defs)
    _save_members(conn, cur, lb_id, data.member_ids)
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "LOGBUCH_ANGELEGT", name)
    return {"status": "success", "id": lb_id}


@router.put("/{lb_id}")
def update_logbook(lb_id: int, data: LogbookIn, request: Request):
    user = check_auth(request, allowed_roles=_MANAGERS)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    _load(cur, lb_id)
    name, defs = _validate_book(data, cur)
    cur.execute("UPDATE logbooks SET name=%s, subject=%s, responsible_role=%s, responsible_pid=%s, open_to_all=%s WHERE id=%s",
                (name, data.subject, data.responsible_role or "", data.responsible_pid, 1 if data.open_to_all else 0, lb_id))
    _save_fields(conn, cur, lb_id, defs)
    _save_members(conn, cur, lb_id, data.member_ids)
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "LOGBUCH_GEAENDERT", name)
    return {"status": "success"}


@router.delete("/{lb_id}")
def delete_logbook(lb_id: int, request: Request):
    user = check_auth(request, allowed_roles=_MANAGERS)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    book = _load(cur, lb_id)
    for t in ("logbook_entries", "logbook_fields", "logbook_members"):
        cur.execute(f"DELETE FROM {t} WHERE logbook_id = %s", (lb_id,))
    cur.execute("UPDATE due_types SET logbook_id = NULL WHERE logbook_id = %s", (lb_id,))
    cur.execute("DELETE FROM logbooks WHERE id = %s", (lb_id,))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "LOGBUCH_GELOESCHT", f"{book['name']} samt Einträgen")
    return {"status": "success"}


@router.get("/{lb_id}/choices")
def choices(lb_id: int, request: Request):
    """Auswahl fürs Eintragen: wer geflogen ist (Zuständige: alle Aktiven, sonst nur man selbst) und der Bezug (Gerät/Fahrzeug)."""
    user, pid, conn, cur, book, fields, acc = _load_access(request, lb_id)
    if acc["sees_all"]:
        cur.execute("SELECT id, name FROM personnel WHERE exit_date IS NULL OR exit_date > CURDATE() ORDER BY name")
        people = cur.fetchall()
    elif pid is not None:
        cur.execute("SELECT id, name FROM personnel WHERE id = %s", (pid,))
        people = cur.fetchall()
    else:
        people = []
    subjects = [{"id": i, "name": n} for i, n in sorted(_subject_names(cur, book["subject"]).items(), key=lambda x: x[1].lower())]
    cur.close(); conn.close()
    return {"people": people, "subjects": subjects}


@router.get("/{lb_id}/entries")
def list_entries(lb_id: int, request: Request, year: int = 0):
    _check_year(year)
    user, pid, conn, cur, book, fields, acc = _load_access(request, lb_id)
    rows = _entries(cur, book, fields, pid, acc, year)
    cur.close(); conn.close()
    for r in rows:
        r["can_edit"] = acc["sees_all"] or (pid is not None and r["personnel_id"] == pid)
    return rows


def _entry_payload(data: EntryIn, book, fields, cur, user_pid, acc):
    try:
        day = datetime.date.fromisoformat(data.entry_date)
    except ValueError:
        raise HTTPException(status_code=400, detail="Bitte ein gültiges Datum angeben.")
    person = data.personnel_id if acc["sees_all"] else user_pid
    if person is None:
        raise HTTPException(status_code=400, detail="Bitte die Person wählen." if acc["sees_all"] else "Dein Login ist mit keiner Person verknüpft - bitte bei der Wehrführung melden.")
    cur.execute("SELECT 1 FROM personnel WHERE id = %s", (person,))
    if not cur.fetchone():
        raise HTTPException(status_code=400, detail="Die Person gibt es nicht.")
    subject = None
    if book["subject"] != "none":
        subject = data.subject_id
        if not subject or subject not in _subject_names(cur, book["subject"]):
            raise HTTPException(status_code=400, detail=f"Bitte {_SUBJECT_LABEL[book['subject']]} wählen.")
    try:
        values = lb.clean_values(fields, data.values or {})
    except lb.FieldError as err:
        raise HTTPException(status_code=400, detail=str(err))
    return day, person, subject, json.dumps(values, ensure_ascii=False)


@router.post("/{lb_id}/entries")
def create_entry(lb_id: int, data: EntryIn, request: Request):
    user, pid, conn, cur, book, fields, acc = _load_access(request, lb_id)
    day, person, subject, values = _entry_payload(data, book, fields, cur, pid, acc)
    cur.execute("INSERT INTO logbook_entries (logbook_id, entry_date, personnel_id, subject_id, data, created_by) VALUES (%s, %s, %s, %s, %s, %s)",
                (lb_id, day, person, subject, values, user["username"]))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}


def _own_or_all(cur, entry_id, user, pid):
    cur.execute("SELECT * FROM logbook_entries WHERE id = %s", (entry_id,))
    e = cur.fetchone()
    if not e:
        raise HTTPException(status_code=404, detail="Eintrag nicht gefunden.")
    book = _load(cur, e["logbook_id"])
    acc = _access(user, pid, book, _members(cur, book["id"]))
    if not (acc["sees_all"] or (acc["can_write"] and pid is not None and e["personnel_id"] == pid)):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    return e, book, acc


@router.put("/entries/{entry_id}")
def update_entry(entry_id: int, data: EntryIn, request: Request):
    user, pid = _me(request)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    e, book, acc = _own_or_all(cur, entry_id, user, pid)
    day, person, subject, values = _entry_payload(data, book, _fields(cur, book["id"]), cur, pid, acc)
    cur.execute("UPDATE logbook_entries SET entry_date=%s, personnel_id=%s, subject_id=%s, data=%s WHERE id=%s", (day, person, subject, values, entry_id))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}


@router.delete("/entries/{entry_id}")
def delete_entry(entry_id: int, request: Request):
    user, pid = _me(request)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    e, book, acc = _own_or_all(cur, entry_id, user, pid)
    cur.execute("DELETE FROM logbook_entries WHERE id = %s", (entry_id,))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "LOGBUCH_EINTRAG_GELOESCHT", f"{book['name']} vom {e['entry_date']}")
    return {"status": "success"}


@router.get("/{lb_id}/evaluation")
def evaluation(lb_id: int, request: Request, year: int = 0):
    """Auswertung über die Einträge, die man sehen darf (Mitglieder: nur die eigenen)."""
    _check_year(year)
    user, pid, conn, cur, book, fields, acc = _load_access(request, lb_id)
    rows = _entries(cur, book, fields, pid, acc, year)
    cur.close(); conn.close()
    result = lb.evaluate(fields, rows)
    result["scope"] = "all" if acc["sees_all"] else "own"
    return result


@router.get("/{lb_id}/export.csv")
def export_csv(lb_id: int, request: Request, year: int = 0):
    _check_year(year)
    user, pid, conn, cur, book, fields, acc = _load_access(request, lb_id, need="sees_all")
    rows = list(reversed(_entries(cur, book, fields, pid, acc, year)))
    cur.close(); conn.close()
    log_audit_action(user["username"], "LOGBUCH_EXPORT", f"{book['name']} ({year or 'alle Jahre'})")
    name = "".join(c if c.isalnum() else "_" for c in book["name"]) or "Logbuch"
    return Response(lb.to_csv(fields, rows, _SUBJECT_LABEL[book["subject"]]), media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="{name}_{year or "alle"}.csv"'})

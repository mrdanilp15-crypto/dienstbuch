"""Lehrgangsplanung (Wünsche der Mitglieder, Zuteilung durch die Wehrführung) sowie Umfragen und Helfersuche."""
import re
from datetime import date
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from core.notify import notify
from core.permissions import has_permission, users_with
from core.utils import check_auth, log_audit_action
from database import get_db_connection

router = APIRouter(prefix="/api", tags=["Planung"])

# Wunsch -> Zugeteilt -> Teilgenommen (steht dann in der Personalakte); Abgelehnt jederzeit davor
WISH_STATUSES = ("Wunsch", "Zugeteilt", "Teilgenommen", "Abgelehnt")
MAX_OPTIONS = 30


class WishIn(BaseModel):
    course_name: str
    note: Optional[str] = ""
    personnel_id: Optional[int] = None  # nur mit dem Recht: für andere eintragen


class WishDecision(BaseModel):
    status: str
    course_date: Optional[str] = None
    reply: Optional[str] = ""


class OptionIn(BaseModel):
    id: Optional[int] = None  # beim Bearbeiten: vorhandener Eintrag (Eintragungen bleiben erhalten)
    label: str
    capacity: Optional[int] = None


class PollIn(BaseModel):
    title: str
    description: Optional[str] = ""
    kind: str = "umfrage"  # umfrage | helfer
    multi: bool = False
    deadline: Optional[str] = None
    event_date: Optional[str] = None  # nur Helfersuche: erscheint im Kalender
    event_time: Optional[str] = ""
    options: List[OptionIn]


class VoteIn(BaseModel):
    option_ids: List[int]


class ClosedIn(BaseModel):
    closed: bool


def _d(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise HTTPException(status_code=400, detail="Ungültiges Datum.")


def _own_person(cur, username: str):
    cur.execute("SELECT p.id, p.name FROM users u JOIN personnel p ON p.id = u.personnel_id WHERE u.username = %s", (username,))
    return cur.fetchone()


def _usernames_of(cur, personnel_id: int):
    cur.execute("SELECT username FROM users WHERE personnel_id = %s", (personnel_id,))
    return [r["username"] for r in cur.fetchall()]


# --- LEHRGANGSPLANUNG -------------------------------------------------------------------------

@router.get("/course-wishes")
def list_wishes(request: Request):
    user = check_auth(request)
    manage = has_permission(user, "courses.manage")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    me = _own_person(cur, user["username"])
    sql = ("SELECT w.id, w.personnel_id, p.name, w.course_name, w.note, w.status, w.course_date, w.reply, w.created_at "
           "FROM course_wishes w JOIN personnel p ON p.id = w.personnel_id")
    order = " ORDER BY FIELD(w.status, 'Wunsch', 'Zugeteilt', 'Teilgenommen', 'Abgelehnt'), w.course_name, w.created_at"
    if manage:
        cur.execute(sql + order)
        rows = cur.fetchall()
    elif me:
        cur.execute(sql + " WHERE w.personnel_id = %s" + order, (me["id"],))
        rows = cur.fetchall()
    else:
        rows = []
    cur.execute("SELECT name FROM lehrgang_types ORDER BY name")
    types = [r["name"] for r in cur.fetchall()]
    cur.close(); conn.close()
    for r in rows:
        r["course_date"] = r["course_date"].isoformat() if r["course_date"] else None
        r["created_at"] = str(r["created_at"])
        r["mine"] = bool(me and r["personnel_id"] == me["id"])
    return {"wishes": rows, "types": types, "manage": manage, "me": me}


@router.post("/course-wishes")
def add_wish(data: WishIn, request: Request):
    user = check_auth(request)
    name = data.course_name.strip()
    note = (data.note or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Bitte einen Lehrgang angeben.")
    if len(name) > 255 or len(note) > 500:
        raise HTTPException(status_code=400, detail="Eingabe ist zu lang.")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    try:
        me = _own_person(cur, user["username"])
        pid = data.personnel_id or (me["id"] if me else None)
        if not pid:
            raise HTTPException(status_code=400, detail="Dein Konto ist mit keiner Person verknüpft. Bitte den Admin, das nachzuholen.")
        if (not me or pid != me["id"]) and not has_permission(user, "courses.manage"):
            raise HTTPException(status_code=403, detail="Für andere eintragen darf nur die Lehrgangsplanung.")
        cur.execute("SELECT name FROM personnel WHERE id = %s", (pid,))
        person = cur.fetchone()
        if not person:
            raise HTTPException(status_code=404, detail="Person nicht gefunden.")
        cur.execute("SELECT 1 FROM course_wishes WHERE personnel_id = %s AND LOWER(course_name) = LOWER(%s) AND status IN ('Wunsch', 'Zugeteilt')", (pid, name))
        if cur.fetchone():
            raise HTTPException(status_code=400, detail="Dieser Wunsch ist schon eingetragen.")
        cur.execute("INSERT INTO course_wishes (personnel_id, course_name, note) VALUES (%s, %s, %s)", (pid, name, note))
        conn.commit()
        new_id = cur.lastrowid
    finally:
        cur.close(); conn.close()
    others = [u for u in users_with("courses.manage") if u != user["username"]]
    if others:
        notify("verwaltung", f"Lehrgangswunsch: {name}", f"{person['name']} möchte an diesem Lehrgang teilnehmen.",
               "/dashboard?open=lehrgaenge", usernames=others)
    return {"status": "success", "id": new_id}


@router.put("/course-wishes/{wish_id}")
def decide_wish(wish_id: int, data: WishDecision, request: Request):
    user = check_auth(request, permission="courses.manage")
    if data.status not in WISH_STATUSES:
        raise HTTPException(status_code=400, detail="Ungültiger Status.")
    reply = (data.reply or "").strip()[:500]
    course_date = _d(data.course_date)
    if data.status == "Teilgenommen" and (not course_date or course_date > date.today()):
        raise HTTPException(status_code=400, detail="Bitte angeben, wann der Lehrgang war (nicht in der Zukunft).")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT * FROM course_wishes WHERE id = %s FOR UPDATE", (wish_id,))
        w = cur.fetchone()
        if not w:
            raise HTTPException(status_code=404, detail="Wunsch nicht gefunden.")
        if w["status"] == "Teilgenommen":
            raise HTTPException(status_code=400, detail="Der Lehrgang steht schon in der Personalakte.")
        cur.execute("UPDATE course_wishes SET status = %s, course_date = %s, reply = %s, decided_by = %s WHERE id = %s",
                    (data.status, course_date, reply, user["username"], wish_id))
        if data.status == "Teilgenommen":
            cur.execute("INSERT INTO lehrgaenge (personnel_id, course_name, date) VALUES (%s, %s, %s)",
                        (w["personnel_id"], w["course_name"], course_date))
        conn.commit()
        recipients = [u for u in _usernames_of(cur, w["personnel_id"]) if u != user["username"]]
    finally:
        cur.close(); conn.close()
    texts = {"Zugeteilt": "Du hast einen Platz bekommen.", "Abgelehnt": "Der Wunsch wurde abgelehnt.",
             "Teilgenommen": "Der Lehrgang steht jetzt in deiner Akte.", "Wunsch": "Dein Wunsch ist wieder offen."}
    body = texts[data.status]
    if data.status == "Zugeteilt" and course_date:
        body += f" Termin: {course_date.strftime('%d.%m.%Y')}."
    if reply:
        body += f" {reply}"
    if recipients:
        notify("termine", f"Lehrgang {w['course_name']}: {data.status}", body, "/dashboard?open=lehrgaenge", usernames=recipients)
    log_audit_action(user["username"], "LEHRGANGSWUNSCH", f"Wunsch {wish_id} ('{w['course_name']}') auf {data.status} gesetzt.")
    return {"status": "success"}


@router.delete("/course-wishes/{wish_id}")
def delete_wish(wish_id: int, request: Request):
    user = check_auth(request)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT * FROM course_wishes WHERE id = %s", (wish_id,))
        w = cur.fetchone()
        if not w:
            raise HTTPException(status_code=404, detail="Wunsch nicht gefunden.")
        if not has_permission(user, "courses.manage"):
            me = _own_person(cur, user["username"])
            if not me or me["id"] != w["personnel_id"]:
                raise HTTPException(status_code=403, detail="Das ist nicht dein Wunsch.")
            if w["status"] != "Wunsch":
                raise HTTPException(status_code=400, detail="Darüber wurde schon entschieden - bitte bei der Wehrführung melden.")
        cur.execute("DELETE FROM course_wishes WHERE id = %s", (wish_id,))
        conn.commit()
    finally:
        cur.close(); conn.close()
    return {"status": "success"}


# --- UMFRAGEN UND HELFERSUCHE -----------------------------------------------------------------

def _is_open(poll) -> bool:
    return not poll["closed"] and (poll["deadline"] is None or poll["deadline"] >= date.today())


def _poll_title(poll_title: str, kind: str) -> str:
    return ("Helfer gesucht: " if kind == "helfer" else "Umfrage: ") + poll_title


@router.get("/polls")
def list_polls(request: Request):
    user = check_auth(request)
    manage = has_permission(user, "polls.manage")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM polls ORDER BY created_at DESC, id DESC LIMIT 100")
    polls = cur.fetchall()
    cur.execute("SELECT * FROM poll_options ORDER BY position, id")
    options = cur.fetchall()
    # nur Stimmen bestehender Konten zählen; Name der verknüpften Person, sonst der Login
    cur.execute("SELECT v.poll_id, v.option_id, v.username, COALESCE(p.name, v.username) AS name FROM poll_votes v "
                "JOIN users u ON u.username = v.username LEFT JOIN personnel p ON p.id = u.personnel_id ORDER BY v.created_at, v.id")
    votes = cur.fetchall()
    cur.close(); conn.close()
    out = []
    for poll in polls:
        mine = [v["option_id"] for v in votes if v["poll_id"] == poll["id"] and v["username"] == user["username"]]
        opts = []
        for o in (o for o in options if o["poll_id"] == poll["id"]):
            who = [v["name"] for v in votes if v["option_id"] == o["id"]]
            opts.append({"id": o["id"], "label": o["label"], "capacity": o["capacity"], "count": len(who), "names": who,
                         "mine": o["id"] in mine, "full": bool(o["capacity"]) and len(who) >= o["capacity"]})
        out.append({"id": poll["id"], "title": poll["title"], "description": poll["description"] or "", "kind": poll["kind"],
                    "multi": bool(poll["multi"]), "deadline": poll["deadline"].isoformat() if poll["deadline"] else None,
                    "closed": bool(poll["closed"]), "open": _is_open(poll),
                    "event_date": poll["event_date"].isoformat() if poll["event_date"] else None, "event_time": poll["event_time"] or "", "answered": bool(mine), "options": opts,
                    "participants": len({v["username"] for v in votes if v["poll_id"] == poll["id"]}),
                    "can_edit": manage})
    out.sort(key=lambda x: not x["open"])  # offene zuerst, sonst neueste zuerst
    return {"polls": out, "manage": manage}


def _check_poll(data: PollIn, kind: str):
    """Gemeinsame Prüfung für Anlegen und Bearbeiten."""
    title = data.title.strip()
    description = (data.description or "").strip()
    if not title or len(title) > 150:
        raise HTTPException(status_code=400, detail="Bitte einen Titel angeben (höchstens 150 Zeichen).")
    if len(description) > 2000:
        raise HTTPException(status_code=400, detail="Die Beschreibung ist zu lang.")
    options = [(o.id, o.label.strip(), o.capacity if kind == "helfer" else None) for o in data.options if o.label.strip()]
    minimum = 1 if kind == "helfer" else 2
    if len(options) < minimum:
        raise HTTPException(status_code=400, detail="Bitte mindestens eine Aufgabe angeben." if minimum == 1 else "Bitte mindestens zwei Antworten angeben.")
    if len(options) > MAX_OPTIONS or any(len(label) > 150 for _, label, _ in options):
        raise HTTPException(status_code=400, detail=f"Höchstens {MAX_OPTIONS} Einträge mit je 150 Zeichen.")
    if len({label.lower() for _, label, _ in options}) != len(options):
        raise HTTPException(status_code=400, detail="Jeder Eintrag darf nur einmal vorkommen.")
    if any(cap is not None and not 1 <= cap <= 999 for _, _, cap in options):
        raise HTTPException(status_code=400, detail="Plätze müssen zwischen 1 und 999 liegen.")
    event_date = _d(data.event_date) if kind == "helfer" else None
    event_time = (data.event_time or "").strip() if event_date else ""
    if event_time and not re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", event_time):
        raise HTTPException(status_code=400, detail="Ungültige Uhrzeit.")
    return title, description, _d(data.deadline), event_date, event_time or None, options


@router.post("/polls")
def create_poll(data: PollIn, request: Request):
    user = check_auth(request, permission="polls.manage")
    if data.kind not in ("umfrage", "helfer"):
        raise HTTPException(status_code=400, detail="Ungültige Art.")
    title, description, deadline, event_date, event_time, options = _check_poll(data, data.kind)
    if deadline and deadline < date.today():
        raise HTTPException(status_code=400, detail="Die Frist liegt in der Vergangenheit.")
    helfer = data.kind == "helfer"
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO polls (title, description, kind, multi, deadline, event_date, event_time, created_by) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (title, description, data.kind, 1 if (helfer or data.multi) else 0, deadline, event_date, event_time, user["username"]))
    poll_id = cur.lastrowid
    cur.executemany("INSERT INTO poll_options (poll_id, label, capacity, position) VALUES (%s, %s, %s, %s)",
                    [(poll_id, label, cap, i) for i, (_, label, cap) in enumerate(options)])
    conn.commit()
    cur.execute("SELECT username FROM users WHERE username != %s", (user["username"],))
    recipients = [r[0] for r in cur.fetchall()]
    cur.close(); conn.close()
    if recipients:
        notify("termine", _poll_title(title, data.kind),
               description[:200] or ("Trag dich ein, wenn du helfen kannst." if helfer else "Deine Meinung ist gefragt."),
               "/dashboard?open=umfragen", usernames=recipients)
    return {"status": "success", "id": poll_id}


@router.put("/polls/{poll_id}")
def update_poll(poll_id: int, data: PollIn, request: Request):
    """Ändern ohne Eintragungen zu verlieren: Einträge mit id bleiben, fehlende werden samt Eintragungen entfernt.
    Art und "mehrere Antworten" stehen fest."""
    user = check_auth(request, permission="polls.manage")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT * FROM polls WHERE id = %s FOR UPDATE", (poll_id,))
        poll = cur.fetchone()
        if not poll:
            raise HTTPException(status_code=404, detail="Umfrage nicht gefunden.")
        title, description, deadline, event_date, event_time, options = _check_poll(data, poll["kind"])
        if deadline and deadline != poll["deadline"] and deadline < date.today():
            raise HTTPException(status_code=400, detail="Die Frist liegt in der Vergangenheit.")
        cur.execute("SELECT o.id, COUNT(u.id) AS n FROM poll_options o LEFT JOIN poll_votes v ON v.option_id = o.id "
                    "LEFT JOIN users u ON u.username = v.username WHERE o.poll_id = %s GROUP BY o.id", (poll_id,))
        counts = {r["id"]: r["n"] for r in cur.fetchall()}
        kept = [i for i, _, _ in options if i is not None]
        if len(set(kept)) != len(kept) or not set(kept) <= set(counts):
            raise HTTPException(status_code=400, detail="Ungültige Auswahl.")
        for option_id, label, cap in options:
            if option_id is not None and cap and counts[option_id] > cap:
                raise HTTPException(status_code=400, detail=f"Bei '{label}' sind schon {counts[option_id]} eingetragen - weniger Plätze gehen nicht.")
        for option_id in set(counts) - set(kept):
            cur.execute("DELETE FROM poll_votes WHERE option_id = %s", (option_id,))
            cur.execute("DELETE FROM poll_options WHERE id = %s", (option_id,))
        for position, (option_id, label, cap) in enumerate(options):
            if option_id is None:
                cur.execute("INSERT INTO poll_options (poll_id, label, capacity, position) VALUES (%s, %s, %s, %s)", (poll_id, label, cap, position))
            else:
                cur.execute("UPDATE poll_options SET label = %s, capacity = %s, position = %s WHERE id = %s", (label, cap, position, option_id))
        reminder_sent = poll["reminder_sent"] if deadline == poll["deadline"] else 0  # neue Frist: wieder erinnern
        cur.execute("UPDATE polls SET title = %s, description = %s, deadline = %s, event_date = %s, event_time = %s, reminder_sent = %s WHERE id = %s",
                    (title, description, deadline, event_date, event_time, reminder_sent, poll_id))
        conn.commit()
        moved = (event_date, event_time) != (poll["event_date"], poll["event_time"])
        cur.execute("SELECT DISTINCT username FROM poll_votes WHERE poll_id = %s AND username != %s", (poll_id, user["username"]))
        signed_up = [r["username"] for r in cur.fetchall()]
    finally:
        cur.close(); conn.close()
    if moved and event_date and signed_up:  # wer eingetragen ist, muss von einem neuen Termin erfahren
        when = event_date.strftime("%d.%m.%Y") + (f", {event_time} Uhr" if event_time else "")
        notify("termine", f"Neuer Termin: {title}", f"Die Helfersuche findet jetzt am {when} statt.", "/dashboard?open=umfragen", usernames=signed_up)
    log_audit_action(user["username"], "UMFRAGE_GEÄNDERT", f"Umfrage {poll_id} ('{title}') bearbeitet.")
    return {"status": "success"}


@router.put("/polls/{poll_id}/vote")
def vote(poll_id: int, data: VoteIn, request: Request):
    """Ersetzt die eigene Auswahl (leere Liste = wieder austragen)."""
    user = check_auth(request)
    wanted = set(data.option_ids)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    try:
        # Sperre auf die Umfrage: zwei Leute können sich nicht gleichzeitig den letzten Platz nehmen
        cur.execute("SELECT * FROM polls WHERE id = %s FOR UPDATE", (poll_id,))
        poll = cur.fetchone()
        if not poll:
            raise HTTPException(status_code=404, detail="Umfrage nicht gefunden.")
        if not _is_open(poll):
            raise HTTPException(status_code=400, detail="Diese Abstimmung ist beendet.")
        cur.execute("SELECT id, label, capacity FROM poll_options WHERE poll_id = %s", (poll_id,))
        options = {o["id"]: o for o in cur.fetchall()}
        if not wanted <= set(options):
            raise HTTPException(status_code=400, detail="Ungültige Auswahl.")
        if len(wanted) > 1 and not poll["multi"]:
            raise HTTPException(status_code=400, detail="Hier ist nur eine Antwort möglich.")
        cur.execute("SELECT option_id FROM poll_votes WHERE poll_id = %s AND username = %s", (poll_id, user["username"]))
        before = {r["option_id"] for r in cur.fetchall()}
        for option_id in wanted - before:
            cap = options[option_id]["capacity"]
            if cap:
                cur.execute("SELECT COUNT(*) AS n FROM poll_votes v JOIN users u ON u.username = v.username WHERE v.option_id = %s FOR UPDATE", (option_id,))
                if cur.fetchone()["n"] >= cap:
                    raise HTTPException(status_code=409, detail=f"'{options[option_id]['label']}' ist leider schon voll.")
        for option_id in before - wanted:
            cur.execute("DELETE FROM poll_votes WHERE option_id = %s AND username = %s", (option_id, user["username"]))
        for option_id in wanted - before:
            cur.execute("INSERT INTO poll_votes (poll_id, option_id, username) VALUES (%s, %s, %s)", (poll_id, option_id, user["username"]))
        conn.commit()
    finally:
        cur.close(); conn.close()
    return {"status": "success"}


@router.put("/polls/{poll_id}/closed")
def set_closed(poll_id: int, data: ClosedIn, request: Request):
    user = check_auth(request, permission="polls.manage")
    conn = get_db_connection(); cur = conn.cursor()
    # beim Wiederöffnen eine abgelaufene Frist entfernen, sonst bliebe die Abstimmung zu
    cur.execute("UPDATE polls SET closed = %s, deadline = IF(%s = 0 AND deadline < CURDATE(), NULL, deadline) WHERE id = %s",
                (1 if data.closed else 0, 1 if data.closed else 0, poll_id))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "UMFRAGE_STATUS", f"Umfrage {poll_id} {'beendet' if data.closed else 'wieder geöffnet'}.")
    return {"status": "success"}


@router.delete("/polls/{poll_id}")
def delete_poll(poll_id: int, request: Request):
    user = check_auth(request, permission="polls.manage")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM poll_votes WHERE poll_id = %s", (poll_id,))
    cur.execute("DELETE FROM poll_options WHERE poll_id = %s", (poll_id,))
    cur.execute("DELETE FROM polls WHERE id = %s", (poll_id,))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "UMFRAGE_GELÖSCHT", f"Umfrage {poll_id} gelöscht.")
    return {"status": "success"}

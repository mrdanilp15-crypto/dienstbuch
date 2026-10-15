import calendar
from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from core.utils import check_auth, log_audit_action
from database import get_db_connection

router = APIRouter(prefix="/api/building", tags=["Gerätehaus"])

_ROLES = ("admin", "leitung", "geratewart")

# Richtwerte als Schnellauswahl - die tatsächlichen Fristen hängen von Bundesland, Gemeinde und
# Versicherung ab und müssen dort abgeglichen werden.
SUGGESTIONS = [
    {"name": "Feuerlöscher (Prüfung/Wartung)", "interval_months": 24},
    {"name": "Elektroprüfung ortsfeste Anlagen (E-Check)", "interval_months": 48},
    {"name": "Elektrische Geräte (DGUV V3, ortsveränderlich)", "interval_months": 12},
    {"name": "Blitzschutzanlage", "interval_months": 48},
    {"name": "Rauchmelder / Brandmeldeanlage (Funktionsprüfung)", "interval_months": 12},
    {"name": "Notbeleuchtung / Rettungswege", "interval_months": 12},
    {"name": "Brandschutzbegehung", "interval_months": 12},
    {"name": "Erste-Hilfe-Material (Verfall prüfen)", "interval_months": 12},
    {"name": "Heizung / Warmwasser (Wartung)", "interval_months": 12},
]


class CheckIn(BaseModel):
    name: str
    interval_months: int = 12
    last_check: Optional[str] = None
    responsible: Optional[str] = ""
    note: Optional[str] = ""


class DoneIn(BaseModel):
    date: Optional[str] = None
    note: Optional[str] = None


def _add_months(d: date, n: int) -> date:
    y, m = divmod(d.month - 1 + n, 12)
    year, month = d.year + y, m + 1
    return date(year, month, min(d.day, calendar.monthrange(year, month)[1]))


def _parse(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise HTTPException(status_code=400, detail="Ungültiges Datum.")


def _validate(data: CheckIn):
    if not data.name.strip():
        raise HTTPException(status_code=400, detail="Bitte eine Bezeichnung angeben.")
    if not 1 <= data.interval_months <= 240:
        raise HTTPException(status_code=400, detail="Intervall: 1 bis 240 Monate.")


@router.get("/checks")
def list_checks(request: Request):
    check_auth(request)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM building_checks ORDER BY next_check IS NULL, next_check ASC, name ASC")
    rows = cur.fetchall(); cur.close(); conn.close()
    for r in rows:
        for k in ("last_check", "next_check"):
            r[k] = r[k].isoformat() if r[k] else None
    return {"checks": rows, "suggestions": SUGGESTIONS}


@router.post("/checks")
def create_check(data: CheckIn, request: Request):
    user = check_auth(request, allowed_roles=_ROLES)
    _validate(data)
    last = _parse(data.last_check)
    # Noch nie geprüft -> sofort fällig (so taucht die Prüfung in den Erinnerungen auf)
    nxt = _add_months(last, data.interval_months) if last else date.today()
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO building_checks (name, interval_months, last_check, next_check, responsible, note) VALUES (%s, %s, %s, %s, %s, %s)",
        (data.name.strip(), data.interval_months, last, nxt, (data.responsible or "").strip(), data.note or ""),
    )
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "GEBAEUDEPRUEFUNG_ANGELEGT", f"{data.name.strip()} (alle {data.interval_months} Monate)")
    return {"status": "success"}


@router.put("/checks/{check_id}")
def update_check(check_id: int, data: CheckIn, request: Request):
    check_auth(request, allowed_roles=_ROLES)
    _validate(data)
    last = _parse(data.last_check)
    conn = get_db_connection(); cur = conn.cursor()
    nxt_sql, nxt_val = ("%s", _add_months(last, data.interval_months)) if last else ("next_check", None)
    params = [data.name.strip(), data.interval_months, last, (data.responsible or "").strip(), data.note or ""]
    if nxt_val is not None:
        params.append(nxt_val)
    params.append(check_id)
    cur.execute(
        f"UPDATE building_checks SET name=%s, interval_months=%s, last_check=%s, responsible=%s, note=%s, next_check={nxt_sql} WHERE id=%s",
        tuple(params),
    )
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}


@router.post("/checks/{check_id}/done")
def mark_done(check_id: int, data: DoneIn, request: Request):
    user = check_auth(request, allowed_roles=_ROLES)
    done = _parse(data.date) or date.today()
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT name, interval_months FROM building_checks WHERE id = %s", (check_id,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close()
        raise HTTPException(status_code=404, detail="Prüfung nicht gefunden.")
    cur.execute(
        "UPDATE building_checks SET last_check = %s, next_check = %s, note = COALESCE(%s, note) WHERE id = %s",
        (done, _add_months(done, row["interval_months"]), data.note, check_id),
    )
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "GEBAEUDEPRUEFUNG_ERLEDIGT", f"{row['name']} am {done.isoformat()} geprüft.")
    return {"status": "success"}


@router.delete("/checks/{check_id}")
def delete_check(check_id: int, request: Request):
    check_auth(request, allowed_roles=_ROLES)
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM building_checks WHERE id = %s", (check_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

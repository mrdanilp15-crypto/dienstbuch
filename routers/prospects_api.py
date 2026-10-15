from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from core.utils import check_auth, log_audit_action
from database import get_db_connection

router = APIRouter(prefix="/api/prospects", tags=["Nachwuchs"])

_ROLES = ("admin", "leitung", "jugendwarte")
STATUSES = ("Interessent", "Probe", "Aufgenommen", "Abgesagt")


class ProspectIn(BaseModel):
    name: str
    phone: Optional[str] = ""
    email: Optional[str] = ""
    source: Optional[str] = ""
    mentor: Optional[str] = ""
    status: str = "Interessent"
    first_contact: Optional[str] = None
    last_contact: Optional[str] = None
    trial_end: Optional[str] = None
    note: Optional[str] = ""


def _d(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise HTTPException(status_code=400, detail="Ungültiges Datum.")


def _check(data: ProspectIn):
    if not data.name.strip():
        raise HTTPException(status_code=400, detail="Bitte einen Namen angeben.")
    if data.status not in STATUSES:
        raise HTTPException(status_code=400, detail="Ungültiger Status.")
    for f in (data.name, data.phone, data.email, data.source, data.mentor):
        if f and len(f) > 255:
            raise HTTPException(status_code=400, detail="Eingaben dürfen höchstens 255 Zeichen lang sein.")


@router.get("")
def list_prospects(request: Request):
    check_auth(request, allowed_roles=_ROLES)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM prospects ORDER BY FIELD(status, 'Probe', 'Interessent', 'Aufgenommen', 'Abgesagt'), first_contact DESC")
    rows = cur.fetchall(); cur.close(); conn.close()
    for r in rows:
        for k in ("first_contact", "last_contact", "trial_end"):
            r[k] = r[k].isoformat() if r[k] else None
        r["created_at"] = str(r["created_at"])
    return rows


@router.post("")
def create_prospect(data: ProspectIn, request: Request):
    user = check_auth(request, allowed_roles=_ROLES)
    _check(data)
    first = _d(data.first_contact) or date.today()
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO prospects (name, phone, email, source, mentor, status, first_contact, last_contact, trial_end, note) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (data.name.strip(), data.phone or "", data.email or "", data.source or "", data.mentor or "", data.status,
         first, _d(data.last_contact) or first, _d(data.trial_end), data.note or ""),
    )
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "INTERESSENT_ANGELEGT", f"Interessent '{data.name.strip()}' erfasst.")
    return {"status": "success"}


@router.put("/{prospect_id}")
def update_prospect(prospect_id: int, data: ProspectIn, request: Request):
    check_auth(request, allowed_roles=_ROLES)
    _check(data)
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute(
        "UPDATE prospects SET name=%s, phone=%s, email=%s, source=%s, mentor=%s, status=%s, first_contact=%s, last_contact=%s, trial_end=%s, note=%s WHERE id=%s",
        (data.name.strip(), data.phone or "", data.email or "", data.source or "", data.mentor or "", data.status,
         _d(data.first_contact), _d(data.last_contact), _d(data.trial_end), data.note or "", prospect_id),
    )
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}


@router.post("/{prospect_id}/contact")
def contacted_today(prospect_id: int, request: Request):
    check_auth(request, allowed_roles=_ROLES)
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("UPDATE prospects SET last_contact = CURDATE() WHERE id = %s", (prospect_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}


@router.delete("/{prospect_id}")
def delete_prospect(prospect_id: int, request: Request):
    # Endgültig löschen: Daten von Personen, die nicht Mitglied werden, sollen nicht auf Vorrat bleiben (DSGVO).
    user = check_auth(request, allowed_roles=_ROLES)
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM prospects WHERE id = %s", (prospect_id,))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "INTERESSENT_GELOESCHT", f"Interessent #{prospect_id} gelöscht.")
    return {"status": "success"}

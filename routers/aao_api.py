import json
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from core.aao import parse_entries, pick_rule
from core.utils import check_auth, check_display_access, log_audit_action
from database import get_db_connection

router = APIRouter(prefix="/api/aao", tags=["AAO"])

_ROLES = ("admin", "leitung")


class AaoEntry(BaseModel):
    vehicle: str
    role: Optional[str] = ""


class AaoRuleIn(BaseModel):
    pattern: str
    entries: List[AaoEntry]


def _clean(data: AaoRuleIn):
    pattern = data.pattern.strip()
    if not pattern or len(pattern) > 120:
        raise HTTPException(status_code=400, detail="Bitte ein Stichwort-Muster angeben (höchstens 120 Zeichen).")
    entries = [{"vehicle": e.vehicle.strip()[:60], "role": (e.role or "").strip()[:100]} for e in data.entries if e.vehicle.strip()]
    if not entries:
        raise HTTPException(status_code=400, detail="Mindestens ein Fahrzeug angeben.")
    if len(entries) > 12:
        raise HTTPException(status_code=400, detail="Höchstens 12 Fahrzeuge je Regel.")
    return pattern, json.dumps(entries, ensure_ascii=False)


def _rules(cur):
    cur.execute("SELECT id, pattern, entries, sort_order FROM aao_rules ORDER BY sort_order ASC, id ASC")
    return [{"id": r["id"], "pattern": r["pattern"], "entries": parse_entries(r["entries"]), "sort_order": r["sort_order"]} for r in cur.fetchall()]


@router.get("")
def list_rules(request: Request):
    user = check_auth(request)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    rules = _rules(cur); cur.close(); conn.close()
    return {"rules": rules, "can_edit": user["role"] in _ROLES}


@router.get("/recommend")
def recommend(request: Request, stichwort: str = ""):
    """Für das Hallen-Display (Session oder Display-Token): die Fahrzeuge der passendsten Regel, sonst eine leere Liste."""
    check_display_access(request)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    rule = pick_rule(_rules(cur), stichwort); cur.close(); conn.close()
    return {"rule": rule["pattern"] if rule else None, "entries": rule["entries"] if rule else []}


@router.post("")
def create_rule(data: AaoRuleIn, request: Request):
    user = check_auth(request, allowed_roles=_ROLES)
    pattern, entries = _clean(data)
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(sort_order), 0) + 1 FROM aao_rules")
    nxt = cur.fetchone()[0]
    cur.execute("INSERT INTO aao_rules (pattern, entries, sort_order) VALUES (%s, %s, %s)", (pattern, entries, nxt))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "AAO_REGEL_ANGELEGT", pattern)
    return {"status": "success"}


@router.put("/{rule_id}")
def update_rule(rule_id: int, data: AaoRuleIn, request: Request):
    user = check_auth(request, allowed_roles=_ROLES)
    pattern, entries = _clean(data)
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("UPDATE aao_rules SET pattern=%s, entries=%s WHERE id=%s", (pattern, entries, rule_id))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "AAO_REGEL_GEAENDERT", pattern)
    return {"status": "success"}


@router.delete("/{rule_id}")
def delete_rule(rule_id: int, request: Request):
    user = check_auth(request, allowed_roles=_ROLES)
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM aao_rules WHERE id=%s", (rule_id,))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "AAO_REGEL_GELOESCHT", f"Regel {rule_id}")
    return {"status": "success"}

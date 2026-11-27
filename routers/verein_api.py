from fastapi import APIRouter, HTTPException, Request
from datetime import date

from database import get_db_connection
from core.utils import get_current_user, log_audit_action
from core.permissions import has_permission

router = APIRouter()


@router.get("/api/verein/inventory")
def get_club_inventory(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM club_inventory ORDER BY item_name ASC")
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@router.post("/api/verein/inventory")
def add_club_inventory(data: dict, request: Request):
    user = get_current_user(request)
    if not user or not has_permission(user, "verein.manage"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    item = data.get("item_name")
    qty = int(data.get("quantity", 1))
    status = data.get("status", "OK")
    if not item: raise HTTPException(status_code=400, detail="Bezeichnung erforderlich")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO club_inventory (item_name, quantity, status) VALUES (%s, %s, %s)", (item, qty, status))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.delete("/api/verein/inventory/{i_id}")
def delete_club_inventory(i_id: int, request: Request):
    user = get_current_user(request)
    if not user or not has_permission(user, "verein.manage"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM club_inventory WHERE id = %s", (i_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.get("/api/verein/donations")
def get_donations(request: Request):
    u = get_current_user(request)
    if not u: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    if not has_permission(u, "verein.manage"): raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT *, DATE_FORMAT(date, '%d.%m.%Y') as formatted_date FROM club_donations ORDER BY date DESC")
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@router.post("/api/verein/donations")
def add_donation(data: dict, request: Request):
    user = get_current_user(request)
    if not user or not has_permission(user, "verein.manage"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    donor = data.get("donor")
    amount = float(data.get("amount", 0))
    date_val = data.get("date")
    if not donor or not date_val: raise HTTPException(status_code=400, detail="Spender und Datum erforderlich")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO club_donations (donor, amount, date) VALUES (%s, %s, %s)", (donor, amount, date_val))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

# --- MITGLIEDSBEITRÄGE ---
# Beitragspflichtig sind alle außer Ausgeschiedenen; wer im Jahr schon einen Eintrag hat, bleibt sichtbar.
# Standardbetrag (in Cent) und Fälligkeit (MMTT, z. B. 331 = 31.03.) liegen in der settings-Tabelle.

def _require_verein(request: Request):
    u = get_current_user(request)
    if not u: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    if not has_permission(u, "verein.manage"): raise HTTPException(status_code=403, detail="Keine Berechtigung")
    return u


def fee_settings() -> dict:
    from core.security import get_int_setting
    cents = get_int_setting("fee_default_cents", 0, ttl=0)
    due = get_int_setting("fee_due_mmdd", 331, ttl=0)
    return {"default_amount": round(cents / 100, 2), "due_month": due // 100 or 3, "due_day": due % 100 or 31}


def _due_date(year: int, st: dict) -> date:
    import calendar
    month = min(max(int(st["due_month"]), 1), 12)
    day = min(max(int(st["due_day"]), 1), calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _fee_rows(year: int):
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT p.id as personnel_id, p.name, p.membership_status,
               mf.id as fee_id, mf.amount, mf.paid_at, mf.note
        FROM personnel p
        LEFT JOIN membership_fees mf ON mf.personnel_id = p.id AND mf.year = %s
        WHERE COALESCE(p.membership_status, 'Aktiv') <> 'Ausgeschieden' OR mf.id IS NOT NULL
        ORDER BY p.name ASC
    """, (year,))
    res = cur.fetchall(); cur.close(); conn.close()
    default = fee_settings()["default_amount"]
    for row in res:
        row["paid_at"] = str(row["paid_at"]) if row.get("paid_at") else None
        row["amount"] = float(row["amount"]) if row.get("fee_id") and row.get("amount") is not None else default
        row["note"] = row.get("note") or ""
    return res


def fee_summary(year: int) -> dict:
    rows = _fee_rows(year)
    st = fee_settings()
    due = _due_date(year, st)
    open_rows = [r for r in rows if not r["paid_at"]]
    return {"year": year, "members": len(rows), "open_count": len(open_rows), "open_sum": round(sum(r["amount"] for r in open_rows), 2),
            "paid_count": len(rows) - len(open_rows), "paid_sum": round(sum(r["amount"] for r in rows if r["paid_at"]), 2),
            "due_date": due.isoformat(), "overdue": date.today() > due and bool(open_rows)}


@router.get("/api/verein/membership-fees")
def get_membership_fees(year: int, request: Request):
    _require_verein(request)
    return _fee_rows(year)


@router.get("/api/verein/membership-fees/summary")
def get_membership_fee_summary(request: Request, year: int = None):
    _require_verein(request)
    return {**fee_summary(year or date.today().year), **fee_settings()}


@router.get("/api/verein/fee-settings")
def get_fee_settings(request: Request):
    _require_verein(request)
    return fee_settings()


@router.put("/api/verein/fee-settings")
def put_fee_settings(data: dict, request: Request):
    from core.security import set_int_setting
    user = _require_verein(request)
    try:
        amount = round(float(data.get("default_amount") or 0), 2)
        month, day = int(data.get("due_month") or 3), int(data.get("due_day") or 31)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Ungültige Angaben")
    if amount < 0 or amount > 10000 or not (1 <= month <= 12) or not (1 <= day <= 31):
        raise HTTPException(status_code=400, detail="Betrag 0–10.000 €, Fälligkeit als gültiger Tag und Monat.")
    set_int_setting("fee_default_cents", int(round(amount * 100)))
    set_int_setting("fee_due_mmdd", month * 100 + day)
    log_audit_action(user["username"], "BEITRAG_EINSTELLUNG", f"Standardbeitrag {amount:.2f} €, fällig am {day:02d}.{month:02d}.")
    return fee_settings()


@router.get("/api/verein/membership-fees.csv")
def export_membership_fees(year: int, request: Request):
    import csv, io
    from fastapi.responses import Response
    _require_verein(request)
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";")
    w.writerow(["Mitglied", "Status", "Betrag", "Bezahlt am", "Notiz"])
    for r in _fee_rows(year):
        paid = date.fromisoformat(r["paid_at"]).strftime("%d.%m.%Y") if r["paid_at"] else "offen"
        w.writerow([r["name"], r["membership_status"] or "", f"{r['amount']:.2f}".replace(".", ","), paid, r["note"]])
    return Response("\ufeff" + buf.getvalue(), media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="Mitgliedsbeitraege_{int(year)}.csv"'})


@router.post("/api/verein/membership-fees")
def set_membership_fee(data: dict, request: Request):
    """Betrag/Notiz speichern; mit "paid" auch den Bezahlt-Status (ohne "paid" bleibt er, wie er ist)."""
    user = _require_verein(request)
    personnel_id = data.get("personnel_id")
    year = data.get("year")
    if not personnel_id or not year:
        raise HTTPException(status_code=400, detail="personnel_id und year erforderlich")
    try:
        amount = round(float(data.get("amount") if data.get("amount") not in (None, "") else fee_settings()["default_amount"]), 2)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Ungültiger Betrag")
    if amount < 0 or amount > 100000:
        raise HTTPException(status_code=400, detail="Ungültiger Betrag")
    note = (data.get("note") or "").strip()[:255]
    conn = get_db_connection(); cur = conn.cursor()
    if "paid" in data:
        paid_at = date.today() if data.get("paid") else None
        cur.execute(
            "INSERT INTO membership_fees (personnel_id, year, amount, paid_at, note) VALUES (%s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE amount=VALUES(amount), paid_at=VALUES(paid_at), note=VALUES(note)",
            (personnel_id, year, amount, paid_at, note))
    else:
        cur.execute(
            "INSERT INTO membership_fees (personnel_id, year, amount, paid_at, note) VALUES (%s, %s, %s, NULL, %s) "
            "ON DUPLICATE KEY UPDATE amount=VALUES(amount), note=VALUES(note)",
            (personnel_id, year, amount, note))
    conn.commit(); cur.close(); conn.close()
    if "paid" in data:
        log_audit_action(user["username"], "BEITRAG", f"Beitrag {year} für Personal-ID {personnel_id}: {'bezahlt' if data.get('paid') else 'offen'} ({amount:.2f} €)")
    return {"status": "success"}


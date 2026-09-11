from fastapi import APIRouter, HTTPException, Request
from datetime import date

from database import get_db_connection
from core.utils import get_current_user

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
    if not user or user["role"] not in ("admin", "leitung"):
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
    if not user or user["role"] not in ("admin", "leitung"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM club_inventory WHERE id = %s", (i_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.get("/api/verein/donations")
def get_donations(request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT *, DATE_FORMAT(date, '%d.%m.%Y') as formatted_date FROM club_donations ORDER BY date DESC")
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@router.post("/api/verein/donations")
def add_donation(data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung"):
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
@router.get("/api/verein/membership-fees")
def get_membership_fees(year: int, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    # LEFT JOIN, damit auch Mitglieder ohne Eintrag für dieses Jahr (= noch nicht bezahlt)
    # mit auftauchen, statt einfach zu fehlen.
    cur.execute("""
        SELECT p.id as personnel_id, p.name, p.membership_status,
               mf.id as fee_id, mf.amount, mf.paid_at, mf.note
        FROM personnel p
        LEFT JOIN membership_fees mf ON mf.personnel_id = p.id AND mf.year = %s
        WHERE p.membership_status = 'Aktiv'
        ORDER BY p.name ASC
    """, (year,))
    res = cur.fetchall(); cur.close(); conn.close()
    for row in res:
        if row.get("paid_at"):
            row["paid_at"] = str(row["paid_at"])
        if row.get("amount") is not None:
            row["amount"] = float(row["amount"])
    return res

@router.post("/api/verein/membership-fees")
def set_membership_fee(data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] not in ("admin", "leitung"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    personnel_id = data.get("personnel_id")
    year = data.get("year")
    if not personnel_id or not year:
        raise HTTPException(status_code=400, detail="personnel_id und year erforderlich")
    amount = float(data.get("amount") or 0)
    paid = bool(data.get("paid"))
    note = (data.get("note") or "").strip()
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO membership_fees (personnel_id, year, amount, paid_at, note) VALUES (%s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE amount=VALUES(amount), paid_at=VALUES(paid_at), note=VALUES(note)",
        (personnel_id, year, amount, date.today() if paid else None, note)
    )
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}


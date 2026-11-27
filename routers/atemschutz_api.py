"""Atemschutz-Übersicht: je Geräteträger G26.3, Belastungsübung, Unterweisung und Einsätze unter Atemschutz auf einen Blick.

Ampel je Person: rot = etwas abgelaufen oder nie erfasst (nicht einsatzfähig als AGT), gelb = läuft in den nächsten
60 Tagen ab, grün = alles gültig. Die Untersuchungsdaten sind Gesundheitsdaten: Datumsangaben sehen nur Admin,
Leitung und Atemschutzwart; wer nur Atemschutz-Einträge im Einsatz führen darf (z. B. Gruppenführer), sieht die Ampel.
"""
import datetime

from fastapi import APIRouter, HTTPException, Request

from core.permissions import has_permission
from core.utils import get_current_user
from database import get_db_connection

router = APIRouter(prefix="/api/atemschutz", tags=["Atemschutz"])
SOON_DAYS = 60
DATE_ROLES = ("admin", "leitung", "atemschutzwart")


def add_months(d: datetime.date, months: int) -> datetime.date:
    # gleiche Rechnung wie bei den Fristen (Monate * 30,44 Tage), damit Übersicht und Erinnerung übereinstimmen
    return d + datetime.timedelta(days=round(months * 30.44))


def _check(due, today):
    if due is None:
        return "missing"
    if due < today:
        return "expired"
    if (due - today).days <= SOON_DAYS:
        return "soon"
    return "ok"


@router.get("/overview")
def overview(request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht angemeldet")
    sees_dates = user["role"] in DATE_ROLES
    if not (sees_dates or has_permission(user, "atemschutz.manage")):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    today = datetime.date.today()
    year_start = datetime.date(today.year, 1, 1)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT setting_key, setting_value FROM settings WHERE setting_key IN ('int_g26', 'int_belastung', 'int_unterweisung')")
    st = {r["setting_key"]: r["setting_value"] for r in cur.fetchall()}
    months = {"g26": st.get("int_g26") or 36, "belastung": st.get("int_belastung") or 12, "unterweisung": st.get("int_unterweisung") or 12}
    cur.execute("SELECT id, name, rank, g26_3_date, belastungslauf_date, unterweisung_date FROM personnel "
                "WHERE is_agt = 1 AND COALESCE(NULLIF(membership_status, ''), 'Aktiv') = 'Aktiv' ORDER BY name")
    people = cur.fetchall()
    cur.execute("SELECT r.personnel_id, COUNT(*) AS n, SUM(COALESCE(r.dauer, 0)) AS minutes, MAX(m.date) AS last FROM respiration_log r "
                "JOIN missions m ON m.id = r.mission_id WHERE m.date >= %s AND m.stichwort NOT LIKE '[TEST]%%' GROUP BY r.personnel_id", (year_start,))
    usage = {r["personnel_id"]: r for r in cur.fetchall()}
    cur.close()
    conn.close()

    rank = {"missing": 3, "expired": 3, "soon": 1, "ok": 0}
    out = []
    for p in people:
        checks = {}
        for key, col in (("g26", "g26_3_date"), ("belastung", "belastungslauf_date"), ("unterweisung", "unterweisung_date")):
            last = p[col]
            due = add_months(last, months[key]) if last else None
            checks[key] = {"status": _check(due, today), "last": last.isoformat() if (last and sees_dates) else None,
                           "due": due.isoformat() if (due and sees_dates) else None}
        worst = max(rank[c["status"]] for c in checks.values())
        u = usage.get(p["id"]) or {}
        out.append({"id": p["id"], "name": p["name"], "rank": p["rank"] or "", "checks": checks,
                    "status": "red" if worst >= 3 else ("yellow" if worst else "green"), "ready": worst < 3,
                    "uses": int(u.get("n") or 0), "minutes": int(u.get("minutes") or 0),
                    "last_use": u["last"].isoformat() if u.get("last") else None})
    out.sort(key=lambda x: ({"red": 0, "yellow": 1, "green": 2}[x["status"]], x["name"]))
    return {"people": out, "months": months, "sees_dates": sees_dates, "year": today.year,
            "counts": {c: sum(1 for x in out if x["status"] == c) for c in ("green", "yellow", "red")}}

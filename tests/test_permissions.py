"""Datensparsamkeit und Rechte: wer sieht Entschädigungen, Beiträge, Anschriften und private Akten anderer Kameraden?"""
import sys

import httpx

from tests.helpers import sql
from tests.ui_helpers import ROLES, make_role_users

B = "http://127.0.0.1:8801"
fails = []


def check(name, cond, extra=""):
    print(("OK    " if cond else "FEHLER"), name, extra)
    if not cond:
        fails.append(name)


def client(user):
    c = httpx.Client(base_url=B, timeout=30)
    assert c.post("/api/login", json={"username": user, "password": "demo1234"}).status_code == 200, user
    return c


make_role_users()
cl = {r: client(f"r_{r}") for r in ROLES}
admin = client("demo")
pid = int(sql("select id from personnel where name='Anna Muster'"))
other = int(sql("select id from personnel where name='Jonas Weber'"))
sql(f"update users set personnel_id={other} where username='r_mannschaft'")
# der Mannschafts-Login ist mit Jonas verknüpft: die eigene Akte darf er vollständig sehen
SENSITIVE_LIST = ("address", "birth_date", "g26_3_date", "belastungslauf_date", "unterweisung_date", "license_checked_date")
SENSITIVE_ONE = SENSITIVE_LIST + ("notes", "skills", "parent_contact", "emergency_contact_name", "emergency_contact_phone", "badge_number")

for role in ("mannschaft", "geratewart", "gruppenfuehrer", "jugendwarte", "atemschutzwart"):
    c = cl[role]
    check(f"{role}: Stunden/Entschädigung aller Kameraden gesperrt", c.get("/api/missions/billing/compensations/list?year=2026&hourly_rate=10").status_code == 403)
    check(f"{role}: Rechnungsliste gesperrt", c.get("/api/missions/billing/list").status_code == 403)
    check(f"{role}: Arbeitgeberbescheinigungen gesperrt", c.get("/api/missions/employer-certificates").status_code == 403)
    check(f"{role}: Spenden und Mitgliedsbeiträge gesperrt", c.get("/api/verein/donations").status_code == 403 and c.get("/api/verein/membership-fees?year=2026").status_code == 403)
    lst = c.get("/api/personnel/list").json()
    leaked = sorted({k for row in lst for k in SENSITIVE_LIST if k in row} - ({"g26_3_date", "belastungslauf_date", "unterweisung_date", "license_checked_date"} if role == "atemschutzwart" else set()))
    check(f"{role}: Kameradenliste ohne Anschrift/Geburtsdatum{'/Gesundheitsdaten' if role != 'atemschutzwart' else ''}", not leaked, str(leaked))
    check(f"{role}: Kameradenliste liefert weiter Name, Rang, Telefon, Qualifikationen, G26-Warnung", all(k in lst[0] for k in ("name", "rank", "phone", "is_agt", "g26_expired")))
    one = c.get(f"/api/personnel/get/{pid}").json()
    leaked = sorted(k for k in SENSITIVE_ONE if k in one and not (role == "atemschutzwart" and k in ("g26_3_date", "belastungslauf_date", "unterweisung_date", "license_checked_date")))
    check(f"{role}: fremde Akte nur als Kurzansicht", not leaked, str(leaked))

own = cl["mannschaft"].get(f"/api/personnel/get/{other}").json()
check("mannschaft: die eigene Akte ist vollständig", all(k in own for k in ("birth_date", "address", "emergency_contact_name")), str([k for k in SENSITIVE_ONE if k not in own]))
check("mannschaft: Statistik der Wehrführung gesperrt", cl["mannschaft"].get("/api/admin/stats?year=2026").status_code == 403)

for role in ("leitung",):
    c = cl[role]
    check(f"{role}: Entschädigung, Rechnungen, Bescheinigungen, Beiträge erlaubt", c.get("/api/missions/billing/compensations/list?year=2026&hourly_rate=10").status_code == 200
          and c.get("/api/missions/billing/list").status_code == 200 and c.get("/api/missions/employer-certificates").status_code == 200
          and c.get("/api/verein/donations").status_code == 200 and c.get("/api/verein/membership-fees?year=2026").status_code == 200)
    lst = c.get("/api/personnel/list").json()
    check(f"{role}: sieht alle Felder der Kameraden", all(k in lst[0] for k in SENSITIVE_LIST))
    check(f"{role}: sieht fremde Akten vollständig", all(k in c.get(f"/api/personnel/get/{pid}").json() for k in SENSITIVE_ONE))
check("admin: alles erlaubt", admin.get("/api/missions/billing/compensations/list?year=2026&hourly_rate=10").status_code == 200 and admin.get("/api/verein/donations").status_code == 200)
check("Vereinsinventar bleibt für alle lesbar", cl["mannschaft"].get("/api/verein/inventory").status_code == 200)

print("\n==> ", "ALLES OK" if not fails else f"{len(fails)} FEHLER: {fails}")
sys.exit(1 if fails else 0)

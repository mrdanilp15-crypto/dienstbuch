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
    adults = [row for row in lst if row.get("membership_status") != "Jugend"]
    check(f"{role}: Kameradenliste liefert weiter Name, Rang, Telefon, Qualifikationen, G26-Warnung", all(k in adults[0] for k in ("name", "rank", "phone", "is_agt", "g26_expired")))
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

# Jugendfeuerwehr: Daten Minderjähriger nur für Wehrführung und Jugendwarte
sql("delete from personnel where name = 'Testjugend Kind'")
sql("insert into personnel (name, membership_status, phone, email, parent_contact) values ('Testjugend Kind', 'Jugend', '0151 99999', 'kind@example.org', 'Eltern 0151 88888')")
kid = int(sql("select id from personnel where name = 'Testjugend Kind'"))
for role in ("mannschaft", "geratewart", "gruppenfuehrer", "atemschutzwart"):
    c = cl[role]
    check(f"{role}: Jugend-Mitglieder und -Dienstberichte gesperrt", c.get("/api/jugend/members").status_code == 403 and c.get("/api/jugend/sessions").status_code == 403)
    row = [x for x in c.get("/api/personnel/list").json() if x["name"] == "Testjugend Kind"][0]
    check(f"{role}: Kontaktdaten des Jugendlichen in der Kameradenliste verborgen", "phone" not in row and "email" not in row, str(sorted(row)))
    one = c.get(f"/api/personnel/get/{kid}").json()
    check(f"{role}: … und in der Einzelansicht (auch keine Elternkontakte)", not any(k in one for k in ("phone", "email", "parent_contact")), str(sorted(one)))
for role in ("jugendwarte", "leitung"):
    c = cl[role]
    check(f"{role}: Jugend-Mitglieder und -Dienstberichte erlaubt", c.get("/api/jugend/members").status_code == 200 and c.get("/api/jugend/sessions").status_code == 200)
    row = [x for x in c.get("/api/personnel/list").json() if x["name"] == "Testjugend Kind"][0]
    check(f"{role}: sieht die Kontaktdaten des Jugendlichen", row.get("phone") == "0151 99999" and row.get("email") == "kind@example.org")
check("admin: Jugend-Mitglieder erlaubt", admin.get("/api/jugend/members").status_code == 200)
check("Jugend: Anlegen/Löschen weiter nur Wehrführung und Jugendwarte", cl["mannschaft"].post("/api/jugend/members", json={"name": "X Y"}).status_code == 403)
sql("delete from personnel where name = 'Testjugend Kind'")

# --- Ohne Anmeldung: nichts aus der Wehr (Objekte mit Schlüsseldepot, Hydranten, Termine, hochgeladene Dateien)
anon = httpx.Client(base_url=B, timeout=30)
for path in ("/api/material/bma", "/api/material/hydrants", "/api/missions/schedules/list"):
    check(f"Ohne Anmeldung gesperrt: {path}", anon.get(path).status_code == 401)
tok = admin.get("/api/settings/access-tokens").json()["display_token"]
for path in ("/api/material/bma", "/api/material/hydrants", "/api/missions/schedules/list"):
    check(f"Hallen-Display mit Token erlaubt: {path}", anon.get(f"{path}?token={tok}").status_code == 200)
    check(f"Falscher Token gesperrt: {path}", anon.get(f"{path}?token=falsch").status_code == 401)
import os
# gleiche Ordnerwahl wie main.py (UPLOAD_DIR)
_up = os.path.join("/app/data/uploads" if (os.path.exists("/app/data") or os.name != "nt") else os.path.join("static", "uploads"), "zz_test_upload.txt")
with open(_up, "w") as fh:
    fh.write("geheim")
check("Hochgeladene Datei ohne Anmeldung gesperrt", anon.get("/static/uploads/zz_test_upload.txt").status_code == 401)
_r = cl["mannschaft"].get("/static/uploads/zz_test_upload.txt")
check("Hochgeladene Datei mit Anmeldung lesbar, nicht öffentlich zwischenspeicherbar", _r.status_code == 200 and _r.text == "geheim" and _r.headers.get("cache-control", "").startswith("private"), f"{_r.status_code} {_r.text[:40]!r} {_r.headers.get('cache-control', '')}")
check("Programmdateien bleiben ohne Anmeldung ladbar", anon.get("/static/js/dashboard.js").status_code == 200)
os.remove(_up)

# --- Berichte maskieren Freitext (kein eingeschleuster Code im Dienstbericht)
from routers import reports
_h = reports.generate_single_report({"date": "2026-01-01", "duration": 2, "category": "Übung", "description": "<script>alert(1)</script>", "gname": "A&B", "instructors": "<b>x</b>", "leader_signature": "javascript:alert(1)" + "x" * 200},
                                    [{"name": "<img src=x onerror=alert(1)>", "is_present": 1, "note": "<i>n</i>", "vehicle": "", "signature": "x' onerror='alert(1)" + "y" * 200}], "Ort<br>")
check("Dienstbericht: kein Roh-HTML aus Freitext", "<script>" not in _h and "<img src=x" not in _h and "onerror='alert" not in _h and "javascript:" not in _h and "&lt;script&gt;" in _h and "A&amp;B" in _h, _h[:200])
_ok = reports.generate_single_report({"date": "2026-01-01", "duration": 2, "category": "Einsatz", "description": "Brand", "leader_signature": "data:image/png;base64," + "A" * 200}, [], "Ort")
check("Dienstbericht: echte Unterschrift bleibt erhalten", "<img src='data:image/png;base64," in _ok and "bg-einsatz" in _ok)

print("\n==> ", "ALLES OK" if not fails else f"{len(fails)} FEHLER: {fails}")
sys.exit(1 if fails else 0)

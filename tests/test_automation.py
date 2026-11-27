"""Automatische Abläufe: offene Berichte, Mangel aus der Fahrzeug-Checkliste, Einladung beim Anlegen, Monatsbericht."""
import datetime
import sys

import httpx
from playwright.sync_api import sync_playwright

from core.automation import build_monthly_report, check_open_reports, previous_month, run_monthly_report
from tests.helpers import sql
from tests.ui_helpers import BASE, browser_page, make_role_users, vm_eval

fails = []


def check(name, cond, extra=""):
    print(("OK    " if cond else "FEHLER"), name, extra)
    if not cond:
        fails.append(name)


def client(user):
    c = httpx.Client(base_url=BASE, timeout=30)
    assert c.post("/api/login", json={"username": user, "password": "demo1234"}).status_code == 200, user
    return c


make_role_users()
sql("delete from notifications; delete from defect_comments; delete from equipment_defect_reports where vehicle_id is not null")
sql("delete from personnel where name = 'Jürgen Groß'; delete from persons where name = 'Jürgen Groß'; delete from users where username = 'juergen.gross'")
admin = client("demo")
count = lambda q: int(sql(q))

# --- offene Berichte
noon = datetime.datetime.combine(datetime.date.today(), datetime.time(12, 0))
old = (datetime.date.today() - datetime.timedelta(days=10)).isoformat()
new = datetime.date.today().isoformat()
sql("update missions set reminded_at = '2099-01-01'")  # vorhandene Einsätze unangetastet, nur aus der Erinnerung nehmen
for stichwort, day, status in (("Alt-Entwurf", old, "Entwurf"), ("Alt-Pruefung", old, "In Prüfung"), ("Neu-Entwurf", new, "Entwurf"), ("[TEST] Probe", old, "Entwurf")):
    sql(f"insert into missions (date, time, stichwort, adresse, meldung, status) values ('{day}', '10:00', '{stichwort}', 'x', 'x', '{status}')")
check("Berichte: nachts keine Erinnerung", check_open_reports(noon.replace(hour=2)) == 0)
check("Berichte: nur alte, keine Test-Alarme", check_open_reports(noon) == 2)
body = sql("select body from notifications where username='r_gruppenfuehrer' and title='1 Einsatzbericht ist noch nicht fertig'")
check("Berichte: Gruppenführer erfährt vom Entwurf", "Alt-Entwurf" in body and "Neu-Entwurf" not in body, body)
check("Berichte: Leitung erfährt von der Freigabe", count("select count(*) from notifications where username='r_leitung' and title='1 Einsatzbericht wartet auf Freigabe'") == 1)
check("Berichte: Mannschaft bleibt verschont", count("select count(*) from notifications where username='r_mannschaft'") == 0)
check("Berichte: nicht sofort wieder", check_open_reports(noon + datetime.timedelta(days=1)) == 0)
check("Berichte: nach einer Woche erneut (der neue ist dann auch alt)", check_open_reports(noon + datetime.timedelta(days=8)) == 3)

# --- Mangel aus der Fahrzeug-Checkliste
veh = count("select id from vehicles order by id limit 1")
chk = {"date": new, "checker_name": "Max", "status": "Mängel", "notes": "Blaulicht links aus", "items_checked": {"Reifendruck": True, "Beleuchtung / Blaulicht": False}}
r = admin.post(f"/api/material/vehicles/{veh}/checks", json=chk)
check("Checkliste: Mangel wird angelegt", r.status_code == 200 and r.json()["defect"]["new"], r.text)
lst = [d for d in admin.get("/api/material/defect-reports").json() if d.get("vehicle_id") == veh]
check("Checkliste: Mangel steht mit Fahrzeugname in der Liste", len(lst) == 1 and lst[0]["equipment_name"].startswith("Fahrzeug: ") and "Blaulicht" in lst[0]["description"], str(lst)[:200])
check("Checkliste: Gerätewart wird benachrichtigt", count("select count(*) from notifications where username='r_geratewart' and category='maengel'") == 1)
r = admin.post(f"/api/material/vehicles/{veh}/checks", json=chk)
check("Checkliste: gleicher Mangel nur einmal, mit Vermerk", r.json()["defect"] == {"id": lst[0]["id"], "new": False} and count(f"select count(*) from defect_comments where report_id={lst[0]['id']}") == 1)
r = admin.post(f"/api/material/vehicles/{veh}/checks", json=dict(chk, status="OK", items_checked={"Reifendruck": True}))
check("Checkliste: alles in Ordnung erzeugt nichts", r.json()["defect"] is None)

# --- Monatsbericht
y, m = previous_month(datetime.date.today())
subject, text = build_monthly_report(y, m)
check("Monatsbericht: Kennzahlen und offene Mängel", subject.startswith("Monatsbericht ") and "Einsätze:" in text and "Personenstunden im Dienst:" in text and "OFFENE MÄNGEL" in text, text[:200])
sql("delete from settings where setting_key='last_monthly_report'")
check("Monatsbericht: ohne Einschalten wird nichts verschickt", run_monthly_report(noon.replace(day=2)) == 0)
check("Monatsbericht: Probeversand nur für Admins", client("r_leitung").post("/api/admin/monthly-report/test").status_code == 403)

# --- Oberfläche
with sync_playwright() as pw:
    browser, page = browser_page(pw, "demo")
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)))
    check("Benutzername aus dem Namen", vm_eval(page, "vm.suggestUsername('  Jürgen Groß ')") == "juergen.gross")
    sent = []
    page.route("**/api/users/add", lambda route: (sent.append(route.request.post_data_json), route.fulfill(status=200, content_type="application/json", body='{"status":"success"}')))
    vm_eval(page, "(vm.activeTab = 'personal', vm.openPersonnelModal(), 1)")
    page.wait_for_selector("#personnelModal.show", timeout=8000)
    check("Einladung: Haken erst mit E-Mail-Adresse", page.locator("#inviteNewMember").count() == 0)
    vm_eval(page, "(vm.activePersonnel.name = 'Jürgen Groß', vm.activePersonnel.email = 'j@example.org', 1)")
    page.locator("#inviteNewMember").check()
    page.locator("#personnelModal").get_by_role("button", name="Speichern", exact=True).click()
    page.wait_for_selector("#appConfirmModal, .modal.show:has-text('Einladung ist unterwegs')", timeout=8000)
    pid = count("select id from personnel where name='Jürgen Groß'")
    check("Einladung: Konto wird mit der neuen Person verknüpft angefragt", sent == [{"username": "juergen.gross", "password": "", "role": "mannschaft", "personnel_id": pid, "invite": True}], str(sent))
    check("Keine Fehler in der Seite", not errors, str(errors))
    browser.close()

# --- Einsatzbericht: Titel, Datum, Status, Freigabe; "Entschuldigt" zählt nicht als Teilnahme
sql("insert into missions (date, time, stichwort, adresse, meldung, description, duration, status, approved_by, approved_at) "
    "values ('2026-11-19', '01:30', 'Bericht-Probe', 'Hauptstr. 5', '', 'Kein Feuer', 0.5, 'Freigegeben', 'demo', '2026-11-27 02:00:00')")
mid = count("select max(id) from missions where stichwort='Bericht-Probe'")
pids = sql("select id from personnel order by id limit 3").split()
for pid, st in zip(pids, ("Abgerückt", "Gerätehaus", "Entschuldigt")):
    sql(f"insert into mission_attendance (mission_id, personnel_id, is_present, vehicle) values ({mid}, {pid}, '{st}', '')")
rep = admin.get(f"/api/missions/{mid}/report").text
check("Bericht: Titel Einsatzbericht", ">Einsatzbericht<" in rep and "Dienstbericht" not in rep)
check("Bericht: deutsches Datum", "19.11.2026" in rep and "2026-11-19" not in rep)
check("Bericht: keine Platzhalter", "Einsatzleiter" not in rep and "Hauptstr. 5" in rep and "Kein Feuer" in rep)
check("Bericht: Status wie erfasst", all(x in rep for x in ("Abgerückt", "Gerätehaus", "Entschuldigt")) and "Abwesend" not in rep)
check("Bericht: Freigabe in zwei Zeilen", "am 27.11.2026 um 02:00 Uhr" in rep and "sig-space" in rep)
check("Bericht: Dauer mit Komma", "0,5 h" in rep)
check("PDF wird erzeugt", admin.get(f"/api/missions/{mid}/pdf").content[:4] == b"%PDF")
excused = pids[2]
check("Bescheinigung für Entschuldigte abgelehnt", admin.get(f"/api/missions/{mid}/employer-certificate/{excused}").status_code == 400)
sql(f"delete from mission_attendance where mission_id={mid}; delete from missions where id={mid}")

# Testdaten wieder entfernen, damit spätere Tests sie nicht sehen
sql("delete from personnel where name = 'Jürgen Groß'; delete from persons where name = 'Jürgen Groß'")
sql("delete from missions where stichwort in ('Alt-Entwurf', 'Alt-Pruefung', 'Neu-Entwurf', '[TEST] Probe'); update missions set reminded_at = null")
sql("delete from defect_comments; delete from equipment_defect_reports where vehicle_id is not null")
print("\n" + ("ALLE TESTS BESTANDEN" if not fails else f"{len(fails)} FEHLER: {fails}"))
sys.exit(1 if fails else 0)

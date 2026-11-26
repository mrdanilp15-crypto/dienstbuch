"""Lehrgangsplanung sowie Umfragen/Helfersuche: Rechte, volle Plätze, Eintrag in die Akte, Oberfläche."""
import sys

import httpx
from playwright.sync_api import sync_playwright

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
for t in ("course_wishes", "polls", "poll_options", "poll_votes"):
    sql(f"delete from {t}")
sql("delete from lehrgaenge where course_name = 'Testlehrgang Planung'")
jonas = int(sql("select id from personnel where name='Jonas Weber'"))
anna = int(sql("select id from personnel where name='Anna Muster'"))
sql(f"update users set personnel_id={jonas} where username='r_mannschaft'")
sql(f"update users set personnel_id={anna} where username='r_geratewart'")
admin, man, ger, grp = client("demo"), client("r_mannschaft"), client("r_geratewart"), client("r_gruppenfuehrer")

# --- Lehrgänge
r = man.post("/api/course-wishes", json={"course_name": "Testlehrgang Planung", "note": "nur samstags"})
check("Lehrgang: Mannschaft kann sich selbst vormerken", r.status_code == 200, r.text)
wid = r.json().get("id")
check("Lehrgang: kein doppelter Wunsch", man.post("/api/course-wishes", json={"course_name": "testlehrgang planung"}).status_code == 400)
check("Lehrgang: nicht für andere eintragen", man.post("/api/course-wishes", json={"course_name": "X", "personnel_id": anna}).status_code == 403)
check("Lehrgang: Konto ohne Person bekommt eine verständliche Meldung", grp.post("/api/course-wishes", json={"course_name": "X"}).status_code == 400)
check("Lehrgang: andere sehen fremde Wünsche nicht", ger.get("/api/course-wishes").json()["wishes"] == [])
check("Lehrgang: ohne Recht nicht entscheiden", man.put(f"/api/course-wishes/{wid}", json={"status": "Zugeteilt"}).status_code == 403)
check("Lehrgang: fremden Wunsch nicht löschen", ger.delete(f"/api/course-wishes/{wid}").status_code == 403)
check("Lehrgang: Planung sieht den Wunsch mit Namen", [w["name"] for w in admin.get("/api/course-wishes").json()["wishes"]] == ["Jonas Weber"])
check("Lehrgang: Platz zuteilen", admin.put(f"/api/course-wishes/{wid}", json={"status": "Zugeteilt", "course_date": "2030-05-04"}).status_code == 200)
check("Lehrgang: Mitglied wird benachrichtigt", int(sql("select count(*) from notifications where username='r_mannschaft' and title like 'Lehrgang Testlehrgang Planung%'")) == 1)
check("Lehrgang: nach der Entscheidung nicht mehr selbst löschen", man.delete(f"/api/course-wishes/{wid}").status_code == 400)
check("Lehrgang: Teilnahme nicht in der Zukunft", admin.put(f"/api/course-wishes/{wid}", json={"status": "Teilgenommen", "course_date": "2030-05-04"}).status_code == 400)
check("Lehrgang: Teilnahme eintragen", admin.put(f"/api/course-wishes/{wid}", json={"status": "Teilgenommen", "course_date": "2026-01-10"}).status_code == 200)
check("Lehrgang: steht in der Personalakte", int(sql(f"select count(*) from lehrgaenge where personnel_id={jonas} and course_name='Testlehrgang Planung'")) == 1)
check("Lehrgang: wird nicht doppelt eingetragen", admin.put(f"/api/course-wishes/{wid}", json={"status": "Teilgenommen", "course_date": "2026-01-10"}).status_code == 400)

# --- Helfersuche und Umfrage
body = {"kind": "helfer", "title": "Osterfeuer", "options": [{"label": "Aufbau", "capacity": 1}, {"label": "Grill"}]}
check("Helfersuche: Mannschaft darf nicht anlegen", man.post("/api/polls", json=body).status_code == 403)
r = grp.post("/api/polls", json=body)
check("Helfersuche: Gruppenführer legt an", r.status_code == 200, r.text)
pid = r.json().get("id")
check("Helfersuche: alle anderen werden benachrichtigt", int(sql("select count(*) from notifications where username='r_mannschaft' and title='Helfer gesucht: Osterfeuer'")) == 1)
opts = man.get("/api/polls").json()["polls"][0]["options"]
auf, grill = opts[0]["id"], opts[1]["id"]
check("Helfersuche: mehrere Aufgaben wählbar", man.put(f"/api/polls/{pid}/vote", json={"option_ids": [auf, grill]}).status_code == 200)
r = ger.put(f"/api/polls/{pid}/vote", json={"option_ids": [auf]})
check("Helfersuche: voller Platz wird abgelehnt", r.status_code == 409, r.text)
p = ger.get("/api/polls").json()["polls"][0]
check("Helfersuche: Name und 'voll' sichtbar", p["options"][0]["names"] == ["Jonas Weber"] and p["options"][0]["full"] and not p["answered"])
man.put(f"/api/polls/{pid}/vote", json={"option_ids": [grill]})
check("Helfersuche: austragen gibt den Platz frei", ger.put(f"/api/polls/{pid}/vote", json={"option_ids": [auf]}).status_code == 200)
check("Helfersuche: ohne Recht nicht beenden oder löschen", man.put(f"/api/polls/{pid}/closed", json={"closed": True}).status_code == 403 and man.delete(f"/api/polls/{pid}").status_code == 403)
grp.put(f"/api/polls/{pid}/closed", json={"closed": True})
check("Helfersuche: nach dem Beenden keine Änderung", man.put(f"/api/polls/{pid}/vote", json={"option_ids": []}).status_code == 400)
uid = admin.post("/api/polls", json={"kind": "umfrage", "title": "Ausflug", "options": [{"label": "Berge"}, {"label": "See"}]}).json()["id"]
o = [x["id"] for x in [p for p in man.get("/api/polls").json()["polls"] if p["id"] == uid][0]["options"]]
check("Umfrage: nur eine Antwort", man.put(f"/api/polls/{uid}/vote", json={"option_ids": o}).status_code == 400)
check("Umfrage: fremde Antwort-Nummer abgelehnt", man.put(f"/api/polls/{uid}/vote", json={"option_ids": [auf]}).status_code == 400)
check("Umfrage: mindestens zwei Antworten", admin.post("/api/polls", json={"title": "x", "options": [{"label": "a"}]}).status_code == 400)
from routers.admin_api import BACKUP_TABLES  # noqa: E402
check("Sicherung enthält die neuen Tabellen", all(t in BACKUP_TABLES for t in ("course_wishes", "polls", "poll_options", "poll_votes")))

# --- Oberfläche
with sync_playwright() as pw:
    browser, page = browser_page(pw, "r_mannschaft", width=390, height=844)
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)))
    todo = vm_eval(page, "vm.todoItems.map(t => t.id)")
    check("Übersicht: offene Umfrage unter 'Zu erledigen'", "umfragen" in todo, str(todo))
    vm_eval(page, "(vm.goTodo('umfragen'), 1)")
    page.wait_for_selector(".poll-card", timeout=8000)
    page.locator(".poll-card", has_text="Ausflug").get_by_role("button", name="Wählen").first.click()
    page.wait_for_selector(".poll-card button[aria-pressed=true]", timeout=8000)
    check("Handy: Abstimmen per Klick", int(sql(f"select count(*) from poll_votes where poll_id={uid} and username='r_mannschaft'")) == 1)
    check("Handy: Mannschaft sieht keinen Anlegen-Knopf", not page.get_by_role("button", name="Neue Umfrage").is_visible())
    check("Handy: nichts läuft quer", page.evaluate("() => document.documentElement.scrollWidth <= window.innerWidth + 1"))
    vm_eval(page, "(vm.activeTab = 'lehrgaenge', 1)")
    page.wait_for_selector("#wishCourse", timeout=8000)
    check("Handy: Mannschaft trägt ohne Personenauswahl für sich ein", page.locator("#wishPerson").count() == 0 and page.get_by_text("Wird für dich eingetragen (Jonas Weber)").is_visible())
    page.fill("#wishCourse", "Maschinist")
    page.get_by_role("button", name="Wunsch eintragen").click()
    page.wait_for_selector(".wish-item:has-text('Maschinist')", timeout=8000)
    check("Handy: Lehrgangswunsch eintragen", True)
    check("Keine Fehler in der Seite", not errors, str(errors))
    browser.close()

    browser, page = browser_page(pw, "demo")
    vm_eval(page, "(vm.activeTab = 'lehrgaenge', 1)")
    page.wait_for_selector("#wishCourse", timeout=8000)
    check("PC: auch der Admin trägt standardmäßig für sich ein", page.locator("#wishPerson").count() == 0)
    page.get_by_role("button", name="Für jemand anderen").click()
    check("PC: Personenauswahl erst auf Wunsch", page.locator("#wishPerson").is_visible())
    vm_eval(page, "(vm.activeTab = 'umfragen', 1)")
    page.get_by_role("button", name="Helfer suchen").click()
    page.fill("#pollTitle", "Tag der offenen Tür")
    page.get_by_label("Eintrag 1").fill("Getränkestand")
    page.get_by_label("Plätze 1").fill("2")
    page.get_by_role("button", name="Veröffentlichen").click()
    page.wait_for_selector(".poll-card:has-text('Getränkestand')", timeout=8000)
    check("PC: Helfersuche über das Formular anlegen", int(sql("select capacity from poll_options where label='Getränkestand'")) == 2)
    browser.close()

print("\n" + ("ALLE TESTS BESTANDEN" if not fails else f"{len(fails)} FEHLER: {fails}"))
sys.exit(1 if fails else 0)

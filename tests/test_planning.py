"""Lehrgangsplanung sowie Umfragen/Helfersuche: Rechte, volle Plätze, Eintrag in die Akte, Oberfläche."""
import sys

import httpx
from playwright.sync_api import sync_playwright

from core.schedule_notify import check_poll_reminders
from core.utils import get_or_create_token
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
for t in ("course_wishes", "polls", "poll_options", "poll_votes", "notifications"):
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

# --- Bearbeiten, Erinnerung, Kalender
base = {"kind": "helfer", "title": "Sommerfest", "event_date": "2030-06-15", "event_time": "14:00", "options": [{"label": "Zelt", "capacity": 3}, {"label": "Kasse"}]}
sid = admin.post("/api/polls", json=base).json()["id"]
get = lambda c: [x for x in c.get("/api/polls").json()["polls"] if x["id"] == sid][0]
zelt, kasse = [x["id"] for x in get(admin)["options"]]
man.put(f"/api/polls/{sid}/vote", json={"option_ids": [zelt]})
ger.put(f"/api/polls/{sid}/vote", json={"option_ids": [zelt, kasse]})
edit = dict(base, title="Sommerfest 2030", options=[{"id": zelt, "label": "Zeltaufbau", "capacity": 2}, {"id": kasse, "label": "Kasse"}, {"label": "Spülen"}])
check("Bearbeiten: ohne Recht nicht", man.put(f"/api/polls/{sid}", json=edit).status_code == 403)
check("Bearbeiten: speichern", admin.put(f"/api/polls/{sid}", json=edit).status_code == 200)
p = get(man)
check("Bearbeiten: Eintragungen bleiben, neuer Eintrag dazu", p["title"] == "Sommerfest 2030" and [o["count"] for o in p["options"]] == [2, 1, 0] and p["options"][0]["mine"])
check("Bearbeiten: nicht weniger Plätze als Eingetragene", admin.put(f"/api/polls/{sid}", json=dict(edit, options=[{"id": zelt, "label": "Zeltaufbau", "capacity": 1}])).status_code == 400)
check("Bearbeiten: fremde Eintrags-Nummer abgelehnt", admin.put(f"/api/polls/{sid}", json=dict(edit, options=[{"id": auf, "label": "X"}])).status_code == 400)
check("Bearbeiten: gleicher Termin meldet nichts", int(sql("select count(*) from notifications where title='Neuer Termin: Sommerfest 2030'")) == 0)
moved = dict(edit, event_date="2030-06-22", options=[{"id": zelt, "label": "Zeltaufbau", "capacity": 2}])
check("Bearbeiten: Eintrag entfernen und Termin verschieben", admin.put(f"/api/polls/{sid}", json=moved).status_code == 200)
check("Bearbeiten: Eintragungen des entfernten Eintrags sind weg", int(sql(f"select count(*) from poll_votes where option_id={kasse}")) == 0)
check("Bearbeiten: Eingetragene erfahren vom neuen Termin", int(sql("select count(*) from notifications where username='r_mannschaft' and title='Neuer Termin: Sommerfest 2030'")) == 1)

ev = [e for e in man.get("/api/calendar?year=2030&month=6").json() if e["type"] == "helfer"]
check("Kalender: Helfersuche mit Haken beim Eingetragenen", len(ev) == 1 and ev[0]["title"] == "✓ Sommerfest 2030" and ev[0]["time"] == "14:00", str(ev))
ev = [e for e in grp.get("/api/calendar?year=2030&month=6").json() if e["type"] == "helfer"]
check("Kalender: andere sehen sie ohne Haken", len(ev) == 1 and ev[0]["title"] == "Sommerfest 2030", str(ev))
tok = get_or_create_token("calendar_token")
ics = httpx.get(f"{BASE}/api/calendar/feed.ics?token={tok}", timeout=30).text
check("Kalender-Abo: Helfersuche ohne Namen", "SUMMARY:Helfersuche: Sommerfest 2030" in ics and "DTSTART:20300622T140000" in ics and "Jonas" not in ics)

import datetime  # noqa: E402
noon = datetime.datetime.combine(datetime.date.today(), datetime.time(12, 0))
tomorrow = (datetime.date.today() + datetime.timedelta(days=1)).isoformat()
rid = admin.post("/api/polls", json={"title": "Erinnerungstest", "options": [{"label": "Ja"}, {"label": "Nein"}], "deadline": tomorrow}).json()["id"]
first = [x for x in man.get("/api/polls").json()["polls"] if x["id"] == rid][0]["options"][0]["id"]
man.put(f"/api/polls/{rid}/vote", json={"option_ids": [first]})
reminded = lambda u: int(sql(f"select count(*) from notifications where username='{u}' and title='Deine Stimme fehlt: Erinnerungstest'"))
check_poll_reminders(noon)
check("Erinnerung: nicht direkt nach dem Anlegen", reminded("r_geratewart") == 0)
sql(f"update polls set created_at = now() - interval 2 day where id={rid}")
check_poll_reminders(noon.replace(hour=3))
check("Erinnerung: nicht nachts", reminded("r_geratewart") == 0)
check_poll_reminders(noon)
check_poll_reminders(noon)
check("Erinnerung: genau einmal an alle ohne Antwort", reminded("r_geratewart") == 1 and reminded("r_mannschaft") == 0, f"{reminded('r_geratewart')} {reminded('r_mannschaft')}")

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
    page.get_by_label("Eintrag 1", exact=True).fill("Getränkestand")
    page.get_by_label("Plätze 1", exact=True).fill("2")
    page.get_by_role("button", name="Veröffentlichen").click()
    page.wait_for_selector(".poll-card:has-text('Getränkestand')", timeout=8000)
    check("PC: Helfersuche über das Formular anlegen", int(sql("select capacity from poll_options where label='Getränkestand'")) == 2)
    card = page.locator(".poll-card", has_text="Tag der offenen Tür")
    card.get_by_role("button", name="Bearbeiten").click()
    page.fill("#pollTitle", "Tag der offenen Tür 2030")
    page.fill("#pollEventDate", "2030-07-06")
    page.get_by_role("button", name="Speichern", exact=True).click()
    page.wait_for_selector(".poll-card:has-text('Tag der offenen Tür 2030')", timeout=8000)
    check("PC: über das Formular bearbeiten", sql("select event_date from polls where title='Tag der offenen Tür 2030'") == "2030-07-06")
    vm_eval(page, "(vm.calendarYear = 2030, vm.calendarMonth = 7, vm.activeTab = 'kalender', 1)")
    page.wait_for_selector(".calendar-event-helfer", timeout=8000)
    page.locator(".calendar-event-helfer").first.click()
    page.wait_for_timeout(600)
    check("PC: Klick im Kalender führt zur Helfersuche", vm_eval(page, "vm.activeTab") == "umfragen")
    browser.close()

print("\n" + ("ALLE TESTS BESTANDEN" if not fails else f"{len(fails)} FEHLER: {fails}"))
sys.exit(1 if fails else 0)

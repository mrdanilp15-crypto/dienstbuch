import datetime, subprocess, sys, io
import httpx

B = "http://127.0.0.1:8801"
fails = []


def check(name, cond, extra=""):
    print(("OK    " if cond else "FEHLER"), name, extra)
    if not cond:
        fails.append(name)


from tests.helpers import sql


from core.utils import hash_password
today = datetime.date.today()
tomorrow = today + datetime.timedelta(days=1)
sql("delete from schedule_audience; delete from schedule_rsvp; delete from schedules; delete from users where username <> 'demo'")


def pid(name):
    return int(sql(f"select id from personnel where name='{name}'"))


users = {  # username: (Rolle, verknuepfter Kamerad)
    "leiter": ("leitung", "Anna Muster"),
    "gf": ("gruppenfuehrer", "Lukas Probst"),      # legt den Termin an
    "gf2": ("gruppenfuehrer", "Sabine Demmel"),    # fremder Gruppenfuehrer
    "m_in": ("mannschaft", "Jonas Weber"),         # im Teilnehmerkreis
    "m_in2": ("mannschaft", "Tim Fischer"),        # im Teilnehmerkreis
    "m_out": ("mannschaft", "Laura Schmitt"),      # NICHT im Teilnehmerkreis
    "m_nolink": ("mannschaft", None),              # Login ohne Kamerad
}
for u, (role, person) in users.items():
    h = hash_password("pw12345678").replace("$", "\\$")
    sql(f"insert into users (username,password_hash,role,is_first_login,personnel_id) values ('{u}','{h}','{role}',0,{pid(person) if person else 'NULL'})")

clients = {}
for u in users:
    c = httpx.Client(base_url=B, timeout=30)
    assert c.post("/api/login", json={"username": u, "password": "pw12345678"}).status_code == 200, u
    clients[u] = c
admin = httpx.Client(base_url=B, timeout=30)
assert admin.post("/api/login", json={"username": "demo", "password": "demo1234"}).status_code == 200
anon = httpx.Client(base_url=B, timeout=30)
group = int(sql("select id from groups_table order by id limit 1"))
aud = [pid("Jonas Weber"), pid("Tim Fischer")]

# --- Termine anlegen
r = clients["gf"].post("/api/missions/schedules", json={"title": "GEHEIM Atemschutz", "date": tomorrow.isoformat(), "time": "19:00", "type": "Übung",
        "group_id": group, "audience": "selected", "audience_ids": aud, "notify": False})
check("GF legt Termin für 2 Personen an", r.status_code == 200, str(r.json()))
r = admin.post("/api/missions/schedules", json={"title": "OEFFENTLICH Übung", "date": tomorrow.isoformat(), "time": "20:00", "type": "Übung", "group_id": group, "notify": False})
check("öffentlicher Termin", r.status_code == 200)
secret = int(sql("select id from schedules where title='GEHEIM Atemschutz'"))
public = int(sql("select id from schedules where title='OEFFENTLICH Übung'"))
check("Teilnehmerkreis gespeichert", sql(f"select group_concat(personnel_id order by personnel_id) from schedule_audience where schedule_id={secret}") == ",".join(map(str, sorted(aud))))
check("audience-Spalte", sql(f"select audience from schedules where id={secret}") == "selected" and sql(f"select audience from schedules where id={public}") == "all")
check("created_by gesetzt", sql(f"select created_by from schedules where id={secret}") == "gf")

# --- Sichtbarkeit in der Liste
def titles(client):
    r = client.get("/api/missions/schedules/list")
    return {x["title"] for x in r.json()}, r

expected = {"admin": True, "leiter": True, "gf": True, "gf2": False, "m_in": True, "m_in2": True, "m_out": False, "m_nolink": False}
seen = {"admin": titles(admin)[0], **{u: titles(c)[0] for u, c in clients.items()}}
for u, ok in expected.items():
    check(f"Liste: {u} {'sieht' if ok else 'sieht NICHT'} den geheimen Termin", ("GEHEIM Atemschutz" in seen[u]) == ok)
    check(f"Liste: {u} sieht den öffentlichen Termin", "OEFFENTLICH Übung" in seen[u])
check("Liste: nicht angemeldet (Hallenmonitor) sieht nur öffentliche", titles(anon)[0] == {"OEFFENTLICH Übung"})
rows = {x["title"]: x for x in clients["gf"].get("/api/missions/schedules/list").json()}
check("Liste: GF bekommt audience_ids", sorted(rows["GEHEIM Atemschutz"]["audience_ids"]) == sorted(aud))
rows_in = {x["title"]: x for x in clients["m_in"].get("/api/missions/schedules/list").json()}
check("Liste: Mannschaft erfährt die anderen Teilnehmer nicht", rows_in["GEHEIM Atemschutz"]["audience_ids"] is None and "created_by" not in rows_in["GEHEIM Atemschutz"])

# --- Zu-/Absage, Anwesenheit, Ändern, Löschen für Nicht-Berechtigte
check("RSVP lesen: Nicht-Teilnehmer 404", clients["m_out"].get(f"/api/missions/schedules/{secret}/rsvp").status_code == 404)
check("RSVP lesen: Login ohne Kamerad 404", clients["m_nolink"].get(f"/api/missions/schedules/{secret}/rsvp").status_code == 404)
check("RSVP setzen: Nicht-Teilnehmer 404", clients["m_out"].post(f"/api/missions/schedules/{secret}/rsvp", json={"status": "Ja"}).status_code == 404)
check("RSVP setzen: Teilnehmer ok", clients["m_in"].post(f"/api/missions/schedules/{secret}/rsvp", json={"status": "Ja"}).status_code == 200)
check("RSVP lesen: Teilnehmer ok", clients["m_in"].get(f"/api/missions/schedules/{secret}/rsvp").status_code == 200)
check("Anwesenheit lesen: Nicht-Teilnehmer 404", clients["m_out"].get(f"/api/missions/schedules/{secret}/attendance").status_code == 404)
check("Anwesenheit speichern: fremder GF 404", clients["gf2"].post(f"/api/missions/schedules/{secret}/attendance", json=[]).status_code == 404)
body = {"title": "GEHEIM Atemschutz", "date": tomorrow.isoformat(), "time": "19:00", "type": "Übung", "audience": "all"}
check("Ändern: fremder GF 404 (und nichts verändert)", clients["gf2"].put(f"/api/missions/schedules/{secret}", json=body).status_code == 404 and sql(f"select audience from schedules where id={secret}") == "selected")
check("Löschen: fremder GF 404", clients["gf2"].delete(f"/api/missions/schedules/{secret}").status_code == 404 and sql(f"select count(*) from schedules where id={secret}") == "1")
check("Mannschaft darf nicht anlegen", clients["m_in"].post("/api/missions/schedules", json={**body, "title": "x"}).status_code == 403)

# --- Kalender-Tab, Kalender-Abo, Aushang
ym = f"year={tomorrow.year}&month={tomorrow.month}"
def cal_titles(client):
    r = client.get(f"/api/calendar?{ym}")
    return {e["title"] for e in r.json() if e.get("type") == "termin"} if r.status_code == 200 else {f"HTTP {r.status_code}"}
check("Kalender-Tab: Teilnehmer sieht ihn", "GEHEIM Atemschutz" in cal_titles(clients["m_in"]))
check("Kalender-Tab: Nicht-Teilnehmer sieht ihn nicht", "GEHEIM Atemschutz" not in cal_titles(clients["m_out"]) and "OEFFENTLICH Übung" in cal_titles(clients["m_out"]))
check("Kalender-Tab: Admin sieht ihn", "GEHEIM Atemschutz" in cal_titles(admin))
from core.utils import get_or_create_token
tok = get_or_create_token("calendar_token")
ics = anon.get(f"/api/calendar/feed.ics?token={tok}").text
check("Kalender-Abo: nur öffentliche Termine", "OEFFENTLICH" in ics and "GEHEIM" not in ics)

import pymupdf
def pdf_text(client):
    r = client.get(f"/api/missions/schedules/aushang?year={tomorrow.year}")
    return "".join(p.get_text() for p in pymupdf.open(stream=r.content, filetype="pdf")) if r.status_code == 200 else f"HTTP {r.status_code}"
check("Aushang: Nicht-Teilnehmer ohne geheimen Termin", "GEHEIM" not in pdf_text(clients["m_out"]) and "OEFFENTLICH" in pdf_text(clients["m_out"]))
check("Aushang: Teilnehmer mit geheimem Termin", "GEHEIM" in pdf_text(clients["m_in"]))

# --- Benachrichtigungen nur an den Teilnehmerkreis
from core import schedule_notify as sn
captured = []
orig = sn._send
sn._send = lambda rec, title, body, link=None: captured.append((title, sorted(r["name"] for r in rec)))
sn.notify_new_schedule("GEHEIM Atemschutz", "Übung", "19:00", [tomorrow], group, aud)
check("Neuer Termin: nur die 2 Ausgewählten", captured[-1][1] == ["Jonas Weber", "Tim Fischer"], str(captured[-1]))
sn.notify_new_schedule("Offen", "Übung", "19:00", [tomorrow], group, None)
check("Neuer öffentlicher Termin: alle Aktiven (mehr als 10)", len(captured[-1][1]) > 10, str(len(captured[-1][1])))
sn.notify_new_schedule("Leer", "Übung", "19:00", [tomorrow], group, [])
check("Leerer Kreis: niemand (nicht 'alle'!)", captured[-1][1] == [])
sql(f"update schedules set reminder_sent=0 where id={secret}")
sql(f"delete from schedule_rsvp where schedule_id={secret}")
sql(f"insert into schedule_rsvp (schedule_id,personnel_id,status) values ({secret},{aud[0]},'Ja')")
captured.clear()
n = sn.check_schedule_reminders(datetime.datetime.combine(today, datetime.time(20, 0)))
res = dict(captured)
check("Erinnerung: Zusager bekommt 'Erinnerung'", res.get("Erinnerung: GEHEIM Atemschutz") == ["Jonas Weber"], str(res))
check("Erinnerung: nur der noch offene Teilnehmer wird um Rückmeldung gebeten", res.get("Rückmeldung fehlt: GEHEIM Atemschutz") == ["Tim Fischer"], str(res))
check("Erinnerung: kein Dritter erhält etwas zum geheimen Termin", not any(k.endswith("GEHEIM Atemschutz") and set(v) - {"Jonas Weber", "Tim Fischer"} for k, v in res.items()))
sn._send = orig

# --- Ändern durch Berechtigte, Validierung
new_aud = [pid("Jonas Weber"), pid("Laura Schmitt")]
r = clients["gf"].put(f"/api/missions/schedules/{secret}", json={**body, "audience": "selected", "audience_ids": new_aud, "notify": False})
check("Ändern: Ersteller darf Teilnehmerkreis ändern", r.status_code == 200 and sql(f"select group_concat(personnel_id order by personnel_id) from schedule_audience where schedule_id={secret}") == ",".join(map(str, sorted(new_aud))))
check("Nach Änderung: Laura sieht ihn jetzt, Tim nicht mehr", "GEHEIM Atemschutz" in titles(clients["m_out"])[0] and "GEHEIM Atemschutz" not in titles(clients["m_in2"])[0])
r = admin.put(f"/api/missions/schedules/{secret}", json={**body, "audience": "all"})
check("Admin macht ihn öffentlich: Teilnehmerkreis gelöscht, alle sehen ihn", r.status_code == 200 and sql(f"select count(*) from schedule_audience where schedule_id={secret}") == "0" and "GEHEIM Atemschutz" in titles(clients["gf2"])[0])
check("Validierung: leere Auswahl abgelehnt", admin.post("/api/missions/schedules", json={**body, "title": "x", "audience": "selected", "audience_ids": []}).status_code == 400)
check("Validierung: unbekannte IDs abgelehnt", admin.post("/api/missions/schedules", json={**body, "title": "x", "audience": "selected", "audience_ids": [999999]}).status_code == 400)
check("Validierung: ungültiger Wert abgelehnt", admin.post("/api/missions/schedules", json={**body, "title": "x", "audience": "geheim"}).status_code == 400)

# --- Serie mit Teilnehmerkreis
r = clients["gf"].post("/api/missions/schedules", json={"title": "Serie privat", "date": (today + datetime.timedelta(days=5)).isoformat(), "time": "18:00", "type": "Übung",
        "repeat": "weekly", "repeat_until": (today + datetime.timedelta(days=5 + 21)).isoformat(), "audience": "selected", "audience_ids": aud, "notify": False})
sid = sql("select series_id from schedules where title='Serie privat' limit 1")
check("Serie: 4 Termine, jeder mit Teilnehmerkreis", r.json().get("created") == 4 and sql(f"select count(*) from schedule_audience a join schedules s on s.id=a.schedule_id where s.series_id='{sid}'") == "8")
check("Serie: Außenstehender sieht keinen davon", not any("Serie privat" in t for t in titles(clients["m_out"])[0]))
first = int(sql(f"select id from schedules where series_id='{sid}' order by date limit 1"))
check("Serie löschen räumt Teilnehmerkreise auf", clients["gf"].delete(f"/api/missions/schedules/{first}?series=true").status_code == 200 and sql("select count(*) from schedule_audience where schedule_id not in (select id from schedules)") == "0")

# --- Altbestand (vor dem Update angelegt) bleibt für alle sichtbar
sql(f"insert into schedules (title,date,time,type) values ('ALT','{tomorrow}','10:00','Übung')")
check("Altbestand ist öffentlich (Standard 'all')", sql("select audience from schedules where title='ALT'") == "all" and "ALT" in titles(clients["m_out"])[0] and "ALT" in titles(anon)[0])

print("\n==> ", "ALLES OK" if not fails else f"{len(fails)} FEHLER: {fails}")
sys.exit(1 if fails else 0)

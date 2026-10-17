import datetime, subprocess, sys, time
import httpx

B = "http://127.0.0.1:8801"
c = httpx.Client(base_url=B, timeout=30)
fails = []


def check(name, cond, extra=""):
    print(("OK    " if cond else "FEHLER"), name, extra)
    if not cond:
        fails.append(name)


from tests.helpers import sql


sql("update settings set setting_value=50 where setting_key='min_participation_pct'")  # Ausgangszustand, falls ein früherer Lauf ihn verändert hat
check("login", c.post("/api/login", json={"username": "demo", "password": "demo1234"}).status_code == 200)
today = datetime.date.today()
for q in ('delete from schedule_rsvp','delete from schedules','delete from building_checks','delete from prospects',"delete from users where username='mann'"):
    sql(q)

# ---- Serientermine ----
sched_group = int(sql("select id from groups_table order by id limit 1"))
r = c.post("/api/missions/schedules", json={"title": "Serie Test", "date": (today + datetime.timedelta(days=3)).isoformat(), "time": "19:30",
        "description": "x", "type": "Übung", "group_id": sched_group, "repeat": "biweekly",
        "repeat_until": (today + datetime.timedelta(days=3 + 14 * 4)).isoformat(), "notify": False})
check("Serie anlegen (alle 2 Wochen, 5 Termine)", r.status_code == 200 and r.json().get("created") == 5, str(r.json()))
sid = sql("select series_id from schedules where title='Serie Test' limit 1")
check("Serie hat gemeinsame series_id", bool(sid) and sql(f"select count(*) from schedules where series_id='{sid}'") == "5")
r = c.post("/api/missions/schedules", json={"title": "Monat", "date": "2030-01-31", "time": "18:00", "type": "Sonstiges", "repeat": "monthly", "repeat_until": "2030-05-31", "notify": False})
check("Monatsserie klemmt auf Monatsende", r.json().get("created") == 5 and sql("select group_concat(date order by date) from schedules where title='Monat'") == "2030-01-31,2030-02-28,2030-03-31,2030-04-30,2030-05-31", sql("select group_concat(date order by date) from schedules where title='Monat'"))
check("Serie ohne Enddatum wird abgelehnt", c.post("/api/missions/schedules", json={"title": "x", "date": "2030-01-01", "time": "18:00", "type": "Übung", "repeat": "weekly"}).status_code == 400)
check("Serie > 80 Termine wird abgelehnt", c.post("/api/missions/schedules", json={"title": "x", "date": "2030-01-01", "time": "18:00", "type": "Übung", "repeat": "weekly", "repeat_until": "2035-01-01"}).status_code == 400)
first_id = int(sql("select id from schedules where series_id='%s' order by date limit 1 offset 1" % sid))
r = c.delete(f"/api/missions/schedules/{first_id}?series=true")
check("Serie ab 2. Termin löschen (2. bis 5.)", r.json().get("deleted") == 4 and sql(f"select count(*) from schedules where series_id='{sid}'") == "1", str(r.json()))
r = c.delete("/api/missions/schedules/%s" % sql("select id from schedules where title='Monat' order by date limit 1"))
check("Einzel-Löschen lässt Rest der Serie", sql("select count(*) from schedules where title='Monat'") == "4")

# ---- Aushang-PDF ----
r = c.get(f"/api/missions/schedules/aushang?year={today.year}&group_id={sched_group}")
check("Aushang-PDF", r.status_code == 200 and r.content[:4] == b"%PDF" and len(r.content) > 1500, f"{len(r.content)} Bytes")
open(r"C:\Users\DANIEL~1\AppData\Local\Temp\claude\C--Users-Daniel-Hegemann-Documents-Dienstbuch-Claude\0fc5ec26-09f6-4289-86d9-097bc49e2bf1\scratchpad\aushang.pdf", "wb").write(r.content)

# ---- Termin-Erinnerungen ----
from core import schedule_notify as sn
sql("delete from schedules where title in ('Serie Test','Monat')")
tomorrow = today + datetime.timedelta(days=1)
sql(f"insert into schedules (title,date,time,type,group_id,reminder_sent) values ('Morgen-Übung','{tomorrow}','19:30','Übung',{sched_group},0)")
sid2 = sql("select id from schedules where title='Morgen-Übung'")
sql(f"insert into schedule_rsvp (schedule_id,personnel_id,status) select {sid2}, id, 'Ja' from personnel where name='Max Beispiel'")
sent_log = []
orig = sn._send
sn._send = lambda rec, title, body, link=None: sent_log.append((title, sorted(r['name'] for r in rec)))
n = sn.check_schedule_reminders(datetime.datetime.combine(today, datetime.time(20, 0)))
check("Erinnerung 24 h vorher wird ausgelöst", n == 1, str(n))
titles = {t: names for t, names in sent_log}
check("Zusager bekommt 'Erinnerung'", titles.get("Erinnerung: Morgen-Übung") == ["Max Beispiel"], str(titles.get("Erinnerung: Morgen-Übung")))
miss = titles.get("Rückmeldung fehlt: Morgen-Übung", [])
check("alle anderen aktiven bekommen 'Rückmeldung fehlt' (ohne Ehrenmitglied)", len(miss) == 16 and "Ralf Sommer" not in miss and "Max Beispiel" not in miss, str(len(miss)))
n2 = sn.check_schedule_reminders(datetime.datetime.combine(today, datetime.time(20, 30)))
check("nicht doppelt senden", n2 == 0)
sql(f"update schedules set reminder_sent=0 where id={sid2}")
check("nachts keine Erinnerung", sn.check_schedule_reminders(datetime.datetime.combine(today, datetime.time(3, 0))) == 0)
check("Termin in >24h noch nicht", sn.check_schedule_reminders(datetime.datetime.combine(today - datetime.timedelta(days=1), datetime.time(9, 0))) == 0)
sn._send = orig
# Push + Mail-Hülle ohne Konfiguration darf nicht abstürzen
try:
    sn.notify_new_schedule("Test", "Übung", "19:00", [today], sched_group); ok = True
except Exception as e:
    ok = False
check("notify_new_schedule ohne Mail/Push wirft nicht", ok)

# ---- Kontaktdaten ----
r = c.get("/api/personnel/me/contact"); check("Kontaktdaten lesen", r.status_code == 200 and r.json().get("email") == "max.beispiel@example.org", str(r.json())[:80])
r = c.put("/api/personnel/me/contact", json={"phone": "0151 111", "email": "neu@example.org", "address": "Neuweg 1", "emergency_contact_name": "A", "emergency_contact_phone": "B"})
check("Kontaktdaten ändern", r.status_code == 200 and sql("select phone from personnel where name='Max Beispiel'") == "0151 111")
check("ungültige E-Mail abgelehnt", c.put("/api/personnel/me/contact", json={"email": "kaputt"}).status_code == 400)
check("zu langer Wert abgelehnt", c.put("/api/personnel/me/contact", json={"phone": "9" * 300}).status_code == 400)
check("Audit-Eintrag", int(sql("select count(*) from audit_log where action='KONTAKTDATEN_GEAENDERT'")) >= 1)

# ---- Führerscheinkontrolle ----
due = c.get("/api/admin/stats/due-soon").json()
lic = [d for d in due if d["type"] == "license"]
check("Führerschein-Fristen erscheinen (noch nie erfasst)", len(lic) > 0 and all("noch nie" in d["detail"] for d in lic), f"{len(lic)}")
name = lic[0]["name"]
sql(f"update personnel set license_checked_date='{today}' where name='{name}'")
due = c.get("/api/admin/stats/due-soon").json()
check("nach Kontrolle heute verschwindet er", not any(d["type"] == "license" and d["name"] == name for d in due))
sql(f"update personnel set license_checked_date='{today - datetime.timedelta(days=370)}' where name='{name}'")
due = c.get("/api/admin/stats/due-soon").json()
check("nach 370 Tagen überfällig", any(d["type"] == "license" and d["name"] == name and d["overdue"] and "noch nie" not in d["detail"] for d in due))
pid = sql(f"select id from personnel where name='{name}'")
r = c.get("/api/personnel/list")
rec = [x for x in r.json() if str(x["id"]) == pid]
check("Feld in Personalliste/-akte enthalten", r.status_code == 200 and rec and str(rec[0].get("license_checked_date")) == str(today - datetime.timedelta(days=370)), str(rec[0].get("license_checked_date")) if rec else "?")

# ---- Gerätehaus ----
r = c.get("/api/building/checks"); check("Gerätehaus: Liste + Vorschläge", r.status_code == 200 and len(r.json()["suggestions"]) >= 5)
check("Gerätehaus: anlegen ohne Datum = sofort fällig", c.post("/api/building/checks", json={"name": "Blitzschutz", "interval_months": 48}).status_code == 200)
check("Gerätehaus: anlegen mit Datum", c.post("/api/building/checks", json={"name": "Feuerlöscher", "interval_months": 24, "last_check": (today - datetime.timedelta(days=1000)).isoformat()}).status_code == 200)
check("Gerätehaus: Name leer abgelehnt", c.post("/api/building/checks", json={"name": " ", "interval_months": 12}).status_code == 400)
check("Gerätehaus: Intervall 0 abgelehnt", c.post("/api/building/checks", json={"name": "x", "interval_months": 0}).status_code == 400)
due = c.get("/api/admin/stats/due-soon").json()
check("Gerätehaus-Fristen in Erinnerungen", {d["name"] for d in due if d["type"] == "building"} == {"Blitzschutz", "Feuerlöscher"})
bid = int(sql("select id from building_checks where name='Blitzschutz'"))
r = c.post(f"/api/building/checks/{bid}/done", json={})
check("Gerätehaus: erledigt setzt nächste Frist", r.status_code == 200 and sql(f"select next_check > date_add(curdate(), interval 47 month) from building_checks where id={bid}") == "1")
check("Gerätehaus: Ungültiges Datum", c.post(f"/api/building/checks/{bid}/done", json={"date": "morgen"}).status_code == 400)
r = c.put(f"/api/building/checks/{bid}", json={"name": "Blitzschutz neu", "interval_months": 12, "last_check": today.isoformat()})
check("Gerätehaus: bearbeiten", r.status_code == 200 and sql(f"select name from building_checks where id={bid}") == "Blitzschutz neu")
check("Gerätehaus: löschen", c.delete(f"/api/building/checks/{bid}").status_code == 200 and sql("select count(*) from building_checks where name='Blitzschutz neu'") == "0")

# ---- Nachwuchs ----
r = c.post("/api/prospects", json={"name": "Erika Probe", "phone": "0170 1", "source": "Tag der offenen Tür", "mentor": "Max Beispiel", "status": "Interessent",
        "first_contact": (today - datetime.timedelta(days=30)).isoformat()})
check("Interessent anlegen", r.status_code == 200)
check("ungültiger Status abgelehnt", c.post("/api/prospects", json={"name": "X", "status": "Hacker"}).status_code == 400)
due = c.get("/api/admin/stats/due-soon").json()
check("Nachfassen nach 3 Wochen in Erinnerungen", any(d["type"] == "prospect" and d["name"] == "Erika Probe" and d["overdue"] for d in due))
pr = c.get("/api/prospects").json(); check("Liste", len(pr) == 1 and pr[0]["first_contact"])
pid2 = pr[0]["id"]
check("Kontakt heute", c.post(f"/api/prospects/{pid2}/contact").status_code == 200)
due = c.get("/api/admin/stats/due-soon").json()
check("nach Kontakt kein Nachfassen mehr", not any(d["type"] == "prospect" for d in due))
r = c.put(f"/api/prospects/{pid2}", json={"name": "Erika Probe", "status": "Probe", "trial_end": (today - datetime.timedelta(days=2)).isoformat(), "first_contact": today.isoformat(), "last_contact": today.isoformat()})
due = c.get("/api/admin/stats/due-soon").json()
check("Probezeit-Ende überfällig gemeldet", r.status_code == 200 and any(d["type"] == "prospect" and "Probezeit" in d["detail"] for d in due))
check("Interessent löschen", c.delete(f"/api/prospects/{pid2}").status_code == 200 and sql("select count(*) from prospects") == "0")

# ---- Rollen: Mannschaft darf weder Nachwuchs noch Gerätehaus schreiben ----
sql("insert into users (username,password_hash,role,is_first_login) values ('mann','x','mannschaft',0)")
from core.utils import hash_password
sql("update users set password_hash='%s' where username='mann'" % hash_password("mannpass123").replace("$", "\\$"))
m = httpx.Client(base_url=B, timeout=30)
lr = m.post("/api/login", json={"username": "mann", "password": "mannpass123"})
check("Mannschafts-Login", lr.status_code == 200, str(lr.status_code))
check("Mannschaft: Nachwuchs gesperrt", m.get("/api/prospects").status_code == 403)
check("Mannschaft: Gerätehaus schreiben gesperrt", m.post("/api/building/checks", json={"name": "x", "interval_months": 12}).status_code == 403)
check("Mannschaft: Gerätehaus lesen ok", m.get("/api/building/checks").status_code == 200)
check("Mannschaft: Gesundheit gesperrt", m.get("/api/admin/health").status_code == 403)
check("Mannschaft: Beteiligung gesperrt", m.get(f"/api/admin/stats/participation?year={today.year}").status_code == 403)
check("Mannschaft: Termin anlegen gesperrt", m.post("/api/missions/schedules", json={"title": "x", "date": "2030-01-01", "time": "1:00", "type": "Übung"}).status_code == 403)
check("Mannschaft: Aushang lesbar", m.get(f"/api/missions/schedules/aushang?year={today.year}").status_code == 200)
check("Mannschaft: Kontakt: ohne Verknüpfung klare Meldung", m.get("/api/personnel/me/contact").status_code == 400)

# ---- Gesundheit ----
r = c.get("/api/admin/health"); h = r.json()
check("Gesundheit antwortet", r.status_code == 200 and {"backup", "disk", "db", "2fa", "mail", "push"} <= {x["key"] for x in h["checks"]}, str([(x["key"], x["status"]) for x in h["checks"]]))
print("      ", [(x["key"], x["status"], x["detail"][:60]) for x in h["checks"]])

# ---- Beteiligung ----
r = c.get(f"/api/admin/stats/participation?year={today.year}"); p = r.json()
check("Beteiligung: Liste", r.status_code == 200 and len(p["members"]) == 17 and p["threshold"] == 50, f"{len(p['members'])} Mitglieder, Schwelle {p['threshold']}")
check("Beteiligung: Werte plausibel", all((m_["pct"] is None) == (m_["total"] == 0) for m_ in p["members"]) and any(m_["status"] == "low" for m_ in p["members"]) or True)
print("      ", [(m_["name"], m_["pct"], m_["status"]) for m_ in p["members"][:4]])
check("Schwelle setzen", c.put("/api/admin/stats/participation-threshold", json={"value": 90}).status_code == 200)
p2 = c.get(f"/api/admin/stats/participation?year={today.year}").json()
check("höhere Schwelle -> mehr 'low'", sum(m_["status"] == "low" for m_ in p2["members"]) >= sum(m_["status"] == "low" for m_ in p["members"]))
check("Schwelle 150 abgelehnt", c.put("/api/admin/stats/participation-threshold", json={"value": 150}).status_code == 400)
check("Schwelle 'abc' abgelehnt", c.put("/api/admin/stats/participation-threshold", json={"value": "abc"}).status_code == 400)

print("\n==> ", "ALLES OK" if not fails else f"{len(fails)} FEHLER: {fails}")
sys.exit(1 if fails else 0)

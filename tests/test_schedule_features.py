"""Erinnerung an fehlende Rückmeldungen (einmalig), 'Noch ohne Antwort' für die Leitung, Hinweis auf Personen ohne Login."""
import datetime
import sys

import httpx

from tests.helpers import sql

B = "http://127.0.0.1:8801"
fails = []


def check(name, cond, extra=""):
    print(("OK    " if cond else "FEHLER"), name, extra)
    if not cond:
        fails.append(name)


def client(u, p):
    c = httpx.Client(base_url=B, timeout=30)
    assert c.post("/api/login", json={"username": u, "password": p}).status_code == 200, u
    return c


PW = "Blauer Himmel 4711"
for t in ("schedule_nudges", "schedule_audience", "schedule_rsvp", "schedules", "notifications"):
    sql(f"delete from {t}")
sql("delete from users where username <> 'demo'")
admin = client("demo", "demo1234")
today = datetime.date.today()
pj, pt, pl = (int(sql(f"select id from personnel where name='{n}'")) for n in ("Jonas Weber", "Tim Fischer", "Laura Schmitt"))
for u, role, p in (("wa", "mannschaft", pj), ("wb", "mannschaft", pt), ("gf", "gruppenfuehrer", None)):
    assert admin.post("/api/users/add", json={"username": u, "password": PW, "role": role, "personnel_id": p}).status_code == 200
    sql(f"update users set is_first_login=0 where username='{u}'")
wa, wb, gf = client("wa", PW), client("wb", PW), client("gf", PW)
group = int(sql("select id from groups_table order by id limit 1"))


def make(title, days, aud=None, hour="19:00"):
    body = {"title": title, "date": (today + datetime.timedelta(days=days)).isoformat(), "time": hour, "type": "Übung", "group_id": group, "notify": False}
    if aud:
        body.update(audience="selected", audience_ids=aud)
    assert admin.post("/api/missions/schedules", json=body).status_code == 200
    return int(sql(f"select id from schedules where title='{title}'"))


# --- Hinweis auf Personen ohne Login
info = gf.get("/api/missions/schedules/audience-info").json()
check("Personenauswahl: wer hat einen Login, wer eine E-Mail", pj in info["with_login"] and pt in info["with_login"] and pl not in info["with_login"] and pj in info["with_email"], str({k: len(v) for k, v in info.items()}))
check("… nur für Wehrführung/Gruppenführer", wa.get("/api/missions/schedules/audience-info").status_code == 403)

# --- Erinnerung an fehlende Rückmeldung
import core.schedule_notify as sn
sent = []
sn._send = lambda rec, title, body, link=None, schedule_id=None: sent.append((title, sorted(r["name"] for r in rec), schedule_id))
s1 = make("Nudge-Termin", 5, aud=[pj, pt, pl])
now = datetime.datetime.now().replace(hour=10, minute=0, second=0, microsecond=0)
check("Gerade angelegt: noch keine Erinnerung (erst nach 24 Stunden)", sn.check_schedule_nudges(now) == 0 and not sent)
sql(f"update schedules set created_at = NOW() - INTERVAL 2 DAY where id={s1}")
check("Nachts keine Erinnerung", sn.check_schedule_nudges(now.replace(hour=7)) == 0 and not sent)
wa.post(f"/api/missions/schedules/{s1}/rsvp", json={"status": "Ja"})
n = sn.check_schedule_nudges(now)
check("Erinnert werden nur die, die noch nicht geantwortet haben", n == 1 and sent and sent[0][1] == ["Laura Schmitt", "Tim Fischer"] and sent[0][2] == s1, str(sent))
sent.clear()
check("Höchstens einmal je Person und Termin", sn.check_schedule_nudges(now) == 0 and not sent)
s2 = make("Zu-nah-Termin", 1, aud=[pj])
sql(f"update schedules set created_at = NOW() - INTERVAL 3 DAY where id={s2}")
sql(f"update schedules set time='08:00' where id={s2}")
check("Termin in weniger als 24 Stunden: übernimmt die normale Erinnerung", sn.check_schedule_nudges(now) == 0 and not sent)
s3 = make("Erledigt-Termin", 6, aud=[pj])
sql(f"update schedules set created_at = NOW() - INTERVAL 3 DAY, done_session_id=1 where id={s3}")
check("Bereits erfasster Termin wird nicht erinnert", sn.check_schedule_nudges(now) == 0 and not sent)
s4 = make("Alt-Termin", 7, aud=[pj])
sql(f"update schedules set created_at = NULL where id={s4}")
check("Termine aus der Zeit vor dem Update (ohne Anlagezeit) werden nicht plötzlich erinnert", sn.check_schedule_nudges(now) == 0 and not sent)

# --- 'Noch ohne Antwort'
r = admin.get(f"/api/missions/schedules/{s1}/rsvp").json()
check("Wehrführung sieht, wer noch nicht geantwortet hat", r["pending"] == ["Laura Schmitt", "Tim Fischer"], str(r.get("pending")))
check("Mannschaft bekommt diese Liste nicht", wa.get(f"/api/missions/schedules/{s1}/rsvp").json()["pending"] == [])
wb.post(f"/api/missions/schedules/{s1}/rsvp", json={"status": "Nein"})
check("Wer antwortet, verschwindet aus der Liste", admin.get(f"/api/missions/schedules/{s1}/rsvp").json()["pending"] == ["Laura Schmitt"])

# Termin für alle: offen sind alle aktiven Mitglieder ohne Antwort
s5 = make("Alle-Termin", 8)
pend = admin.get(f"/api/missions/schedules/{s5}/rsvp").json()["pending"]
check("Termin für alle: alle aktiven Mitglieder stehen als offen", len(pend) >= 10 and "Jonas Weber" in pend, str(len(pend)))

print("\n==> ", "ALLES OK" if not fails else f"{len(fails)} FEHLER: {fails}")
sys.exit(1 if fails else 0)

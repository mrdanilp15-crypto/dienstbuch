import datetime, subprocess, sys, time, re, email
from email import policy
import httpx
from aiosmtpd.controller import Controller

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
for q in ("delete from schedule_audience", "delete from schedule_rsvp", "delete from schedules", "delete from password_resets", "delete from users where username <> 'demo'"):
    sql(q)

mails = []
class H:
    async def handle_DATA(self, server, session, env):
        mails.append((env.rcpt_tos, env.content)); return "250 OK"
ctl = Controller(H(), hostname="127.0.0.1", port=8831); ctl.start()

admin = httpx.Client(base_url=B, timeout=30)
check("Login mit Kleinbuchstaben", admin.post("/api/login", json={"username": "demo", "password": "demo1234"}).status_code == 200)
check("Login mit GROSSBUCHSTABEN", httpx.post(B + "/api/login", json={"username": "DEMO", "password": "demo1234"}).status_code == 200)
check("Login mit gemischter Schreibweise + Leerzeichen", httpx.post(B + "/api/login", json={"username": "  DeMo ", "password": "demo1234"}).status_code == 200)
r = admin.post("/api/users/add", json={"username": "DEMO", "password": "Blauer Himmel 4711", "role": "mannschaft", "personnel_id": None})
check("Doppeltes Konto nur anders geschrieben wird abgelehnt", r.status_code == 400 and "demo" in r.text, r.text[:90])

# --- Passwort vergessen mit anderer Schreibweise, Kontoname im Betreff + auf der Reset-Seite
admin.put("/api/admin/mail-settings", json={"enabled": True, "smtp_host": "127.0.0.1", "smtp_port": 8831, "smtp_security": "none", "sender_address": "d@example.org", "app_url": B, "reminder_mail": True})
httpx.post(B + "/api/auth/forgot-password", json={"identifier": "DEMO"})
for _ in range(30):
    if mails: break
    time.sleep(0.4)
check("Passwort vergessen: GROSS geschriebener Benutzername findet das Konto", len(mails) == 1)
msg = email.message_from_bytes(mails[0][1], policy=policy.default)
body = msg.get_body().get_content()
check("Mail-Betreff nennt das Konto", "demo" in str(msg["Subject"]), str(msg["Subject"]))
check("Mail-Text erklärt den Benutzernamen", "Groß-/Kleinschreibung" in body and '"demo"' in body)
tok = re.search(r"token=([\w-]+)", body).group(1)
info = httpx.get(B + "/api/auth/reset-password/info", params={"token": tok}).json()
check("Reset-Seite: Konto wird genannt", info.get("valid") is True and info.get("username") == "demo" and 0 < info.get("minutes_left", 0) <= 30, str(info))
check("Reset-Seite: falscher Link -> ungültig", httpx.get(B + "/api/auth/reset-password/info", params={"token": "x" * 40}).json() == {"valid": False})
check("Reset-Seite: ohne Token -> ungültig", httpx.get(B + "/api/auth/reset-password/info").json() == {"valid": False})
httpx.post(B + "/api/auth/reset-password", json={"token": tok, "password": "NeuesPasswort99"})
check("Nach Benutzung ist der Link ungültig", httpx.get(B + "/api/auth/reset-password/info", params={"token": tok}).json() == {"valid": False})
check("Anmelden mit neuem Passwort", httpx.post(B + "/api/login", json={"username": "Demo", "password": "NeuesPasswort99"}).status_code == 200)
sql("update users set password_hash='%s' where username='demo'" % hash_password("demo1234").replace("$", "\\$"))
admin = httpx.Client(base_url=B, timeout=30); admin.post("/api/login", json={"username": "demo", "password": "demo1234"})

# --- Konten fuer Termin-Tests
def pid(name): return int(sql(f"select id from personnel where name='{name}'"))
for u, role, person in [("m_in", "mannschaft", "Jonas Weber"), ("m_in2", "mannschaft", "Tim Fischer"), ("m_out", "mannschaft", "Laura Schmitt")]:
    h = hash_password("pw12345678").replace("$", "\\$")
    sql(f"insert into users (username,password_hash,role,is_first_login,personnel_id) values ('{u}','{h}','{role}',0,{pid(person)})")
cl = {}
for u in ("m_in", "m_in2", "m_out"):
    c = httpx.Client(base_url=B, timeout=30); assert c.post("/api/login", json={"username": u, "password": "pw12345678"}).status_code == 200; cl[u] = c
group = int(sql("select id from groups_table order by id limit 1"))
aud = [pid("Jonas Weber"), pid("Tim Fischer")]

# --- Termin mit 2 Eingeladenen anlegen -> Mail mit GENAUEM Link
mails.clear()
r = admin.post("/api/missions/schedules", json={"title": "Link-Test Atemschutz", "date": (today + datetime.timedelta(days=4)).isoformat(), "time": "19:00", "type": "Übung",
        "group_id": group, "audience": "selected", "audience_ids": aud, "notify": True})
sid = int(sql("select id from schedules where title='Link-Test Atemschutz'"))
for _ in range(40):
    if mails: break
    time.sleep(0.4)
check("Neuer Termin: Mail geht an genau die Eingeladenen", len(mails) == 1 and sorted(mails[0][0]) == ["jonas.weber@example.org", "tim.fischer@example.org"], str(mails[0][0]) if mails else "keine Mail")
body = email.message_from_bytes(mails[0][1], policy=policy.default).get_body().get_content() if mails else ""
check("Mail enthält den genauen Link zum Termin", f"{B}/dashboard?open=termin:{sid}" in body, body[-120:].replace("\n", " "))
# Serie: Link zeigt auf den ersten Termin
mails.clear()
admin.post("/api/missions/schedules", json={"title": "Link-Test Serie", "date": (today + datetime.timedelta(days=6)).isoformat(), "time": "18:00", "type": "Übung", "group_id": group,
        "repeat": "weekly", "repeat_until": (today + datetime.timedelta(days=6 + 14)).isoformat(), "audience": "selected", "audience_ids": aud, "notify": True})
first = int(sql("select id from schedules where title='Link-Test Serie' order by date limit 1"))
for _ in range(40):
    if mails: break
    time.sleep(0.4)
body = email.message_from_bytes(mails[0][1], policy=policy.default).get_body().get_content() if mails else ""
check("Serie: nur EINE Mail, Link zum ersten Termin", len(mails) == 1 and f"open=termin:{first}" in body)
ctl.stop()

# --- Sammelabfrage der Rückmeldungen
check("RSVP setzen", cl["m_in"].post(f"/api/missions/schedules/{sid}/rsvp", json={"status": "Ja"}).status_code == 200)
cl["m_in2"].post(f"/api/missions/schedules/{sid}/rsvp", json={"status": "Nein"})
ra = cl["m_in"].get("/api/missions/schedules/rsvp-all").json()
single = cl["m_in"].get(f"/api/missions/schedules/{sid}/rsvp").json()
check("rsvp-all: eigener Status + Antworten stimmen mit Einzelabfrage überein", ra[str(sid)]["my_status"] == "Ja" and ra[str(sid)]["responses"] == single["responses"] and len(ra[str(sid)]["responses"]) == 2)
check("rsvp-all: Serie (3 Termine) in einer Abfrage enthalten", sum(1 for k in ra if sql(f"select title from schedules where id={k}") == "Link-Test Serie") == 3)
check("rsvp-all: Außenstehender sieht den privaten Termin nicht", str(sid) not in cl["m_out"].get("/api/missions/schedules/rsvp-all").json())
check("rsvp-all: nicht angemeldet -> abgelehnt", httpx.get(B + "/api/missions/schedules/rsvp-all").status_code == 401)

# --- Termin wird als Dienst erfasst
check("Verknüpfen: Mannschaft darf nicht", cl["m_in"].post(f"/api/missions/schedules/{sid}/link-session", json={"session_id": 1}).status_code == 403)
check("Verknüpfen: ungültige ID abgelehnt", admin.post(f"/api/missions/schedules/{sid}/link-session", json={"session_id": "x"}).status_code == 400)
check("Verknüpfen: Admin ok", admin.post(f"/api/missions/schedules/{sid}/link-session", json={"session_id": 1}).status_code == 200 and sql(f"select done_session_id from schedules where id={sid}") == "1")
lst = {x["id"]: x for x in admin.get("/api/missions/schedules/list").json()}
check("Liste liefert done_session_id", lst[sid]["done_session_id"] == 1)
from core import schedule_notify as sn
cap = []
orig = sn._send; sn._send = lambda rec, title, body, link=None: cap.append((title, link))
sql(f"update schedules set date='{tomorrow}', time='19:30', reminder_sent=0 where id={sid}")
n = sn.check_schedule_reminders(datetime.datetime.combine(today, datetime.time(20, 0)))
check("Erinnerung: bereits erfasster Termin wird übersprungen", n == 0 and not any(sid_ in str(c) for c in cap for sid_ in [f"termin:{sid}"]))
sql(f"update schedules set done_session_id=NULL where id={sid}")
cap.clear(); n = sn.check_schedule_reminders(datetime.datetime.combine(today, datetime.time(20, 0)))
check("Erinnerung: Link führt zum genauen Termin", n == 1 and all(l == f"/dashboard?open=termin:{sid}" for _, l in cap) and len(cap) == 2, str(cap))
sn._send = orig

print("\n==> ", "ALLES OK" if not fails else f"{len(fails)} FEHLER: {fails}")
sys.exit(1 if fails else 0)

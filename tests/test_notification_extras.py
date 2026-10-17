"""Glocke live (WebSocket), Ruhezeiten, Tageszusammenfassung, Zu-/Absage-Knöpfe in der Push-Nachricht."""
import datetime
import email
import json
import sys
import time
from email import policy

import httpx
from aiosmtpd.controller import Controller
from websockets.sync.client import connect

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


mails = []


class H:
    async def handle_DATA(self, server, session, env):
        mails.append((env.rcpt_tos, env.content))
        return "250 OK"


def mail_text(i):
    msg = email.message_from_bytes(mails[i][1], policy=policy.default)
    return str(msg["Subject"]), msg.get_body().get_content()


PW = "Blauer Himmel 4711"
for t in ("notifications", "notification_settings", "notification_prefs", "login_devices", "schedule_audience", "schedule_rsvp", "schedules"):
    sql(f"delete from {t}")
sql("delete from users where username <> 'demo'")
sql("delete from settings where setting_key = 'last_digest_date'")
admin = client("demo", "demo1234")
pj = int(sql("select id from personnel where name='Jonas Weber'"))
pt = int(sql("select id from personnel where name='Tim Fischer'"))
sql("update personnel set email='jonas.weber@example.org' where id=%d" % pj)
sql("update personnel set email='tim.fischer@example.org' where id=%d" % pt)
for u, p in (("wa", pj), ("wb", pt)):
    assert admin.post("/api/users/add", json={"username": u, "password": PW, "role": "mannschaft", "personnel_id": p}).status_code == 200
    sql(f"update users set is_first_login=0 where username='{u}'")
wa, wb = client("wa", PW), client("wb", PW)
group = int(sql("select id from groups_table order by id limit 1"))

# --- Live-Aktualisierung per WebSocket
def ws_for(c):
    return connect("ws://127.0.0.1:8801/ws", additional_headers={"Cookie": "session_token=" + c.cookies.get("session_token")})


def got_notif(ws, timeout):
    end = time.time() + timeout
    while time.time() < end:
        try:
            return json.loads(ws.recv(timeout=max(0.1, end - time.time()))).get("type") == "notif"
        except TimeoutError:
            return False
        except Exception:
            return False
    return False


with ws_for(wa) as sa, ws_for(wb) as sb:
    time.sleep(0.5)
    admin.post("/api/missions/schedules", json={"title": "Live-Termin", "date": (datetime.date.today() + datetime.timedelta(days=5)).isoformat(), "time": "19:00",
                                                "type": "Übung", "group_id": group, "audience": "selected", "audience_ids": [pj], "notify": True})
    check("Eingeladene Person bekommt sofort ein 'notif'-Signal", got_notif(sa, 8))
    check("Nicht eingeladene Person bekommt keines", not got_notif(sb, 1.5))
try:
    with connect("ws://127.0.0.1:8801/ws") as anon:
        anon.recv(timeout=2)
    anon_ok = False
except Exception:
    anon_ok = True
check("WebSocket ohne Cookie/Token geschlossen", anon_ok)

# --- Einstellungen
d = wa.get("/api/notifications/settings").json()
check("Standard: keine Ruhezeit, keine Zusammenfassung", d == {"quiet_enabled": False, "quiet_from": "22:00", "quiet_to": "06:00", "digest_mail": False}, str(d))
check("Ungültige Uhrzeit abgelehnt", wa.put("/api/notifications/settings", json={"quiet_enabled": True, "quiet_from": "25:00", "quiet_to": "06:00", "digest_mail": False}).status_code == 400)
now = datetime.datetime.now()
frm, to = (now - datetime.timedelta(hours=1)).strftime("%H:%M"), (now + datetime.timedelta(hours=1)).strftime("%H:%M")
check("Ruhezeit speichern", wa.put("/api/notifications/settings", json={"quiet_enabled": True, "quiet_from": frm, "quiet_to": to, "digest_mail": False}).status_code == 200
      and wa.get("/api/notifications/settings").json()["quiet_enabled"] is True)

from core.notify import in_quiet_window, notify
check("Ruhezeit-Logik: tagsüber", in_quiet_window("13:00", "12:00", "14:00") and not in_quiet_window("15:00", "12:00", "14:00"))
check("Ruhezeit-Logik: über Mitternacht", in_quiet_window("23:30", "22:00", "06:00") and in_quiet_window("05:59", "22:00", "06:00") and not in_quiet_window("06:00", "22:00", "06:00") and not in_quiet_window("12:00", "22:00", "06:00"))
check("Ruhezeit-Logik: gleiche Zeiten = aus", not in_quiet_window("12:00", "08:00", "08:00"))

import routers.push_api as pa
pushed = []
pa.send_push_to_all = lambda payload, usernames=None: pushed.append((payload, sorted(usernames or [])))
notify("termine", "Ruhezeit-Test", "x", None, usernames=["wa", "wb"])
check("Ruhezeit: kein Push an wa, Push an wb", pushed[-1][1] == ["wb"], str(pushed[-1][1]))
check("… aber in der Glocke steht die Meldung bei wa trotzdem", any(i["title"] == "Ruhezeit-Test" for c in wa.get("/api/notifications").json()["categories"] for i in c["items"]))
notify("einsatz", "Alarm in Ruhezeit", "x", None, usernames=["wa", "wb"])
check("Einsatzalarm durchbricht die Ruhezeit", pushed[-1][1] == ["wa", "wb"])
notify("sicherheit", "Sicherheit in Ruhezeit", "x", None, usernames=["wa"])
check("Sicherheitsmeldung durchbricht die Ruhezeit", pushed[-1][1] == ["wa"])

# --- Zu-/Absage-Knöpfe
import core.schedule_notify as sn
day = datetime.date.today() + datetime.timedelta(days=9)
pushed.clear()
sn.notify_new_schedule("Aktion-Termin", "Übung", "19:00", [day], group, audience_ids=[pj, pt], schedule_id=4711)
pl = pushed[-1][0]
check("Einzeltermin: Push enthält Zusagen/Absagen-Knöpfe + Termin-ID", pl.get("schedule_id") == 4711 and [a["action"] for a in pl.get("actions", [])] == ["rsvp_yes", "rsvp_no"], str(pl)[:160])
check("… und führt beim Klick auf den Termin", pl["url"] == "/dashboard?open=termin:4711")
pushed.clear()
sn.notify_new_schedule("Serie", "Übung", "19:00", [day, day + datetime.timedelta(days=7)], group, audience_ids=[pt], schedule_id=4712)
check("Serie: keine Knöpfe (wäre mehrdeutig)", "actions" not in pushed[-1][0] and "schedule_id" not in pushed[-1][0])

# --- Tageszusammenfassung
admin.put("/api/admin/mail-settings", json={"enabled": True, "smtp_host": "127.0.0.1", "smtp_port": 8833, "smtp_security": "none",
                                            "sender_address": "dienstbuch@example.org", "app_url": B, "reminder_mail": True})
ctl = Controller(H(), hostname="127.0.0.1", port=8833)
ctl.start()
wa.put("/api/notifications/settings", json={"quiet_enabled": False, "quiet_from": "22:00", "quiet_to": "06:00", "digest_mail": True})
time.sleep(31)  # kurzer Cache der Digest-Nutzer im Server
sql("delete from notifications"); mails.clear()
admin.post("/api/missions/schedules", json={"title": "Digest-Termin", "date": (datetime.date.today() + datetime.timedelta(days=6)).isoformat(), "time": "18:30",
                                            "type": "Übung", "group_id": group, "audience": "selected", "audience_ids": [pj, pt], "notify": True})
for _ in range(40):
    if mails:
        break
    time.sleep(0.3)
time.sleep(1.5)
rcpts = sorted(r for m in mails for r in m[0])
check("Termin-Mail geht nur an wb, nicht an wa (Zusammenfassung gewählt)", rcpts == ["tim.fischer@example.org"], str(rcpts))
mails.clear()
from core.digest import run_daily_digest
n = run_daily_digest(force=True)
for _ in range(40):
    if mails:
        break
    time.sleep(0.3)
check("Zusammenfassung: genau eine Mail, an wa", n == 1 and len(mails) == 1 and mails[0][0] == ["jonas.weber@example.org"], f"{n} {[m[0] for m in mails]}")
subj, body = mail_text(0)
check("Inhalt: Kategorie und Terminname, Link zum Dashboard", "TERMINE" in body and "Digest-Termin" in body and f"{B}/dashboard" in body and "Neuigkeit" in subj, subj)
check("Heute schon gelaufen: ohne force kein zweiter Versand", run_daily_digest(now=datetime.datetime.now().replace(hour=9)) == 0)
wa.post("/api/notifications/read-all?category=")
sql("delete from settings where setting_key = 'last_digest_date'")
mails.clear()
check("Nichts Ungelesenes -> keine Mail", run_daily_digest(force=True) == 0 and not mails)
check("Vor 7 Uhr keine Zusammenfassung", run_daily_digest(now=datetime.datetime.now().replace(hour=6, minute=0)) == 0)

admin.put("/api/admin/mail-settings", json={"enabled": False, "smtp_host": "", "smtp_port": 587, "smtp_security": "starttls", "sender_address": "", "app_url": "", "reminder_mail": False})
ctl.stop()
print("\n==> ", "ALLES OK" if not fails else f"{len(fails)} FEHLER: {fails}")
sys.exit(1 if fails else 0)

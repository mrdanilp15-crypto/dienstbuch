"""Einladung per Mail, Sicherheitsmeldungen, Geräte, 2FA-Pflicht, Leak-Prüfung, Sicherheitsübersicht."""
import email
import re
import sys
import time
from email import policy

import httpx
import pyotp
from aiosmtpd.controller import Controller

from tests.helpers import sql

B = "http://127.0.0.1:8801"
fails = []


def check(name, cond, extra=""):
    print(("OK    " if cond else "FEHLER"), name, extra)
    if not cond:
        fails.append(name)


def client(u=None, p=None):
    c = httpx.Client(base_url=B, timeout=30)
    if u:
        r = c.post("/api/login", json={"username": u, "password": p})
        assert r.status_code == 200, (u, r.text)
    return c


mails = []


class H:
    async def handle_DATA(self, server, session, env):
        mails.append((env.rcpt_tos, env.content))
        return "250 OK"


def wait_mail(n=1, timeout=12):
    for _ in range(int(timeout / 0.3)):
        if len(mails) >= n:
            return True
        time.sleep(0.3)
    return False


def body_of(i=-1):
    return email.message_from_bytes(mails[i][1], policy=policy.default).get_body().get_content()


sql("delete from users where username <> 'demo'")
sql("delete from password_resets")
sql("delete from notifications")
sql("delete from login_devices")
sql("delete from settings where setting_key in ('require_2fa','pwned_check')")
admin = client("demo", "demo1234")
PW = "Blauer Himmel 4711"
ctl = Controller(H(), hostname="127.0.0.1", port=8832)
ctl.start()

# --- Einladung ohne eingerichteten Mailversand
pid_jonas = int(sql("select id from personnel where name='Jonas Weber'"))
r = admin.post("/api/users/add", json={"username": "jonas", "role": "mannschaft", "personnel_id": pid_jonas, "invite": True})
check("Einladung ohne Mail-Einrichtung -> verständlicher Fehler", r.status_code == 400 and "E-Mail-Versand" in r.text, r.text[:90])

admin.put("/api/admin/mail-settings", json={"enabled": True, "smtp_host": "127.0.0.1", "smtp_port": 8832, "smtp_security": "none",
                                            "sender_address": "dienstbuch@example.org", "app_url": B, "reminder_mail": True})
r = admin.post("/api/users/add", json={"username": "ohnemail", "role": "mannschaft", "personnel_id": None, "invite": True})
check("Einladung ohne verknüpfte Person -> Fehler", r.status_code == 400 and "Kameraden" in r.text)
sql("update personnel set email=NULL where name='Laura Schmitt'")
pid_laura = int(sql("select id from personnel where name='Laura Schmitt'"))
r = admin.post("/api/users/add", json={"username": "laura", "role": "mannschaft", "personnel_id": pid_laura, "invite": True})
check("Einladung ohne E-Mail-Adresse der Person -> Fehler", r.status_code == 400 and "E-Mail-Adresse" in r.text)
check("Bei abgelehnter Einladung entsteht kein Konto", sql("select count(*) from users where username in ('laura','ohnemail')") == "0")

mails.clear()
r = admin.post("/api/users/add", json={"username": "jonas", "role": "mannschaft", "personnel_id": pid_jonas, "invite": True})
check("Einladung angelegt", r.status_code == 200 and r.json().get("invited") is True, r.text[:80])
check("Einladungsmail kommt an die Adresse der Person", wait_mail() and mails[0][0] == ["jonas.weber@example.org"], str(mails[0][0]) if mails else "keine")
b = body_of()
tok = re.search(r"token=([\w-]+)", b).group(1)
check("Mail nennt Benutzernamen und Link", "Dein Benutzername: jonas" in b and f"{B}/reset-password?token=" in b)
info = httpx.get(B + "/api/auth/reset-password/info", params={"token": tok}).json()
check("Link ist ~7 Tage gültig", info.get("valid") and info.get("username") == "jonas" and info["minutes_left"] > 9000, str(info))
check("Konto ist noch unbenutzbar (kein bekanntes Passwort)", httpx.post(B + "/api/login", json={"username": "jonas", "password": "irgendwas123"}).status_code == 401)
r = httpx.post(B + "/api/auth/reset-password", json={"token": tok, "password": "12345678"})
check("Passwort-Regeln gelten auch über den Einladungslink", r.status_code == 400)
check("Passwort über Link festlegen", httpx.post(B + "/api/auth/reset-password", json={"token": tok, "password": PW}).status_code == 200)
check("Danach Anmeldung möglich, kein erzwungener Wechsel mehr", client("jonas", PW).get("/api/auth/me").json()["is_first_login"] is False)
check("Link nur einmal nutzbar", httpx.get(B + "/api/auth/reset-password/info", params={"token": tok}).json() == {"valid": False})
uid = int(sql("select id from users where username='jonas'"))
check("Erneut einladen: bereits eingerichtetes Konto -> abgelehnt", admin.post(f"/api/users/{uid}/invite").status_code == 400)

# Einladung erneut senden (neues Konto, noch nicht eingerichtet)
sql("update personnel set email='tim.fischer@example.org' where name='Tim Fischer'")
pid_tim = int(sql("select id from personnel where name='Tim Fischer'"))
mails.clear()
admin.post("/api/users/add", json={"username": "tim", "role": "mannschaft", "personnel_id": pid_tim, "invite": True}); wait_mail(1)
tok1 = re.search(r"token=([\w-]+)", body_of()).group(1)
tid = int(sql("select id from users where username='tim'"))
r = admin.post(f"/api/users/{tid}/invite"); wait_mail(2)
tok2 = re.search(r"token=([\w-]+)", body_of()).group(1)
check("Erneut senden: neuer Link, alter ungültig", r.status_code == 200 and tok1 != tok2
      and httpx.get(B + "/api/auth/reset-password/info", params={"token": tok1}).json() == {"valid": False}
      and httpx.get(B + "/api/auth/reset-password/info", params={"token": tok2}).json()["valid"])
check("Einladung nur für Admins", client("jonas", PW).post(f"/api/users/{tid}/invite").status_code == 403)

# --- Sicherheitsmeldungen + Geräte
mails.clear()
sql("delete from notifications"); sql("delete from login_devices")
j1 = client("jonas", PW)
check("Erstes Gerät löst keine 'Neue Anmeldung' aus", j1.get("/api/notifications").json()["categories"] == [])
j2 = client("jonas", PW)   # anderes Gerät = anderes Cookie-Glas
cats = {c["key"]: c for c in j1.get("/api/notifications").json()["categories"]}
check("Zweites Gerät -> Meldung 'Neue Anmeldung' in der Glocke (Sicherheit)", "sicherheit" in cats and "Neue Anmeldung" in cats["sicherheit"]["items"][0]["title"], str(list(cats)))
check("… und per Mail an die Person", wait_mail() and any("jonas.weber@example.org" in m[0] for m in mails))
devs = j1.get("/api/auth/devices").json()
check("Geräteliste: 2 Geräte, dieses markiert", len(devs) == 2 and sum(d["current"] for d in devs) == 1 and devs[0]["label"], str(devs)[:160])
check("IP wird nur grob gespeichert", all(re.fullmatch(r"\d+\.\d+\.\d+\.x|[0-9a-f:]*::", d["ip"] or "") for d in devs), str([d["ip"] for d in devs]))
mails.clear()
r = j1.put("/api/auth/change-password", json={"old_password": PW, "new_password": "Roter Apfel 8821"})
def subjects():
    return [str(email.message_from_bytes(m[1], policy=policy.default)["Subject"]) for m in mails]


for _ in range(40):  # Mails kommen im Hintergrund, auch verspätete 'Neue Anmeldung'-Mails können noch eintreffen
    if any("geändert" in x for x in subjects()):
        break
    time.sleep(0.3)
check("Passwortwechsel meldet sich per Mail", r.status_code == 200 and any("geändert" in x for x in subjects()), str(subjects()))
check("… Meldung in der Glocke", any("Passwort wurde geändert" in i["title"] for c in j1.get("/api/notifications").json()["categories"] for i in c["items"]))
check("Anderes Gerät wurde dabei abgemeldet", j2.get("/api/auth/me").status_code == 401)
j3 = client("jonas", "Roter Apfel 8821")
check("Auf allen anderen Geräten abmelden", j3.post("/api/auth/logout-all").status_code == 200 and j1.get("/api/auth/me").status_code == 401 and j3.get("/api/auth/me").status_code == 200)
check("Nach logout-all nur noch dieses Gerät gelistet", len(j3.get("/api/auth/devices").json()) == 1)
check("Sicherheit lässt sich nicht stummschalten", j3.put("/api/notifications/prefs/sicherheit", json={"push": False}).status_code == 400)

# --- 2FA-Pflicht
sql("update users set is_first_login=0")
for u, role in (("lead", "leitung"), ("grp", "gruppenfuehrer")):
    assert admin.post("/api/users/add", json={"username": u, "password": PW, "role": role, "personnel_id": None}).status_code == 200
    sql(f"update users set is_first_login=0 where username='{u}'")
check("Sicherheitsübersicht nur für Admins", client("jonas", "Roter Apfel 8821").get("/api/admin/security").status_code == 403)
ov = admin.get("/api/admin/security").json()
check("Sicherheitsübersicht liefert Zahlen und Listen", {"failed_24h", "failed_7d", "locked", "no_2fa", "pending_invites", "last_logins", "events", "settings"} <= set(ov), str(list(ov)))
check("Leitung ohne 2FA wird gemeldet", any(u["username"] == "lead" for u in ov["no_2fa"]))
lead, grp = client("lead", PW), client("grp", PW)
def write(c):
    return c.put("/api/personnel/me/contact", json={}).status_code


check("Ohne Pflicht: Leitung wird bei Schreibzugriffen nicht gebremst", write(lead) != 403, str(write(lead)))
check("Einstellung: ungültiger Wert abgelehnt", admin.put("/api/admin/security-settings", json={"require_2fa": 5, "pwned_check": 0}).status_code == 400)
check("Pflicht einschalten ohne eigenes 2FA -> abgelehnt (kein Selbst-Aussperren)", admin.put("/api/admin/security-settings", json={"require_2fa": 2, "pwned_check": 0}).status_code == 400)
asec = admin.post("/api/auth/2fa/setup").json()["secret"]
admin.post("/api/auth/2fa/confirm", json={"code": pyotp.TOTP(asec).now()})
time.sleep(16)
check("Einstellung speichern", admin.put("/api/admin/security-settings", json={"require_2fa": 2, "pwned_check": 0}).status_code == 200)
time.sleep(16)  # kurzer Cache der Einstellung (15 s) im Server
me = lead.get("/api/auth/me").json()
check("Pflicht: /me meldet must_setup_2fa für Leitung, nicht für Gruppenführer", me["must_setup_2fa"] is True and grp.get("/api/auth/me").json()["must_setup_2fa"] is False)
r = lead.put("/api/personnel/me/contact", json={})
check("Pflicht: Leitung ohne 2FA bekommt bei Schreibzugriffen 403 mit Hinweis", r.status_code == 403 and "Zwei-Faktor" in r.text, r.text[:90])
check("Pflicht: Lesen bleibt möglich", lead.get("/api/auth/me").status_code == 200)
check("Pflicht: Gruppenführer nicht betroffen", write(grp) != 403)
sec = lead.post("/api/auth/2fa/setup").json()["secret"]
check("Pflicht: 2FA-Einrichtung bleibt erlaubt", lead.post("/api/auth/2fa/confirm", json={"code": pyotp.TOTP(sec).now()}).status_code == 200)
time.sleep(16)
check("Nach der Einrichtung: Schreiben wieder erlaubt, Hinweis weg", lead.get("/api/auth/me").json()["must_setup_2fa"] is False and write(lead) != 403)

# Admin setzt 2FA zurück
lid = int(sql("select id from users where username='lead'"))
check("2FA zurücksetzen: Nicht-Admin verboten", grp.post(f"/api/users/{lid}/2fa-reset").status_code == 403)
gid = int(sql("select id from users where username='grp'"))
check("2FA zurücksetzen: Konto ohne 2FA -> Hinweis", admin.post(f"/api/users/{gid}/2fa-reset").status_code == 400)
check("2FA zurücksetzen durch Admin", admin.post(f"/api/users/{lid}/2fa-reset").status_code == 200)
check("… beendet die Sitzungen und schaltet 2FA ab", lead.get("/api/auth/me").status_code == 401 and sql("select totp_enabled from users where username='lead'") == "0")
check("… Anmeldung wieder ohne Code", httpx.post(B + "/api/login", json={"username": "lead", "password": PW}).json().get("status") == "success")
admin.put("/api/admin/security-settings", json={"require_2fa": 0, "pwned_check": 0})
sql("update users set totp_enabled=0, totp_secret=NULL, totp_recovery_codes=NULL where username='demo'")

# --- Leak-Prüfung (ohne echtes Netzwerk)
from core import password_policy as pp
from core.security import set_int_setting
import requests as _rq


class _Resp:
    status_code = 200

    def __init__(self, text):
        self.text = text


import hashlib
sha = hashlib.sha1("Geheimnis-Leak-77".encode()).hexdigest().upper()
orig = _rq.get
_rq.get = lambda url, **kw: _Resp(f"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA:0\r\n{sha[5:]}:4242\r\n")
set_int_setting("pwned_check", 0)
check("Leak-Prüfung aus: Passwort wird nicht abgelehnt", pp.check_password("Geheimnis-Leak-77", "x") is None)
set_int_setting("pwned_check", 1)
msg = pp.check_password("Geheimnis-Leak-77", "x")
check("Leak-Prüfung an: bekanntes Leak-Passwort abgelehnt (mit Anzahl)", msg and "Datenlecks" in msg and "4.242" in msg, str(msg))
_rq.get = lambda url, **kw: (_ for _ in ()).throw(RuntimeError("Netz weg"))
check("Leak-Prüfung: Netzfehler blockiert nichts", pp.check_password("Geheimnis-Leak-77", "x") is None)
_rq.get = lambda url, **kw: _Resp("")
sent = []
_rq.get = lambda url, **kw: (sent.append(url), _Resp(""))[1]
pp.check_password("Geheimnis-Leak-77", "x")
check("Nur 5 Zeichen des Hashes verlassen den Server", sent and sent[0].rsplit("/", 1)[1] == sha[:5] and "Geheimnis" not in sent[0], str(sent))
_rq.get = orig
set_int_setting("pwned_check", 0)

admin.put("/api/admin/mail-settings", json={"enabled": False, "smtp_host": "", "smtp_port": 587, "smtp_security": "starttls", "sender_address": "", "app_url": "", "reminder_mail": False})
ctl.stop()
print("\n==> ", "ALLES OK" if not fails else f"{len(fails)} FEHLER: {fails}")
sys.exit(1 if fails else 0)

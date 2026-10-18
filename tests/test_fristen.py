"""Fristen nach Zuständigkeit: wer sieht welche Fristen, wer wird benachrichtigt, Erstkontrolle, Erledigt-Aktionen."""
import datetime
import email
import sys
import time
from email import policy

import httpx
from aiosmtpd.controller import Controller

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


mails = []


class H:
    async def handle_DATA(self, server, session, env):
        mails.append((env.rcpt_tos, env.content))
        return "250 OK"


today = datetime.date.today()
make_role_users()
for t in ("notifications", "building_checks", "lehrgaenge", "prospects", "hvo_material", "vehicle_checks"):
    sql(f"delete from {t}")
sql("delete from system_broadcasts")
sql("delete from settings where setting_key = 'last_reminder_check'")
sql("update equipment set next_inspection = NULL")
sql("update personnel set g26_3_date = NULL, license_checked_date = CURDATE(), is_agt = 0")
sql("update vehicles set tuv_date = NULL, sp_date = NULL")
sql("update consumables set min_stock = 0")
sql("delete from clothing_stock")

_orig_mail = {n: sql(f"select coalesce(email,'') from personnel where name='{n}'") for n in ("Jonas Weber", "Tim Fischer", "Anna Muster", "David Keller")}
pid = {n: int(sql(f"select id from personnel where name='{n}'")) for n in ("Jonas Weber", "Tim Fischer", "Laura Schmitt")}
sql(f"update users set personnel_id={pid['Jonas Weber']} where username='r_mannschaft'")
sql(f"update users set personnel_id={pid['Tim Fischer']} where username='r_atemschutzwart'")
sql("update personnel set email='jonas.weber@example.org' where name='Jonas Weber'")
sql("update personnel set email='gewart@example.org' where name='Tim Fischer'")
# Technik: Fahrzeug-TÜV überfällig, Gerätehaus-Prüfung, Sanitätsmaterial
vid = int(sql("select id from vehicles order by id limit 1"))
sql(f"update vehicles set tuv_date = CURDATE() - INTERVAL 5 DAY where id={vid}")
sql("insert into building_checks (name, interval_months, last_check, next_check) values ('Blitzschutz', 48, CURDATE() - INTERVAL 5 YEAR, CURDATE() - INTERVAL 1 YEAR)")
sql("insert into hvo_material (name, quantity, expiry_date) values ('Pflaster', 3, CURDATE() + INTERVAL 5 DAY)")
# Personen: G26 für Jonas (überfällig), Atemschutz-Lehrgang für Laura (läuft ab), anderer Lehrgang für Jonas, Führerschein nie kontrolliert für Laura
sql(f"update personnel set is_agt=1, g26_3_date = CURDATE() - INTERVAL 40 MONTH where id={pid['Jonas Weber']}")
sql(f"insert into lehrgaenge (personnel_id, course_name, date, valid_until) values ({pid['Laura Schmitt']}, 'Atemschutzgeräteträger', CURDATE() - INTERVAL 3 YEAR, CURDATE() + INTERVAL 3 DAY)")
sql(f"insert into lehrgaenge (personnel_id, course_name, date, valid_until) values ({pid['Jonas Weber']}, 'Maschinist Fortbildung', CURDATE() - INTERVAL 3 YEAR, CURDATE() + INTERVAL 4 DAY)")
sql(f"update personnel set is_maschinist=1, license_checked_date = NULL where id={pid['Laura Schmitt']}")

cl = {r: client(f"r_{r}") for r in ROLES}
admin = client("demo")


def due(c):
    return c.get("/api/due-items").json()


def keys(items):
    return {(i["type"], i["name"]) for i in items}


a = due(admin)
check("Admin sieht alle Bereiche", {i["area"] for i in a} >= {"fahrzeuge", "gebaeude", "hvo", "personal"}, str(sorted({i["area"] for i in a})))
check("Jede Frist trägt Bereich, Tage und Verweise", all({"area", "days", "first_check", "ref_id", "personnel_id"} <= set(i) for i in a))
lic = [i for i in a if i["type"] == "license" and i["name"] == "Laura Schmitt"]
check("Führerschein nie kontrolliert = Erstkontrolle, kein Verzug", lic and lic[0]["first_check"] and not lic[0]["overdue"], str(lic))
check("Alter Pfad liefert dasselbe", admin.get("/api/admin/stats/due-soon").json() == a)
check("Leitung sieht alles", len(due(cl["leitung"])) == len(a))

g = due(cl["geratewart"])
check("Gerätewart sieht nur Technik (Fahrzeuge, Geräte, Gerätehaus)", g and {i["area"] for i in g} <= {"fahrzeuge", "material", "gebaeude"} and "vehicle" in {i["type"] for i in g}, str(sorted({i["area"] for i in g})))
check("Gerätewart sieht keine Personendaten (G26, Lehrgänge, Führerschein, Nachwuchs)", not any(i["area"] == "personal" for i in g))
check("Gerätewart sieht kein Sanitätsmaterial", not any(i["area"] == "hvo" for i in g))

s = due(cl["atemschutzwart"])
check("Atemschutzwart sieht G26.3 und Atemschutz-Lehrgänge", ("g26", "Jonas Weber") in keys(s) and ("lehrgang", "Laura Schmitt") in keys(s), str(keys(s)))
check("… aber nicht Maschinisten-Lehrgang, Führerschein, Fahrzeuge", ("lehrgang", "Jonas Weber") not in keys(s) and not any(i["type"] in ("license", "vehicle", "building") for i in s))

m = due(cl["mannschaft"])
check("Mannschaft (verknüpft mit Jonas) sieht nur eigene persönliche Fristen", keys(m) == {("g26", "Jonas Weber"), ("lehrgang", "Jonas Weber")}, str(keys(m)))
for role in ("gruppenfuehrer", "jugendwarte"):
    check(f"{role} ohne Verknüpfung sieht nichts", due(cl[role]) == [])
check("Ohne Anmeldung gesperrt", httpx.get(B + "/api/due-items").status_code == 401)

# ------------------------------------------------------------ Erledigt-Aktionen
check("Führerschein-Kontrolle: nur Admin/Leitung", cl["mannschaft"].post("/api/personnel/license-checked", json={"ids": [pid["Laura Schmitt"]]}).status_code == 403)
check("Ohne Personen abgelehnt", cl["leitung"].post("/api/personnel/license-checked", json={"ids": []}).status_code == 400)

# ------------------------------------------------------------ Benachrichtigungen + Mails
admin.put("/api/admin/mail-settings", json={"enabled": True, "smtp_host": "127.0.0.1", "smtp_port": 8834, "smtp_security": "none",
                                            "sender_address": "dienstbuch@example.org", "app_url": B, "reminder_mail": True})
ctl = Controller(H(), hostname="127.0.0.1", port=8834)
ctl.start()
sql("update personnel set email='leitung@example.org' where name='Anna Muster'")
sql("update personnel set email='lead2@example.org' where name='David Keller'")
aid = int(sql("select id from personnel where name='Anna Muster'"))
did = int(sql("select id from personnel where name='David Keller'"))
sql(f"update users set personnel_id={aid} where username='r_leitung'")
sql(f"update users set personnel_id={did} where username='r_geratewart'")
from core.reminders import check_due_reminders
check_due_reminders()
for _ in range(40):
    if len(mails) >= 3:
        break
    time.sleep(0.3)
time.sleep(1)


def cats(c):
    return {k["key"]: k for k in c.get("/api/notifications").json()["categories"]}


gw = cats(cl["geratewart"])
check("Gerätewart: 'Fristen: Technik' ja, 'Personen' nein", "fristen_technik" in gw and "fristen_personen" not in gw, str(list(gw)))
check("… Meldung nennt die Bereiche", "Fahrzeuge" in gw["fristen_technik"]["items"][0]["body"] and "Gerätehaus" in gw["fristen_technik"]["items"][0]["body"])
lt = cats(cl["leitung"])
check("Leitung: beide Fristen-Fenster", {"fristen_technik", "fristen_personen"} <= set(lt), str(list(lt)))
at = cats(cl["atemschutzwart"])
check("Atemschutzwart: nur Personen (seine Teilmenge), keine Technik", "fristen_personen" in at and "fristen_technik" not in at, str(list(at)))
mn = cats(cl["mannschaft"])
check("Mannschaft: nur die eigene 'Deine Frist läuft ab'", "fristen_personen" in mn and mn["fristen_personen"]["total"] == 1 and mn["fristen_personen"]["items"][0]["title"] == "Deine Frist läuft ab" and "fristen_technik" not in mn, str(mn.get("fristen_personen")))
check("Kein gelbes Banner für alle mehr", sql("select count(*) from system_broadcasts where title like '%Fristen%'") == "0")


def mail_to(addr):
    for rcpt, raw in mails:
        if addr in rcpt:
            return email.message_from_bytes(raw, policy=policy.default).get_body().get_content()
    return None


bm = mail_to("gewart@example.org")  # Atemschutzwart-Mail (Tim) nur G26/Atemschutz
check("Atemschutzwart-Mail: nur seine Fristen", bm and "G26.3" in bm and "Blitzschutz" not in bm and "Führerschein" not in bm, str(bm)[:200])
gm = mail_to("lead2@example.org")
check("Gerätewart-Mail: Technik ohne Personen", gm and "Blitzschutz" in gm and "FAHRZEUGE" in gm and "Jonas" not in gm and "G26" not in gm, str(gm)[:200])
lm = mail_to("leitung@example.org")
check("Leitungs-Mail: alles, nach Bereich gegliedert, Erstkontrolle getrennt", lm and "FAHRZEUGE" in lm and "PERSONEN" in lm and "ERSTKONTROLLE NOCH OFFEN" in lm and "Laura Schmitt" in lm, str(lm)[:300])
pm = mail_to("jonas.weber@example.org")
check("Jonas bekommt seine eigenen Fristen per Mail (nur eigene)", pm and "G26.3" in pm and "Laura" not in pm and "Blitzschutz" not in pm, str(pm)[:200])

# ------------------------------------------------------------ Erledigt: Wirkung
n_before = len([i for i in due(admin) if i["type"] == "license"])
r = cl["leitung"].post("/api/personnel/license-checked", json={"ids": [pid["Laura Schmitt"]]})
check("Führerscheinkontrolle als erledigt eintragen", r.status_code == 200 and r.json()["updated"] == 1)
check("… Erstkontrolle verschwindet", len([i for i in due(admin) if i["type"] == "license"]) == n_before - 1)
bid = int(sql("select id from building_checks where name='Blitzschutz'"))
check("Gerätehaus-Prüfung abhaken entfernt die Frist", cl["geratewart"].post(f"/api/building/checks/{bid}/done", json={}).status_code == 200 and not any(i["type"] == "building" for i in due(admin)))

admin.put("/api/admin/mail-settings", json={"enabled": False, "smtp_host": "", "smtp_port": 587, "smtp_security": "starttls", "sender_address": "", "app_url": "", "reminder_mail": False})
ctl.stop()
for n, e in _orig_mail.items():  # Ausgangszustand für die folgenden Tests wiederherstellen
    sql("update personnel set email=%s where name='%s'" % ("NULL" if not e else "'" + e + "'", n))
print("\n==> ", "ALLES OK" if not fails else f"{len(fails)} FEHLER: {fails}")
sys.exit(1 if fails else 0)

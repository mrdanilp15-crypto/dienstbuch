import datetime, subprocess, sys, time
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
for q in ("delete from notifications", "delete from login_devices", "delete from notification_prefs", "delete from schedule_audience", "delete from schedule_rsvp", "delete from schedules", "delete from users where username <> 'demo'"):
    sql(q)
def pid(name): return int(sql(f"select id from personnel where name='{name}'"))
for u, role, person in [("m_in", "mannschaft", "Jonas Weber"), ("m_in2", "mannschaft", "Tim Fischer"), ("m_out", "mannschaft", "Laura Schmitt"), ("gw", "geratewart", "David Keller")]:
    h = hash_password("pw12345678").replace("$", "\\$")
    sql(f"insert into users (username,password_hash,role,is_first_login,personnel_id) values ('{u}','{h}','{role}',0,{pid(person)})")
admin = httpx.Client(base_url=B, timeout=30); admin.post("/api/login", json={"username": "demo", "password": "demo1234"})
cl = {}
for u in ("m_in", "m_in2", "m_out", "gw"):
    c = httpx.Client(base_url=B, timeout=30); assert c.post("/api/login", json={"username": u, "password": "pw12345678"}).status_code == 200; cl[u] = c

check("Nicht angemeldet -> 401", httpx.get(B + "/api/notifications").status_code == 401)
o = cl["m_in"].get("/api/notifications").json()
check("Ohne Meldungen: keine Fenster, 0 ungelesen", o == {"unread": 0, "categories": []}, str(o))

# Termin -> nur Eingeladene, Kategorie termine, exakter Link
group = int(sql("select id from groups_table order by id limit 1"))
aud = [pid("Jonas Weber"), pid("Tim Fischer")]
admin.post("/api/missions/schedules", json={"title": "Glocken-Termin", "date": (today + datetime.timedelta(days=3)).isoformat(), "time": "19:00", "type": "Übung",
           "group_id": group, "audience": "selected", "audience_ids": aud, "notify": True})
sid = int(sql("select id from schedules where title='Glocken-Termin'"))
for _ in range(40):  # die Benachrichtigung läuft nach der Antwort im Hintergrund
    o = cl["m_in"].get("/api/notifications").json()
    if o["categories"]:
        break
    time.sleep(0.25)
check("Termin: Eingeladener sieht genau das Fenster 'Termine'", [c["key"] for c in o["categories"]] == ["termine"] and o["unread"] == 1, str(o)[:200])
check("Termin: Link führt zum genauen Termin", o["categories"][0]["items"][0]["link"] == f"/dashboard?open=termin:{sid}")
check("Termin: Außenstehender bekommt nichts", cl["m_out"].get("/api/notifications").json()["categories"] == [])

# Rundschreiben -> Verwaltung, alle
admin.post("/api/broadcasts", json={"title": "Dienstplan geändert", "content": "Bitte lesen", "role_target": "all", "is_mandatory": False})
o = cl["m_out"].get("/api/notifications").json()
check("Rundschreiben: Fenster 'Verwaltung' bei allen", [c["key"] for c in o["categories"]] == ["verwaltung"], str(o)[:160])
o = cl["m_in"].get("/api/notifications").json()
check("Reihenfolge der Fenster: Termine vor Verwaltung", [c["key"] for c in o["categories"]] == ["termine", "verwaltung"] and o["unread"] == 2)

# Rolle-gezielt
admin.post("/api/broadcasts", json={"title": "Nur Gerätewarte", "content": "x", "role_target": "geratewart", "is_mandatory": False})
check("Rundschreiben an Rolle: nur diese Rolle", len(cl["gw"].get("/api/notifications").json()["categories"][0]["items"]) == 2 and len(cl["m_out"].get("/api/notifications").json()["categories"][0]["items"]) == 1)

# Direkt: Einsatz, Fristen, System
from core.notify import notify, notify_admins
import core.notify as cn
sent = []
import routers.push_api as pa
pa.send_push_to_all = lambda payload, usernames=None: sent.append((payload["title"], sorted(usernames or [])))
notify("einsatz", "Neuer Alarm: B3", "Ort", "/dashboard?open=alarm")
check("Einsatz geht an alle (inkl. Push)", cl["m_out"].get("/api/notifications").json()["categories"][0]["key"] == "einsatz" and any(t == "Neuer Alarm: B3" and len(u) >= 5 for t, u in sent), str(sent[-1:]))
notify("fristen", "Anstehende Prüffristen", "3 Fristen", "/dashboard?open=fristen", roles=["admin", "leitung", "geratewart"])
check("Fristen nur an Verwaltungsrollen", "fristen" in [c["key"] for c in cl["gw"].get("/api/notifications").json()["categories"]] and "fristen" not in [c["key"] for c in cl["m_in"].get("/api/notifications").json()["categories"]])
notify_admins("E-Mail-Versand fehlgeschlagen", "SMTP down"); notify_admins("E-Mail-Versand fehlgeschlagen", "SMTP down")
check("System: nur Admin/Leitung, Tagesdedupe (1 Eintrag)", sql("select count(*) from notifications where category='system'") == "1" and sql("select username from notifications where category='system'") == "demo")

# Push-Einstellung
r = cl["m_in"].put("/api/notifications/prefs/termine", json={"push": False}); check("Push für Termine abschalten", r.status_code == 200)
check("Einsatz-Push nicht abschaltbar", cl["m_in"].put("/api/notifications/prefs/einsatz", json={"push": False}).status_code == 400)
prefs = {p["key"]: p for p in cl["m_in"].get("/api/notifications/prefs").json()}
check("Einstellungen geben Zustand zurück", prefs["termine"]["push"] is False and prefs["einsatz"]["locked"] and prefs["fristen"]["push"])
sent.clear(); notify("termine", "Stumm-Test", "x", None, usernames=["m_in", "m_in2"])
check("Stumm: Push nur an m_in2, Glocke trotzdem bei beiden", sent and sent[-1][1] == ["m_in2"] and sql("select count(*) from notifications where title='Stumm-Test'") == "2", str(sent[-1:]))

# Lesen, aufräumen, nachladen
items = cl["m_in"].get("/api/notifications").json()["categories"]
nid = items[0]["items"][0]["id"]
check("Fremde Meldung lässt sich nicht als gelesen markieren", cl["m_out"].post(f"/api/notifications/{nid}/read").status_code == 200 and cl["m_in"].get("/api/notifications").json()["unread"] == sum(c["unread"] for c in items))
cl["m_in"].post(f"/api/notifications/{nid}/read")
check("Einzeln gelesen", cl["m_in"].get("/api/notifications/count").json()["unread"] == sum(c["unread"] for c in items) - 1)
for i in range(20): notify("verwaltung", f"Mitteilung {i}", "text", None, usernames=["m_in"], push=False)
o = {c["key"]: c for c in cl["m_in"].get("/api/notifications").json()["categories"]}
v = o["verwaltung"]
check("Viele Meldungen: nur die ersten 8 geladen, Gesamtzahl bekannt", len(v["items"]) == 8 and v["total"] == 21, f"{len(v['items'])}/{v['total']}")
more = cl["m_in"].get(f"/api/notifications/category/verwaltung?before_id={v['items'][-1]['id']}").json()
check("Nachladen liefert die nächsten, ohne Doppelte", len(more) == 8 and not set(i["id"] for i in more) & set(i["id"] for i in v["items"]))
cl["m_in"].post("/api/notifications/read-all?category=verwaltung")
o = {c["key"]: c for c in cl["m_in"].get("/api/notifications").json()["categories"]}
check("Alle in einer Kategorie gelesen (andere bleiben)", o["verwaltung"]["unread"] == 0 and o["termine"]["unread"] >= 1)
cl["m_in"].delete("/api/notifications/category/verwaltung")
o = {c["key"]: c for c in cl["m_in"].get("/api/notifications").json()["categories"]}
check("Aufräumen löscht nur Gelesene -> Fenster verschwindet, wenn leer", "verwaltung" not in o and "termine" in o)
cl["m_in"].post("/api/notifications/read-all?category=")
check("Alle gelesen (ohne Kategorie)", cl["m_in"].get("/api/notifications/count").json()["unread"] == 0)
sql("update notifications set created_at = NOW() - INTERVAL 61 DAY where title='Stumm-Test'")
notify("system", "Trigger Aufräumen", "", None, usernames=["m_in2"], push=False)
check("Alte Meldungen (>60 Tage) werden entfernt", sql("select count(*) from notifications where title='Stumm-Test'") == "0")

print("\n==> ", "ALLES OK" if not fails else f"{len(fails)} FEHLER: {fails}")
sys.exit(1 if fails else 0)

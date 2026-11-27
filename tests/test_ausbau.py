"""Ausbau 2.90: Fahrzeugausfall, Mitgliedsbeiträge, Inventur, Jahresbericht, Atemschutz-Übersicht, Server-Zustand, Muster-Mails."""
import datetime
import sys

import httpx

from tests.helpers import sql
from tests.ui_helpers import make_role_users

B = "http://127.0.0.1:8801"
fails = []


def check(name, cond, extra=""):
    print(("OK    " if cond else "FEHLER"), name, extra)
    if not cond:
        fails.append(name)


def client(user):
    c = httpx.Client(base_url=B, timeout=60)
    assert c.post("/api/login", json={"username": user, "password": "demo1234"}).status_code == 200, user
    return c


def iso(days):
    return (datetime.date.today() + datetime.timedelta(days=days)).isoformat()


make_role_users()
admin = client("demo")
crew = client("r_mannschaft")

# ------------------------------------------------------------ Fahrzeugausfall
sql("delete from vehicles where name like 'TST-%'; delete from aao_rules where pattern = 'TSTAUSFALL'")
sql("insert into vehicles (name, radio_name, status) values ('TST-HLF', 'Florian Test 40/1', 2), ('TST-LF', 'Florian Test 44/1', 2)")
hlf = int(sql("select id from vehicles where name = 'TST-HLF'"))
sql("delete from notifications where title like '%TST-HLF%'")

r = crew.put(f"/api/vehicles/{hlf}/status", json={"status": 6, "out_reason": "x"})
check("Ausfall: Mannschaft darf den Status nicht setzen", r.status_code == 403, str(r.status_code))
r = admin.put(f"/api/vehicles/{hlf}/status", json={"status": 6, "out_reason": "Pumpe defekt", "out_until": iso(5)})
check("Ausfall: außer Dienst melden", r.status_code == 200, r.text)
row = sql(f"select status, out_reason, out_until, out_since is not null from vehicles where id = {hlf}").split("\t")
check("Ausfall: Grund, Datum und Beginn gespeichert", row == ["6", "Pumpe defekt", iso(5), "1"], str(row))
check("Ausfall: Zuständige werden benachrichtigt", sql("select count(*) from notifications where title = 'Außer Dienst: TST-HLF'") not in ("", "0"))
v = next(x for x in admin.get("/api/vehicles").json() if x["id"] == hlf)
check("Ausfall: Liste liefert Grund und Datum", v["out_reason"] == "Pumpe defekt" and v["out_until"] == iso(5) and v["out_since"], str(v))
anon = httpx.get(B + "/api/vehicles")
check("Ausfall: ohne Anmeldung/Display-Token keine Fahrzeugdaten", anon.status_code in (401, 403), str(anon.status_code))

r = admin.post("/api/aao", json={"pattern": "TSTAUSFALL", "entries": [{"vehicle": "Florian Test 40/1", "role": "Angriff"}, {"vehicle": "TST-LF", "role": "Wasser"}]})
check("AAO-Regel angelegt", r.status_code == 200, r.text)
rec = admin.get("/api/aao/recommend", params={"stichwort": "TSTAUSFALL Brand"}).json()["entries"]
check("AAO: Fahrzeug außer Dienst ist markiert (auch über Funkrufname)", rec[0].get("out_of_service") is True and rec[0]["out_reason"] == "Pumpe defekt", str(rec))
check("AAO: einsatzbereites Fahrzeug ist nicht markiert", not rec[1].get("out_of_service"), str(rec))

r = admin.put(f"/api/vehicles/{hlf}/status", json={"status": 6, "out_reason": "Pumpe defekt, Teil bestellt", "out_until": iso(9)})
row = sql(f"select out_reason, out_until from vehicles where id = {hlf}").split("\t")
check("Ausfall: Grund/Datum lassen sich ändern", row == ["Pumpe defekt, Teil bestellt", iso(9)], str(row))
check("Ausfall: Ändern meldet nicht noch einmal", sql("select count(*) from notifications where title = 'Außer Dienst: TST-HLF' and username = 'demo'") == "1")

# Erinnerung, wenn das Datum vorbei ist
from core.automation import check_vehicle_returns  # noqa: E402
sql(f"update vehicles set out_until = '{iso(-1)}', out_reminded = NULL where id = {hlf}")
noon = datetime.datetime.combine(datetime.date.today(), datetime.time(12, 0))
check("Ausfall: Erinnerung, wenn das Datum vorbei ist", check_vehicle_returns(noon) >= 1)
check("Ausfall: Erinnerung kommt nur einmal", check_vehicle_returns(noon) == 0)
check("Ausfall: Erinnerung in der Glocke", sql("select count(*) from notifications where title = 'Wieder einsatzbereit? TST-HLF'") not in ("", "0"))
check("Ausfall: nachts keine Erinnerung", check_vehicle_returns(noon.replace(hour=3)) == 0)

# Bearbeiten-Formular mit Status 6 behält Ausfall-Angaben
veh = next(x for x in admin.get("/api/vehicles").json() if x["id"] == hlf)
veh["milage"] = 1234
r = admin.put(f"/api/vehicles/{hlf}", json=veh)
row = sql(f"select status, out_reason, milage from vehicles where id = {hlf}").split("\t")
check("Ausfall: Bearbeiten behält Ausfall-Angaben", r.status_code == 200 and row == ["6", "Pumpe defekt, Teil bestellt", "1234"], f"{r.status_code} {row}")

r = admin.put(f"/api/vehicles/{hlf}/status", json={"status": 2})
row = sql(f"select status, out_reason, out_until from vehicles where id = {hlf}").split("\t")
check("Ausfall: wieder einsatzbereit löscht die Angaben", row == ["2", "", "NULL"], str(row))
check("Ausfall: Rückmeldung wird gemeldet", sql("select count(*) from notifications where title = 'Wieder einsatzbereit: TST-HLF'") not in ("", "0"))
check("Ausfall: unbekannter Status wird abgelehnt", admin.put(f"/api/vehicles/{hlf}/status", json={"status": 12}).status_code == 400)
check("Ausfall: falsches Datum wird abgelehnt", admin.put(f"/api/vehicles/{hlf}/status", json={"status": 6, "out_until": "morgen"}).status_code == 400)
sql("delete from vehicles where name like 'TST-%'; delete from aao_rules where pattern = 'TSTAUSFALL'")

print("\n==> ", "ALLES OK" if not fails else f"{len(fails)} FEHLER: {fails}")
sys.exit(1 if fails else 0)

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

# ------------------------------------------------------------ Mitgliedsbeiträge
YEAR = 2031  # eigenes Jahr, damit nichts aus den Demo-Daten stört
sql(f"delete from membership_fees where year = {YEAR}")
check("Beiträge: Mannschaft sieht nichts", crew.get(f"/api/verein/membership-fees?year={YEAR}").status_code == 403)
r = admin.put("/api/verein/fee-settings", json={"default_amount": 24.5, "due_month": 3, "due_day": 31})
check("Beiträge: Standardbetrag speichern", r.status_code == 200 and r.json()["default_amount"] == 24.5, r.text)
check("Beiträge: ungültige Fälligkeit abgelehnt", admin.put("/api/verein/fee-settings", json={"default_amount": 5, "due_month": 13, "due_day": 1}).status_code == 400)
rows = admin.get(f"/api/verein/membership-fees?year={YEAR}").json()
check("Beiträge: ohne Eintrag gilt der Standardbetrag", rows and all(x["amount"] == 24.5 and not x["paid_at"] for x in rows), str(rows[:2]))
statuses = {x["membership_status"] for x in rows}
check("Beiträge: Ausgeschiedene nicht in der Liste", "Ausgeschieden" not in statuses, str(statuses))
p1, p2 = rows[0]["personnel_id"], rows[1]["personnel_id"]
r = admin.post("/api/verein/membership-fees", json={"personnel_id": p1, "year": YEAR, "amount": 12, "note": "ermäßigt"})
check("Beiträge: Betrag und Notiz ohne Bezahlt-Status speichern", r.status_code == 200 and
      sql(f"select amount, paid_at, note from membership_fees where personnel_id = {p1} and year = {YEAR}") == "12.00\tNULL\termäßigt")
admin.post("/api/verein/membership-fees", json={"personnel_id": p1, "year": YEAR, "amount": 12, "note": "ermäßigt", "paid": True})
admin.post("/api/verein/membership-fees", json={"personnel_id": p1, "year": YEAR, "amount": 15, "note": "ermäßigt"})
check("Beiträge: Betrag ändern lässt 'bezahlt' stehen", sql(f"select amount, paid_at is not null from membership_fees where personnel_id = {p1} and year = {YEAR}") == "15.00\t1")
sm = admin.get(f"/api/verein/membership-fees/summary?year={YEAR}").json()
check("Beiträge: Übersicht zählt bezahlt/offen", sm["paid_count"] == 1 and sm["paid_sum"] == 15 and sm["open_count"] == len(rows) - 1
      and abs(sm["open_sum"] - 24.5 * (len(rows) - 1)) < 0.01, str(sm))
check("Beiträge: Fälligkeit in der Zukunft = nicht überfällig", sm["due_date"] == f"{YEAR}-03-31" and sm["overdue"] is False, str(sm))
past = admin.get("/api/verein/membership-fees/summary?year=2020").json()
check("Beiträge: vergangenes Jahr mit offenen Beiträgen = überfällig", past["overdue"] is True, str(past))
csv = admin.get(f"/api/verein/membership-fees.csv?year={YEAR}")
check("Beiträge: CSV-Export", csv.status_code == 200 and "Mitglied;Status;Betrag" in csv.text and "15,00" in csv.text and "ermäßigt" in csv.text, csv.text[:200])
check("Beiträge: ungültiger Betrag abgelehnt", admin.post("/api/verein/membership-fees", json={"personnel_id": p2, "year": YEAR, "amount": -3}).status_code == 400)
from core.automation import build_monthly_report  # noqa: E402
check("Monatsbericht: Fahrzeuge außer Dienst stehen drin", "Fahrzeuge außer Dienst:" in build_monthly_report(2026, 10)[1])
sql(f"delete from membership_fees where year = {YEAR}")
admin.put("/api/verein/fee-settings", json={"default_amount": 0, "due_month": 3, "due_day": 31})

# ------------------------------------------------------------ Inventur
sql("delete from inventories; delete from equipment where barcode like 'TSTINV%'; delete from consumables where name like 'TST-%'")
sql("insert into equipment (name, barcode, category, archived) values ('TST Säge', 'TSTINV1', 'TST-Kat', 0), ('TST Lüfter', 'TSTINV2', 'TST-Kat', 0), "
    "('TST Leiter', 'TSTINV3', 'TST-Kat', 0), ('TST Alt', 'TSTINV4', 'TST-Kat', 1)")
sql("insert into consumables (name, unit, current_stock, min_stock) values ('TST-Bindemittel', 'Sack', 10, 2)")
check("Inventur: Mannschaft darf nicht", crew.get("/api/inventur").status_code == 403)
cats = admin.get("/api/inventur").json()["categories"]
check("Inventur: Kategorien zur Auswahl", "TST-Kat" in cats, str(cats))
r = admin.post("/api/inventur", json={"category": "TST-Kat", "include_consumables": True})
check("Inventur: starten", r.status_code == 200, r.text)
inv = r.json()["id"]
check("Inventur: nur eine gleichzeitig", admin.post("/api/inventur", json={}).status_code == 409)
d = admin.get(f"/api/inventur/{inv}").json()
names = sorted(i["name"] for i in d["items"])
check("Inventur: Kategorie ohne archivierte Geräte, plus Verbrauchsmaterial", names == ["TST Leiter", "TST Lüfter", "TST Säge", "TST-Bindemittel"], str(names))
check("Inventur: Titel automatisch", d["title"].startswith("Inventur TST-Kat"), d["title"])
item = {i["name"]: i for i in d["items"]}
iu = lambda name, **kw: admin.put(f"/api/inventur/{inv}/items/{item[name]['id']}", json=kw)
check("Inventur: vorhanden abhaken", iu("TST Säge", result="ok").status_code == 200)
iu("TST Lüfter", result="defekt", note="Lager quietscht")
check("Inventur: falsches Ergebnis abgelehnt", iu("TST Leiter", result="kaputt").status_code == 400)
iu("TST-Bindemittel", counted=7)
s = admin.get(f"/api/inventur/{inv}").json()["summary"]
check("Inventur: Zwischenstand zählt richtig", s == {"total": 4, "ok": 2, "fehlt": 0, "defekt": 1, "open": 1}, str(s))
r = admin.post(f"/api/inventur/{inv}/finish", json={})
check("Inventur: mit offenen Einträgen nur auf ausdrücklichen Wunsch", r.status_code == 409, r.text)
iu("TST Leiter", result="fehlt")
pdf = admin.get(f"/api/inventur/{inv}/pdf")
check("Inventur: Zwischenstand als PDF", pdf.status_code == 200 and pdf.content[:4] == b"%PDF")
r = admin.post(f"/api/inventur/{inv}/finish", json={"note": "Jahresinventur"})
check("Inventur: abschließen", r.status_code == 200 and r.json()["defects"] == 2, r.text)
check("Inventur: Mängel angelegt (fehlt + defekt) mit Notiz",
      sql("select count(*) from equipment_defect_reports where description like 'Inventur%Lager quietscht%'") == "1"
      and sql("select count(*) from equipment_defect_reports d join equipment e on e.id = d.equipment_id where e.barcode = 'TSTINV3' and d.description like 'Inventur%fehlt%'") == "1")
check("Inventur: gezählte Menge wird Bestand", sql("select current_stock from consumables where name = 'TST-Bindemittel'") == "7.00")
check("Inventur: danach nicht mehr änderbar", iu("TST Säge", result="fehlt").status_code == 409)
check("Inventur: abgeschlossene bleibt erhalten", admin.delete(f"/api/inventur/{inv}").status_code == 409)
check("Inventur: Gerätewart wird über Mängel informiert", sql("select count(*) from notifications where title = 'Inventur: 2 Mängel'") not in ("", "0"))
lst = admin.get("/api/inventur").json()["inventories"]
check("Inventur: Liste mit Ergebnis", lst[0]["id"] == inv and lst[0]["finished_at"] and lst[0]["fehlt"] == 1 and lst[0]["defekt"] == 1, str(lst[0]))
inv2 = admin.post("/api/inventur", json={"category": "TST-Kat"}).json()["id"]
check("Inventur: laufende lässt sich löschen", admin.delete(f"/api/inventur/{inv2}").status_code == 200
      and sql(f"select count(*) from inventory_items where inventory_id = {inv2}") == "0")
check("Inventur: leere Auswahl abgelehnt", admin.post("/api/inventur", json={"category": "gibt es nicht"}).status_code == 400)
sql("delete from equipment_defect_reports where description like 'Inventur%'; delete from inventories; "
    "delete from equipment where barcode like 'TSTINV%'; delete from consumables where name like 'TST-%'")

# ------------------------------------------------------------ Jahresbericht
JB = 2010  # Jahr ohne Demo-Daten
sql(f"delete from missions where stichwort like '%TSTJB%'; delete from personnel_honors where title like 'TSTJB%'")
sql(f"insert into missions (date, time, stichwort, adresse, meldung, duration, status) values ('{JB}-03-04', '10:00', 'B2 Zimmerbrand TSTJB', 'x', '', 2, 'Freigegeben'), "
    f"('{JB}-03-20', '10:00', 'THL Baum TSTJB', 'x', '', 1.5, 'Freigegeben'), ('{JB}-07-01', '10:00', '[TEST] TSTJB Probealarm', 'x', '', 1, 'Entwurf')")
m1 = int(sql("select id from missions where stichwort = 'B2 Zimmerbrand TSTJB'"))
pids = sql("select id from personnel where membership_status = 'Aktiv' order by id limit 3").split("\n")
sql(f"insert into mission_attendance (mission_id, personnel_id, is_present, vehicle) values ({m1}, {pids[0]}, 'Abgerückt', 'HLF 20'), "
    f"({m1}, {pids[1]}, 'Gerätehaus', ''), ({m1}, {pids[2]}, 'Entschuldigt', '')")
sql(f"insert into personnel_honors (personnel_id, kind, title, date) values ({pids[0]}, 'Beförderung', 'TSTJB Oberfeuerwehrmann', '{JB}-05-01')")
check("Jahresbericht: Mannschaft darf nicht", crew.get(f"/api/admin/annual-report?year={JB}").status_code == 403)
d = admin.get(f"/api/admin/annual-report?year={JB}").json()
check("Jahresbericht: Einsätze ohne Test-Alarm", d["missions"]["total"] == 2, str(d["missions"]))
check("Jahresbericht: Einsätze nach Art", {t["type"] for t in d["missions"]["by_type"]} == {"Brand", "Technische Hilfe"}, str(d["missions"]["by_type"]))
check("Jahresbericht: März hat 2 Einsätze", d["missions"]["by_month"][2] == 2, str(d["missions"]["by_month"]))
check("Jahresbericht: Entschuldigt zählt nicht als Einsatzstunde", d["missions"]["hours"] == 4.0 and d["missions"]["people"] == 2, str(d["missions"]))
check("Jahresbericht: Beförderung steht drin", any(h["title"] == "TSTJB Oberfeuerwehrmann" for h in d["honors"]), str(d["honors"]))
hlf = next((v for v in d["vehicles"] if v["name"] == "HLF 20"), None)
check("Jahresbericht: Fahrzeug-Einsätze gezählt", hlf is None or hlf["missions"] >= 1, str(hlf))
pdf = admin.get(f"/api/admin/annual-report.pdf?year={JB}")
check("Jahresbericht: PDF", pdf.status_code == 200 and pdf.content[:4] == b"%PDF" and len(pdf.content) > 3000, str(pdf.status_code))
check("Jahresbericht: unsinniges Jahr abgelehnt", admin.get("/api/admin/annual-report?year=1800").status_code == 400)
check("Jahresbericht: aktuelles Jahr ohne Angabe", admin.get("/api/admin/annual-report").json()["year"] == datetime.date.today().year)
sql(f"delete from mission_attendance where mission_id = {m1}; delete from missions where stichwort like '%TSTJB%'; delete from personnel_honors where title like 'TSTJB%'")

# ------------------------------------------------------------ Atemschutz-Übersicht
sql("delete from personnel where name like 'TST AGT%'")
today = datetime.date.today()
d = lambda days: (today + datetime.timedelta(days=days)).isoformat()
sql("insert into personnel (name, membership_status, is_agt, g26_3_date, belastungslauf_date, unterweisung_date) values "
    f"('TST AGT Grün', 'Aktiv', 1, '{d(-100)}', '{d(-100)}', '{d(-100)}'), "
    f"('TST AGT Gelb', 'Aktiv', 1, '{d(-100)}', '{d(-340)}', '{d(-100)}'), "
    f"('TST AGT Rot', 'Aktiv', 1, '{d(-100)}', '{d(-400)}', NULL), "
    f"('TST AGT Kein', 'Aktiv', 0, NULL, NULL, NULL)")
for role in ("mannschaft", "geratewart"):
    check(f"Atemschutz: {role} sieht die Übersicht nicht", client(f"r_{role}").get("/api/atemschutz/overview").status_code == 403)
ov = admin.get("/api/atemschutz/overview").json()
by = {p["name"]: p for p in ov["people"] if p["name"].startswith("TST AGT")}
check("Atemschutz: nur Geräteträger", set(by) == {"TST AGT Grün", "TST AGT Gelb", "TST AGT Rot"}, str(set(by)))
check("Atemschutz: alles gültig = grün/einsatzbereit", by["TST AGT Grün"]["status"] == "green" and by["TST AGT Grün"]["ready"], str(by["TST AGT Grün"]))
check("Atemschutz: Belastungsübung läuft bald ab = gelb", by["TST AGT Gelb"]["status"] == "yellow" and by["TST AGT Gelb"]["checks"]["belastung"]["status"] == "soon",
      str(by["TST AGT Gelb"]["checks"]))
check("Atemschutz: abgelaufen bzw. nie erfasst = rot, nicht einsatzbereit", by["TST AGT Rot"]["status"] == "red" and not by["TST AGT Rot"]["ready"]
      and by["TST AGT Rot"]["checks"]["belastung"]["status"] == "expired" and by["TST AGT Rot"]["checks"]["unterweisung"]["status"] == "missing")
check("Atemschutz: Rote stehen oben", ov["people"][0]["status"] == "red")
check("Atemschutz: Wehrführung sieht die Daten", by["TST AGT Grün"]["checks"]["g26"]["last"] == d(-100))
gf = client("r_gruppenfuehrer").get("/api/atemschutz/overview")
gby = {p["name"]: p for p in gf.json()["people"]} if gf.status_code == 200 else {}
check("Atemschutz: Gruppenführer sieht nur die Ampel, keine Gesundheitsdaten", gf.status_code == 200 and gby["TST AGT Grün"]["checks"]["g26"]["last"] is None
      and gby["TST AGT Grün"]["checks"]["g26"]["due"] is None and gby["TST AGT Rot"]["status"] == "red", str(gf.status_code))
aw = client("r_atemschutzwart").get("/api/atemschutz/overview").json()
check("Atemschutz: Atemschutzwart sieht die Daten", next(p for p in aw["people"] if p["name"] == "TST AGT Grün")["checks"]["g26"]["due"] is not None)
from core.reminders import get_due_items  # noqa: E402
due = [i for i in get_due_items(90) if i["name"].startswith("TST AGT")]
check("Fristen: Belastungsübung erscheint", any(i["name"] == "TST AGT Rot" and i["detail"].startswith("Belastungsübung") and i["overdue"] for i in due)
      and any(i["name"] == "TST AGT Gelb" and i["detail"].startswith("Belastungsübung") and not i["overdue"] for i in due), str(due))
sql("delete from personnel where name like 'TST AGT%'")

print("\n==> ", "ALLES OK" if not fails else f"{len(fails)} FEHLER: {fails}")
sys.exit(1 if fails else 0)

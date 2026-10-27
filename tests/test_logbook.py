"""Frei definierbare Logbücher (Flugbuch): Definition, Rechte, Einträge, Auswertung, Export, Kopplung an Fristen."""
import datetime
import sys

import httpx

from core import logbook as lb
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


def day(delta=0):
    return (datetime.date.today() + datetime.timedelta(days=delta)).isoformat()


# ------------------------------------------------------------ reine Logik
F = [{"id": 1, "label": "Start", "ftype": "time", "unit": "", "options": [], "required": True, "evaluate": "none", "config": {}},
     {"id": 2, "label": "Ende", "ftype": "time", "unit": "", "options": [], "required": True, "evaluate": "none", "config": {}},
     {"id": 3, "label": "Dauer", "ftype": "duration", "unit": "", "options": [], "required": False, "evaluate": "sum", "config": {"start": 1, "end": 2}},
     {"id": 4, "label": "Art", "ftype": "select", "unit": "", "options": ["Einsatz", "Übung"], "required": True, "evaluate": "group", "config": {}},
     {"id": 5, "label": "Höhe", "ftype": "number", "unit": "m", "options": [], "required": False, "evaluate": "sum", "config": {}}]
check("Dauer aus Start/Ende, auch über Mitternacht", lb.duration_minutes(F[2], {"1": "22:30", "2": "23:15"}) == 45 and lb.duration_minutes(F[2], {"1": "23:30", "2": "00:15"}) == 45)
check("Dauer ohne Zeit = None", lb.duration_minutes(F[2], {"1": "22:30"}) is None)
check("Eingaben werden geprüft und bereinigt", lb.clean_values(F, {"1": "9:05", "2": "10:00", "4": "Übung", "5": "12,5"}) == {"1": "09:05", "2": "10:00", "4": "Übung", "5": 12.5})
for name, raw in (("Pflichtfeld fehlt", {"1": "09:00", "2": "10:00"}), ("Uhrzeit ungültig", {"1": "25:00", "2": "10:00", "4": "Übung"}),
                  ("Auswahl nicht in der Liste", {"1": "09:00", "2": "10:00", "4": "Quatsch"}), ("Zahl ungültig", {"1": "09:00", "2": "10:00", "4": "Übung", "5": "viel"})):
    try:
        lb.clean_values(F, raw)
        check(f"Ablehnen: {name}", False)
    except lb.FieldError:
        check(f"Ablehnen: {name}", True)
ev = lb.evaluate(F, [{"entry_date": "2026-01-02", "person": "A", "subject": "D1", "values": {"1": "09:00", "2": "10:30", "4": "Übung", "5": 100}},
                     {"entry_date": "2026-02-02", "person": "A", "subject": "D2", "values": {"1": "09:00", "2": "09:30", "4": "Einsatz", "5": 50}},
                     {"entry_date": "2026-03-02", "person": "B", "subject": "D1", "values": {"1": "20:00", "2": "21:00", "4": "Übung"}}])
check("Auswertung: Summen, je Person, je Bezug, nach Auswahl", ev["count"] == 3 and ev["totals"]["Dauer"] == "3:00 h" and ev["totals"]["Höhe"] == "150 m"
      and {p["name"]: p["count"] for p in ev["by_person"]} == {"A": 2, "B": 1} and ev["by_person"][0]["last_date"] == "2026-02-02"
      and ev["by_person"][0]["sums"]["Dauer"] == "2:00 h" and {r["value"]: r["count"] for r in ev["by_field"][0]["rows"]} == {"Übung": 2, "Einsatz": 1}, str(ev["totals"]))
csv_text = lb.to_csv(F, [{"entry_date": "2026-01-02", "person": "=cmd", "subject": "D1", "values": {"1": "09:00", "2": "10:30", "4": "Übung", "5": 12.5}}], "Gerät")
check("CSV: BOM, Semikolon, Dauer als h:mm, Komma, Formel entschärft", csv_text.startswith("﻿Datum;Person;Gerät;Start") and "1:30" in csv_text and "12,5" in csv_text and "'=cmd" in csv_text, csv_text[:160])

# ------------------------------------------------------------ Vorbereitung
make_role_users()
for t in ("logbook_entries", "logbook_fields", "logbook_members", "logbooks", "due_entries", "due_types", "notifications"):
    sql(f"delete from {t}")
sql("delete from settings where setting_key = 'last_reminder_check'")
pid = {n: int(sql(f"select id from personnel where name='{n}'")) for n in ("Jonas Weber", "Tim Fischer", "Laura Schmitt")}
sql(f"update users set personnel_id={pid['Jonas Weber']} where username='r_mannschaft'")
sql(f"update users set personnel_id={pid['Tim Fischer']} where username='r_atemschutzwart'")
sql("update users set personnel_id = NULL where username in ('r_geratewart', 'r_gruppenfuehrer', 'r_jugendwarte', 'r_leitung')")
sql("update personnel set exit_date = NULL")
eq = int(sql("select id from equipment order by id limit 1"))
cl = {r: client(f"r_{r}") for r in ROLES}
admin = client("demo")


def book(**kw):
    base = {"name": "Flugbuch", "subject": "equipment", "responsible_role": "geratewart", "responsible_pid": None, "open_to_all": False,
            "member_ids": [pid["Jonas Weber"], pid["Tim Fischer"]],
            "fields": [{"uid": "a", "label": "Start", "ftype": "time", "required": True},
                       {"uid": "b", "label": "Landung", "ftype": "time", "required": True},
                       {"uid": "c", "label": "Flugdauer", "ftype": "duration", "evaluate": "sum", "config": {"start": "a", "end": "b"}},
                       {"uid": "d", "label": "Art", "ftype": "select", "options": ["Einsatz", "Übung", "Training"], "required": True, "evaluate": "group"},
                       {"uid": "e", "label": "Akku", "ftype": "number", "unit": "%", "evaluate": "sum"},
                       {"uid": "f", "label": "Bemerkung", "ftype": "text"}]}
    base.update(kw)
    return base


# ------------------------------------------------------------ Definition
check("Anlegen: Mannschaft/Gerätewart gesperrt", cl["mannschaft"].post("/api/logbooks", json=book()).status_code == 403 and cl["geratewart"].post("/api/logbooks", json=book()).status_code == 403)
check("Anlegen: anonym gesperrt", httpx.post(B + "/api/logbooks", json=book()).status_code == 401)
bad = {
    "ohne Name": book(name=" "), "ohne Felder": book(fields=[]), "unbekannter Bezug": book(subject="haus"), "Rolle admin": book(responsible_role="admin"),
    "unbekannte Person": book(member_ids=[999999]),
    "doppelte Bezeichnung": book(fields=[{"uid": "a", "label": "X", "ftype": "text"}, {"uid": "b", "label": "x", "ftype": "text"}]),
    "Auswahl ohne Werte": book(fields=[{"uid": "a", "label": "Art", "ftype": "select", "options": []}]),
    "Dauer ohne Start/Ende": book(fields=[{"uid": "a", "label": "D", "ftype": "duration"}]),
    "Dauer mit Nicht-Zeitfeld": book(fields=[{"uid": "a", "label": "T", "ftype": "text"}, {"uid": "b", "label": "E", "ftype": "time"}, {"uid": "c", "label": "D", "ftype": "duration", "config": {"start": "a", "end": "b"}}]),
    "Feldtyp unbekannt": book(fields=[{"uid": "a", "label": "X", "ftype": "bild"}]),
}
for n, payload in bad.items():
    check(f"Anlegen abgelehnt: {n}", admin.post("/api/logbooks", json=payload).status_code in (400, 422))
check("Anlegen: Leitung legt Flugbuch an", cl["leitung"].post("/api/logbooks", json=book()).status_code == 200)
lid = int(sql("select id from logbooks where name='Flugbuch'"))

ls = {r: cl[r].get("/api/logbooks").json() for r in ROLES}
check("Sichtbar für Mitglieder (Mannschaft, Atemschutzwart), Zuständige Rolle (Gerätewart), Leitung - nicht für Gruppenführer/Jugendwart",
      all(len(ls[r]) == 1 for r in ("mannschaft", "atemschutzwart", "geratewart", "leitung")) and ls["gruppenfuehrer"] == [] and ls["jugendwarte"] == [])
check("Mitglied darf eintragen, sieht aber nur Eigenes; Gerätewart sieht alles", ls["mannschaft"][0]["can_write"] and not ls["mannschaft"][0]["sees_all"] and ls["geratewart"][0]["sees_all"] and not ls["geratewart"][0]["manage"])
check("Mitgliederliste nur für Leitung/Admin sichtbar", ls["mannschaft"][0]["member_ids"] == [] and len(ls["leitung"][0]["member_ids"]) == 2)
fid = {f["label"]: f["id"] for f in ls["leitung"][0]["fields"]}
dur = [f for f in ls["leitung"][0]["fields"] if f["ftype"] == "duration"][0]
check("Dauer-Feld verweist auf die echten Zeit-Felder", dur["config"] == {"start": fid["Start"], "end": fid["Landung"]}, str(dur["config"]))


def entry(c, **kw):
    v = {str(fid["Start"]): "09:00", str(fid["Landung"]): "09:45", str(fid["Art"]): "Übung", str(fid["Akku"]): "40", str(fid["Bemerkung"]): "Platzrunde"}
    v.update(kw.pop("values", {}))
    return c.post(f"/api/logbooks/{lid}/entries", json={"entry_date": day(-2), "personnel_id": None, "subject_id": eq, "values": v, **kw})


# ------------------------------------------------------------ Einträge
check("Eintrag: Gruppenführer (kein Mitglied) gesperrt", entry(cl["gruppenfuehrer"]).status_code == 403 and cl["gruppenfuehrer"].get(f"/api/logbooks/{lid}/entries").status_code == 403)
check("Eintrag: ohne Gerät abgelehnt", entry(cl["mannschaft"], subject_id=None).status_code == 400)
check("Eintrag: Pflichtfeld Art fehlt", entry(cl["mannschaft"], values={str(fid["Art"]): ""}).status_code == 400)
check("Eintrag: ungültige Uhrzeit", entry(cl["mannschaft"], values={str(fid["Start"]): "99:99"}).status_code == 400)
check("Eintrag: Auswahl nicht in Liste", entry(cl["mannschaft"], values={str(fid["Art"]): "Quatsch"}).status_code == 400)
check("Eintrag: ungültiges Datum", entry(cl["mannschaft"], entry_date="gestern").status_code == 400)
check("Eintrag: Mitglied trägt für sich ein (fremde Person wird ignoriert)", entry(cl["mannschaft"], personnel_id=pid["Laura Schmitt"]).status_code == 200
      and sql(f"select personnel_id from logbook_entries where logbook_id={lid}") == str(pid["Jonas Weber"]))
check("Eintrag: Atemschutzwart-Mitglied", entry(cl["atemschutzwart"], entry_date=day(-1), values={str(fid["Art"]): "Einsatz", str(fid["Landung"]): "10:30"}).status_code == 200)
check("Eintrag: Gerätewart trägt für Laura ein (Zuständige dürfen für andere)", entry(cl["geratewart"], personnel_id=pid["Laura Schmitt"], values={str(fid["Art"]): "Training"}).status_code == 200)
check("Eintrag: Zuständige ohne Person gesperrt", entry(cl["geratewart"], personnel_id=None).status_code == 400)
mine = cl["mannschaft"].get(f"/api/logbooks/{lid}/entries").json()
check("Mannschaft sieht nur die eigenen Einträge, mit berechneter Dauer", len(mine) == 1 and mine[0]["person"] == "Jonas Weber" and list(mine[0]["durations"].values()) == [45], str(mine))
allr = cl["geratewart"].get(f"/api/logbooks/{lid}/entries").json()
check("Gerätewart (zuständig) sieht alle drei", len(allr) == 3 and {e["person"] for e in allr} == {"Jonas Weber", "Tim Fischer", "Laura Schmitt"})
eid_tim = [e["id"] for e in allr if e["person"] == "Tim Fischer"][0]
eid_jonas = mine[0]["id"]
check("Ändern: fremden Eintrag nicht (Mannschaft)", cl["mannschaft"].put(f"/api/logbooks/entries/{eid_tim}", json={"entry_date": day(), "subject_id": eq, "values": {}}).status_code == 403
      and cl["mannschaft"].delete(f"/api/logbooks/entries/{eid_tim}").status_code == 403)
check("Ändern: den eigenen Eintrag", cl["mannschaft"].put(f"/api/logbooks/entries/{eid_jonas}", json={"entry_date": day(-2), "subject_id": eq, "values": {str(fid["Start"]): "09:00", str(fid["Landung"]): "10:00", str(fid["Art"]): "Übung"}}).status_code == 200
      and list(cl["mannschaft"].get(f"/api/logbooks/{lid}/entries").json()[0]["durations"].values()) == [60])

# ------------------------------------------------------------ Auswertung, Export, Jahr
ev = cl["geratewart"].get(f"/api/logbooks/{lid}/evaluation").json()
check("Auswertung (Zuständige): alle Einträge, Summe Flugdauer, nach Art gruppiert", ev["scope"] == "all" and ev["count"] == 3 and ev["totals"]["Flugdauer"] == "3:15 h"
      and {r["value"]: r["count"] for r in ev["by_field"][0]["rows"]} == {"Übung": 1, "Einsatz": 1, "Training": 1} and len(ev["by_person"]) == 3, str(ev["totals"]))
evm = cl["mannschaft"].get(f"/api/logbooks/{lid}/evaluation").json()
check("Auswertung (Mitglied): nur die eigenen Flüge", evm["scope"] == "own" and evm["count"] == 1 and evm["totals"]["Flugdauer"] == "1:00 h")
check("Auswertung: Gruppenführer gesperrt", cl["gruppenfuehrer"].get(f"/api/logbooks/{lid}/evaluation").status_code == 403)
check("Jahresfilter: leeres Vorjahr", cl["geratewart"].get(f"/api/logbooks/{lid}/evaluation?year=2001").json()["count"] == 0 and cl["geratewart"].get(f"/api/logbooks/{lid}/evaluation?year=1").status_code == 400)
check("Export: Mitglied gesperrt", cl["mannschaft"].get(f"/api/logbooks/{lid}/export.csv").status_code == 403)
sql(f"update logbook_entries set data = replace(data, 'Platzrunde', '=1+1') where id={eid_tim}")
r = cl["geratewart"].get(f"/api/logbooks/{lid}/export.csv")
check("Export: CSV mit Kopf, drei Zeilen, Dateiname, Formel entschärft", r.status_code == 200 and r.text.startswith("﻿Datum;Person;Gerät;Start") and len(r.text.strip().splitlines()) == 4
      and "attachment" in r.headers["content-disposition"] and ".csv" in r.headers["content-disposition"] and "'=1+1" in r.text, r.text[:200])
check("Export: Leitung erlaubt", cl["leitung"].get(f"/api/logbooks/{lid}/export.csv?year={datetime.date.today().year}").status_code == 200)

# ------------------------------------------------------------ Kopplung an Fristen (Pflicht-Übungsflug)
t = {"name": "Pflicht-Übungsflug", "subject": "person", "interval_months": 3, "lead_days": 30, "responsible_role": "geratewart", "responsible_pid": None, "logbook_id": lid}
check("Fristart mit Logbuch: ohne Intervall/Person-Bezug abgelehnt", admin.post("/api/due/types", json={**t, "interval_months": None}).status_code == 400 and admin.post("/api/due/types", json={**t, "subject": "vehicle"}).status_code == 400
      and admin.post("/api/due/types", json={**t, "logbook_id": 99999}).status_code == 400)
check("Fristart mit Logbuch angelegt", admin.post("/api/due/types", json=t).status_code == 200)
tid = int(sql("select id from due_types where name='Pflicht-Übungsflug'"))
sql(f"update logbook_entries set entry_date = CURDATE() - INTERVAL 100 DAY where personnel_id={pid['Jonas Weber']} and logbook_id={lid}")
sql(f"update logbook_entries set entry_date = CURDATE() - INTERVAL 10 DAY where personnel_id={pid['Tim Fischer']} and logbook_id={lid}")


def auto(c):
    return {i["name"]: i for i in c.get("/api/due-items").json() if i["type"] == "custom" and i.get("auto")}


a = auto(admin)
check("Frist aus Logbuch: Jonas (letzter Flug vor 100 Tagen) überfällig, Tim (vor 10 Tagen) nicht fällig", "Jonas Weber" in a and a["Jonas Weber"]["overdue"] and "Tim Fischer" not in a, str(list(a)))
check("… Frist nennt den letzten Eintrag, ist nicht von Hand abhakbar", "letzter Eintrag" in a["Jonas Weber"]["detail"] and not a["Jonas Weber"]["can_done"] and a["Jonas Weber"]["logbook_id"] == lid)
check("… Gerätewart (zuständig) sieht sie, Mannschaft (= Jonas) auch, Atemschutzwart (Tim) nicht", "Jonas Weber" in auto(cl["geratewart"]) and "Jonas Weber" in auto(cl["mannschaft"]) and auto(cl["atemschutzwart"]) == {})
sql(f"delete from logbook_entries where personnel_id={pid['Tim Fischer']}")
check("Mitglied ohne jeden Eintrag = Erstkontrolle statt Verzug", auto(admin).get("Tim Fischer", {}).get("first_check") is True and not auto(admin)["Tim Fischer"]["overdue"])
check("Von Hand eintragen bei Logbuch-Fristart abgelehnt", admin.post("/api/due/entries", json={"type_id": tid, "subject_id": pid["Jonas Weber"], "due_date": day(5), "note": ""}).status_code == 400)
check("Ein neuer Eintrag erledigt die Frist", entry(cl["mannschaft"], entry_date=day()).status_code == 200 and "Jonas Weber" not in auto(admin))
check("Ausgetretene Mitglieder werden nicht erinnert", (sql(f"update personnel set exit_date = CURDATE() - INTERVAL 1 DAY where id={pid['Tim Fischer']}") or True) and "Tim Fischer" not in auto(admin))
sql("update personnel set exit_date = NULL")

# ------------------------------------------------------------ Ändern, Felder entfernen, Löschen
cur = cl["leitung"].get("/api/logbooks").json()[0]
fields = [{"uid": str(f["id"]), "id": f["id"], "label": f["label"], "ftype": f["ftype"], "unit": f["unit"], "options": f["options"], "required": f["required"], "evaluate": f["evaluate"],
           "config": ({"start": str(f["config"]["start"]), "end": str(f["config"]["end"])} if f["ftype"] == "duration" else None)} for f in cur["fields"] if f["label"] != "Akku"]
fields.append({"uid": "neu", "label": "Windstärke", "ftype": "number", "unit": "Bft", "evaluate": "sum"})
check("Logbuch ändern: Feld entfernen, neues Feld, Mitglieder ändern", cl["leitung"].put(f"/api/logbooks/{lid}", json=book(name="Drohnen-Flugbuch", fields=fields, member_ids=[pid["Jonas Weber"]])).status_code == 200)
cur = cl["leitung"].get("/api/logbooks").json()[0]
labels = [f["label"] for f in cur["fields"]]
check("… Felder und Name übernommen, Einträge bleiben, Dauer-Verweis noch richtig", cur["name"] == "Drohnen-Flugbuch" and "Akku" not in labels and "Windstärke" in labels and len(cl["leitung"].get(f"/api/logbooks/{lid}/entries").json()) >= 2
      and [f for f in cur["fields"] if f["ftype"] == "duration"][0]["config"]["start"] == fid["Start"], str(labels))
check("… Atemschutzwart ist kein Mitglied mehr (kein Zugriff)", cl["atemschutzwart"].get("/api/logbooks").json() == [] and entry(cl["atemschutzwart"]).status_code == 403)
check("Löschen: Gerätewart gesperrt", cl["geratewart"].delete(f"/api/logbooks/{lid}").status_code == 403)
check("Löschen: Admin löscht samt Einträgen, Fristart bleibt ohne Logbuch", admin.delete(f"/api/logbooks/{lid}").status_code == 200 and sql("select count(*) from logbook_entries") == "0"
      and sql(f"select count(*) from logbook_fields where logbook_id={lid}") == "0" and sql(f"select coalesce(logbook_id, 'NULL') from due_types where id={tid}") == "NULL")

for t_ in ("logbook_entries", "logbook_fields", "logbook_members", "logbooks", "due_entries", "due_types", "notifications"):
    sql(f"delete from {t_}")
print("\n==> ", "ALLES OK" if not fails else f"{len(fails)} FEHLER: {fails}")
sys.exit(1 if fails else 0)

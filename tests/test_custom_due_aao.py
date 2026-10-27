"""Eigene Fristarten (z.B. Drohnenpilot) und die frei einstellbare AAO fürs Hallen-Display."""
import datetime
import sys
from pathlib import Path

import httpx

from core.aao import match_score, pick_rule
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


def iso(days):
    return (datetime.date.today() + datetime.timedelta(days=days)).isoformat()


# ------------------------------------------------------------ AAO: Zuordnung (ohne Server)
check("Muster passt, wenn es im Stichwort vorkommt (egal ob groß/klein)", match_score("b2", "B2 - Wohnungsbrand") == 2)
check("Mehrere Muster: das längste passende zählt", match_score("B2, Wohnungsbrand", "B2 - Wohnungsbrand") == len("wohnungsbrand"))
check("'*' passt immer, aber mit dem niedrigsten Wert", match_score("*", "irgendwas") == 0)
check("Nicht passend = None", match_score("THL", "B2 Brand") is None)
rules = [{"pattern": "*", "n": "alle"}, {"pattern": "B2", "n": "b2"}, {"pattern": "B2 Wohnung", "n": "wohnung"}]
check("Die spezifischste Regel gewinnt", pick_rule(rules, "B2 Wohnungsbrand")["n"] == "wohnung" and pick_rule(rules, "B2 Keller")["n"] == "b2"
      and pick_rule(rules, "THL")["n"] == "alle")
check("Ohne Regel nichts", pick_rule([], "B2") is None)

# ------------------------------------------------------------ Vorbereitung
make_role_users()
for t in ("aao_rules", "due_entries", "due_types", "notifications"):
    sql(f"delete from {t}")
sql("delete from settings where setting_key = 'last_reminder_check'")
pid = {n: int(sql(f"select id from personnel where name='{n}'")) for n in ("Jonas Weber", "Tim Fischer", "Laura Schmitt")}
sql(f"update users set personnel_id={pid['Jonas Weber']} where username='r_mannschaft'")
sql(f"update users set personnel_id={pid['Tim Fischer']} where username='r_atemschutzwart'")
sql("update users set personnel_id = NULL where username in ('r_geratewart', 'r_gruppenfuehrer', 'r_jugendwarte', 'r_leitung')")
sql("update personnel set exit_date = NULL")

cl = {r: client(f"r_{r}") for r in ROLES}
admin = client("demo")
token = admin.get("/api/settings/access-tokens").json()["display_token"]


def due(c):
    return c.get("/api/due-items").json()


def custom(c):
    return [i for i in due(c) if i["type"] == "custom"]


# ------------------------------------------------------------ AAO: Verwaltung und Display
check("AAO: Anonym gesperrt", httpx.get(B + "/api/aao").status_code == 401 and httpx.get(B + "/api/aao/recommend?stichwort=B2").status_code == 401)
check("AAO: Mit Display-Token abrufbar", httpx.get(B + f"/api/aao/recommend?stichwort=B2&token={token}").status_code == 200)
check("AAO: Falscher Token gesperrt", httpx.get(B + "/api/aao/recommend?stichwort=B2&token=falsch").status_code == 401)
body = lambda p, v: {"pattern": p, "entries": [{"vehicle": x, "role": r} for x, r in v]}
check("AAO: Mannschaft darf nicht anlegen", cl["mannschaft"].post("/api/aao", json=body("B2", [("LF 10", "Angriff")])).status_code == 403)
check("AAO: Gerätewart darf nicht anlegen", cl["geratewart"].post("/api/aao", json=body("B2", [("LF 10", "Angriff")])).status_code == 403)
check("AAO: Ohne Fahrzeug abgelehnt", admin.post("/api/aao", json={"pattern": "B2", "entries": []}).status_code == 400)
check("AAO: Ohne Muster abgelehnt", admin.post("/api/aao", json=body("  ", [("LF 10", "")])).status_code == 400)
check("AAO: Ohne Regel keine Empfehlung (kein erfundenes HLF/MZF mehr)", httpx.get(B + f"/api/aao/recommend?stichwort=B2&token={token}").json()["entries"] == [])
check("AAO: Leitung legt an", cl["leitung"].post("/api/aao", json=body("*", [("MTF", "Erkundung")])).status_code == 200)
check("AAO: Admin legt an", admin.post("/api/aao", json=body("B2, Wohnungsbrand", [("LF 10", "Angriffstrupp"), ("DLK 23", "Rettung")])).status_code == 200)
rec = lambda st: httpx.get(B + "/api/aao/recommend", params={"stichwort": st, "token": token}).json()
r = rec("B2 - Wohnungsbrand")
check("AAO: Display bekommt die passende Regel in Reihenfolge", [e["vehicle"] for e in r["entries"]] == ["LF 10", "DLK 23"] and r["entries"][0]["role"] == "Angriffstrupp", str(r))
check("AAO: Standardregel für alles andere", [e["vehicle"] for e in rec("Ölspur")["entries"]] == ["MTF"])
lst = cl["mannschaft"].get("/api/aao").json()
check("AAO: Jeder Angemeldete sieht die Regeln, nur Leitung/Admin bearbeiten", len(lst["rules"]) == 2 and not lst["can_edit"] and admin.get("/api/aao").json()["can_edit"])
rid = [x["id"] for x in lst["rules"] if x["pattern"].startswith("B2")][0]
check("AAO: Ändern", admin.put(f"/api/aao/{rid}", json=body("B2", [("HLF 20", "Erstangriff")])).status_code == 200 and rec("B2 Keller")["entries"][0]["vehicle"] == "HLF 20")
check("AAO: Mannschaft darf nicht löschen", cl["mannschaft"].delete(f"/api/aao/{rid}").status_code == 403)
check("AAO: Löschen", admin.delete(f"/api/aao/{rid}").status_code == 200 and rec("B2 Keller")["entries"][0]["vehicle"] == "MTF")
check("AAO: Display hat keine fest eingebauten Fahrzeuge mehr", "HLF 20" not in (Path(__file__).resolve().parent.parent / "static" / "alarmdisplay.html").read_text(encoding="utf-8"))

# ------------------------------------------------------------ Fristarten: Rechte
tp = {"name": "Drohnenpilot Landkreis", "subject": "person", "interval_months": 12, "lead_days": 30, "responsible_role": "geratewart", "responsible_pid": pid["Tim Fischer"]}
check("Fristart: Mannschaft darf nicht anlegen", cl["mannschaft"].post("/api/due/types", json=tp).status_code == 403)
check("Fristart: Gerätewart darf nicht anlegen", cl["geratewart"].post("/api/due/types", json=tp).status_code == 403)
check("Fristart: ohne Namen abgelehnt", admin.post("/api/due/types", json={**tp, "name": " "}).status_code == 400)
check("Fristart: unbekannter Bezug abgelehnt", admin.post("/api/due/types", json={**tp, "subject": "haus"}).status_code == 400)
check("Fristart: Rolle 'admin' nicht wählbar", admin.post("/api/due/types", json={**tp, "responsible_role": "admin"}).status_code == 400)
check("Fristart: unbekannte Person abgelehnt", admin.post("/api/due/types", json={**tp, "responsible_pid": 999999}).status_code == 400)
check("Fristart: Wiederholung außerhalb 1-240 abgelehnt", admin.post("/api/due/types", json={**tp, "interval_months": 0}).status_code == 400)
check("Fristart: Leitung legt an", cl["leitung"].post("/api/due/types", json=tp).status_code == 200)
tid = int(sql("select id from due_types where name = 'Drohnenpilot Landkreis'"))
check("Fristart: Zuständige sehen sie (Rolle und Person), Mannschaft nicht",
      [t["id"] for t in cl["geratewart"].get("/api/due/types").json()["types"]] == [tid]
      and [t["id"] for t in cl["atemschutzwart"].get("/api/due/types").json()["types"]] == [tid]
      and cl["mannschaft"].get("/api/due/types").json()["types"] == [] and cl["gruppenfuehrer"].get("/api/due/types").json()["types"] == [])

# ------------------------------------------------------------ Fristen eintragen
e_laura = {"type_id": tid, "subject_id": pid["Laura Schmitt"], "due_date": iso(10), "note": "Zeugnis 4711"}
check("Frist: Mannschaft darf nicht eintragen", cl["mannschaft"].post("/api/due/entries", json={**e_laura, "subject_id": pid["Jonas Weber"]}).status_code == 403)
check("Frist: ohne Person abgelehnt", cl["geratewart"].post("/api/due/entries", json={**e_laura, "subject_id": None}).status_code == 400)
check("Frist: ohne Datum abgelehnt", cl["geratewart"].post("/api/due/entries", json={**e_laura, "due_date": None}).status_code == 400)
check("Frist: Gerätewart (zuständige Rolle) trägt ein", cl["geratewart"].post("/api/due/entries", json=e_laura).status_code == 200)
check("Frist: Atemschutzwart (zuständige Person) trägt ein", cl["atemschutzwart"].post("/api/due/entries", json={**e_laura, "subject_id": pid["Jonas Weber"], "due_date": iso(5), "note": ""}).status_code == 200)
check("Frist: weit entfernte (200 Tage, Vorlauf 30) taucht nicht auf", cl["leitung"].post("/api/due/entries", json={**e_laura, "subject_id": pid["Tim Fischer"], "due_date": iso(200)}).status_code == 200
      and not any(i["name"] == "Tim Fischer" for i in custom(admin)))
check("Subjekte: Mannschaft gesperrt, Gerätewart (Personen-Fristart) bekommt die Liste",
      cl["mannschaft"].get("/api/due/subjects?kind=person").status_code == 403 and len(cl["geratewart"].get("/api/due/subjects?kind=person").json()) > 5)
check("Subjekte: Gerätewart ohne Fahrzeug-Fristart gesperrt", cl["geratewart"].get("/api/due/subjects?kind=vehicle").status_code == 403)

# ------------------------------------------------------------ Wer sieht was
a = custom(admin)
check("Admin sieht beide fälligen eigenen Fristen", {i["name"] for i in a} == {"Laura Schmitt", "Jonas Weber"}, str([i["name"] for i in a]))
it = [i for i in a if i["name"] == "Laura Schmitt"][0]
check("Eintrag trägt Bereich Personen, Fristart und Notiz", it["area"] == "personal" and "Drohnenpilot Landkreis" in it["detail"] and "4711" in it["detail"] and it["can_done"], str(it))
check("Leitung sieht beide", len(custom(cl["leitung"])) == 2)
check("Gerätewart (zuständige Rolle) sieht beide und darf abhaken", len(custom(cl["geratewart"])) == 2 and all(i["can_done"] for i in custom(cl["geratewart"])))
check("Atemschutzwart (zuständige Person) sieht beide", len(custom(cl["atemschutzwart"])) == 2)
mi = custom(cl["mannschaft"])
check("Mannschaft (verknüpft mit Jonas) sieht nur die eigene, ohne Abhaken", [i["name"] for i in mi] == ["Jonas Weber"] and not mi[0]["can_done"], str(mi))
check("Gruppenführer/Jugendwart: nichts", custom(cl["gruppenfuehrer"]) == [] and custom(cl["jugendwarte"]) == [])
check("Alte Fristen bleiben unberührt (kein Absturz ohne eigene)", isinstance(due(admin), list))

# ------------------------------------------------------------ Bezug Fahrzeug / Gerät / allgemein
vid = int(sql("select id from vehicles order by id limit 1"))
eid = int(sql("select id from equipment order by id limit 1"))
for name, subj in (("Fahrzeug-Zulassung", "vehicle"), ("Drohnen-Versicherung", "equipment"), ("Landkreis-Meldung", "general")):
    assert admin.post("/api/due/types", json={"name": name, "subject": subj, "interval_months": None, "lead_days": None, "responsible_role": "", "responsible_pid": None}).status_code == 200
t_id = {n: int(sql(f"select id from due_types where name='{n}'")) for n in ("Fahrzeug-Zulassung", "Drohnen-Versicherung", "Landkreis-Meldung")}
admin.post("/api/due/entries", json={"type_id": t_id["Fahrzeug-Zulassung"], "subject_id": vid, "due_date": iso(-3), "note": ""})
admin.post("/api/due/entries", json={"type_id": t_id["Drohnen-Versicherung"], "subject_id": eid, "due_date": iso(4), "note": ""})
admin.post("/api/due/entries", json={"type_id": t_id["Landkreis-Meldung"], "subject_id": None, "due_date": iso(6), "note": "Jahresmeldung"})
areas = {i["detail"].split(" - ")[0]: i for i in custom(admin)}
check("Fahrzeug-Frist: Bereich Fahrzeuge, überfällig", areas["Fahrzeug-Zulassung"]["area"] == "fahrzeuge" and areas["Fahrzeug-Zulassung"]["overdue"])
check("Geräte-Frist: Bereich Geräte & Material", areas["Drohnen-Versicherung"]["area"] == "material")
check("Allgemeine Frist: Bereich 'Sonstige', Name = Fristart, Notiz als Detail", areas["Jahresmeldung"]["area"] == "sonstige" and areas["Jahresmeldung"]["name"] == "Landkreis-Meldung")
check("Gerätewart sieht diese NICHT (keine Zuständigkeit, auch wenn Fahrzeuge/Geräte sonst Technik sind)", not any(i["detail"].startswith(("Fahrzeug-Zulassung", "Drohnen-Versicherung", "Jahresmeldung")) for i in custom(cl["geratewart"])))

# ------------------------------------------------------------ Benachrichtigungen
from core.reminders import check_due_reminders
check_due_reminders()


def cats(c):
    return {k["key"]: k for k in c.get("/api/notifications").json()["categories"]}


check("Glocke: Gerätewart bekommt 'Fristen: Eigene'", "fristen_eigene" in cats(cl["geratewart"]) and "Laura Schmitt" in cats(cl["geratewart"])["fristen_eigene"]["items"][0]["body"])
check("Glocke: Zuständige Person (Atemschutzwart) bekommt sie auch", "fristen_eigene" in cats(cl["atemschutzwart"]))
mc = cats(cl["mannschaft"])
check("Glocke: Betroffene Person nur mit der eigenen Frist", "fristen_eigene" in mc and "Jonas" in mc["fristen_eigene"]["items"][0]["body"] and "Laura" not in mc["fristen_eigene"]["items"][0]["body"], str(mc.get("fristen_eigene")))
check("Glocke: Gruppenführer ohne Zuständigkeit bekommt nichts", "fristen_eigene" not in cats(cl["gruppenfuehrer"]))
check("Glocke: Eigene Fristen verfälschen nicht 'Technik'/'Personen' des Gerätewarts", "fristen_personen" not in cats(cl["geratewart"]))

# ------------------------------------------------------------ Abhaken
eid_laura = int(sql(f"select id from due_entries where personnel_id={pid['Laura Schmitt']} and type_id={tid}"))
check("Abhaken: Betroffene Person (Mannschaft) darf nicht", cl["mannschaft"].post(f"/api/due/entries/{eid_laura}/done", json={}).status_code == 403)
check("Abhaken: Gruppenführer darf nicht", cl["gruppenfuehrer"].post(f"/api/due/entries/{eid_laura}/done", json={}).status_code == 403)
r = cl["geratewart"].post(f"/api/due/entries/{eid_laura}/done", json={})
exp = (datetime.date.today() + datetime.timedelta(days=10))
nxt = datetime.date.fromisoformat(r.json()["next_due"]) if r.status_code == 200 else None
check("Abhaken mit Intervall: nächstes Datum = Fälligkeit + 12 Monate", nxt is not None and nxt.year == exp.year + 1 and nxt.month == exp.month, str(nxt))
check("… Frist verschwindet aus den fälligen, Zuletzt-erledigt gemerkt", not any(i["name"] == "Laura Schmitt" for i in custom(admin))
      and any(e["last_done"] == datetime.date.today().isoformat() for e in cl["geratewart"].get("/api/due/entries").json()))
eid_ver = int(sql(f"select id from due_entries where type_id={t_id['Drohnen-Versicherung']}"))
r = admin.post(f"/api/due/entries/{eid_ver}/done", json={})
check("Abhaken ohne Intervall: erledigt, kein nächstes Datum", r.status_code == 200 and r.json()["next_due"] is None and not any(i["detail"].startswith("Drohnen-Versicherung") for i in custom(admin)))
check("Mannschaft sieht keine Verwaltungsliste", cl["mannschaft"].get("/api/due/entries").json() == [])
check("Gerätewart sieht nur Einträge seiner Fristart", {e["type_name"] for e in cl["geratewart"].get("/api/due/entries").json()} == {"Drohnenpilot Landkreis"})

# ------------------------------------------------------------ Ändern, Vorlauf, Löschen
eid_jonas = int(sql(f"select id from due_entries where personnel_id={pid['Jonas Weber']} and type_id={tid}"))
check("Ändern: Datum weit nach hinten -> nicht mehr im Vorlauf", cl["geratewart"].put(f"/api/due/entries/{eid_jonas}", json={"type_id": tid, "subject_id": pid["Jonas Weber"], "due_date": iso(90), "note": ""}).status_code == 200
      and not any(i["name"] == "Jonas Weber" for i in custom(admin)))
check("Ändern: Mannschaft darf nicht", cl["mannschaft"].put(f"/api/due/entries/{eid_jonas}", json={"type_id": tid, "subject_id": pid["Jonas Weber"], "due_date": iso(1), "note": ""}).status_code == 403)
sql(f"update personnel set exit_date = CURDATE() - INTERVAL 1 DAY where id={pid['Laura Schmitt']}")
sql(f"update due_entries set due_date = CURDATE() + INTERVAL 3 DAY where id={eid_laura}")
check("Ausgetretene Person: Frist wird nicht mehr gemeldet", not any(i["name"] == "Laura Schmitt" for i in custom(admin)))
sql("update personnel set exit_date = NULL")
check("Fristart löschen: nur Admin/Leitung", cl["geratewart"].delete(f"/api/due/types/{tid}").status_code == 403)
check("Fristart löschen entfernt ihre Fristen", admin.delete(f"/api/due/types/{tid}").status_code == 200 and sql(f"select count(*) from due_entries where type_id={tid}") == "0")

# aufräumen, damit die folgenden Tests nichts davon sehen
for t in ("aao_rules", "due_entries", "due_types"):
    sql(f"delete from {t}")
sql("delete from notifications")
print("\n==> ", "ALLES OK" if not fails else f"{len(fails)} FEHLER: {fails}")
sys.exit(1 if fails else 0)

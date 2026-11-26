"""Datensparsamkeit und Rechte: wer sieht Entschädigungen, Beiträge, Anschriften und private Akten anderer Kameraden?"""
import sys

import httpx

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


make_role_users()
cl = {r: client(f"r_{r}") for r in ROLES}
admin = client("demo")
pid = int(sql("select id from personnel where name='Anna Muster'"))
other = int(sql("select id from personnel where name='Jonas Weber'"))
sql(f"update users set personnel_id={other} where username='r_mannschaft'")
# der Mannschafts-Login ist mit Jonas verknüpft: die eigene Akte darf er vollständig sehen
SENSITIVE_LIST = ("address", "birth_date", "g26_3_date", "belastungslauf_date", "unterweisung_date", "license_checked_date")
SENSITIVE_ONE = SENSITIVE_LIST + ("notes", "skills", "parent_contact", "emergency_contact_name", "emergency_contact_phone", "badge_number")

for role in ("mannschaft", "geratewart", "gruppenfuehrer", "jugendwarte", "atemschutzwart"):
    c = cl[role]
    check(f"{role}: Stunden/Entschädigung aller Kameraden gesperrt", c.get("/api/missions/billing/compensations/list?year=2026&hourly_rate=10").status_code == 403)
    check(f"{role}: Rechnungsliste gesperrt", c.get("/api/missions/billing/list").status_code == 403)
    check(f"{role}: Arbeitgeberbescheinigungen gesperrt", c.get("/api/missions/employer-certificates").status_code == 403)
    check(f"{role}: Spenden und Mitgliedsbeiträge gesperrt", c.get("/api/verein/donations").status_code == 403 and c.get("/api/verein/membership-fees?year=2026").status_code == 403)
    lst = c.get("/api/personnel/list").json()
    leaked = sorted({k for row in lst for k in SENSITIVE_LIST if k in row} - ({"g26_3_date", "belastungslauf_date", "unterweisung_date", "license_checked_date"} if role == "atemschutzwart" else set()))
    check(f"{role}: Kameradenliste ohne Anschrift/Geburtsdatum{'/Gesundheitsdaten' if role != 'atemschutzwart' else ''}", not leaked, str(leaked))
    adults = [row for row in lst if row.get("membership_status") != "Jugend"]
    check(f"{role}: Kameradenliste liefert weiter Name, Rang, Telefon, Qualifikationen, G26-Warnung", all(k in adults[0] for k in ("name", "rank", "phone", "is_agt", "g26_expired")))
    one = c.get(f"/api/personnel/get/{pid}").json()
    leaked = sorted(k for k in SENSITIVE_ONE if k in one and not (role == "atemschutzwart" and k in ("g26_3_date", "belastungslauf_date", "unterweisung_date", "license_checked_date")))
    check(f"{role}: fremde Akte nur als Kurzansicht", not leaked, str(leaked))

own = cl["mannschaft"].get(f"/api/personnel/get/{other}").json()
check("mannschaft: die eigene Akte ist vollständig", all(k in own for k in ("birth_date", "address", "emergency_contact_name")), str([k for k in SENSITIVE_ONE if k not in own]))
check("mannschaft: Statistik der Wehrführung gesperrt", cl["mannschaft"].get("/api/admin/stats?year=2026").status_code == 403)

for role in ("leitung",):
    c = cl[role]
    check(f"{role}: Entschädigung, Rechnungen, Bescheinigungen, Beiträge erlaubt", c.get("/api/missions/billing/compensations/list?year=2026&hourly_rate=10").status_code == 200
          and c.get("/api/missions/billing/list").status_code == 200 and c.get("/api/missions/employer-certificates").status_code == 200
          and c.get("/api/verein/donations").status_code == 200 and c.get("/api/verein/membership-fees?year=2026").status_code == 200)
    lst = c.get("/api/personnel/list").json()
    check(f"{role}: sieht alle Felder der Kameraden", all(k in lst[0] for k in SENSITIVE_LIST))
    check(f"{role}: sieht fremde Akten vollständig", all(k in c.get(f"/api/personnel/get/{pid}").json() for k in SENSITIVE_ONE))
check("admin: alles erlaubt", admin.get("/api/missions/billing/compensations/list?year=2026&hourly_rate=10").status_code == 200 and admin.get("/api/verein/donations").status_code == 200)
check("Vereinsinventar bleibt für alle lesbar", cl["mannschaft"].get("/api/verein/inventory").status_code == 200)

# Jugendfeuerwehr: Daten Minderjähriger nur für Wehrführung und Jugendwarte
sql("delete from personnel where name = 'Testjugend Kind'")
sql("insert into personnel (name, membership_status, phone, email, parent_contact) values ('Testjugend Kind', 'Jugend', '0151 99999', 'kind@example.org', 'Eltern 0151 88888')")
kid = int(sql("select id from personnel where name = 'Testjugend Kind'"))
for role in ("mannschaft", "geratewart", "gruppenfuehrer", "atemschutzwart"):
    c = cl[role]
    check(f"{role}: Jugend-Mitglieder und -Dienstberichte gesperrt", c.get("/api/jugend/members").status_code == 403 and c.get("/api/jugend/sessions").status_code == 403)
    row = [x for x in c.get("/api/personnel/list").json() if x["name"] == "Testjugend Kind"][0]
    check(f"{role}: Kontaktdaten des Jugendlichen in der Kameradenliste verborgen", "phone" not in row and "email" not in row, str(sorted(row)))
    one = c.get(f"/api/personnel/get/{kid}").json()
    check(f"{role}: … und in der Einzelansicht (auch keine Elternkontakte)", not any(k in one for k in ("phone", "email", "parent_contact")), str(sorted(one)))
for role in ("jugendwarte", "leitung"):
    c = cl[role]
    check(f"{role}: Jugend-Mitglieder und -Dienstberichte erlaubt", c.get("/api/jugend/members").status_code == 200 and c.get("/api/jugend/sessions").status_code == 200)
    row = [x for x in c.get("/api/personnel/list").json() if x["name"] == "Testjugend Kind"][0]
    check(f"{role}: sieht die Kontaktdaten des Jugendlichen", row.get("phone") == "0151 99999" and row.get("email") == "kind@example.org")
check("admin: Jugend-Mitglieder erlaubt", admin.get("/api/jugend/members").status_code == 200)
check("Jugend: Anlegen/Löschen weiter nur Wehrführung und Jugendwarte", cl["mannschaft"].post("/api/jugend/members", json={"name": "X Y"}).status_code == 403)
sql("delete from personnel where name = 'Testjugend Kind'")

# --- Ohne Anmeldung: nichts aus der Wehr (Objekte mit Schlüsseldepot, Hydranten, Termine, hochgeladene Dateien)
anon = httpx.Client(base_url=B, timeout=30)
for path in ("/api/material/bma", "/api/material/hydrants", "/api/missions/schedules/list"):
    check(f"Ohne Anmeldung gesperrt: {path}", anon.get(path).status_code == 401)
tok = admin.get("/api/settings/access-tokens").json()["display_token"]
for path in ("/api/material/bma", "/api/material/hydrants", "/api/missions/schedules/list"):
    check(f"Hallen-Display mit Token erlaubt: {path}", anon.get(f"{path}?token={tok}").status_code == 200)
    check(f"Falscher Token gesperrt: {path}", anon.get(f"{path}?token=falsch").status_code == 401)
# Einsatz-Freigabe: nur gültige Unterschrift, freigegebene Berichte sind für Nicht-Admins gesperrt, Entfernen nimmt die Freigabe zurück
_m = admin.post("/api/missions", json={"date": "2026-02-02", "time": "10:00", "stichwort": "Sig-Test", "adresse": "Teststr. 1", "meldung": ""}).json()["id"]
_sig = "data:image/png;base64," + "A" * 200
gf = cl["gruppenfuehrer"]
check("Freigabe: ungültige Unterschrift abgelehnt", gf.post(f"/api/missions/{_m}/signature", json={"signature": "javascript:alert(1)"}).status_code == 400)
check("Freigabe: unbekannter Einsatz 404", gf.post("/api/missions/99999999/signature", json={"signature": _sig}).status_code == 404)
check("Freigabe: Gruppenführer darf unterschreiben", gf.post(f"/api/missions/{_m}/signature", json={"signature": _sig}).status_code == 200)
check("Freigabe: danach für Gruppenführer gesperrt", gf.post(f"/api/missions/{_m}/signature", json={"signature": None}).status_code == 403)
check("Freigabe: Admin kann Unterschrift entfernen", admin.post(f"/api/missions/{_m}/signature", json={"signature": None}).status_code == 200)
_d = admin.get(f"/api/missions/{_m}").json()
check("Freigabe: Entfernen nimmt Freigabe zurück", _d.get("status") == "In Prüfung" and not _d.get("leader_signature"), str(_d.get("status")))
admin.delete(f"/api/missions/{_m}")

# --- Freigabe per Klick (Huehnerleiter)
admin.post("/api/admin/permissions/reset")
_body = {"date": "2026-11-26", "time": "10:00", "stichwort": "ZZ-Leiter", "adresse": "x", "meldung": "y", "status": "Entwurf"}
_a = admin.post("/api/missions", json=_body).json()["id"]
gf, lt = cl["gruppenfuehrer"], cl["leitung"]
check("Leiter: Standard - Gruppenfuehrer hat das Freigaberecht", "missions.approve" in gf.get("/api/auth/me").json()["permissions"])
admin.put("/api/admin/permissions", json={"role": "gruppenfuehrer", "permission": "missions.approve", "allowed": False})
check("Leiter: ohne Recht keine Freigabe per Klick", gf.post(f"/api/missions/{_a}/approve", json={}).status_code == 403)
check("Leiter: ohne Recht keine Unterschrift", gf.post(f"/api/missions/{_a}/signature", json={"signature": _sig}).status_code == 403)
check("Leiter: ohne Recht keine Unterschrift ueber die Dienstliste", gf.post(f"/sessions/m_{_a}/leader_signature", json={"signature": _sig}).status_code == 403)
check("Leiter: ohne Recht kein Status Freigegeben beim Speichern", gf.put(f"/api/missions/{_a}", json={**_body, "status": "Freigegeben"}).status_code == 403)
check("Leiter: ohne Recht kein freigegebener neuer Einsatz", gf.post("/api/missions", json={**_body, "status": "Freigegeben"}).status_code == 403)
check("Leiter: Mannschaft darf nicht einreichen", cl["mannschaft"].post(f"/api/missions/{_a}/submit").status_code == 403)
check("Leiter: Gruppenfuehrer reicht ein", gf.post(f"/api/missions/{_a}/submit").status_code == 200)
check("Leiter: Status In Pruefung", admin.get(f"/api/missions/{_a}").json()["status"] == "In Prüfung")
check("Leiter: Leitung wird benachrichtigt", int(sql("select count(*) from notifications where username='r_leitung' and title like '%ZZ-Leiter%'")) == 1)
check("Leiter: Leitung gibt per Klick frei", lt.post(f"/api/missions/{_a}/approve", json={}).status_code == 200)
_d = admin.get(f"/api/missions/{_a}").json()
check("Leiter: freigegeben mit Name und Zeit", _d["status"] == "Freigegeben" and _d["approved_by"] == "r_leitung" and _d["approved_at"], str(_d.get("approved_by")))
check("Leiter: Bericht nennt die Freigabe", "Digital freigegeben von" in admin.get(f"/api/missions/{_a}/report").text)
check("Leiter: PDF wird erzeugt", admin.get(f"/api/missions/{_a}/pdf").status_code == 200)
check("Leiter: danach gesperrt", gf.put(f"/api/missions/{_a}", json=_body).status_code == 403)
check("Leiter: nur Admin nimmt zurueck", lt.post(f"/api/missions/{_a}/approve", json={"approve": False}).status_code == 403)
check("Leiter: Admin nimmt zurueck", admin.post(f"/api/missions/{_a}/approve", json={"approve": False}).status_code == 200 and not admin.get(f"/api/missions/{_a}").json()["approved_by"])
admin.delete(f"/api/missions/{_a}")
admin.post("/api/admin/permissions/reset")

# --- Alarm-Webhook: Schluessel, Doppelte, Protokoll
sql("delete from apager_logs"); sql("delete from apager_webhook_log")
check("Webhook: Mannschaft sieht den Schluessel nicht", cl["mannschaft"].get("/api/apager/config").status_code == 403)
check("Webhook: Geraetewart sieht den Schluessel nicht", cl["geratewart"].get("/api/apager/config").status_code == 403)
_key = cl["leitung"].get("/api/apager/config").json()["api_key"]
_hook = httpx.Client(base_url=B, timeout=30)
_p = {"title": "ZZ-Hook F2", "message": "Rauch", "location": "Hauptstr. 1", "token": _key}
_r1 = _hook.post("/api/alarm/webhook", json=_p)
check("Webhook: Alarm kommt an", _r1.status_code == 200 and not _r1.json().get("duplicate"), _r1.text[:100])
check("Webhook: gleicher Alarm wird nur einmal gemeldet", _hook.post("/api/alarm/webhook", json=_p).json().get("duplicate") is True)
check("Webhook: nur ein Eintrag im Alarmprotokoll", int(sql("select count(*) from apager_logs where stichwort='ZZ-Hook F2'")) == 1)
check("Webhook: anderer Alarm kommt durch", not _hook.post("/api/alarm/webhook", json={**_p, "message": "anders"}).json().get("duplicate"))
check("Webhook: Formulardaten und Schluessel in der Adresse", _hook.post(f"/api/alarm/webhook?api_key={_key}", data={"stichwort": "ZZ-Form"}).status_code == 200)
check("Webhook: Text ohne JSON stuerzt nicht", _hook.post(f"/api/alarm/webhook?api_key={_key}", content=b"[1,2]", headers={"content-type": "application/json"}).status_code == 200)
check("Webhook: falscher Schluessel abgelehnt", _hook.post("/api/alarm/webhook", json={"token": "x"}).status_code == 401)
_log = admin.get("/api/apager/webhook-log").json()
check("Webhook: Protokoll zeigt die Aufrufe", len(_log) == 5 and any("ZZ-Hook F2" in e["body_data"] for e in _log), str(len(_log)))
check("Webhook: Protokoll enthaelt keinen Schluessel", _key not in str(_log))
check("Webhook: Protokoll nur Admin/Leitung", cl["gruppenfuehrer"].get("/api/apager/webhook-log").status_code == 403)
sql("delete from missions where stichwort like 'ZZ-%' or stichwort='Alarmierung'"); sql("delete from apager_logs")

# --- Einsaetze erscheinen nur in der Dienstliste ihrer Gruppe (und ohne Gruppe in allen)
_gs = admin.get("/groups").json()
check("Dienstliste-Test: mindestens zwei Gruppen vorhanden", isinstance(_gs, list) and len(_gs) >= 2, str(_gs)[:80])
if isinstance(_gs, list) and len(_gs) >= 2:
    _g1, _g2 = _gs[0]["id"], _gs[1]["id"]
    _ma = admin.post("/api/missions", json={"date": "2026-03-03", "time": "10:00", "stichwort": "GrpA-Test", "adresse": "A", "meldung": "", "group_id": _g1}).json()["id"]
    _mn = admin.post("/api/missions", json={"date": "2026-03-04", "time": "10:00", "stichwort": "OhneGrp-Test", "adresse": "B", "meldung": ""}).json()["id"]
    _l1 = [x.get("real_mission_id") for x in admin.get(f"/groups/{_g1}/sessions").json() if x.get("is_mission")]
    _l2 = [x.get("real_mission_id") for x in admin.get(f"/groups/{_g2}/sessions").json() if x.get("is_mission")]
    check("Dienstliste: Einsatz steht in seiner Gruppe", _ma in _l1 and _mn in _l1)
    check("Dienstliste: Einsatz einer anderen Gruppe fehlt, ohne Gruppe steht überall", _ma not in _l2 and _mn in _l2, str(_l2))
    admin.delete(f"/api/missions/{_ma}"); admin.delete(f"/api/missions/{_mn}")

# --- Alarm-Einsaetze bekommen eine feste Gruppe (stehen nicht mehr in jeder Dienstliste)
admin.post("/api/apager/test-alarm", json={"stichwort": "GrpFix", "prefix_test": False})
_mg = sql("select group_id from missions where stichwort='GrpFix' order by id desc limit 1")
check("Alarm-Einsatz hat eine Gruppe", str(_mg).strip() not in ("", "None", "NULL"), str(_mg))
sql("delete from missions where stichwort='GrpFix'")

# --- Beförderungen & Ehrungen
_h = {"kind": "Ehrung", "title": "Ehrenzeichen Test", "date": "2026-05-01", "note": ""}
check("Ehrung: Mannschaft darf nicht eintragen", cl["mannschaft"].post(f"/api/material/personnel/{pid}/honors", json=_h).status_code == 403)
check("Ehrung: ungültige Art abgelehnt", admin.post(f"/api/material/personnel/{pid}/honors", json={**_h, "kind": "Quatsch"}).status_code == 400)
check("Ehrung: ungültiges Datum abgelehnt", admin.post(f"/api/material/personnel/{pid}/honors", json={**_h, "date": "morgen"}).status_code == 400)
check("Ehrung: unbekanntes Mitglied 404", admin.post("/api/material/personnel/99999999/honors", json=_h).status_code == 404)
check("Ehrung: Admin trägt ein", admin.post(f"/api/material/personnel/{pid}/honors", json=_h).status_code == 200)
_hl = admin.get(f"/api/material/personnel/{pid}/honors").json()
check("Ehrung: erscheint in der Liste", any(x["title"] == "Ehrenzeichen Test" for x in _hl), str(_hl))
check("Ehrung: Mannschaft darf nicht löschen", cl["mannschaft"].delete(f"/api/material/personnel/honors/{_hl[0]['id']}").status_code == 403)
for _x in _hl:
    admin.delete(f"/api/material/personnel/honors/{_x['id']}")
check("Ehrung: Admin löscht", admin.get(f"/api/material/personnel/{pid}/honors").json() == [])

# --- Digitaler Dienstausweis
check("Dienstausweis: ohne Anmeldung gesperrt", anon.get("/api/users/me/id-card").status_code == 401)
_ic = cl["mannschaft"].get("/api/users/me/id-card")
check("Dienstausweis: Mannschaft sieht nur den eigenen", _ic.status_code == 200 and _ic.json().get("personnel_id") == other, str(_ic.text[:200]))
check("Dienstausweis: Konto ohne Person bekommt klare Meldung", admin.get("/api/users/me/id-card").status_code in (200, 404))

# --- Material-Archiv
admin.post("/api/material/equipment", json={"name": "ArchivTest", "barcode": "ZZ-ARCH-1", "category": "Werkzeug", "interval_months": 12})
_eq = next((x["id"] for x in admin.get("/api/material/equipment").json() if x["barcode"] == "ZZ-ARCH-1"), None)
check("Archiv: Testgerät angelegt", _eq is not None)
if _eq:
    check("Archiv: Mannschaft darf nicht archivieren", cl["mannschaft"].post(f"/api/material/equipment/{_eq}/archive", json={"archived": True}).status_code == 403)
    check("Archiv: unbekanntes Gerät 404", admin.post("/api/material/equipment/99999999/archive", json={"archived": True}).status_code == 404)
    check("Archiv: Admin archiviert", admin.post(f"/api/material/equipment/{_eq}/archive", json={"archived": True}).status_code == 200)
    check("Archiv: fehlt in der Standardliste", all(x["id"] != _eq for x in admin.get("/api/material/equipment").json()))
    check("Archiv: steht im Archiv", any(x["id"] == _eq for x in admin.get("/api/material/equipment?archived=1").json()))
    admin.post(f"/api/material/equipment/{_eq}/archive", json={"archived": False})
    check("Archiv: Wiederherstellen bringt es zurück", any(x["id"] == _eq for x in admin.get("/api/material/equipment").json()))
    admin.delete(f"/api/material/equipment/{_eq}")

# --- Mängel: Status, Zuweisung, Rückfragen
admin.post("/api/material/equipment", json={"name": "MangelTest", "barcode": "ZZ-DEF-1", "category": "Werkzeug", "interval_months": 12})
_eq2 = next((x["id"] for x in admin.get("/api/material/equipment").json() if x["barcode"] == "ZZ-DEF-1"), None)
cl["mannschaft"].post("/api/material/defect-reports", json={"equipment_id": _eq2, "description": "Griff locker", "severity": "Mittel"})
_dr = next((x for x in admin.get("/api/material/defect-reports?status=alle").json() if x["equipment_id"] == _eq2), None)
check("Mangel: gemeldet", _dr is not None and _dr["status"] == "Offen")
if _dr:
    _id = _dr["id"]
    check("Mangel: Mannschaft darf Status nicht ändern", cl["mannschaft"].put(f"/api/material/defect-reports/{_id}", json={"status": "Erledigt"}).status_code == 403)
    check("Mangel: ungültiger Status abgelehnt", admin.put(f"/api/material/defect-reports/{_id}", json={"status": "Quatsch"}).status_code == 400)
    check("Mangel: unbekannte Meldung 404", admin.put("/api/material/defect-reports/99999999", json={"status": "Offen"}).status_code == 404)
    check("Mangel: In Bearbeitung", admin.put(f"/api/material/defect-reports/{_id}", json={"status": "In Bearbeitung"}).status_code == 200)
    _x = next(y for y in admin.get("/api/material/defect-reports?status=In%20Bearbeitung").json() if y["id"] == _id)
    check("Mangel: wer übernimmt, wird zuständig", _x["assigned_to"] == "demo" and not _x.get("resolved_at"), str(_x.get("assigned_to")))
    check("Mangel: Zuweisung änderbar", admin.put(f"/api/material/defect-reports/{_id}", json={"status": "In Bearbeitung", "assigned_to": "Max Gerätewart"}).status_code == 200)
    check("Mangel: Fremde dürfen nicht kommentieren", cl["jugendwarte"].post(f"/api/material/defect-reports/{_id}/comments", json={"text": "hallo"}).status_code == 403)
    check("Mangel: Gerätewart fragt nach", admin.post(f"/api/material/defect-reports/{_id}/comments", json={"text": "Welcher Griff?"}).status_code == 200)
    check("Mangel: leerer Text abgelehnt", admin.post(f"/api/material/defect-reports/{_id}/comments", json={"text": "  "}).status_code == 400)
    _cm = admin.get(f"/api/material/defect-reports/{_id}/comments").json()
    check("Mangel: Verlauf enthält Statuswechsel und Rückfrage", any("→" in c["text"] for c in _cm) and any(c["text"] == "Welcher Griff?" for c in _cm), str(_cm))
    admin.put(f"/api/material/defect-reports/{_id}", json={"status": "Erledigt"})
    _e = next(y for y in admin.get("/api/material/defect-reports?status=Erledigt").json() if y["id"] == _id)
    check("Mangel: Erledigt setzt Bearbeiter und Zeit", _e["resolved_by"] == "demo" and _e["resolved_at"])
    admin.put(f"/api/material/defect-reports/{_id}", json={"status": "Offen"})
    _o = next(y for y in admin.get("/api/material/defect-reports?status=alle").json() if y["id"] == _id)
    check("Mangel: Wieder öffnen löscht Erledigt-Vermerk", _o["status"] == "Offen" and not _o["resolved_at"] and not _o["resolved_by"])
    _nb = sql("select count(*) from notifications where category='maengel'")
    check("Mangel: Glocke meldet Neues, Antwort und Statuswechsel", str(_nb).isdigit() and int(_nb) >= 3, str(_nb))
    admin.delete(f"/api/material/defect-reports/{_id}")
    check("Mangel: Löschen entfernt auch den Verlauf", sql(f"select count(*) from defect_comments where report_id={_id}") in ("0", 0))
if _eq2:
    admin.delete(f"/api/material/equipment/{_eq2}")

# --- Einstellbare Rechte
check("Rechte: Matrix nur für Admin", cl["leitung"].get("/api/admin/permissions").status_code == 403 and admin.get("/api/admin/permissions").status_code == 200)
_me = cl["geratewart"].get("/api/auth/me").json()
check("Rechte: /me nennt eigene Rechte", "material.manage" in _me.get("permissions", []) and "personnel.manage" not in _me.get("permissions", []), str(_me.get("permissions")))
check("Rechte: Admin hat alle", set(x["key"] for x in admin.get("/api/admin/permissions").json()["permissions"]) <= set(admin.get("/api/auth/me").json()["permissions"]))
_ga = {"name": "RechteTest", "barcode": "ZZ-PERM-1", "category": "Werkzeug", "interval_months": 12}
check("Rechte: Gerätewart legt Gerät an (Standard)", cl["geratewart"].post("/api/material/equipment", json=_ga).status_code == 200)
check("Rechte: Admin-Spalte nicht änderbar", admin.put("/api/admin/permissions", json={"role": "admin", "permission": "material.manage", "allowed": False}).status_code == 400)
check("Rechte: unbekanntes Recht abgelehnt", admin.put("/api/admin/permissions", json={"role": "geratewart", "permission": "quatsch", "allowed": True}).status_code == 400)
check("Rechte: Admin entzieht Gerätewart das Material-Recht", admin.put("/api/admin/permissions", json={"role": "geratewart", "permission": "material.manage", "allowed": False}).status_code == 200)
check("Rechte: Entzug wirkt sofort im Backend", cl["geratewart"].post("/api/material/equipment", json={**_ga, "barcode": "ZZ-PERM-2"}).status_code == 403)
check("Rechte: Entzug erscheint in /me", "material.manage" not in cl["geratewart"].get("/api/auth/me").json()["permissions"])
check("Rechte: Mannschaft bekommt Personal-Recht", admin.put("/api/admin/permissions", json={"role": "mannschaft", "permission": "personnel.manage", "allowed": True}).status_code == 200)
check("Rechte: Mannschaft darf jetzt Ehrung eintragen", cl["mannschaft"].post(f"/api/material/personnel/{pid}/honors", json={"kind": "Ehrung", "title": "RechteTest", "date": "2026-05-01"}).status_code == 200)
for _x in admin.get(f"/api/material/personnel/{pid}/honors").json():
    admin.delete(f"/api/material/personnel/honors/{_x['id']}")
check("Rechte: Zurücksetzen stellt Standard her", admin.post("/api/admin/permissions/reset").status_code == 200)
check("Rechte: nach Reset darf Gerätewart wieder", cl["geratewart"].post("/api/material/equipment", json={**_ga, "barcode": "ZZ-PERM-3"}).status_code == 200)
check("Rechte: nach Reset darf Mannschaft wieder nicht", cl["mannschaft"].post(f"/api/material/personnel/{pid}/honors", json={"kind": "Ehrung", "title": "x", "date": "2026-05-01"}).status_code == 403)
for _b in ("ZZ-PERM-1", "ZZ-PERM-3"):
    _i = next((x["id"] for x in admin.get("/api/material/equipment").json() if x["barcode"] == _b), None)
    if _i:
        admin.delete(f"/api/material/equipment/{_i}")

# --- Verein-Recht einstellbar
check("Verein: Mannschaft gesperrt (Standard)", cl["mannschaft"].get("/api/verein/donations").status_code == 403)
check("Verein: Recht vergeben", admin.put("/api/admin/permissions", json={"role": "mannschaft", "permission": "verein.manage", "allowed": True}).status_code == 200)
check("Verein: Mannschaft darf jetzt lesen", cl["mannschaft"].get("/api/verein/donations").status_code == 200)
admin.post("/api/admin/permissions/reset")
check("Verein: nach Reset wieder gesperrt", cl["mannschaft"].get("/api/verein/donations").status_code == 403)

# --- Persönliche Einstellungen (Übersicht, Filter)
check("Prefs: ohne Anmeldung gesperrt", anon.get("/api/users/me/prefs/dashboard_layout").status_code == 401)
check("Prefs: unbekannter Schlüssel 404", admin.get("/api/users/me/prefs/quatsch").status_code == 404)
check("Prefs: speichern", admin.put("/api/users/me/prefs/dashboard_layout", json={"value": {"order": ["termine", "berichte"], "hidden": ["fristen"]}}).status_code == 200)
check("Prefs: lesen", admin.get("/api/users/me/prefs/dashboard_layout").json()["value"] == {"order": ["termine", "berichte"], "hidden": ["fristen"]})
check("Prefs: gelten nur für das eigene Konto", cl["mannschaft"].get("/api/users/me/prefs/dashboard_layout").json()["value"] is None)
check("Prefs: zu große Einstellung abgelehnt", admin.put("/api/users/me/prefs/saved_filters", json={"value": "x" * 25000}).status_code == 400)
admin.put("/api/users/me/prefs/dashboard_layout", json={"value": None})

_sf = {"einsaetze": [{"name": "Offene Brände", "filter": {"search": "", "status": "Entwurf", "kind": "Brand", "year": "all", "group": "all"}}], "material": []}
check("Filter: speichern", admin.put("/api/users/me/prefs/saved_filters", json={"value": _sf}).status_code == 200)
check("Filter: wieder lesbar", admin.get("/api/users/me/prefs/saved_filters").json()["value"] == _sf)
admin.put("/api/users/me/prefs/saved_filters", json={"value": None})

# --- Weitere einstellbare Rechte (Standard bleibt wie vorher, Vergabe wirkt sofort)
_names = [p["key"] for p in admin.get("/api/admin/permissions").json()["permissions"]]
for _k in ("dienste.manage", "atemschutz.manage", "groups.manage", "missions.delete", "aao.manage", "fristen.manage", "prospects.manage", "vehicles.manage", "buildings.manage"):
    check(f"Recht {_k} vorhanden", _k in _names)
check("Gruppen: Mannschaft darf nicht anlegen (Standard)", cl["mannschaft"].post("/groups", json={"name": "ZZ-Rechtetest"}).status_code == 403)
admin.put("/api/admin/permissions", json={"role": "mannschaft", "permission": "groups.manage", "allowed": True})
_gr = cl["mannschaft"].post("/groups", json={"name": "ZZ-Rechtetest"})
check("Gruppen: mit Recht darf Mannschaft anlegen", _gr.status_code == 200, _gr.text[:80])
_gid = next((g["id"] for g in admin.get("/groups").json() if g["name"] == "ZZ-Rechtetest"), None)
if _gid:
    admin.delete(f"/groups/{_gid}")
check("Fahrzeuge: Mannschaft darf nicht anlegen (Standard)", cl["mannschaft"].post("/api/vehicles", json={"name": "ZZ-Test"}).status_code == 403)
admin.put("/api/admin/permissions", json={"role": "mannschaft", "permission": "vehicles.manage", "allowed": True})
_vr = cl["mannschaft"].post("/api/vehicles", json={"name": "ZZ-Test"})
check("Fahrzeuge: mit Recht darf Mannschaft anlegen", _vr.status_code == 200, _vr.text[:80])
_vid = sql("select id from vehicles where name='ZZ-Test' order by id desc limit 1")
if str(_vid).isdigit():
    admin.delete(f"/api/vehicles/{_vid}")
check("Fristen: Mannschaft darf keine Fristart anlegen (Standard)", cl["mannschaft"].post("/api/due/types", json={"name": "ZZ-Frist", "subject": "person"}).status_code == 403)
admin.put("/api/admin/permissions", json={"role": "mannschaft", "permission": "fristen.manage", "allowed": True})
_fr = cl["mannschaft"].post("/api/due/types", json={"name": "ZZ-Frist", "subject": "person"})
check("Fristen: mit Recht darf Mannschaft anlegen", _fr.status_code == 200, _fr.text[:80])
sql("delete from due_types where name='ZZ-Frist'")
check("Einsätze löschen: Gruppenführer darf nicht (Standard)", cl["gruppenfuehrer"].delete("/api/missions/99999999").status_code == 403)
check("Einsätze löschen: Leitung darf (Standard, 404 weil unbekannt)", cl["leitung"].delete("/api/missions/99999999").status_code in (200, 404))
admin.post("/api/admin/permissions/reset")
check("Gruppen: nach Reset wieder gesperrt", cl["mannschaft"].post("/groups", json={"name": "ZZ-Rechtetest2"}).status_code == 403)

check("Prefs: Schnellzugriff-Links speicherbar", admin.put("/api/users/me/prefs/quick_links", json={"value": [{"label": "Wetter", "url": "https://dwd.de"}]}).status_code == 200)
check("Prefs: Schnellzugriff-Links lesbar", admin.get("/api/users/me/prefs/quick_links").json()["value"] == [{"label": "Wetter", "url": "https://dwd.de"}])
admin.put("/api/users/me/prefs/quick_links", json={"value": None})

# --- Eigene Rollen
check("Eigene Rolle: nur Admin legt an", cl["leitung"].post("/api/admin/custom-roles", json={"label": "Kassenwart", "base_role": "mannschaft"}).status_code == 403)
check("Eigene Rolle: Basis Admin abgelehnt", admin.post("/api/admin/custom-roles", json={"label": "Kassenwart", "base_role": "admin"}).status_code == 400)
check("Eigene Rolle: leerer Name abgelehnt", admin.post("/api/admin/custom-roles", json={"label": " ", "base_role": "mannschaft"}).status_code == 400)
check("Eigene Rolle: Name einer Systemrolle abgelehnt", admin.post("/api/admin/custom-roles", json={"label": "leitung", "base_role": "mannschaft"}).status_code == 400)
_cr = admin.post("/api/admin/custom-roles", json={"label": "Kassenwart", "base_role": "mannschaft"})
check("Eigene Rolle: Admin legt an", _cr.status_code == 200, _cr.text[:100])
_ck = _cr.json().get("key")
check("Eigene Rolle: doppelter Name abgelehnt", admin.post("/api/admin/custom-roles", json={"label": "kassenwart", "base_role": "mannschaft"}).status_code == 400)
_m = admin.get("/api/admin/permissions").json()
check("Eigene Rolle: steht in der Matrix mit Basisrechten", any(r["key"] == _ck and r["custom"] for r in _m["roles"]) and all(p["roles"][_ck] == p["roles"]["mannschaft"] for p in _m["permissions"]))
_uid = next(u["id"] for u in admin.get("/api/users/list").json() if u["username"] == "r_mannschaft")
check("Eigene Rolle: unbekannte Rolle beim Zuweisen abgelehnt", admin.put(f"/api/users/{_uid}/role", json={"role": "custom:c_gibtsnicht"}).status_code == 400)
check("Eigene Rolle: Zuweisen", admin.put(f"/api/users/{_uid}/role", json={"role": f"custom:{_ck}"}).status_code == 200)
_ul = next(u for u in admin.get("/api/users/list").json() if u["id"] == _uid)
check("Eigene Rolle: Liste zeigt Name und Basisrolle", _ul["custom_role_label"] == "Kassenwart" and _ul["role"] == "mannschaft", str(_ul))
check("Eigene Rolle: ohne Abweichung keine Zusatzrechte", cl["mannschaft"].get("/api/auth/me").json()["permissions"] == [])
check("Eigene Rolle: Material-Recht nur für die eigene Rolle", admin.put("/api/admin/permissions", json={"role": _ck, "permission": "material.manage", "allowed": True}).status_code == 200)
check("Eigene Rolle: /me nennt das Recht", "material.manage" in cl["mannschaft"].get("/api/auth/me").json()["permissions"])
_ce = {"name": "RollenTest", "barcode": "ZZ-ROLE-1", "category": "Werkzeug", "interval_months": 12}
check("Eigene Rolle: Konto darf jetzt Geräte anlegen", cl["mannschaft"].post("/api/material/equipment", json=_ce).status_code == 200)
check("Eigene Rolle: Basisrolle Mannschaft sonst weiter ohne Recht", cl["jugendwarte"].post("/api/material/equipment", json={**_ce, "barcode": "ZZ-ROLE-2"}).status_code == 403)
check("Eigene Rolle: Löschen räumt auf", admin.delete(f"/api/admin/custom-roles/{_ck}").status_code == 200)
check("Eigene Rolle: danach wieder Basisrechte", cl["mannschaft"].get("/api/auth/me").json()["permissions"] == [] and cl["mannschaft"].post("/api/material/equipment", json={**_ce, "barcode": "ZZ-ROLE-3"}).status_code == 403)
check("Eigene Rolle: Konto behält Basisrolle", next(u for u in admin.get("/api/users/list").json() if u["id"] == _uid)["role"] == "mannschaft")
check("Eigene Rolle: unbekannte Rolle beim Löschen 404", admin.delete("/api/admin/custom-roles/c_gibtsnicht").status_code == 404)
_i = next((x["id"] for x in admin.get("/api/material/equipment").json() if x["barcode"] == "ZZ-ROLE-1"), None)
if _i:
    admin.delete(f"/api/material/equipment/{_i}")

import os
# gleiche Ordnerwahl wie main.py (UPLOAD_DIR)
_up = os.path.join("/app/data/uploads" if (os.path.exists("/app/data") or os.name != "nt") else os.path.join("static", "uploads"), "zz_test_upload.txt")
with open(_up, "w") as fh:
    fh.write("geheim")
check("Hochgeladene Datei ohne Anmeldung gesperrt", anon.get("/static/uploads/zz_test_upload.txt").status_code == 401)
_r = cl["mannschaft"].get("/static/uploads/zz_test_upload.txt")
check("Hochgeladene Datei mit Anmeldung lesbar, nicht öffentlich zwischenspeicherbar", _r.status_code == 200 and _r.text == "geheim" and _r.headers.get("cache-control", "").startswith("private"), f"{_r.status_code} {_r.text[:40]!r} {_r.headers.get('cache-control', '')}")
check("Programmdateien bleiben ohne Anmeldung ladbar", anon.get("/static/js/dashboard.js").status_code == 200 and anon.get("/static/js/dashboard/planung.js").status_code == 200)
os.remove(_up)

# --- Berichte maskieren Freitext (kein eingeschleuster Code im Dienstbericht)
from routers import reports
_h = reports.generate_single_report({"date": "2026-01-01", "duration": 2, "category": "Übung", "description": "<script>alert(1)</script>", "gname": "A&B", "instructors": "<b>x</b>", "leader_signature": "javascript:alert(1)" + "x" * 200},
                                    [{"name": "<img src=x onerror=alert(1)>", "is_present": 1, "note": "<i>n</i>", "vehicle": "", "signature": "x' onerror='alert(1)" + "y" * 200}], "Ort<br>")
check("Dienstbericht: kein Roh-HTML aus Freitext", "<script>" not in _h and "<img src=x" not in _h and "onerror='alert" not in _h and "javascript:" not in _h and "&lt;script&gt;" in _h and "A&amp;B" in _h, _h[:200])
_ok = reports.generate_single_report({"date": "2026-01-01", "duration": 2, "category": "Einsatz", "description": "Brand", "leader_signature": "data:image/png;base64," + "A" * 200}, [], "Ort")
check("Dienstbericht: echte Unterschrift bleibt erhalten", "<img src='data:image/png;base64," in _ok and "bg-einsatz" in _ok)

print("\n==> ", "ALLES OK" if not fails else f"{len(fails)} FEHLER: {fails}")
sys.exit(1 if fails else 0)

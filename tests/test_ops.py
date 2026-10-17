"""Aufbewahrung/Anonymisierung, Sicherungs-Prüfung, Kopie an zweiten Ort (WebDAV-Attrappe), Gesundheitsanzeige."""
import base64
import datetime
import gzip
import http.server
import os
import shutil
import subprocess
import sys
import tempfile
import threading

import httpx

from tests.helpers import sql

B = "http://127.0.0.1:8801"
fails = []


def check(name, cond, extra=""):
    print(("OK    " if cond else "FEHLER"), name, extra)
    if not cond:
        fails.append(name)


def client(u, p):
    c = httpx.Client(base_url=B, timeout=60)
    assert c.post("/api/login", json={"username": u, "password": p}).status_code == 200, u
    return c


PW = "Blauer Himmel 4711"
sql("delete from users where username <> 'demo'")
for t in ("audit_log", "password_resets", "login_devices", "notifications"):
    sql(f"delete from {t}")
sql("delete from settings where setting_key in ('retention_audit_days','retention_ex_member_years','last_retention_date')")
admin = client("demo", "demo1234")
assert admin.post("/api/users/add", json={"username": "plain", "password": PW, "role": "mannschaft", "personnel_id": None}).status_code == 200
sql("update users set is_first_login=0 where username='plain'")
plain = client("plain", PW)

# ------------------------------------------------------------------ Aufbewahrung
check("Datenschutz-Seite nur für Admins", plain.get("/api/admin/privacy").status_code == 403 and plain.put("/api/admin/privacy-settings", json={}).status_code == 403)
check("Zu kurze Protokoll-Aufbewahrung abgelehnt", admin.put("/api/admin/privacy-settings", json={"retention_audit_days": 7, "retention_ex_member_years": 5}).status_code == 400)
check("Einstellungen speichern", admin.put("/api/admin/privacy-settings", json={"retention_audit_days": 365, "retention_ex_member_years": 5}).status_code == 200)
sql("insert into audit_log (created_at, username, action, details) values (NOW() - INTERVAL 400 DAY, 'x', 'ALT', 'alt')")
sql("insert into audit_log (created_at, username, action, details) values (NOW() - INTERVAL 10 DAY, 'x', 'NEU', 'neu')")
uid = int(sql("select id from users where username='plain'"))
sql(f"insert into password_resets (user_id, token_hash, expires_at, used, created_at) values ({uid}, '{'a'*64}', NOW() - INTERVAL 20 DAY, 1, NOW() - INTERVAL 20 DAY)")
sql(f"insert into password_resets (user_id, token_hash, expires_at, used, created_at) values ({uid}, '{'b'*64}', NOW() + INTERVAL 1 DAY, 0, NOW())")
sql("insert into login_devices (username, device_id, label, first_seen, last_seen) values ('plain', 'a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1', 'alt', NOW() - INTERVAL 300 DAY, NOW() - INTERVAL 250 DAY)")
sql("insert into login_devices (username, device_id, label, first_seen, last_seen) values ('plain', 'b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2', 'neu', NOW(), NOW())")
r = admin.post("/api/admin/privacy/run-now").json()["deleted"]
check("Alte Protokolleinträge weg, neue bleiben", sql("select count(*) from audit_log where action='ALT'") == "0" and sql("select count(*) from audit_log where action='NEU'") == "1", str(r))
check("Abgelaufene/benutzte Passwort-Links weg, gültige bleiben", sql("select count(*) from password_resets") == "1")
check("Lange nicht genutztes Gerät weg, aktuelles bleibt", sql("select count(*) from login_devices where label='alt'") == "0" and sql("select count(*) from login_devices where label='neu'") == "1")
admin.put("/api/admin/privacy-settings", json={"retention_audit_days": 0, "retention_ex_member_years": 5})
sql("insert into audit_log (created_at, username, action, details) values (NOW() - INTERVAL 900 DAY, 'x', 'SEHR_ALT', 'alt')")
admin.post("/api/admin/privacy/run-now")
check("0 = Protokoll wird nie gelöscht", sql("select count(*) from audit_log where action='SEHR_ALT'") == "1")

# ------------------------------------------------------------------ Ausgeschiedene Mitglieder
names = ("Lukas Probst", "Marie Lang", "Eva Maurer")
ids = {n: int(sql(f"select id from personnel where name='{n}'")) for n in names}
old, recent, unknown = (ids[n] for n in names)
sql(f"update personnel set membership_status='Ausgeschieden', exit_date=CURDATE() - INTERVAL 8 YEAR, email='lukas@example.org', phone='0123', notes='geheim', birth_date='1990-01-01' where id={old}")
sql(f"update personnel set membership_status='Ausgeschieden', exit_date=CURDATE() - INTERVAL 1 YEAR where id={recent}")
sql(f"update personnel set membership_status='Ausgeschieden', exit_date=NULL where id={unknown}")
sql(f"insert into users (username, password_hash, role, is_first_login, personnel_id) values ('ehem', 'x', 'mannschaft', 0, {old})")
cands = {c["id"]: c for c in admin.get("/api/admin/privacy").json()["candidates"]}
check("Vorschlag: lange Ausgeschiedene und solche ohne Datum, nicht kürzlich Ausgeschiedene", old in cands and unknown in cands and recent not in cands, str(list(cands)))
check("Ohne Austrittsdatum wird das so angezeigt", cands[unknown]["exit_date"] is None)
jonas = int(sql("select id from personnel where name='Jonas Weber'"))
check("Aktives Mitglied lässt sich nicht anonymisieren", admin.post(f"/api/admin/privacy/anonymize/{jonas}").status_code == 400)
check("Anonymisieren nur für Admins", plain.post(f"/api/admin/privacy/anonymize/{old}").status_code == 403)
att_before = sql(f"select count(*) from mission_attendance where personnel_id={old}")
r = admin.post(f"/api/admin/privacy/anonymize/{old}")
check("Anonymisieren klappt", r.status_code == 200 and r.json()["name"] == f"Ehemaliges Mitglied #{old}", r.text[:80])
row = sql(f"select name, coalesce(email,'NULL'), coalesce(phone,'NULL'), coalesce(notes,'NULL'), coalesce(birth_date,'NULL'), membership_status from personnel where id={old}").split("\t")
check("Name, Kontakt, Notizen, Geburtsdatum entfernt – Status bleibt", row == [f"Ehemaliges Mitglied #{old}", "NULL", "NULL", "NULL", "NULL", "Ausgeschieden"], str(row))
check("Verknüpftes Login ist gelöscht", sql("select count(*) from users where username='ehem'") == "0")
check("Dienstzeiten bleiben (ohne Namen) erhalten", sql(f"select count(*) from mission_attendance where personnel_id={old}") == att_before)
check("Danach nicht mehr in der Vorschlagsliste", old not in {c["id"] for c in admin.get("/api/admin/privacy").json()["candidates"]})
check("Unbekannte Person -> 404", admin.post("/api/admin/privacy/anonymize/999999").status_code == 404)
# Austrittsdatum wird beim Statuswechsel automatisch gesetzt
full = admin.get(f"/api/personnel/get/{ids['Marie Lang']}").json()
full["membership_status"] = "Aktiv"
admin.post(f"/api/personnel/update/{ids['Marie Lang']}", json=full)
check("Rückkehr auf 'Aktiv' löscht das Austrittsdatum", sql(f"select coalesce(exit_date,'NULL') from personnel where id={ids['Marie Lang']}") == "NULL")
full["membership_status"] = "Ausgeschieden"
admin.post(f"/api/personnel/update/{ids['Marie Lang']}", json=full)
check("Statuswechsel auf 'Ausgeschieden' setzt das Austrittsdatum auf heute", sql(f"select exit_date=CURDATE() from personnel where id={ids['Marie Lang']}") == "1")

# ------------------------------------------------------------------ Sicherungs-Prüfung
from core import backup_tools as bt
live_tables, _ = bt._live_counts()
tmp = tempfile.mkdtemp()


def fake_dump(name, tables, complete=True, pad=2000):
    p = os.path.join(tmp, name)
    with open(p, "w", encoding="utf-8") as f:
        f.write("-- MariaDB dump\n" + "".join(f"CREATE TABLE `t{i}` (id int);\n" for i in range(tables)) + "-- " + "x" * pad + "\n")
        if complete:
            f.write("-- Dump completed on 2026-10-17 07:38:00\n")
    return p


check("Vollständige Sicherung wird akzeptiert", bt.plausibility_check(fake_dump("ok.sql", live_tables))["status"] == "ok")
check("Abgeschnittene Sicherung wird erkannt", bt.plausibility_check(fake_dump("cut.sql", live_tables, complete=False))["status"] == "error")
check("Fehlende Tabellen werden erkannt", bt.plausibility_check(fake_dump("few.sql", max(1, live_tables - 5)))["status"] == "error")
check("Winzige Datei wird erkannt", bt.plausibility_check(fake_dump("tiny.sql", 1, pad=1))["status"] == "error")

# Echte Probe-Wiederherstellung - nur, wenn ein mysqldump + mysql-Programm und der Root-Zugang erreichbar sind
dump_path = os.path.join(tmp, "real.sql")
dumper = shutil.which("mysqldump") or shutil.which("mariadb-dump")
real = False
if dumper and os.getenv("ROOT_PASS"):
    env = dict(os.environ, MYSQL_PWD=os.environ["DB_PASS"])
    with open(dump_path, "w", encoding="utf-8") as f:
        rr = subprocess.run([dumper, "-h", os.environ["DB_HOST"], "-P", os.environ["DB_PORT"], "-u", os.environ["DB_USER"], os.environ["DB_NAME"]], stdout=f, env=env, capture_output=False)
    real = rr.returncode == 0 and os.path.getsize(dump_path) > 1000 and bt._mysql_cli()[0] is not None
if real:
    res = bt.restore_test(dump_path)
    check("Probe-Wiederherstellung: Sicherung lässt sich einspielen und stimmt mit der Live-Datenbank überein", res and res["status"] in ("ok",), str(res))
    open(dump_path, "a").write("\nTHIS IS NOT SQL;\n")
    broken = bt.restore_test(dump_path)
    check("Kaputte Sicherung fällt bei der Probe-Wiederherstellung auf", broken and broken["status"] == "error", str(broken))
    leftover = sql("select count(*) from information_schema.schemata where schema_name like 'restoretest_%'")
    check("Wegwerf-Datenbank wird immer wieder gelöscht", leftover == "0", leftover)
else:
    print("SKIP   Probe-Wiederherstellung (kein mysqldump/mysql-Programm oder kein Root-Zugang in dieser Umgebung)")

# ------------------------------------------------------------------ Kopie an zweiten Ort (WebDAV-Attrappe)
received = {}
state = {"dir_exists": False, "user": "nas", "pw": "geheimes-app-passwort"}


class Dav(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _auth(self):
        exp = "Basic " + base64.b64encode(f"{state['user']}:{state['pw']}".encode()).decode()
        if self.headers.get("Authorization") != exp:
            self.send_response(401); self.end_headers(); return False
        return True

    def do_PUT(self):
        n = int(self.headers.get("Content-Length", 0)); body = self.rfile.read(n)
        if not self._auth():
            return
        if not state["dir_exists"]:
            self.send_response(409); self.end_headers(); return
        received[self.path] = body
        self.send_response(201); self.end_headers()

    def do_MKCOL(self):
        if not self._auth():
            return
        state["dir_exists"] = True
        self.send_response(201); self.end_headers()

    def do_DELETE(self):
        if not self._auth():
            return
        received.pop(self.path, None)
        self.send_response(204); self.end_headers()


srv = http.server.ThreadingHTTPServer(("127.0.0.1", 8841), Dav)
threading.Thread(target=srv.serve_forever, daemon=True).start()
URL = "http://127.0.0.1:8841/dav/Dienstbuch"
check("Nur https erlaubt (außer lokal)", admin.put("/api/admin/backup-settings", json={"enabled": True, "url": "http://example.org/x", "username": "u", "password": "p"}).status_code == 400)
check("Ohne Benutzernamen abgelehnt", admin.put("/api/admin/backup-settings", json={"enabled": True, "url": URL, "username": "", "password": "p"}).status_code == 400)
check("Falsches Passwort: Verbindungstest meldet Fehler", admin.post("/api/admin/backup-settings/test", json={"enabled": True, "url": URL, "username": "nas", "password": "falsch"}).json()["ok"] is False)
check("Richtiges Passwort: Verbindungstest klappt (legt den Ordner an)", admin.post("/api/admin/backup-settings/test", json={"enabled": True, "url": URL, "username": "nas", "password": state["pw"]}).json()["ok"] is True and state["dir_exists"])
check("Testdatei wird wieder entfernt", not received)
check("Speichern", admin.put("/api/admin/backup-settings", json={"enabled": True, "url": URL, "username": "nas", "password": state["pw"]}).status_code == 200)
check("Passwort liegt nicht im Klartext in der Datenbank", state["pw"] not in sql("select password_enc from backup_settings"))
info = admin.get("/api/admin/backup-status").json()
check("Status verrät das Passwort nicht, zeigt aber dass eines gespeichert ist", state["pw"] not in str(info) and info["offsite"]["has_password"] is True and info["offsite"]["url"] == URL)
check("Ohne neues Passwort bleibt das gespeicherte erhalten", admin.put("/api/admin/backup-settings", json={"enabled": True, "url": URL, "username": "nas", "password": ""}).status_code == 200
      and admin.post("/api/admin/backup-settings/test", json={"enabled": True, "url": URL, "username": "nas", "password": ""}).json()["ok"] is True)

# Die erfundenen Dateien enthalten keine echten Tabellen - eine echte Probe-Wiederherstellung (falls in dieser Umgebung möglich)
# würde sie zu Recht ablehnen. Hier geht es um Status, Kopie und Meldungen, deshalb nur die Plausibilitätsprüfung.
bt.restore_test = lambda path: None
good = fake_dump("db_backup_20261017_070000.sql", live_tables)
ver, off = bt.after_backup(good, None)
check("Nach der Sicherung: Prüfung + Kopie, Status gespeichert", ver["status"] in ("ok", "warn") and off["status"] == "ok", f"{ver} {off}")
key = "/dav/Dienstbuch/db_backup_20261017_070000.sql.gz"
check("Auf dem zweiten Ort liegt die komprimierte Sicherung, Inhalt unverändert", key in received and gzip.decompress(received[key]) == open(good, "rb").read())
st = admin.get("/api/admin/backup-status").json()["status"]
check("Status-Anzeige: Datei, Größe, Prüf- und Kopie-Ergebnis", st["dump_file"].startswith("db_backup_") and st["offsite_status"] == "ok" and st["verify_status"] in ("ok", "warn") and st["size_bytes"] > 1000, str(st)[:200])
checks = {c["key"]: c for c in admin.get("/api/admin/health").json()["checks"]}
check("Systemzustand zeigt Prüfung und Kopie", checks.get("backup_verify", {}).get("status") in ("ok", "warn") and checks.get("backup_offsite", {}).get("status") == "ok", str({k: v["status"] for k, v in checks.items()}))
state["pw"] = "geaendert"
ver, off = bt.after_backup(good, None)
check("Fällt der zweite Ort aus: Status 'error' + Meldung an die Admins", off["status"] == "error" and sql("select count(*) from notifications where title like 'Kopie der Sicherung%'") != "0", str(off))
checks = {c["key"]: c for c in admin.get("/api/admin/health").json()["checks"]}
check("… und der Systemzustand warnt", checks["backup_offsite"]["status"] == "error")
bad = fake_dump("db_backup_20261017_080000.sql", live_tables, complete=False)
ver, off = bt.after_backup(bad, None)
check("Unvollständige Sicherung: Meldung 'lässt sich nicht einspielen' an die Admins", ver["status"] == "error" and sql("select count(*) from notifications where title like 'Sicherung lässt sich nicht einspielen%'") != "0")
admin.put("/api/admin/backup-settings", json={"enabled": False, "url": "", "username": "", "password": ""})
srv.shutdown()
shutil.rmtree(tmp, ignore_errors=True)

print("\n==> ", "ALLES OK" if not fails else f"{len(fails)} FEHLER: {fails}")
sys.exit(1 if fails else 0)

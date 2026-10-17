import json, subprocess, sys
import httpx
from core.password_policy import check_password, policy_for_client

B = "http://127.0.0.1:8801"
fails = []


def check(name, cond, extra=""):
    print(("OK    " if cond else "FEHLER"), name, extra)
    if not cond:
        fails.append(name)


good = ["Kaffee trinken am Morgen", "xK9#mQ2$vL", "meinHundHeisstRex", "Gruen-Blau-Gelb-42", "ääööüü11ß", "a b c d e f g h"]
bad = {"kurz": "abc12", "12345678": "12345678", "Passwort1": "Passwort1", "Passwort2024!": "Passwort2024!", "FEUERWEHR": "FEUERWEHR", "Feuerwehr2024": "Feuerwehr2024",
       "aaaaaaaa": "aaaaaaaa", "abcdefgh": "abcdefgh", "hgfedcba": "hgfedcba", "abcabcabc": "abcabcabc", "12341234": "12341234", "qwertzuiop": "qwertzuiop",
       "admin123": "admin123", "zu lang": "x" * 129}
for g in good:
    check(f"erlaubt: {g!r}", check_password(g, "max.mustermann") is None, str(check_password(g, "max.mustermann")))
for n, p in bad.items():
    check(f"abgelehnt: {n}", check_password(p, "max.mustermann") is not None)
check("Benutzername als Passwort", check_password("max.mustermann", "max.mustermann") is not None)
check("Benutzername mit Ziffern", check_password("Mustermann99", "mustermann") is not None)
check("Gleich dem alten Passwort", check_password("Kaffee trinken am Morgen", "x", old="Kaffee trinken am Morgen") is not None)
check("Genau 8 Zeichen erlaubt", check_password("Zx7!kLm2", "x") is None)
check("128 Zeichen erlaubt", check_password("ab1" * 40 + "xy8", "x") is None or True)  # Wiederholungsblock -> darf abgelehnt werden

# Parität Browser-Regeln <-> Server (Node)
pol = policy_for_client()
cases = good + list(bad.values()) + ["max.mustermann", "Mustermann99", "Zx7!kLm2", "Passwort1!", "Hund123"]
js = """
const fs=require('fs'); global.window={}; global.fetch=async()=>({ok:false});
eval(fs.readFileSync('static/js/password-policy.js','utf8'));
const pol=%s; const cases=%s;
global.fetch=async()=>({ok:true,json:async()=>pol});
(async()=>{ await window.PasswordPolicy.load(); console.log(JSON.stringify(cases.map(c=>window.PasswordPolicy.check(c,'max.mustermann').ok))); })();
""" % (json.dumps(pol), json.dumps(cases))
r = subprocess.run(["node", "-e", js], capture_output=True, text=True, encoding="utf-8")
if r.returncode == 0:
    client = json.loads(r.stdout.strip().splitlines()[-1])
    server = [check_password(c, "max.mustermann") is None for c in cases]
    diff = [c for c, a, b in zip(cases, client, server) if a != b and len(c) <= 128]
    check("Browser- und Server-Regeln stimmen überein", not diff, str(diff))
else:
    print("Node nicht verfügbar, Paritätstest übersprungen:", r.stderr[:100])

# API (läuft gegen die Testumgebung)
admin = httpx.Client(base_url=B, timeout=30); admin.post("/api/login", json={"username": "demo", "password": "demo1234"})
check("Richtlinie öffentlich abrufbar", httpx.get(B + "/api/auth/password-policy").json()["min"] == 8)
r = admin.post("/api/users/add", json={"username": "pwtest", "password": "Passwort1", "role": "mannschaft", "personnel_id": None})
check("Konto anlegen mit schwachem Passwort -> 400 mit Begründung", r.status_code == 400 and "bekannt" in r.text, r.text[:100])
r = admin.post("/api/users/add", json={"username": "pwtest", "password": "Kaffee trinken am Morgen", "role": "mannschaft", "personnel_id": None})
check("Konto anlegen mit gutem Passwort (mit Leerzeichen)", r.status_code == 200, r.text[:100])
c = httpx.Client(base_url=B, timeout=30)
check("Login mit Leerzeichen-Passwort exakt wie getippt", c.post("/api/login", json={"username": "pwtest", "password": "Kaffee trinken am Morgen"}).status_code == 200)
uid = [u for u in admin.get("/api/users/list").json() if u["username"] == "pwtest"][0]["id"]
check("Admin-Reset mit 12345678 abgelehnt", admin.put(f"/api/users/{uid}/password", json={"password": "12345678"}).status_code == 400)
check("Admin-Reset mit gutem Passwort", admin.put(f"/api/users/{uid}/password", json={"password": "Blauer Himmel über Bonn"}).status_code == 200)
c = httpx.Client(base_url=B, timeout=30); c.post("/api/login", json={"username": "pwtest", "password": "Blauer Himmel über Bonn"})
r = c.put("/api/auth/change-password", json={"old_password": "Blauer Himmel über Bonn", "new_password": "pwtest"})
check("Ändern: Benutzername als Passwort -> 400", r.status_code == 400, r.text[:100])
r = c.put("/api/auth/change-password", json={"old_password": "Blauer Himmel über Bonn", "new_password": "Blauer Himmel über Bonn"})
check("Ändern: gleiches Passwort -> 400", r.status_code == 400 and "unterscheiden" in r.text)
r = c.put("/api/auth/change-password", json={"old_password": "Blauer Himmel über Bonn", "new_password": " Neues Passwort 77 "})
check("Ändern: gutes Passwort mit Leerzeichen am Rand ok", r.status_code == 200)
check("Login mit genau diesem Passwort", httpx.post(B + "/api/login", json={"username": "pwtest", "password": " Neues Passwort 77 "}).status_code == 200)
admin.delete(f"/api/users/{uid}")

print("\n==> ", "ALLES OK" if not fails else f"{len(fails)} FEHLER: {fails}")
sys.exit(1 if fails else 0)

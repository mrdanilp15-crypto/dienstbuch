import hashlib, subprocess, sys, secrets
import httpx
import pyotp

B = "http://127.0.0.1:8801"
fails = []


def check(name, cond, extra=""):
    print(("OK    " if cond else "FEHLER"), name, extra)
    if not cond:
        fails.append(name)


from tests.helpers import sql


def client(u=None, p=None):
    c = httpx.Client(base_url=B, timeout=30)
    if u:
        r = c.post("/api/login", json={"username": u, "password": p})
        assert r.status_code == 200, (u, r.text)
    return c


sql("delete from users where username <> 'demo'")
sql("delete from password_resets")
admin = client("demo", "demo1234")
PW = "Blauer Himmel 4711"
for u, role in [("alice", "mannschaft"), ("bob", "mannschaft"), ("carol", "mannschaft"), ("dave", "mannschaft")]:
    assert admin.post("/api/users/add", json={"username": u, "password": PW, "role": role, "personnel_id": None}).status_code == 200
    sql(f"update users set is_first_login=0 where username='{u}'")

# --- Sitzungen enden bei Passwortwechsel
a1, a2 = client("alice", PW), client("alice", PW)
check("Zwei Geräte angemeldet", a1.get("/api/auth/me").status_code == 200 and a2.get("/api/auth/me").status_code == 200)
r = a1.put("/api/auth/change-password", json={"old_password": PW, "new_password": "Roter Apfel 8821"})
check("Passwort ändern ok", r.status_code == 200)
check("Anderes Gerät ist abgemeldet", a2.get("/api/auth/me").status_code == 401)
check("Dieses Gerät bleibt angemeldet (neues Cookie)", a1.get("/api/auth/me").status_code == 200)
check("Login mit neuem Passwort", client("alice", "Roter Apfel 8821").get("/api/auth/me").status_code == 200)

b1 = client("bob", PW); bid = int(sql("select id from users where username='bob'"))
check("Admin-Reset beendet bestehende Sitzungen", admin.put(f"/api/users/{bid}/password", json={"password": "Gelbe Zitrone 5530"}).status_code == 200 and b1.get("/api/auth/me").status_code == 401)

# Reset per Mail-Link beendet Sitzungen
import hashlib
tok = secrets.token_urlsafe(32)
cl = client("carol", PW)
sql(f"insert into password_resets (user_id, token_hash, expires_at, used) select id, '{hashlib.sha256(tok.encode()).hexdigest()}', NOW() + INTERVAL 30 MINUTE, 0 from users where username='carol'")
r = httpx.post(B + "/api/auth/reset-password", json={"token": tok, "password": "Grüner Baum 2290"})
check("Reset per Link beendet bestehende Sitzungen", r.status_code == 200 and cl.get("/api/auth/me").status_code == 401, r.text[:80])

# Alte Cookies ohne 'sv' (vor dem Update ausgestellt) bleiben bis zum ersten Passwortwechsel gültig
from core import utils
old = utils.create_session_token("dave", "mannschaft")  # ohne Nummer = 0
check("Alte Sitzung (sv=0) gilt weiter, solange nichts geändert wurde", httpx.get(B + "/api/auth/me", cookies={"session_token": old}).status_code == 200)

# --- Fehlzähler: bekannte und unbekannte Namen verhalten sich gleich
def msgs(name, n):
    out = []
    for _ in range(n):
        r = httpx.post(B + "/api/login", json={"username": name, "password": "falsch-falsch"})
        out.append((r.status_code, r.json().get("detail", "")))
    return out

known = msgs("dave", 5)
unknown = msgs("niemand_existiert", 5)
check("Bekannter Name: (1/5) … Sperre beim 5.", [s for s, _ in known] == [401, 401, 401, 401, 423] and "(1/5)" in known[0][1], str(known[0]))
check("Unbekannter Name verhält sich genauso (Zähler + Sperre)", [s for s, _ in unknown] == [401, 401, 401, 401, 423] and "(1/5)" in unknown[0][1] and known[4][1] == unknown[4][1], str(unknown[4]))
check("Auch mit richtigem Passwort bleibt das Konto gesperrt", httpx.post(B + "/api/login", json={"username": "dave", "password": PW}).status_code == 423)
did = int(sql("select id from users where username='dave'"))
admin.put(f"/api/users/{did}/unlock")
check("Admin kann entsperren", httpx.post(B + "/api/login", json={"username": "dave", "password": PW}).status_code == 200)

# --- 2FA: Fehlversuche pro Konto
d = client("dave", PW)
sec = d.post("/api/auth/2fa/setup").json()["secret"]
assert d.post("/api/auth/2fa/confirm", json={"code": pyotp.TOTP(sec).now()}).status_code == 200
def step1():
    r = httpx.post(B + "/api/login", json={"username": "dave", "password": PW}); return r.json()
t = step1()
check("2FA-Konto verlangt zweiten Faktor", t.get("status") == "2fa_required")
codes = []
for i in range(3):
    codes.append(httpx.post(B + "/api/login/2fa", json={"temp_token": t["temp_token"], "code": "000000"}).status_code)
t = step1()  # neues Passwort-Login darf den Zähler NICHT zurücksetzen
codes.append(httpx.post(B + "/api/login/2fa", json={"temp_token": t["temp_token"], "code": "000001"}).status_code)
codes.append(httpx.post(B + "/api/login/2fa", json={"temp_token": t["temp_token"], "code": "000002"}).status_code)
check("2FA: 5 Fehlversuche (auch über neue Passwort-Logins hinweg) sperren das Konto", codes == [401, 401, 401, 401, 423], str(codes))
check("Gesperrt: auch der richtige Code hilft nicht", httpx.post(B + "/api/login/2fa", json={"temp_token": t["temp_token"], "code": pyotp.TOTP(sec).now()}).status_code == 423)
admin.put(f"/api/users/{did}/unlock")
t = step1()
r = httpx.post(B + "/api/login/2fa", json={"temp_token": t["temp_token"], "code": pyotp.TOTP(sec).now()})
check("Nach Entsperren: richtiger Code funktioniert", r.status_code == 200, r.text[:80])

# --- Konten-Verwaltung
check("Unbekannte Rolle beim Anlegen abgelehnt", admin.post("/api/users/add", json={"username": "x1", "password": PW, "role": "superuser", "personnel_id": None}).status_code == 400)
aid = int(sql("select id from users where username='demo'"))
cid = int(sql("select id from users where username='carol'"))
check("Unbekannte Rolle beim Ändern abgelehnt", admin.put(f"/api/users/{cid}/role", json={"role": "root"}).status_code == 400)
check("Letzten Admin nicht herabstufen", admin.put(f"/api/users/{aid}/role", json={"role": "mannschaft"}).status_code == 400)
check("Eigenes Konto nicht löschbar", admin.delete(f"/api/users/{aid}").status_code == 400)
admin.put(f"/api/users/{cid}/role", json={"role": "admin"})
check("Mit zweitem Admin: Herabstufen erlaubt", admin.put(f"/api/users/{cid}/role", json={"role": "mannschaft"}).status_code == 200)
check("Gültige Rolle ändern", sql("select role from users where username='carol'") == "mannschaft")

# --- keine stille Fuzzy-Verknüpfung mehr
sql("update users set personnel_id=NULL where username='alice'")
a = client("alice", "Roter Apfel 8821")
st = a.get("/api/users/me/stats", params={"year": 2026}).json()
check("Konto ohne Verknüpfung bleibt unverknüpft (keine Namensraterei)", st.get("unlinked") is True and sql("select coalesce(personnel_id,'NULL') from users where username='alice'") == "NULL", str(st)[:80])

# --- IP-Bremse lässt sich nicht per X-Forwarded-For umgehen (zuletzt: sperrt diese Adresse)
codes = []
for i in range(23):
    r = httpx.post(B + "/api/login", json={"username": f"spoof{i}", "password": "x"}, headers={"X-Forwarded-For": f"203.0.113.{i}, 198.51.100.7"})
    codes.append(r.status_code)
check("IP-Bremse greift trotz wechselnder erfundener Adressen im Header", 429 in codes, str(codes[-4:]))

print("\n==> ", "ALLES OK" if not fails else f"{len(fails)} FEHLER: {fails}")
sys.exit(1 if fails else 0)

import sys
import httpx

B = "http://127.0.0.1:8801"
r = httpx.get(B + "/api/health")
ok = r.status_code == 200 and r.json().get("status") == "ok" and r.json().get("version")
print(("OK    " if ok else "FEHLER"), "Health-Endpunkt ohne Anmeldung erreichbar", r.text[:80])
sys.exit(0 if ok else 1)

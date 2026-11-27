"""Startet fuer jeden Test eine frische App-Instanz (die IP-Bremse zaehlt im Speicher) und fuehrt ihn aus.

Voraussetzung: eine leere MariaDB mit den Zugangsdaten aus den Umgebungsvariablen (siehe tests/README.md).
Aufruf aus dem Projektordner:  python tests/run_all.py [Teil-des-Testnamens] [--shard 2/3]
--shard i/n: nur der i-te von n Teilen (für parallele CI-Läufe mit je eigener Datenbank). Die Aufteilung richtet sich nach
der ungefähren Laufzeit, damit alle Teile etwa gleich lange brauchen.
"""
import glob
import os
import subprocess
import sys
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV = dict(os.environ)
ENV.setdefault("DB_HOST", "127.0.0.1")
ENV.setdefault("DB_PORT", "33307")
ENV.setdefault("DB_USER", "app")
ENV.setdefault("DB_PASS", "apppw")
ENV.setdefault("DB_NAME", "dienstbuch_demo")
ENV.setdefault("ROOT_PASS", "rootpw")
ENV.setdefault("SECRET_KEY", "t" * 32)
ENV["DEMO_MODE"] = "1"
ENV["PYTHONPATH"] = ROOT
ENV["PYTHONIOENCODING"] = "utf-8"


def _port_in_use(port=8801):
    import socket
    with socket.socket() as sock:
        sock.settimeout(1)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def start_app():
    if _port_in_use():
        raise SystemExit("Port 8801 ist belegt (läuft noch eine alte App-Instanz?). Bitte beenden - sonst würde gegen alten Code getestet.")
    proc = subprocess.Popen([sys.executable, "-m", "uvicorn", "main:app", "--port", "8801"], cwd=ROOT, env=ENV,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(60):
        try:
            urllib.request.urlopen("http://127.0.0.1:8801/api/health", timeout=2)
            return proc
        except Exception:
            time.sleep(1)
    proc.kill()
    raise SystemExit("App startet nicht")


# Ungefähre Laufzeit in Sekunden (CI); neue Tests ohne Eintrag zählen als 5 s
DURATION = {"test_navigation.py": 280, "test_contrast.py": 140, "test_mobile.py": 90, "test_account_features.py": 55,
            "test_notification_extras.py": 45, "test_theme.py": 45, "test_ausbau.py": 20, "test_logbook_ui.py": 14,
            "test_planning.py": 11, "test_login_links.py": 8, "test_automation.py": 8}


def shard_of(names, index, count):
    """Längste zuerst auf den jeweils kürzesten Teil verteilen (gleichmäßig und bei jedem Lauf gleich)."""
    parts = [[0, []] for _ in range(count)]
    for name in sorted(names, key=lambda n: (-DURATION.get(n, 5), n)):
        target = min(parts, key=lambda p: p[0])
        target[0] += DURATION.get(name, 5)
        target[1].append(name)
    return set(parts[index - 1][1])


def main():
    args = sys.argv[1:]
    shard = None
    if "--shard" in args:
        i = args.index("--shard")
        index, count = (int(x) for x in args[i + 1].split("/"))
        if not 1 <= index <= count:
            raise SystemExit("--shard i/n mit 1 <= i <= n")
        shard = (index, count)
        del args[i:i + 2]
    only = args[0] if args else ""
    # Tabellen anlegen lassen und Demo-Daten einspielen
    subprocess.run([sys.executable, "-c", "import main"], cwd=ROOT, env=ENV, check=True, stdout=subprocess.DEVNULL)
    subprocess.run([sys.executable, "tests/seed_demo.py"], cwd=ROOT, env=ENV, check=True, stdout=subprocess.DEVNULL)
    failed = []
    paths = sorted(glob.glob(os.path.join(ROOT, "tests", "test_*.py")))
    chosen = shard_of([os.path.basename(p) for p in paths], *shard) if shard else None
    if chosen is not None:
        print(f"Teil {shard[0]}/{shard[1]}: " + ", ".join(sorted(chosen)), flush=True)
    for path in paths:
        name = os.path.basename(path)
        if only and only not in name:
            continue
        if chosen is not None and name not in chosen:
            continue
        app = start_app()
        try:
            print(f"\n=== {name}", flush=True)
            r = subprocess.run([sys.executable, path], cwd=ROOT, env=ENV)
            if r.returncode != 0:
                failed.append(name)
        finally:
            app.kill()
            app.wait()
    print("\n" + ("ALLE TESTS BESTANDEN" if not failed else "FEHLGESCHLAGEN: " + ", ".join(failed)))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()

"""Startet fuer jeden Test eine frische App-Instanz (die IP-Bremse zaehlt im Speicher) und fuehrt ihn aus.

Voraussetzung: eine leere MariaDB mit den Zugangsdaten aus den Umgebungsvariablen (siehe tests/README.md).
Aufruf aus dem Projektordner:  python tests/run_all.py [Teil-des-Testnamens]
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


def start_app():
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


def main():
    only = sys.argv[1] if len(sys.argv) > 1 else ""
    # Tabellen anlegen lassen und Demo-Daten einspielen
    subprocess.run([sys.executable, "-c", "import main"], cwd=ROOT, env=ENV, check=True, stdout=subprocess.DEVNULL)
    subprocess.run([sys.executable, "tests/seed_demo.py"], cwd=ROOT, env=ENV, check=True, stdout=subprocess.DEVNULL)
    failed = []
    for path in sorted(glob.glob(os.path.join(ROOT, "tests", "test_*.py"))):
        name = os.path.basename(path)
        if only and only not in name:
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

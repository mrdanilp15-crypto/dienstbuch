# Tests

Die Tests laufen gegen eine echte (leere) MariaDB und eine frisch gestartete App - mit frei erfundenen
Demo-Daten (`tests/seed_demo.py`). Sie loeschen und ueberschreiben Daten, deshalb: **niemals gegen die
echte Wehr-Datenbank** (das Seed-Skript bricht ab, wenn der Datenbankname nicht "demo" enthaelt).

## Lokal

```bash
docker run -d --name demo_test_db -e MYSQL_ROOT_PASSWORD=rootpw -e MYSQL_DATABASE=dienstbuch_demo \
  -e MYSQL_USER=app -e MYSQL_PASSWORD=apppw -p 33307:3306 mariadb:10.6
pip install -r requirements.txt -r tests/requirements.txt
python tests/run_all.py            # alle
python tests/run_all.py login      # nur Tests, deren Dateiname "login" enthaelt
python tools/check.py              # Routen-/Vertrags-Pruefung
```

## Bei GitHub

`.github/workflows/docker-publish.yml` fuehrt vor jedem Bau des Images `tools/check.py` und alle Tests aus.
Schlaegt etwas fehl, wird **kein neues Image** veroeffentlicht - Portainer bleibt dann auf dem letzten
funktionierenden Stand.

## Oberflaechen-Tests (Playwright)

`test_contrast.py` prueft jeden sichtbaren Text auf jeder Seite, in Dunkel und Hell, auf Kontrast (WCAG 4,5:1) und
Mindestschriftgroesse (12 px). `test_navigation.py` prueft je Rolle Seitenleiste, Reiter und dass keine verbotenen Abfragen
gestellt werden. Dafuer einmalig: `python -m playwright install chromium`. Ohne Playwright werden beide uebersprungen.
Neue Farben bitte ueber die Variablen am Ende von `static/css/dashboard.css` ("LESBARKEIT") waehlen, nicht fest eintragen.

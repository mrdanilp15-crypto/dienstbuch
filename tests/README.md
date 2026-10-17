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

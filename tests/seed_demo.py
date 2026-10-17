"""Befuellt eine DEMO-Instanz mit frei erfundenen Daten (Feuerwehr Musterstadt).

ACHTUNG: loescht vorher die Inhalte der Dienstbuch-Tabellen! Deshalb mit zwei Sperren:
  1. Umgebungsvariable DEMO_MODE=1 muss gesetzt sein
  2. der Datenbankname muss "demo" enthalten
So kann das Skript nicht versehentlich eine echte Wehr-Datenbank leeren.

Aufruf (nachdem die Demo-Instanz einmal gestartet ist, damit die Tabellen existieren):
    docker exec -e DEMO_MODE=1 <api-container> python tools/seed_demo.py
Fuer den naechtlichen Reset denselben Befehl per Cron/Portainer-Schedule ausfuehren.
"""
import os
import random
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_NAME = os.getenv("DB_NAME", "")
if os.getenv("DEMO_MODE") != "1" or "demo" not in DB_NAME.lower():
    sys.exit(f"Abbruch: Nur fuer Demo-Instanzen (DEMO_MODE=1 und DB_NAME mit 'demo'). Aktuell DB_NAME='{DB_NAME}'.")

from core.utils import hash_password  # noqa: E402
from database import get_db_connection  # noqa: E402

DEMO_USER = "demo"
DEMO_PASSWORD = "demo1234"

random.seed(42)
TODAY = date.today()
conn = get_db_connection()
cur = conn.cursor(dictionary=True)
_cols = {}


def columns(table):
    if table not in _cols:
        cur.execute(f"SHOW COLUMNS FROM {table}")
        _cols[table] = {r["Field"] for r in cur.fetchall()}
    return _cols[table]


def insert(table, row):
    """Schreibt nur Felder, die es in der Tabelle gibt - so bricht das Skript nicht, wenn
    eine neuere/aeltere App-Version Spalten ergaenzt oder umbenennt."""
    row = {k: v for k, v in row.items() if k in columns(table)}
    names = ", ".join(row)
    cur.execute(f"INSERT INTO {table} ({names}) VALUES ({', '.join(['%s'] * len(row))})", tuple(row.values()))
    return cur.lastrowid


def d(days_ago):
    return TODAY - timedelta(days=days_ago)


# ---------------------------------------------------------------- aufraeumen
cur.execute("SET FOREIGN_KEY_CHECKS = 0")
for t in ["respiration_log", "mission_attendance", "missions", "attendance", "sessions", "persons", "schedule_attendance",
          "schedules", "personal_inventar", "clothing_stock", "lehrgaenge", "equipment_inspections", "equipment",
          "vehicle_checks", "vehicle_log", "vehicles", "users", "personnel", "groups_table", "audit_log",
          "system_broadcasts", "broadcast_reads", "push_subscriptions", "mail_settings", "password_resets",
          "apager_logs", "apager_feedbacks"]:
    try:
        cur.execute(f"DELETE FROM {t}")
    except Exception:
        pass
cur.execute("SET FOREIGN_KEY_CHECKS = 1")
_cols.clear()

# ---------------------------------------------------------------- Gruppen
g_main = insert("groups_table", {"name": "Feuerwehr Musterstadt"})
g_basis = insert("groups_table", {"name": "Grundausbildung"})
insert("groups_table", {"name": "Jugendfeuerwehr"})

# ---------------------------------------------------------------- Personal
# (Name, Dienstgrad, Truppmann, Funk, AGT, Maschinist, TF, GF, Jahre dabei, G26-vor-Tagen)
members = [
    ("Max Beispiel", "Hauptbrandmeister", 1, 1, 1, 1, 1, 1, 24, 300),
    ("Anna Muster", "Oberbrandmeisterin", 1, 1, 1, 0, 1, 1, 15, 700),
    ("Lukas Probst", "Brandmeister", 1, 1, 1, 1, 1, 0, 11, 1050),
    ("Sabine Demmel", "Löschmeisterin", 1, 1, 0, 0, 1, 0, 9, None),
    ("Jonas Weber", "Oberfeuerwehrmann", 1, 1, 1, 0, 0, 0, 6, 200),
    ("Tim Fischer", "Oberfeuerwehrmann", 1, 0, 1, 1, 0, 0, 7, 880),
    ("Laura Schmitt", "Feuerwehrfrau", 1, 1, 1, 0, 0, 0, 4, 450),
    ("Felix Braun", "Feuerwehrmann", 1, 0, 0, 0, 0, 0, 3, None),
    ("Nina Hofmann", "Feuerwehrfrau", 1, 1, 1, 0, 0, 0, 3, 100),
    ("Paul Wagner", "Oberfeuerwehrmann", 1, 0, 1, 1, 0, 0, 8, 1120),
    ("Julia Bauer", "Feuerwehrfrau", 1, 0, 0, 0, 0, 0, 2, None),
    ("David Keller", "Feuerwehrmann", 1, 1, 1, 0, 0, 0, 5, 600),
    ("Marie Lang", "Feuerwehrfrau", 1, 0, 0, 0, 0, 0, 1, None),
    ("Simon Roth", "Hauptfeuerwehrmann", 1, 1, 1, 1, 0, 0, 13, 950),
    ("Eva Maurer", "Feuerwehrfrau", 1, 1, 0, 0, 0, 0, 2, None),
    ("Tobias Engel", "Feuerwehrmann", 0, 0, 0, 0, 0, 0, 0, None),
    ("Katharina Vogel", "Feuerwehrfrau", 0, 0, 0, 0, 0, 0, 0, None),
    ("Ralf Sommer", "Ehrenmitglied", 0, 0, 0, 0, 0, 0, 40, None),
]
pids = {}
for i, (name, rank, trupp, funk, agt, masch, tf, gf, years, g26) in enumerate(members):
    status = "Ehren" if rank == "Ehrenmitglied" else "Aktiv"
    first, last = name.lower().split()[0], name.lower().split()[1]
    pids[name] = insert("personnel", {
        "name": name, "rank": rank, "membership_status": status,
        "phone": f"0170 555 {1000 + i}", "email": f"{first}.{last}@example.org",
        "address": f"Musterweg {i + 1}, 12345 Musterstadt",
        "birth_date": date(1975 + (i * 3) % 25, 1 + i % 12, 1 + (i * 5) % 27).isoformat(),
        "entry_date": date(TODAY.year - years, 3, 1).isoformat() if years else d(120).isoformat(),
        "is_truppmann": trupp, "is_funk": funk, "is_agt": agt, "is_maschinist": masch, "is_tf": tf, "is_gf": gf,
        "lic_b": 1, "lic_be": int(masch), "lic_c": int(masch), "lic_ce": int(masch and i % 2 == 0),
        "g26_3_date": d(g26).isoformat() if (agt and g26 is not None) else None,
        "belastungslauf_date": d(150 + i * 12).isoformat() if agt else None,
        "unterweisung_date": d(60 + i * 9).isoformat(),
        "emergency_contact_name": "Angehörige(r) (Demo)", "emergency_contact_phone": "0170 555 0000",
    })

cur.execute("INSERT INTO persons (group_id, name) SELECT g.id, p.name FROM groups_table g CROSS JOIN personnel p")
cur.execute("SELECT id, group_id, name FROM persons")
person_ids = {(r["group_id"], r["name"]): r["id"] for r in cur.fetchall()}

# ---------------------------------------------------------------- Demo-Login
insert("users", {"username": DEMO_USER, "password_hash": hash_password(DEMO_PASSWORD), "role": "admin",
                 "is_first_login": 0, "personnel_id": pids["Max Beispiel"]})

# ---------------------------------------------------------------- Dienste/Uebungen (letzte 12 Monate)
active = [m[0] for m in members if m[1] != "Ehrenmitglied"]
topics = ["Atemschutz-Übung", "Knoten und Stiche", "Fahrzeug- und Gerätekunde", "Löschangriff", "Technische Hilfeleistung",
          "Erste Hilfe", "Funkübung", "Wasserentnahme Hydrant", "Absturzsicherung", "Gefahrstoffe"]
for week in range(0, 52, 2):
    when = d(week * 7 + 3)
    cat = "Übung" if week % 10 else "Sonstiges"
    sid = insert("sessions", {"group_id": g_main, "date": when.isoformat(), "category": cat,
                              "duration": random.choice([2, 2.5, 3]),
                              "description": random.choice(topics) if cat == "Übung" else "Kameradschaftsabend / Sonstiges",
                              "instructors": random.choice(["Max Beispiel", "Anna Muster", "Lukas Probst"])})
    for n in active:
        insert("attendance", {"session_id": sid, "person_id": person_ids[(g_main, n)],
                              "is_present": 1 if random.random() < 0.72 else 0})

# ---------------------------------------------------------------- Einsaetze (letzte 12 Monate)
vehicles_names = ["HLF 20", "MTW", "LF 10"]
mission_defs = [
    ("B2 Wohnungsbrand", "Hauptstraße 12, Musterstadt", "Rauch aus Küchenfenster, Person vermisst"),
    ("THL 1 Verkehrsunfall", "B 300 Höhe Musterstadt-Nord", "PKW gegen Baum, eine Person eingeklemmt"),
    ("BMA Auslösung", "Gewerbepark 5, Musterstadt", "Brandmeldeanlage, Auslösung Melder 12"),
    ("B1 Containerbrand", "Industriestraße 3, Musterstadt", "Papiercontainer in Vollbrand"),
    ("THL 1 Türöffnung", "Lindenweg 7, Musterstadt", "Hilflose Person hinter verschlossener Tür"),
    ("Unwetter Baum auf Straße", "Dorfstraße, Musterstadt-Süd", "Umgestürzter Baum blockiert Fahrbahn"),
    ("B3 Dachstuhlbrand", "Am Markt 2, Musterstadt", "Dachstuhl brennt, Nachbargebäude gefährdet"),
    ("ABC 1 Ölspur", "Kreisstraße 12", "Ölspur auf 300 m Länge"),
    ("RD Tragehilfe", "Birkenallee 9, Musterstadt", "Tragehilfe für Rettungsdienst"),
    ("BMA Auslösung", "Seniorenheim Sonnenhof", "Auslösung durch Kochdunst"),
    ("B2 Kaminbrand", "Schulstraße 4, Musterstadt", "Rußbrand im Schornstein"),
    ("THL 2 Wassereinbruch", "Bachgasse 1, Musterstadt", "Keller unter Wasser nach Starkregen"),
]
agt_members = [m[0] for m in members if m[4]]
for i, (stich, adr, meldung) in enumerate(mission_defs):
    when = d(20 + i * 29)
    hh, mm = random.choice([(2, 14), (7, 41), (13, 5), (17, 52), (21, 30), (23, 18)])
    start = f"{hh:02d}:{mm:02d}"
    mid = insert("missions", {
        "date": when.isoformat(), "time": start, "end_time": f"{(hh + 2) % 24:02d}:{mm:02d}",
        "ausrueck_zeit": f"{hh:02d}:{(mm + 4) % 60:02d}", "eintreff_zeit": f"{hh:02d}:{(mm + 11) % 60:02d}",
        "stichwort": stich, "adresse": adr, "meldung": meldung,
        "description": "Einsatz ordnungsgemäß abgearbeitet. (Demo-Daten)", "duration": random.choice([1.0, 1.5, 2.0, 3.0]),
        "status": "Freigegeben" if i > 0 else "Entwurf",
    })
    crew = random.sample(active, random.randint(6, 10))
    for n in crew:
        insert("mission_attendance", {"mission_id": mid, "personnel_id": pids[n], "is_present": "Ja",
                                      "vehicle": random.choice(vehicles_names)})
    if stich.startswith("B") and not stich.startswith("BMA"):
        for n in [c for c in crew if c in agt_members][:2]:
            insert("respiration_log", {"mission_id": mid, "personnel_id": pids[n], "druck_start": 300,
                                       "druck_10": 250, "druck_20": 190, "druck_ende": 150, "dauer": 22, "fit_ok": 1})

# ---------------------------------------------------------------- Termine (naechste Wochen)
plan = [(5, "Atemschutz-Übung", "Übung", "19:30"), (12, "Maschinistenausbildung", "Schulung", "19:00"),
        (19, "Jahreshauptversammlung", "Sonstiges", "19:00"), (26, "Löschangriff mit Wasserentnahme", "Übung", "19:30"),
        (40, "Erste-Hilfe-Auffrischung", "Schulung", "18:30")]
for days, title, typ, t in plan:
    insert("schedules", {"title": title, "date": (TODAY + timedelta(days=days)).isoformat(), "time": t,
                         "description": "Demo-Termin", "type": typ, "group_id": g_main})

# ---------------------------------------------------------------- Fahrzeuge
for name, funk, tuv, sp, km in [("HLF 20", "Florian Musterstadt 40/1", 140, 200, 48210),
                                 ("MTW", "Florian Musterstadt 19/1", 25, 300, 61800),
                                 ("LF 10", "Florian Musterstadt 42/1", -10, 90, 89420)]:
    insert("vehicles", {"name": name, "radio_name": funk, "status": 2,
                        "tuv_date": (TODAY + timedelta(days=tuv)).isoformat(),
                        "sp_date": (TODAY + timedelta(days=sp)).isoformat(), "milage": km})

# ---------------------------------------------------------------- Lehrgaenge
for n, course, years_ago, valid in [("Max Beispiel", "Gruppenführer", 12, None), ("Max Beispiel", "Atemschutzgeräteträger", 3, 60),
                                     ("Anna Muster", "Gruppenführer", 8, None), ("Lukas Probst", "Maschinist", 6, None),
                                     ("Tim Fischer", "Maschinist", 5, None), ("Jonas Weber", "Atemschutzgeräteträger", 2, 400),
                                     ("Paul Wagner", "Maschinist", 7, None), ("Simon Roth", "Atemschutzgeräteträger", 4, -20),
                                     ("Laura Schmitt", "Truppführer", 2, None), ("Nina Hofmann", "Funkgeräteführer", 1, None)]:
    insert("lehrgaenge", {"personnel_id": pids[n], "course_name": course, "date": d(int(years_ago * 365)).isoformat(),
                          "valid_until": (TODAY + timedelta(days=valid)).isoformat() if valid is not None else None})

# ---------------------------------------------------------------- Geraete, Kleiderkammer
for name, code, cat, due in [("Atemschutzgerät 1", "DEMO-0001", "Atemschutz", 20), ("Motorsäge", "DEMO-0002", "Geräte", 150),
                              ("Hydraulischer Spreizer", "DEMO-0003", "Rettungsgerät", -5), ("Tauchpumpe", "DEMO-0004", "Pumpen", 80)]:
    insert("equipment", {"name": name, "barcode": code, "category": cat, "interval_months": 12,
                         "last_inspection": (TODAY + timedelta(days=due - 365)).isoformat(),
                         "next_inspection": (TODAY + timedelta(days=due)).isoformat()})
for item, size, qty, minimum in [("Einsatzjacke", "M", 3, 2), ("Einsatzjacke", "L", 1, 2), ("Einsatzhose", "L", 4, 2),
                                  ("Einsatzstiefel", "43", 0, 1), ("Helm", "Einheitsgröße", 6, 3)]:
    insert("clothing_stock", {"item_name": item, "size": size, "quantity": qty, "min_quantity": minimum})
for n, item, size in [("Jonas Weber", "Einsatzjacke", "L"), ("Laura Schmitt", "Einsatzhose", "M"), ("Felix Braun", "Helm", "Einheitsgröße")]:
    insert("personal_inventar", {"personnel_id": pids[n], "item_name": item, "size": size, "issue_date": d(200).isoformat()})

conn.commit()
cur.close()
conn.close()
print(f"Demo-Daten eingespielt: {len(members)} Kameraden, {len(mission_defs)} Einsätze, Login '{DEMO_USER}' / '{DEMO_PASSWORD}'.")

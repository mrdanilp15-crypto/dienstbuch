"""Wer darf welchen Termin sehen?

Ein Termin gilt entweder fuer "alle" (audience = 'all') oder nur fuer ausgewaehlte Personen
(audience = 'selected', Liste in schedule_audience). Nicht dazugehoerige Personen bekommen
ihn weder per Push/Mail noch sehen sie ihn in Liste, Kalender, Kalender-Abo oder auf dem
Hallenmonitor. Immer sichtbar: Admin und Leitung (sie verwalten alles) sowie wer den Termin
selbst angelegt hat. Die Zuordnung Login -> Kamerad laeuft bewusst NUR ueber die ausdrueckliche
Verknuepfung users.personnel_id (kein Namens-Raten), damit ein Termin nie versehentlich bei
der falschen Person landet.
"""

MANAGER_ROLES = ("admin", "leitung")
EDITOR_ROLES = ("admin", "leitung", "gruppenfuehrer")


def _first(row, key):
    if row is None:
        return None
    return row[key] if isinstance(row, dict) else row[0]


def visibility_sql(cur, user, alias: str = "s"):
    """Liefert (sql-Bedingung, parameter) fuer eine Abfrage auf schedules (mit Tabellen-Alias)."""
    a = alias
    if not user:  # nicht angemeldet (z.B. Hallenmonitor): nur oeffentliche Termine
        return f"({a}.audience = 'all' OR {a}.audience IS NULL)", []
    if user.get("role") in MANAGER_ROLES:
        return "1=1", []
    cur.execute("SELECT personnel_id FROM users WHERE username = %s", (user["username"],))
    pid = _first(cur.fetchone(), "personnel_id")
    sql = f"({a}.audience = 'all' OR {a}.audience IS NULL OR {a}.created_by = %s"
    params = [user["username"]]
    if pid:
        sql += f" OR {a}.id IN (SELECT schedule_id FROM schedule_audience WHERE personnel_id = %s)"
        params.append(pid)
    return sql + ")", params


def can_view_schedule(cur, user, schedule_id: int) -> bool:
    sql, params = visibility_sql(cur, user, "s")
    cur.execute(f"SELECT 1 FROM schedules s WHERE s.id = %s AND {sql}", [schedule_id] + params)
    return cur.fetchone() is not None

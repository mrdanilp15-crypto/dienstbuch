"""Einsätze für Statistiken zählen: neue Einsatzberichte (missions) und alte Diensteinträge mit Kategorie "Einsatz".

Vor dem Einsatz-Editor wurden Einsätze im Logbuch einer Gruppe als Dienst mit Kategorie "Einsatz" erfasst. Die stehen
weiter in sessions und müssen in Einsatzstatistiken mitzählen (und dürfen dort nicht als Dienst zählen).
Hat mehr als eine Gruppe denselben alten Einsatz eingetragen (gleiches Datum, gleiche Uhrzeit, gleiche Beschreibung),
zählt er einmal.
"""

# Bedingung für alte Einsatz-Einträge in sessions (Alias s)
LEGACY = "s.category = 'Einsatz'"
NOT_LEGACY = "COALESCE(s.category, '') <> 'Einsatz'"


def mission_rows(cur, start, end):
    """Alle echten Einsätze im Zeitraum [start, end) als Liste von {"date", "stichwort", "legacy"}."""
    cur.execute("SELECT date, stichwort FROM missions WHERE date >= %s AND date < %s "
                "AND COALESCE(stichwort, '') NOT LIKE '[TEST]%%'", (start, end))
    rows = [{"date": r["date"], "stichwort": r["stichwort"] or "", "legacy": False} for r in cur.fetchall()]
    cur.execute(f"SELECT s.date, s.time, s.description FROM sessions s WHERE {LEGACY} AND s.date >= %s AND s.date < %s "
                "ORDER BY s.date, s.id", (start, end))
    seen = set()
    for r in cur.fetchall():
        desc = (r["description"] or "").strip()
        key = (r["date"], str(r["time"] or "").strip()[:5], desc.lower())
        if key in seen:
            continue
        seen.add(key)
        rows.append({"date": r["date"], "stichwort": desc, "legacy": True})
    return rows


def legacy_hours(cur, start, end, not_present):
    """Personenstunden und Teilnahmen aus alten Einsatz-Einträgen (Anwesenheit im Logbuch)."""
    cur.execute("SELECT COALESCE(SUM(s.duration), 0) AS h, COUNT(*) AS n FROM attendance a JOIN sessions s ON s.id = a.session_id "
                f"WHERE {LEGACY} AND s.date >= %s AND s.date < %s AND CAST(a.is_present AS CHAR) NOT IN {not_present}", (start, end))
    r = cur.fetchone()
    return float(r["h"] or 0), int(r["n"] or 0)

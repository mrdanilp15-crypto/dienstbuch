"""Jahresbericht für die Jahreshauptversammlung: alle Zahlen eines Jahres in einem Dokument.

Die Daten werden einmal gesammelt (collect) und dann als PDF gesetzt (render_pdf). Test-Alarme zählen nicht, „Entschuldigt“
gilt wie überall als nicht anwesend. Nur Wehrführung (Admin/Leitung) kann den Bericht abrufen.
"""
import datetime
import io
from html import escape

from database import get_db_connection

_NOT_PRESENT = "('Nein', 'Entschuldigt', '0', 'false', 'False', '')"
MONTHS = ("Jan", "Feb", "Mär", "Apr", "Mai", "Jun", "Jul", "Aug", "Sep", "Okt", "Nov", "Dez")


def _hours(v):
    return round(float(v or 0), 1)


def collect(year: int) -> dict:
    from routers.admin_api import _mission_type
    from routers.personnel_mgr import _compute_anniversaries
    start, end = datetime.date(year, 1, 1), datetime.date(year + 1, 1, 1)
    period = (start, end)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)

    def one(sql, params=period):
        cur.execute(sql, params)
        row = cur.fetchone()
        return list(row.values())[0] or 0 if row else 0

    from core.mission_stats import mission_rows, legacy_hours, NOT_LEGACY
    real = "m.date >= %s AND m.date < %s AND COALESCE(m.stichwort, '') NOT LIKE '[TEST]%%'"
    # Neue Einsatzberichte und alte Logbuch-Einträge "Einsatz"
    missions = mission_rows(cur, start, end)
    by_type, by_month = {}, [0] * 12
    for m in missions:
        t = _mission_type(m["stichwort"])
        by_type[t] = by_type.get(t, 0) + 1
        if m["date"]:
            by_month[m["date"].month - 1] += 1
    prev_missions = len(mission_rows(cur, datetime.date(year - 1, 1, 1), start))
    mission_hours = one("SELECT SUM(m.duration) FROM mission_attendance a JOIN missions m ON m.id = a.mission_id "
                        f"WHERE {real} AND CAST(a.is_present AS CHAR) NOT IN {_NOT_PRESENT}")
    mission_people = one("SELECT COUNT(*) FROM mission_attendance a JOIN missions m ON m.id = a.mission_id "
                         f"WHERE {real} AND CAST(a.is_present AS CHAR) NOT IN {_NOT_PRESENT}")
    old_h, old_n = legacy_hours(cur, start, end, _NOT_PRESENT)
    mission_hours = float(mission_hours or 0) + old_h
    mission_people += old_n

    cur.execute("SELECT COALESCE(NULLIF(s.category, ''), 'Sonstiges') AS cat, COUNT(*) AS n, SUM(s.duration) AS h FROM sessions s "
                f"WHERE s.date >= %s AND s.date < %s AND {NOT_LEGACY} GROUP BY cat ORDER BY n DESC", period)
    service_cats = [{"category": r["cat"], "count": r["n"], "hours": _hours(r["h"])} for r in cur.fetchall()]
    service_hours = one("SELECT SUM(s.duration) FROM attendance a JOIN sessions s ON s.id = a.session_id "
                        f"WHERE s.date >= %s AND s.date < %s AND {NOT_LEGACY} AND CAST(a.is_present AS CHAR) NOT IN {_NOT_PRESENT}")

    cur.execute("SELECT COALESCE(NULLIF(membership_status, ''), 'Aktiv') AS st, COUNT(*) AS n FROM personnel "
                "WHERE entry_date IS NULL OR entry_date < %s GROUP BY st", (end,))
    status_counts = {r["st"]: r["n"] for r in cur.fetchall()}
    cur.execute("SELECT name, entry_date FROM personnel WHERE entry_date >= %s AND entry_date < %s ORDER BY entry_date", period)
    joined = [{"name": r["name"], "date": r["entry_date"]} for r in cur.fetchall()]
    cur.execute("SELECT name, exit_date FROM personnel WHERE exit_date >= %s AND exit_date < %s ORDER BY exit_date", period)
    left = [{"name": r["name"], "date": r["exit_date"]} for r in cur.fetchall()]

    cur.execute("SELECT l.course_name, l.date, p.name FROM lehrgaenge l JOIN personnel p ON p.id = l.personnel_id "
                "WHERE l.date >= %s AND l.date < %s ORDER BY l.course_name, p.name", period)
    courses = {}
    for r in cur.fetchall():
        courses.setdefault(r["course_name"], []).append(r["name"])
    cur.execute("SELECT h.kind, h.title, h.date, p.name FROM personnel_honors h JOIN personnel p ON p.id = h.personnel_id "
                "WHERE h.date >= %s AND h.date < %s ORDER BY h.kind, h.date, p.name", period)
    honors = cur.fetchall()

    cur.execute("SELECT v.name, SUM(GREATEST(COALESCE(l.mileage_end, 0) - COALESCE(l.mileage_start, 0), 0)) AS km, COUNT(l.id) AS rides "
                "FROM vehicles v LEFT JOIN vehicle_log l ON l.vehicle_id = v.id AND l.date >= %s AND l.date < %s GROUP BY v.id, v.name ORDER BY v.name", period)
    vehicles = [{"name": r["name"], "km": int(r["km"] or 0), "rides": r["rides"]} for r in cur.fetchall()]
    vehicle_missions = {}
    cur.execute(f"SELECT a.vehicle, COUNT(DISTINCT a.mission_id) AS n FROM mission_attendance a JOIN missions m ON m.id = a.mission_id "
                f"WHERE {real} AND a.vehicle IS NOT NULL AND a.vehicle <> '' GROUP BY a.vehicle", period)
    for r in cur.fetchall():
        vehicle_missions[r["vehicle"]] = r["n"]
    defects_new = one("SELECT COUNT(*) FROM equipment_defect_reports WHERE created_at >= %s AND created_at < %s")
    defects_done = one("SELECT COUNT(*) FROM equipment_defect_reports WHERE resolved_at >= %s AND resolved_at < %s")
    inspections = one("SELECT COUNT(*) FROM equipment_inspections WHERE date >= %s AND date < %s")
    equipment_count = one("SELECT COUNT(*) FROM equipment WHERE COALESCE(archived, 0) = 0", ())

    youth_members = status_counts.get("Jugend", 0)
    youth_sessions = one("SELECT COUNT(*) FROM youth_sessions WHERE date >= %s AND date < %s")
    youth_hours = one("SELECT SUM(duration) FROM youth_sessions WHERE date >= %s AND date < %s")
    cur.close()
    conn.close()

    ann = _compute_anniversaries(year)
    return {
        "year": year,
        "missions": {"total": len(missions), "prev": prev_missions, "hours": _hours(mission_hours), "people": mission_people,
                     "by_type": sorted(({"type": k, "count": v} for k, v in by_type.items()), key=lambda x: -x["count"]),
                     "by_month": by_month},
        "services": {"total": sum(c["count"] for c in service_cats), "hours": _hours(service_hours), "by_category": service_cats},
        "members": {"by_status": status_counts, "joined": joined, "left": left},
        "courses": [{"course": k, "names": v} for k, v in sorted(courses.items())],
        "honors": [{"kind": h["kind"], "title": h["title"], "date": h["date"], "name": h["name"]} for h in honors],
        "anniversaries": [{"name": a["name"], "years": a["years"]} for a in ann["service_anniversaries"]],
        "vehicles": [{**v, "missions": vehicle_missions.get(v["name"], 0)} for v in vehicles],
        "technik": {"defects_new": defects_new, "defects_done": defects_done, "inspections": inspections, "equipment": equipment_count},
        "youth": {"members": youth_members, "sessions": youth_sessions, "hours": _hours(youth_hours)},
    }


def _n(v):
    return f"{v:,.1f}".replace(",", "X").replace(".", ",").replace("X", ".").replace(",0", "") if isinstance(v, float) else f"{v:,}".replace(",", ".")


def _bars(by_month):
    """Einsätze je Monat als waagerechte Balken in zwei Spalten (Jan–Jun, Jul–Dez). xhtml2pdf zeichnet nur Tabellenzellen
    mit Hintergrundfarbe zuverlässig, deshalb ist jeder Balken eine Zelle mit Prozentbreite."""
    top = max(by_month) or 1

    def col(months):
        rows = ""
        for i in months:
            c = by_month[i]
            w = min(max(int(100 * c / top), 4), 100) if c else 0
            rest = f"<td style='width:{100 - w}%; padding:0;'></td>" if w < 100 else ""
            bar = (f"<table style='width:100%; border-collapse:collapse;'><tr><td style='width:{w}%; background-color:#b91c1c; padding:0; font-size:6pt;'>&nbsp;</td>"
                   f"{rest}</tr></table>") if w else ""
            rows += (f"<tr><td style='width:14%; font-size:8.5pt; color:#6b7280; padding:2px 0;'>{MONTHS[i]}</td>"
                     f"<td style='width:72%; padding:2px 4px;'>{bar}</td><td style='width:14%; font-size:8.5pt; text-align:right; padding:2px 0;'>{c}</td></tr>")
        return f"<table style='width:100%; border-collapse:collapse;'>{rows}</table>"
    return (f"<table style='width:100%; margin:4px 0 8px;'><tr><td style='width:48%; vertical-align:top;'>{col(range(6))}</td>"
            f"<td style='width:4%'></td><td style='width:48%; vertical-align:top;'>{col(range(6, 12))}</td></tr></table>")


def render_pdf(d: dict, station: str) -> bytes:
    e = lambda v: escape(str(v)) if v is not None else ""
    de = lambda v: v.strftime("%d.%m.%Y") if hasattr(v, "strftime") else e(v)
    y = d["year"]
    m, s, mem = d["missions"], d["services"], d["members"]
    diff = m["total"] - m["prev"]
    trend = (f"{'+' if diff >= 0 else ''}{diff} gegenüber {y - 1}" if m["prev"] else "")
    active = mem["by_status"].get("Aktiv", 0)
    total_members = sum(v for k, v in mem["by_status"].items() if k != "Ausgeschieden")

    def kpi(value, label, sub=""):
        return (f"<td class='kpi'><div class='kv'>{value}</div><div class='kl'>{label}</div>"
                f"{f'<div class=ks>{sub}</div>' if sub else ''}</td>")

    def table(head, rows, widths=None, empty="Keine Einträge."):
        if not rows:
            return f"<p class='muted'>{empty}</p>"
        if head is None:  # Wort/Wert-Liste ohne Kopfzeile
            trs = "".join(f"<tr><td style='width:70%'>{k}</td><td class='num' style='width:30%'><strong>{_n(v)}</strong></td></tr>" for k, v in rows)
            return f"<table class='list'><tbody>{trs}</tbody></table>"
        ths = "".join(f"<th{f' style=width:{widths[i]}' if widths else ''}>{h}</th>" for i, h in enumerate(head))
        trs = "".join("<tr>" + "".join(f"<td{' class=num' if isinstance(c, (int, float)) else ''}>{_n(c) if isinstance(c, (int, float)) else c}</td>"
                                        for c in r) + "</tr>" for r in rows)
        return f"<table class='list'><thead><tr>{ths}</tr></thead><tbody>{trs}</tbody></table>"

    type_rows = [[e(t["type"]), t["count"]] for t in m["by_type"]]
    cat_rows = [[e(c["category"]), c["count"], c["hours"]] for c in s["by_category"]]
    status_order = ["Aktiv", "Jugend", "Passiv", "Ehrenabteilung", "Ehren"]
    st_rows = [[e(k), v] for k, v in sorted(mem["by_status"].items(), key=lambda kv: (status_order.index(kv[0]) if kv[0] in status_order else 99, kv[0]))
               if k != "Ausgeschieden"]
    course_rows = [[e(c["course"]), len(c["names"]), e(", ".join(c["names"]))] for c in d["courses"]]
    promo = [[e(h["name"]), e(h["title"]), de(h["date"])] for h in d["honors"] if h["kind"] == "Beförderung"]
    honor = [[e(h["name"]), e(h["title"]), de(h["date"])] for h in d["honors"] if h["kind"] != "Beförderung"]
    ann_rows = [[e(a["name"]), f"{a['years']} Jahre"] for a in d["anniversaries"]]
    veh_rows = [[e(v["name"]), v["missions"], v["rides"], v["km"]] for v in d["vehicles"]]
    t = d["technik"]
    yo = d["youth"]
    joined = ", ".join(e(j["name"]) for j in mem["joined"]) or "keine"
    left = ", ".join(e(j["name"]) for j in mem["left"]) or "keine"

    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8"><style>
        @page {{ size: a4 portrait; margin: 1.8cm 1.8cm 2cm; @frame footer {{ -pdf-frame-content: foot; bottom: 0.8cm; margin-left: 1.8cm; margin-right: 1.8cm; height: 1cm; }} }}
        body {{ font-family: Helvetica, Arial, sans-serif; font-size: 10pt; color: #1f2937; }}
        .station {{ font-size: 12pt; color: #b91c1c; font-weight: bold; }}
        .title {{ font-size: 24pt; font-weight: bold; margin-top: 4px; }}
        .sub {{ color: #6b7280; font-size: 10pt; }}
        table.kpis {{ width: 100%; border-collapse: separate; border-spacing: 6px; margin: 4px -6px 6px; }}
        td.kpi {{ background: #f5f6f8; padding: 10px; text-align: center; width: 25%; vertical-align: top; }}
        .kv {{ font-size: 20pt; font-weight: bold; color: #b91c1c; }}
        .kl {{ font-size: 9pt; font-weight: bold; }}
        .ks {{ font-size: 8pt; color: #6b7280; }}
        h2 {{ font-size: 13pt; color: #b91c1c; border-bottom: 1px solid #e5e7eb; padding-bottom: 3px; margin: 18px 0 8px; -pdf-keep-with-next: true; }}
        h3 {{ font-size: 10.5pt; margin: 10px 0 4px; -pdf-keep-with-next: true; }}
        table.list {{ width: 100%; border-collapse: collapse; margin-bottom: 6px; }}
        table.list th {{ background: #f3f4f6; text-align: left; padding: 4px 6px; font-size: 8.5pt; border-bottom: 1px solid #d1d5db; }}
        table.list td {{ padding: 4px 6px; border-bottom: 1px solid #eef0f2; font-size: 9pt; vertical-align: top; }}
        td.num {{ text-align: right; }}
        .muted {{ color: #6b7280; font-size: 9pt; }}
        .two td.col {{ width: 50%; vertical-align: top; padding-right: 10px; }}
    </style></head><body>
    <div id="foot" style="font-size:8pt; color:#9ca3af; text-align:center;">{e(station)} &middot; Jahresbericht {y} &middot; Seite <pdf:pagenumber> von <pdf:pagecount></div>
    <table style="width:100%; margin-bottom:14px;"><tr><td style="border-bottom:3px solid #b91c1c; padding-bottom:8px;">
        <div class="station">{e(station)}</div><div class="title">Jahresbericht {y}</div>
        <div class="sub">Erstellt am {datetime.date.today():%d.%m.%Y} aus dem Digitalen Dienstbuch</div></td></tr></table>

    <table class="kpis"><tr>
        {kpi(m['total'], 'Einsätze', trend)}{kpi(_n(m['hours']), 'Personenstunden im Einsatz', f"{m['people']} Einsatzteilnahmen")}
        {kpi(s['total'], 'Dienste und Übungen', f"{_n(s['hours'])} Personenstunden")}{kpi(total_members, 'Mitglieder', f'davon {active} aktiv')}
    </tr></table>

    <h2>Einsätze</h2>
    {_bars(m['by_month'])}
    <table class="two" style="width:100%"><tr><td class="col"><h3>Nach Art</h3>{table(['Art', 'Anzahl'], type_rows, empty='Keine Einsätze.')}</td>
        <td class="col"><h3>Auf einen Blick</h3>{table(None, [['Einsätze gesamt', m['total']], [f'Vorjahr {y - 1}', m['prev']],
                                                                 ['Personenstunden', m['hours']], ['Einsatzteilnahmen', m['people']]])}</td></tr></table>

    <h2>Dienste und Ausbildung</h2>
    {table(['Art', 'Anzahl', 'Stunden (Dauer)'], cat_rows, empty='Keine Dienste erfasst.')}
    <p class="muted">Personenstunden im Dienst (alle Anwesenden zusammen): {_n(s['hours'])}</p>
    <h3>Lehrgänge</h3>
    {table(['Lehrgang', 'Teilnehmer', 'Namen'], course_rows, ['30%', '12%', '58%'], 'Keine Lehrgänge abgeschlossen.')}

    <h2>Mannschaft</h2>
    <table class="two" style="width:100%"><tr><td class="col"><h3>Mitgliederstand</h3>{table(['Abteilung', 'Anzahl'], st_rows)}</td>
        <td class="col"><h3>Veränderungen</h3><p><strong>Eingetreten:</strong> {joined}</p><p><strong>Ausgetreten:</strong> {left}</p></td></tr></table>
    <h3>Beförderungen</h3>{table(['Name', 'Beförderung', 'Datum'], promo, empty='Keine Beförderungen eingetragen.')}
    <h3>Ehrungen</h3>{table(['Name', 'Ehrung', 'Datum'], honor, empty='Keine Ehrungen eingetragen.')}
    <h3>Dienstjubiläen</h3>{table(['Name', 'Dienstzeit'], ann_rows, empty='Keine runden Dienstjubiläen.')}

    <h2>Technik</h2>
    {table(['Fahrzeug', 'Einsätze', 'Fahrten', 'km'], veh_rows, empty='Keine Fahrzeuge erfasst.')}
    <p class="muted">Geräte im Bestand: {t['equipment']} &middot; Prüfungen: {t['inspections']} &middot; Mängel gemeldet: {t['defects_new']} &middot; Mängel erledigt: {t['defects_done']}</p>

    <h2>Jugendfeuerwehr</h2>
    <p>{yo['members']} Mitglieder &middot; {yo['sessions']} Übungsabende &middot; {_n(yo['hours'])} Stunden</p>
    </body></html>"""
    import xhtml2pdf.pisa as pisa
    buf = io.BytesIO()
    pisa.CreatePDF(html, dest=buf, encoding="utf-8")
    return buf.getvalue()

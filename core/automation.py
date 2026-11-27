"""Automatische Abläufe, die im Hintergrund laufen: Erinnerung an liegengebliebene Einsatzberichte und der Monatsbericht."""
import datetime

from core.mailer import get_mail_settings, is_mail_configured, send_mail_background
from core.security import get_int_setting, set_int_setting
from database import get_db_connection

OPEN_REPORT_DAYS = 3     # so alt muss ein Einsatz sein, bevor erinnert wird
OPEN_REPORT_REPEAT = 7   # danach höchstens einmal pro Woche erneut
MONTHS = ("Januar", "Februar", "März", "April", "Mai", "Juni", "Juli", "August", "September", "Oktober", "November", "Dezember")
_NOT_PRESENT = "('Nein', 'Entschuldigt', '0', 'false', 'False', '')"


def check_open_reports(now: datetime.datetime = None) -> int:
    """Entwürfe gehen an alle, die Berichte führen; Berichte "In Prüfung" an alle, die freigeben dürfen.
    Je Gruppe eine gebündelte Meldung, nur tagsüber (8-21 Uhr). Gibt die Zahl der angemahnten Berichte zurück."""
    from core.notify import notify
    from core.permissions import users_with
    now = now or datetime.datetime.now()
    if not (8 <= now.hour < 21):
        return 0
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT id, date, stichwort, status FROM missions WHERE status IN ('Entwurf', 'In Prüfung') "
        "AND (leader_signature IS NULL OR leader_signature = '') AND date <= %s AND stichwort NOT LIKE '[TEST]%%' "
        "AND (reminded_at IS NULL OR reminded_at <= %s) ORDER BY date",
        (now.date() - datetime.timedelta(days=OPEN_REPORT_DAYS), now - datetime.timedelta(days=OPEN_REPORT_REPEAT)))
    rows = cur.fetchall()
    total = 0
    for status, permission, one, many in (
        ("Entwurf", "missions.manage", "1 Einsatzbericht ist noch nicht fertig", "{} Einsatzberichte sind noch nicht fertig"),
        ("In Prüfung", "missions.approve", "1 Einsatzbericht wartet auf Freigabe", "{} Einsatzberichte warten auf Freigabe"),
    ):
        part = [r for r in rows if r["status"] == status]
        users = users_with(permission) if part else []
        if not users:
            continue
        names = ", ".join(f"{r['stichwort']} ({r['date'].strftime('%d.%m.')})" for r in part[:5])
        if len(part) > 5:
            names += f" und {len(part) - 5} weitere"
        notify("verwaltung", one if len(part) == 1 else many.format(len(part)), names, "/dashboard?open=einsaetze", usernames=users)
        cur.executemany("UPDATE missions SET reminded_at = %s WHERE id = %s", [(now, r["id"]) for r in part])
        total += len(part)
    conn.commit()
    cur.close()
    conn.close()
    return total


def check_vehicle_returns(now: datetime.datetime = None) -> int:
    """Fahrzeuge außer Dienst, deren „voraussichtlich bis“ vorbei ist: die Zuständigen einmal fragen, ob es wieder einsatzbereit ist
    (bei geändertem Datum erneut). Nur tagsüber. Gibt die Zahl der angemahnten Fahrzeuge zurück."""
    from core.notify import notify
    from core.permissions import users_with
    now = now or datetime.datetime.now()
    if not (8 <= now.hour < 21):
        return 0
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, name, out_reason, out_until FROM vehicles WHERE status = 6 AND out_until IS NOT NULL AND out_until < %s "
                "AND out_reminded IS NULL", (now.date(),))
    rows = cur.fetchall()
    users = users_with("vehicles.manage") if rows else []
    for r in rows:
        if users:
            notify("maengel", f"Wieder einsatzbereit? {r['name']}",
                   f"Sollte seit {r['out_until']:%d.%m.%Y} zurück sein" + (f" (Grund: {r['out_reason']})" if r["out_reason"] else "") +
                   ". Bitte Status setzen oder das Datum verlängern.", "/dashboard?open=fahrzeuge", usernames=users)
        cur.execute("UPDATE vehicles SET out_reminded = %s WHERE id = %s", (now.date(), r["id"]))
    conn.commit()
    cur.close()
    conn.close()
    return len(rows)


def check_system_health() -> int:
    """Stündlich: steht im Systemzustand etwas auf Rot, bekommen Admin und Leitung eine Meldung (je Punkt höchstens einmal am Tag)."""
    from core.notify import notify_admins
    from routers.admin_api import compute_health
    try:
        health = compute_health()
    except Exception as e:
        print(f"Systemzustand nicht prüfbar: {e}")
        return 0
    bad = [c for c in health["checks"] if c["status"] == "error"]
    for c in bad:
        notify_admins(f"Systemwarnung: {c['label']}", c["detail"][:300], "/dashboard?open=system", dedupe_hours=24)
    return len(bad)


def build_monthly_report(year: int, month: int):
    """(Betreff, Text) für den angegebenen Monat. Fristen und Mängel sind der Stand von heute."""
    from core.reminders import get_due_items
    start = datetime.date(year, month, 1)
    end = datetime.date(year + (month == 12), month % 12 + 1, 1)
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)

    def one(sql, params=()):
        cur.execute(sql, params)
        row = cur.fetchone()
        return list(row.values())[0] or 0

    period = (start, end)
    real = "date >= %s AND date < %s AND stichwort NOT LIKE '[TEST]%%'"
    missions = one(f"SELECT COUNT(*) FROM missions WHERE {real}", period)
    unfinished = one(f"SELECT COUNT(*) FROM missions WHERE {real} AND status != 'Freigegeben' AND (leader_signature IS NULL OR leader_signature = '')", period)
    mission_hours = one("SELECT SUM(m.duration) FROM mission_attendance a JOIN missions m ON m.id = a.mission_id "
                        f"WHERE m.date >= %s AND m.date < %s AND m.stichwort NOT LIKE '[TEST]%%' AND CAST(a.is_present AS CHAR) NOT IN {_NOT_PRESENT}", period)
    services = one("SELECT COUNT(*) FROM sessions WHERE date >= %s AND date < %s", period)
    service_hours = one("SELECT SUM(s.duration) FROM attendance a JOIN sessions s ON s.id = a.session_id "
                        f"WHERE s.date >= %s AND s.date < %s AND CAST(a.is_present AS CHAR) NOT IN {_NOT_PRESENT}", period)
    cur.execute("SELECT COALESCE(e.name, CONCAT('Fahrzeug ', v.name), 'Unbekannt') AS name, d.status, d.description FROM equipment_defect_reports d "
                "LEFT JOIN equipment e ON e.id = d.equipment_id LEFT JOIN vehicles v ON v.id = d.vehicle_id "
                "WHERE d.status != 'Erledigt' ORDER BY d.created_at")
    defects = cur.fetchall()
    cur.execute("SELECT name, out_reason, out_until FROM vehicles WHERE status = 6 ORDER BY name")
    vehicles_out = cur.fetchall()
    cur.close()
    conn.close()
    try:
        from routers.verein_api import fee_summary
        fees = fee_summary(datetime.date.today().year)
    except Exception as e:
        print(f"Beitragsstand nicht lesbar: {e}")
        fees = None

    due = [i for i in get_due_items() if not i.get("first_check")]
    overdue = [i for i in due if i["overdue"]]
    hours = lambda h: f"{float(h):.1f}".replace(".", ",")
    label = f"{MONTHS[month - 1]} {year}"
    lines = [f"Hallo,", "", f"das war der {label}.", "", "EINSATZ UND DIENST",
             f"Einsätze: {missions}", f"Personenstunden im Einsatz: {hours(mission_hours)}",
             f"Dienste: {services}", f"Personenstunden im Dienst: {hours(service_hours)}",
             f"Einsatzberichte noch offen: {unfinished}", "", "STAND HEUTE",
             f"Offene Mängel: {len(defects)}", f"Überfällige Fristen: {len(overdue)}", f"Bald fällig: {len(due) - len(overdue)}",
             f"Fahrzeuge außer Dienst: {len(vehicles_out)}"]
    if fees and fees["overdue"]:
        lines.append(f"Mitgliedsbeiträge {fees['year']} offen: {fees['open_count']}")
    if vehicles_out:
        lines += ["", "AUSSER DIENST"] + [f"  - {v['name']}" + (f": {v['out_reason']}" if v["out_reason"] else "")
                                          + (f" (bis {v['out_until']:%d.%m.%Y})" if v["out_until"] else "") for v in vehicles_out]
    if defects:
        lines += ["", "OFFENE MÄNGEL"] + [f"  - {d['name']}: {d['description'].splitlines()[0][:100]} ({d['status']})" for d in defects[:10]]
        if len(defects) > 10:
            lines.append(f"  - und {len(defects) - 10} weitere")
    if overdue:
        lines += ["", "ÜBERFÄLLIGE FRISTEN"] + [f"  - {i['name']} ({i['detail']})" for i in overdue[:10]]
        if len(overdue) > 10:
            lines.append(f"  - und {len(overdue) - 10} weitere")
    return f"Monatsbericht {label}", "\n".join(lines)


def previous_month(day: datetime.date):
    last = day.replace(day=1) - datetime.timedelta(days=1)
    return last.year, last.month


def leadership_emails() -> list:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT p.email FROM users u JOIN personnel p ON p.id = u.personnel_id "
                "WHERE u.role IN ('admin', 'leitung') AND p.email IS NOT NULL AND p.email != ''")
    emails = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return emails


def run_monthly_report(now: datetime.datetime = None) -> int:
    """Einmal je Monat in den ersten sieben Tagen (ab 7 Uhr) an Admin und Leitung, wenn eingeschaltet.
    Die Wochen-Grenze verhindert, dass beim Einschalten mitten im Monat sofort etwas rausgeht."""
    now = now or datetime.datetime.now()
    marker = now.year * 100 + now.month
    if now.day > 7 or now.hour < 7 or get_int_setting("last_monthly_report", 0, ttl=0) == marker:
        return 0
    settings = get_mail_settings()
    if not (is_mail_configured(settings) and settings.get("monthly_report")):
        return 0
    recipients = leadership_emails()
    if not recipients:
        return 0
    set_int_setting("last_monthly_report", marker)
    subject, body = build_monthly_report(*previous_month(now.date()))
    base = (settings.get("app_url") or "").rstrip("/")
    if base:
        body += f"\n\nStatistiken im Dienstbuch öffnen: {base}/dashboard"
    send_mail_background(recipients, subject, body, bcc=True)
    return len(recipients)

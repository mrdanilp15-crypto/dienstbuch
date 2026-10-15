import datetime

from core.mailer import get_mail_settings, is_mail_configured, send_mail_background
from database import get_db_connection

_WEEKDAYS = ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"]


def _fmt_day(d: datetime.date) -> str:
    return f"{_WEEKDAYS[d.weekday()]}, {d.strftime('%d.%m.%Y')}"


def _recipients(cur, group_id):
    """Aktive Mitglieder, die zum Termin passen (Jugend-Termine -> Jugendmitglieder), mit Login-Name und E-Mail."""
    youth = False
    if group_id:
        cur.execute("SELECT name FROM groups_table WHERE id = %s", (group_id,))
        g = cur.fetchone()
        youth = bool(g and "jugend" in (g["name"] or "").lower())
    wanted = "Jugend" if youth else "Aktiv"
    cur.execute("""
        SELECT p.id, p.name, p.email, u.username FROM personnel p
        LEFT JOIN users u ON u.personnel_id = p.id
        WHERE p.membership_status = %s OR (%s = 'Aktiv' AND p.membership_status IS NULL)
    """, (wanted, wanted))
    return cur.fetchall()


def _send(recipients, title: str, body: str):
    """Push an die Geraete der Empfaenger, dazu E-Mail (nur wenn die Wehr den Versand eingerichtet hat)."""
    if not recipients:
        return
    usernames = sorted({r["username"] for r in recipients if r.get("username")})
    try:
        from routers.push_api import send_push_to_all
        if usernames:
            send_push_to_all({"title": title, "body": body, "icon": "/static/favicon.png", "url": "/dashboard"}, usernames=usernames)
    except Exception as e:
        print(f"Termin-Push fehlgeschlagen: {e}")
    try:
        s = get_mail_settings()
        if is_mail_configured(s) and s.get("reminder_mail"):
            emails = [r["email"] for r in recipients if r.get("email")]
            text = body + ("\n\nDienstbuch: " + s["app_url"] + "/dashboard" if s.get("app_url") else "")
            send_mail_background(emails, title, text, bcc=True)
    except Exception as e:
        print(f"Termin-Mail fehlgeschlagen: {e}")


def notify_new_schedule(title: str, typ: str, time_str: str, dates: list, group_id):
    try:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        rec = _recipients(cur, group_id)
        cur.close()
        conn.close()
        if len(dates) == 1:
            head = f"Neuer Termin: {title}"
            body = f"{_fmt_day(dates[0])}, {time_str} Uhr ({typ}). Bitte im Dienstbuch zu- oder absagen."
        else:
            head = f"Neue Terminserie: {title}"
            body = f"{len(dates)} Termine ab {_fmt_day(dates[0])}, {time_str} Uhr ({typ}). Bitte im Dienstbuch zu- oder absagen."
        _send(rec, head, body)
    except Exception as e:
        print(f"Benachrichtigung zum neuen Termin fehlgeschlagen: {e}")


def _parse_time(value) -> datetime.time:
    if isinstance(value, datetime.timedelta):
        secs = int(value.total_seconds())
        return datetime.time(secs // 3600 % 24, secs % 3600 // 60)
    try:
        h, m = str(value).split(":")[:2]
        return datetime.time(int(h), int(m))
    except Exception:
        return datetime.time(19, 0)


def check_schedule_reminders(now: datetime.datetime = None) -> int:
    """Erinnert 24 Stunden vor einem Termin: wer noch nicht geantwortet hat, wird um Rueckmeldung
    gebeten, wer zu-/vielleicht-gesagt hat, bekommt eine Erinnerung. Wer abgesagt hat, nicht.
    Laeuft stuendlich, versendet aber nur tagsueber (8-21 Uhr), jeder Termin hoechstens einmal."""
    now = now or datetime.datetime.now()
    if not (8 <= now.hour < 21):
        return 0
    sent = 0
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT * FROM schedules WHERE reminder_sent = 0 AND date BETWEEN %s AND %s",
        (now.date(), now.date() + datetime.timedelta(days=1)),
    )
    for s in cur.fetchall():
        start = datetime.datetime.combine(s["date"], _parse_time(s["time"]))
        delta = start - now
        if delta > datetime.timedelta(hours=24):
            continue
        if delta > datetime.timedelta(0):
            rec = _recipients(cur, s["group_id"])
            cur.execute("SELECT personnel_id, status FROM schedule_rsvp WHERE schedule_id = %s", (s["id"],))
            answers = {r["personnel_id"]: r["status"] for r in cur.fetchall()}
            when = f"{_fmt_day(s['date'])}, {_parse_time(s['time']).strftime('%H:%M')} Uhr"
            _send([r for r in rec if r["id"] not in answers],
                  f"Rückmeldung fehlt: {s['title']}", f"{when}. Du hast noch nicht zu- oder abgesagt - bitte kurz im Dienstbuch zurückmelden.")
            _send([r for r in rec if answers.get(r["id"]) in ("Ja", "Vielleicht")],
                  f"Erinnerung: {s['title']}", f"{when} - du hast zugesagt.")
            sent += 1
        # auch bereits begonnene Termine als erledigt markieren, damit nichts nachtraeglich verschickt wird
        cur.execute("UPDATE schedules SET reminder_sent = 1 WHERE id = %s", (s["id"],))
    conn.commit()
    cur.close()
    conn.close()
    return sent

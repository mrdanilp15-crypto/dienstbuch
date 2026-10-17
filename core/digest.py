"""Tageszusammenfassung per Mail: wer sie in der Glocke einschaltet, bekommt statt einer Mail je Termin/Frist
einmal täglich (ab 7 Uhr) eine Übersicht der noch ungelesenen Meldungen der letzten 24 Stunden.
Einsatzalarme und Sicherheitsmeldungen bleiben davon unberührt und kommen sofort."""
import datetime
import time

from core.mailer import get_mail_settings, is_mail_configured, send_mail_background
from core.security import get_int_setting, set_int_setting
from database import get_db_connection

DIGEST_HOUR = 7
_DIGEST_CATEGORIES = ("termine", "fristen", "verwaltung", "system")

_cache = {"at": 0.0, "users": set(), "emails": set()}


def _load():
    if time.time() - _cache["at"] < 30:
        return
    users, emails = set(), set()
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT ns.username, p.email FROM notification_settings ns
            LEFT JOIN users u ON u.username = ns.username LEFT JOIN personnel p ON p.id = u.personnel_id
            WHERE ns.digest_mail = 1
        """)
        for username, email in cur.fetchall():
            users.add(username)
            if email:
                emails.add(email)
        cur.close()
        conn.close()
    except Exception:
        pass
    _cache.update(at=time.time(), users=users, emails=emails)


def digest_usernames() -> set:
    _load()
    return _cache["users"]


def digest_emails() -> set:
    _load()
    return _cache["emails"]


def run_daily_digest(force: bool = False, now=None) -> int:
    """Verschickt die Zusammenfassung, wenn es nach 7 Uhr ist und heute noch keine lief. Gibt die Zahl der Mails zurück."""
    now = now or datetime.datetime.now()
    today = int(now.strftime("%Y%m%d"))
    if not force and (now.hour < DIGEST_HOUR or get_int_setting("last_digest_date", 0, ttl=0) == today):
        return 0
    settings = get_mail_settings()
    if not is_mail_configured(settings):
        return 0
    set_int_setting("last_digest_date", today)
    base = (settings.get("app_url") or "").rstrip("/")
    marks = ", ".join(["%s"] * len(_DIGEST_CATEGORIES))
    sent = 0
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT ns.username, p.email FROM notification_settings ns
        JOIN users u ON u.username = ns.username JOIN personnel p ON p.id = u.personnel_id
        WHERE ns.digest_mail = 1 AND p.email IS NOT NULL AND p.email <> ''
    """)
    for person in cur.fetchall():
        cur.execute(
            f"SELECT category, title, body FROM notifications WHERE username = %s AND read_at IS NULL "
            f"AND category IN ({marks}) AND created_at > NOW() - INTERVAL 24 HOUR ORDER BY category, id",
            (person["username"], *_DIGEST_CATEGORIES),
        )
        rows = cur.fetchall()
        if not rows:
            continue
        from core.notify import CATEGORIES
        lines = ["Guten Morgen,", "", "das ist deine Zusammenfassung aus dem Dienstbuch:", ""]
        last = None
        for r in rows:
            if r["category"] != last:
                last = r["category"]
                lines += [CATEGORIES.get(last, (last,))[0].upper()]
            lines.append(f"  • {r['title']}" + (f" - {r['body'].splitlines()[0]}" if r["body"] else ""))
        lines += ["", f"Alles ansehen: {base}/dashboard" if base else "", "",
                  "Diese Zusammenfassung kannst du in der Glocke (Zahnrad) wieder abschalten."]
        send_mail_background([person["email"]], f"Dienstbuch: {len(rows)} Neuigkeit(en) für dich", "\n".join(lines))
        sent += 1
    cur.close()
    conn.close()
    return sent

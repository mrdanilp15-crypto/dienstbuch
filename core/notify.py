"""Zentrale Benachrichtigungen (Glocke im Dashboard).

Jede Benachrichtigung wird pro Person in der Tabelle `notifications` abgelegt und - je nach
Einstellung der Person - zusaetzlich als Push aufs Handy geschickt. Die Glocke sortiert sie in
Kategorien (Fenster), die nur erscheinen, wenn es Eintraege gibt.
"""
import datetime

from database import get_db_connection

# key -> (Anzeigename, Font-Awesome-Icon). Reihenfolge = Reihenfolge in der Glocke.
CATEGORIES = {
    "einsatz": ("Einsatz", "fa-fire"),
    "termine": ("Termine", "fa-calendar-days"),
    "fristen": ("Fristen", "fa-clock"),
    "verwaltung": ("Verwaltung", "fa-clipboard-list"),
    "system": ("System", "fa-gear"),
}
# Einsatzalarme kommen immer als Push - das laesst sich nicht abschalten.
ALWAYS_PUSH = {"einsatz"}
RETENTION_DAYS = 60


def _users(cur, usernames=None, roles=None):
    if usernames is not None:
        return sorted({u for u in usernames if u})
    if roles:
        cur.execute("SELECT username FROM users WHERE role IN (%s)" % ", ".join(["%s"] * len(roles)), tuple(roles))
    else:
        cur.execute("SELECT username FROM users")
    return sorted({r["username"] for r in cur.fetchall()})


def notify(category, title, body="", link=None, usernames=None, roles=None, push=True, dedupe_hours=None):
    """Legt die Benachrichtigung fuer die Empfaenger an (usernames > roles > alle) und schickt Push.
    dedupe_hours: gleiche Kategorie + gleicher Titel innerhalb dieser Zeit -> nichts doppelt.
    Gibt die Anzahl der Empfaenger zurueck; Fehler brechen den Aufrufer nie ab."""
    if category not in CATEGORIES:
        category = "system"
    try:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        users = _users(cur, usernames, roles)
        if not users:
            cur.close(); conn.close()
            return 0
        if dedupe_hours:
            cur.execute(
                "SELECT 1 FROM notifications WHERE category=%s AND title=%s AND created_at > NOW() - INTERVAL %s HOUR LIMIT 1",
                (category, title[:200], int(dedupe_hours)),
            )
            if cur.fetchone():
                cur.close(); conn.close()
                return 0
        cur.executemany(
            "INSERT INTO notifications (username, category, title, body, link) VALUES (%s,%s,%s,%s,%s)",
            [(u, category, title[:200], (body or "")[:1000], link) for u in users],
        )
        cur.execute("DELETE FROM notifications WHERE created_at < NOW() - INTERVAL %s DAY", (RETENTION_DAYS,))
        push_users = users
        if push and category not in ALWAYS_PUSH:
            cur.execute("SELECT username FROM notification_prefs WHERE category=%s AND push=0", (category,))
            muted = {r["username"] for r in cur.fetchall()}
            push_users = [u for u in users if u not in muted]
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        print(f"Benachrichtigung fehlgeschlagen: {e}")
        return 0
    if push and push_users:
        try:
            from routers.push_api import send_push_to_all
            send_push_to_all({"title": title, "body": body or "", "icon": "/static/favicon.png", "url": link or "/dashboard"}, usernames=push_users)
        except Exception as e:
            print(f"Push fehlgeschlagen: {e}")
    return len(users)


def notify_admins(title, body="", link=None, dedupe_hours=24):
    """Systemhinweis (Sicherung, Mailversand ...) an Admin + Leitung - einmal pro Tag und Titel."""
    return notify("system", title, body, link, roles=["admin", "leitung"], dedupe_hours=dedupe_hours)

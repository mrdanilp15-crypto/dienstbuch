"""Kleines Fehlerprotokoll für den Systemzustand (fehlgeschlagene Mails und Push-Nachrichten).

Bewusst schlank: nur Art, Text und Zeitpunkt, 30 Tage aufbewahrt. Fehler beim Protokollieren brechen nie den Aufrufer ab.
"""
import datetime

from database import get_db_connection

KEEP_DAYS = 30
STARTED_AT = datetime.datetime.now()  # Start der App (für „läuft seit“)


def ensure_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS system_events (
            id INT AUTO_INCREMENT PRIMARY KEY,
            kind VARCHAR(30) NOT NULL,
            detail VARCHAR(500) NOT NULL DEFAULT '',
            created_at DATETIME NOT NULL,
            INDEX (kind, created_at)
        ) ENGINE=InnoDB
    """)


def record(kind: str, detail: str = ""):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("INSERT INTO system_events (kind, detail, created_at) VALUES (%s, %s, NOW())", (kind[:30], (detail or "")[:500]))
        cur.execute("DELETE FROM system_events WHERE created_at < NOW() - INTERVAL %s DAY", (KEEP_DAYS,))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Systemereignis nicht gespeichert ({kind}): {e}")


def recent(cur, kind: str, days: int = 7):
    """(Anzahl, letzter Eintrag oder None) der letzten `days` Tage."""
    cur.execute("SELECT COUNT(*) AS n FROM system_events WHERE kind = %s AND created_at >= NOW() - INTERVAL %s DAY", (kind, days))
    n = cur.fetchone()["n"]
    cur.execute("SELECT detail, created_at FROM system_events WHERE kind = %s ORDER BY id DESC LIMIT 1", (kind,))
    return n, cur.fetchone()

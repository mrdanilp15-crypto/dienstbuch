"""Aufbewahrung und Löschen (Datenschutz): was automatisch verschwindet und was die Wehrführung entscheidet.

Automatisch, täglich:
  - Sicherheits-/Änderungsprotokoll älter als `retention_audit_days` (Standard 365; 0 = nie löschen)
  - benutzte oder abgelaufene Passwort-Links, bekannte Geräte ohne Anmeldung seit 180 Tagen,
    Benachrichtigungen (60 Tage, siehe core/notify.py), erledigte Rückmelde-Erinnerungen
Nie automatisch: Personen. Ausgeschiedene Mitglieder werden ab `retention_ex_member_years` (Standard 5) als
Vorschlag angezeigt; die Wehrführung entscheidet pro Person, ob der Personenbezug entfernt wird (Anonymisierung).
Dienstzeiten, Einsatz- und Atemschutzprotokolle bleiben dabei als anonymisierte Statistik erhalten, weil dafür
gesetzliche Aufbewahrungspflichten gelten.
"""
import datetime

from core.security import get_int_setting, set_int_setting
from database import get_db_connection

DEFAULT_AUDIT_DAYS = 365
DEFAULT_EX_MEMBER_YEARS = 5


def run_retention(force: bool = False, now=None) -> dict:
    now = now or datetime.datetime.now()
    today = int(now.strftime("%Y%m%d"))
    if not force and get_int_setting("last_retention_date", 0, ttl=0) == today:
        return {}
    set_int_setting("last_retention_date", today)
    out = {}
    conn = get_db_connection()
    cur = conn.cursor()
    days = get_int_setting("retention_audit_days", DEFAULT_AUDIT_DAYS, ttl=0)
    if days > 0:
        cur.execute("DELETE FROM audit_log WHERE created_at < NOW() - INTERVAL %s DAY", (days,))
        out["audit_log"] = cur.rowcount
    cur.execute("DELETE FROM password_resets WHERE (used = 1 OR expires_at < NOW()) AND created_at < NOW() - INTERVAL 7 DAY")
    out["password_resets"] = cur.rowcount
    cur.execute("DELETE FROM login_devices WHERE last_seen < NOW() - INTERVAL 180 DAY")
    out["login_devices"] = cur.rowcount
    cur.execute("DELETE FROM notifications WHERE created_at < NOW() - INTERVAL 60 DAY")
    out["notifications"] = cur.rowcount
    cur.execute("DELETE FROM schedule_nudges WHERE schedule_id NOT IN (SELECT id FROM schedules)")
    out["schedule_nudges"] = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    cands = ex_member_candidates()
    if cands:
        from core.notify import notify
        notify("verwaltung", "Datenschutz: ausgeschiedene Mitglieder prüfen",
               f"{len(cands)} ausgeschiedene Mitglieder liegen über der Aufbewahrungsfrist. Unter Einstellungen → Sicherheit & Datenschutz kann der Personenbezug entfernt werden.",
               "/dashboard", roles=["admin"], push=False, dedupe_hours=24 * 30)
    return out


def ex_member_candidates():
    years = get_int_setting("retention_ex_member_years", DEFAULT_EX_MEMBER_YEARS, ttl=0)
    if years <= 0:
        return []
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT id, name, exit_date FROM personnel
        WHERE membership_status = 'Ausgeschieden' AND name NOT LIKE 'Ehemaliges Mitglied #%%'
          AND (exit_date IS NULL OR exit_date < CURDATE() - INTERVAL %s YEAR)
        ORDER BY (exit_date IS NULL), exit_date, name
    """, (years,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{"id": r["id"], "name": r["name"], "exit_date": r["exit_date"].strftime("%d.%m.%Y") if r["exit_date"] else None} for r in rows]


# Direkte Identifikationsmerkmale; Qualifikationen (Häkchen) und Dienstzeiten bleiben ohne Namen als Statistik
_CLEAR = ("phone", "email", "address", "badge_number", "birth_date", "honors", "profile_picture", "notes", "skills", "parent_contact",
          "emergency_contact_name", "emergency_contact_phone", "g26_3_date", "belastungslauf_date", "unterweisung_date",
          "dsgvo_ack_date", "dienstordnung_ack_date", "license_checked_date")
_DELETE_ROWS = ("employer_certificates", "personnel_availability", "schedule_rsvp", "schedule_audience", "schedule_nudges", "schedule_attendance")


def anonymize_person(personnel_id: int) -> str:
    """Entfernt den Personenbezug eines AUSGESCHIEDENEN Mitglieds. Nicht umkehrbar. Gibt den neuen Platzhalter-Namen zurück."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, membership_status FROM personnel WHERE id = %s", (personnel_id,))
    p = cur.fetchone()
    if not p:
        cur.close(); conn.close()
        raise LookupError("Person nicht gefunden.")
    if p["membership_status"] != "Ausgeschieden":
        cur.close(); conn.close()
        raise PermissionError("Nur ausgeschiedene Mitglieder können anonymisiert werden.")
    placeholder = f"Ehemaliges Mitglied #{personnel_id}"
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'personnel'")
    existing = {r["column_name"] for r in cur.fetchall()}
    sets = ["name = %s"] + [f"`{c}` = NULL" for c in _CLEAR if c in existing]
    cur.execute(f"UPDATE personnel SET {', '.join(sets)} WHERE id = %s", (placeholder, personnel_id))
    for t in _DELETE_ROWS:
        try:
            cur.execute(f"DELETE FROM `{t}` WHERE personnel_id = %s", (personnel_id,))
        except Exception:
            pass  # Tabelle existiert in dieser Installation nicht
    cur.execute("SELECT username FROM users WHERE personnel_id = %s", (personnel_id,))
    names = [r["username"] for r in cur.fetchall()]
    for u in names:
        for tbl in ("login_devices", "notifications", "notification_prefs", "notification_settings"):
            cur.execute(f"DELETE FROM `{tbl}` WHERE username = %s", (u,))
    cur.execute("DELETE FROM users WHERE personnel_id = %s", (personnel_id,))
    conn.commit()
    cur.close()
    conn.close()
    return placeholder

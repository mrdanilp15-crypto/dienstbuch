import base64
import hashlib
import smtplib
import ssl
import threading
from email.message import EmailMessage
from email.utils import formataddr

from cryptography.fernet import Fernet, InvalidToken

from core.utils import SECRET_KEY
from database import get_db_connection

# Jede Wehr trägt ihr eigenes Absender-Postfach in der Verwaltung ein (Tabelle mail_settings,
# genau eine Zeile mit id=1). Das SMTP-Passwort liegt verschlüsselt in der DB, damit es in
# Datenbank-Sicherungen nicht im Klartext steht.


def _fernet() -> Fernet:
    key = base64.urlsafe_b64encode(hashlib.sha256(("smtp-password:" + SECRET_KEY).encode()).digest())
    return Fernet(key)


def encrypt_secret(value: str) -> str:
    return _fernet().encrypt(value.encode()).decode()


def decrypt_secret(value: str) -> str:
    if not value:
        return ""
    try:
        return _fernet().decrypt(value.encode()).decode()
    except InvalidToken:
        # SECRET_KEY wurde seit dem Speichern geändert - Passwort muss in der Verwaltung neu eingegeben werden.
        return ""


def get_mail_settings() -> dict:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM mail_settings WHERE id = 1")
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row or {}


def is_mail_configured(settings: dict = None) -> bool:
    s = settings if settings is not None else get_mail_settings()
    return bool(s.get("enabled") and s.get("smtp_host") and s.get("sender_address"))


def _one_line(text: str) -> str:
    # Header-Injection verhindern: Betreff kann z.B. ein Alarm-Stichwort von außen enthalten
    return " ".join((text or "").split())


def send_mail(recipients, subject: str, body: str, bcc: bool = False, settings: dict = None) -> int:
    """Verschickt eine Klartext-Mail. bcc=True versteckt die Empfänger voreinander
    (Rundmails an die ganze Wehr). Wirft bei SMTP-Fehlern - Aufrufer entscheidet, ob melden oder loggen."""
    s = settings if settings is not None else get_mail_settings()
    if not is_mail_configured(s):
        raise RuntimeError("E-Mail-Versand ist nicht eingerichtet.")
    to = sorted({r.strip() for r in recipients if r and "@" in r})
    if not to:
        return 0

    msg = EmailMessage()
    msg["Subject"] = _one_line(subject)
    msg["From"] = formataddr((_one_line(s.get("sender_name")) or "Dienstbuch", s["sender_address"]))
    msg["To"] = s["sender_address"] if bcc else ", ".join(to)
    msg.set_content(body)

    port = int(s.get("smtp_port") or 587)
    security = s.get("smtp_security") or "starttls"
    ctx = ssl.create_default_context()
    if security == "ssl":
        server = smtplib.SMTP_SSL(s["smtp_host"], port, context=ctx, timeout=20)
    else:
        server = smtplib.SMTP(s["smtp_host"], port, timeout=20)
        if security == "starttls":
            server.starttls(context=ctx)
    try:
        if s.get("smtp_user"):
            server.login(s["smtp_user"], decrypt_secret(s.get("smtp_password_enc")))
        server.send_message(msg, to_addrs=to)
    finally:
        try:
            server.quit()
        except Exception:
            pass
    return len(to)


def record_mail_result(error):
    """Merkt sich den letzten Versandfehler (None = zuletzt erfolgreich), damit Admins im
    Systemzustand sehen, wenn z.B. "Passwort vergessen" still ins Leere laeuft."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        if error is None:
            cur.execute("UPDATE mail_settings SET last_error = NULL, last_error_at = NULL WHERE id = 1")
        else:
            cur.execute("UPDATE mail_settings SET last_error = %s, last_error_at = NOW() WHERE id = 1", (str(error)[:500],))
        conn.commit()
        cur.close()
        conn.close()
        if error is not None:
            from core.notify import notify_admins
            notify_admins("E-Mail-Versand fehlgeschlagen", str(error)[:300])
    except Exception as e:
        print(f"Mail-Status konnte nicht gespeichert werden: {e}")


def send_mail_background(recipients, subject: str, body: str, bcc: bool = False):
    """Für Alarme/Erinnerungen: ein langsamer oder hängender Mailserver darf die eigentliche
    Verarbeitung (Alarm anlegen, Push senden) nicht ausbremsen."""
    recipients = list(recipients)

    def run():
        try:
            if is_mail_configured():
                send_mail(recipients, subject, body, bcc=bcc)
                record_mail_result(None)
        except Exception as e:
            print(f"E-Mail-Versand fehlgeschlagen ({subject}): {e}")
            record_mail_result(f"{subject}: {e}")

    threading.Thread(target=run, daemon=True).start()


def get_active_member_emails() -> list:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT email FROM personnel WHERE email IS NOT NULL AND email != '' AND (membership_status IS NULL OR membership_status != 'Ausgeschieden')")
    emails = [r["email"] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return emails


def send_alarm_mail(title: str, stichwort: str, adresse: str, meldung: str):
    """Ersatzweg für Alarme, falls Web-Push auf einem Handy nicht ankommt - nur wenn in der
    Verwaltung "Alarme auch per E-Mail" aktiviert ist. Empfänger per BCC (keine Adressliste sichtbar)."""
    try:
        s = get_mail_settings()
        if not (is_mail_configured(s) and s.get("alarm_mail")):
            return
        body = f"{title}\n\nStichwort: {stichwort}\nOrt: {adresse}\nMeldung: {meldung}\n"
        if s.get("app_url"):
            body += f"\nAlarm im Dienstbuch öffnen: {s['app_url']}/dashboard?open=alarm\n"
        send_mail_background(get_active_member_emails(), f"{title}: {stichwort}", body, bcc=True)
    except Exception as e:
        print(f"Alarm-Mail fehlgeschlagen: {e}")


def _due_lines(items):
    """Fristen als Text, nach Bereich gegliedert; Erstkontrollen getrennt (kein Verzug)."""
    from core.reminders import AREA_LABEL, AREA_ORDER
    lines = []
    real = [i for i in items if not i.get("first_check")]
    for area in AREA_ORDER:
        part = [i for i in real if i["area"] == area]
        if not part:
            continue
        lines.append(AREA_LABEL[area].upper())
        for it in part:
            due = it["due_date"].split("-")
            status = "ÜBERFÄLLIG" if it["overdue"] else f"fällig am {due[2]}.{due[1]}.{due[0]}"
            lines.append(f"  - {it['name']} ({it['detail']}): {status}")
        lines.append("")
    first = [i for i in items if i.get("first_check")]
    if first:
        lines.append("ERSTKONTROLLE NOCH OFFEN (noch nie dokumentiert)")
        lines += [f"  - {it['name']} ({it['detail'].replace(' (noch nie erfasst)', '')})" for it in first]
    return lines


def send_reminder_mails(items: list, summary_lines: list = None):
    """Fristen-Erinnerung nach Zuständigkeit: Admin/Leitung bekommen alles, der Gerätewart nur Technik, der
    Atemschutzwart nur G26.3/Atemschutz-Lehrgänge - und jede Person ihre eigenen persönlichen Fristen."""
    try:
        s = get_mail_settings()
        if not (is_mail_configured(s) and s.get("reminder_mail")):
            return
        from core.reminders import OWN_TYPES, visible_items
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        mails_by_role = {}
        for role in ("admin", "leitung", "geratewart", "atemschutzwart"):
            cur.execute("""
                SELECT DISTINCT p.email FROM users u JOIN personnel p ON p.id = u.personnel_id
                WHERE u.role = %s AND p.email IS NOT NULL AND p.email != ''
            """, (role,))
            mails_by_role[role] = [r["email"] for r in cur.fetchall()]
        cur.execute("SELECT id, email FROM personnel WHERE email IS NOT NULL AND email != ''")
        email_by_pid = {r["id"]: r["email"] for r in cur.fetchall()}
        cur.close()
        conn.close()

        sent_to = set()
        for role in ("admin", "leitung", "geratewart", "atemschutzwart"):
            part = visible_items(items, role)
            recipients = [e for e in mails_by_role[role] if e not in sent_to]
            if part and recipients:
                send_mail_background(recipients, "Anstehende Prüf- und Fälligkeitsfristen",
                                     "Folgende Fristen in deinem Bereich sind in Kürze fällig oder bereits überfällig:\n\n" + "\n".join(_due_lines(part)), bcc=True)
                sent_to.update(recipients)

        personal = {}
        for it in items:
            if it["type"] in OWN_TYPES and not it.get("first_check") and it.get("personnel_id") in email_by_pid:
                due = it["due_date"].split("-")
                status = "ÜBERFÄLLIG" if it["overdue"] else f"fällig am {due[2]}.{due[1]}.{due[0]}"
                personal.setdefault(email_by_pid[it["personnel_id"]], []).append(f"- {it['detail']}: {status}")
            elif it["type"] == "custom" and not it.get("first_check"):  # eigene Fristarten: die betroffene Person und die zuständige Person
                due = it["due_date"].split("-")
                status = "ÜBERFÄLLIG" if it["overdue"] else f"fällig am {due[2]}.{due[1]}.{due[0]}"
                for pid in {it.get("personnel_id"), it.get("responsible_pid")} - {None}:
                    if pid in email_by_pid:
                        personal.setdefault(email_by_pid[pid], []).append(f"- {it['name']} ({it['detail']}): {status}")
        from core.digest import digest_emails
        skip = digest_emails()
        for email, lines in personal.items():
            if email in skip or email in sent_to:
                continue  # Tageszusammenfassung gewählt bzw. steht schon in der Übersicht oben
            send_mail_background([email], "Deine anstehenden Fristen im Dienstbuch",
                                 "Hallo,\n\nfür dich stehen folgende Fristen an:\n\n" + "\n".join(lines) +
                                 "\n\nBitte kümmere dich rechtzeitig um einen Termin.")
    except Exception as e:
        print(f"Erinnerungs-Mails fehlgeschlagen: {e}")

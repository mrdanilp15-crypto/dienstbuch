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


def send_mail_background(recipients, subject: str, body: str, bcc: bool = False):
    """Für Alarme/Erinnerungen: ein langsamer oder hängender Mailserver darf die eigentliche
    Verarbeitung (Alarm anlegen, Push senden) nicht ausbremsen."""
    recipients = list(recipients)

    def run():
        try:
            if is_mail_configured():
                send_mail(recipients, subject, body, bcc=bcc)
        except Exception as e:
            print(f"E-Mail-Versand fehlgeschlagen ({subject}): {e}")

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
            body += f"\nDienstbuch öffnen: {s['app_url']}/dashboard\n"
        send_mail_background(get_active_member_emails(), f"{title}: {stichwort}", body, bcc=True)
    except Exception as e:
        print(f"Alarm-Mail fehlgeschlagen: {e}")


def send_reminder_mails(items: list, summary_lines: list):
    """Fristen-Erinnerung: Zusammenfassung an Admins/Leitung, dazu jedem Kameraden seine
    eigenen persönlichen Fristen (G26.3, Lehrgänge)."""
    try:
        s = get_mail_settings()
        if not (is_mail_configured(s) and s.get("reminder_mail")):
            return
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT DISTINCT p.email FROM users u JOIN personnel p ON p.id = u.personnel_id
            WHERE u.role IN ('admin', 'leitung') AND p.email IS NOT NULL AND p.email != ''
        """)
        leaders = [r["email"] for r in cur.fetchall()]
        cur.execute("SELECT name, email FROM personnel WHERE email IS NOT NULL AND email != ''")
        email_by_name = {r["name"]: r["email"] for r in cur.fetchall()}
        cur.close()
        conn.close()

        if leaders:
            send_mail_background(leaders, "Anstehende Prüf- und Fälligkeitsfristen",
                                 "Folgende Fristen sind in Kürze fällig oder bereits überfällig:\n\n" + "\n".join(summary_lines), bcc=True)

        personal = {}
        for it in items:
            if it["type"] in ("g26", "lehrgang") and it["name"] in email_by_name:
                due = it["due_date"].split("-")
                status = "ÜBERFÄLLIG" if it["overdue"] else f"fällig am {due[2]}.{due[1]}.{due[0]}"
                personal.setdefault(email_by_name[it["name"]], []).append(f"- {it['detail']}: {status}")
        for email, lines in personal.items():
            send_mail_background([email], "Deine anstehenden Fristen im Dienstbuch",
                                 "Hallo,\n\nfür dich stehen folgende Fristen an:\n\n" + "\n".join(lines) +
                                 "\n\nBitte kümmere dich rechtzeitig um einen Termin.")
    except Exception as e:
        print(f"Erinnerungs-Mails fehlgeschlagen: {e}")

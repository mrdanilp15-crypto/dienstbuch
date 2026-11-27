"""HTML-Mails: Gestaltung, Maskierung, Klartext-Alternative (ohne Datenbank, ohne Server)."""
import sys
from email.message import EmailMessage

sys.path.insert(0, ".")
from core.mail_html import render_html  # noqa: E402

fails = []


def check(name, cond, extra=""):
    print(("OK    " if cond else "FEHLER"), name, extra)
    if not cond:
        fails.append(name)


alarm = render_html("ALARM: F2 <b>Wohnungsbrand</b>", "Stichwort: F2\nOrt: Hauptstr. 1\nMeldung: Rauch\n\nAlarm im Dienstbuch öffnen: https://x.de/dashboard?open=alarm\n", "FF Test", alarm=True)
check("Alarm: rotes Kopfband", "#b71c1c" in alarm and "FF Test" in alarm)
check("Alarm: Betreff wird maskiert", "<b>Wohnungsbrand" not in alarm and "&lt;b&gt;" in alarm)
check("Alarm: Stichwort/Ort/Meldung als Infokasten", alarm.count("color:#6b7280;vertical-align:top;white-space:nowrap") == 3)
check("Alarm: Link wird ein Knopf", "Im Dienstbuch öffnen" in alarm or "Alarm im Dienstbuch öffnen" in alarm)

fr = render_html("Fristen", "Folgende Fristen:\n\nFAHRZEUGE\n  - HLF (TÜV): ÜBERFÄLLIG\n  - LF (HU): fällig am 01.02.2027\n", "FF Test")
check("Fristen: Überschrift und Liste", "letter-spacing:.08em;color:#b71c1c\">FAHRZEUGE" in fr and fr.count("&bull;</td>") == 2)
check("Fristen: überfällig rot hervorgehoben", "<strong style=\"color:#b71c1c\">ÜBERFÄLLIG" in fr)

evil = render_html("x", "Hallo <script>alert(1)</script>\nhttps://ok.de/a?b=1&c=2", "S")
check("Fremder Text kann kein HTML einschleusen", "<script>" not in evil and "&lt;script&gt;" in evil)
check("Adresse bleibt anklickbar", 'href="https://ok.de/a?b=1&amp;c=2"' in evil)

inv = render_html("Dein Zugang", """Dein Benutzername: max

Über diesen Link:
https://x.de/reset-password?token=a

Danach: https://x.de/login""", "S")
check("Einladung: Knopf heißt Passwort festlegen", "Passwort festlegen" in inv and "Im Dienstbuch öffnen" not in inv)
check("Einladung: Benutzername im Infokasten", "Dein Benutzername</td>" in inv)
check("Vorschautext für die Postfachliste", 'display:none;max-height:0' in inv and "Dein Benutzername: max" in inv.split("display:none")[1][:200])
mb = render_html("Monatsbericht", """ZAHLEN
Einsätze: 3
Stunden: 12,5

ERSTKONTROLLE OFFEN (noch nie dokumentiert)
  - A""", "S")
check("Statistik: Zahlen rechtsbündig", mb.count("text-align:right") == 2)
check("Überschrift mit Klammerzusatz erkannt", "ERSTKONTROLLE OFFEN <span" in mb)
hk = render_html("Helfer gesucht: Grillfest", "Hallo", "S")
check("Betreff mit Vorsatz: kleine Zeile + Titel", ">Helfer gesucht</div>" in hk and ">Grillfest</div>" in hk)
check("Kein Umfärben im Dunkelmodus", 'content="light only"' in hk)

msg = EmailMessage()
msg.set_content("Text")
msg.add_alternative(render_html("T", "Text", "S"), subtype="html")
check("Mail hat Text und HTML", msg.get_body(("plain",)) is not None and msg.get_body(("html",)) is not None)

print("\n==> ", "ALLES OK" if not fails else f"{len(fails)} FEHLER: {fails}")
sys.exit(1 if fails else 0)

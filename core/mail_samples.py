"""Muster-Mails: je eine Mail jeder Art an die eigene Adresse, um die Darstellung in Gmail, Outlook und am Handy zu prüfen.

Die Texte sind so aufgebaut wie die echten Mails (gleiche Zeilen, gleiche Links), aber mit erfundenen Angaben und einem
deutlichen Hinweis, dass nichts passiert ist.
"""
import datetime

NOTE = "Muster-Mail zum Prüfen der Darstellung - es ist nichts passiert."


def build_samples(app_url: str, station: str):
    from core.automation import build_monthly_report, previous_month
    url = (app_url or "").rstrip("/")
    day = datetime.date.today() + datetime.timedelta(days=5)
    link = lambda path, label: f"\n\n{label}: {url}{path}" if url else ""
    out = [
        ("Alarm: MUSTER B3 Wohnungsbrand",
         f"Stichwort: MUSTER B3 Wohnungsbrand\nOrt: Hauptstraße 5\nMeldung: {NOTE}\n" + (f"\nAlarm im Dienstbuch öffnen: {url}/dashboard?open=alarm\n" if url else "")),
        ("Helfer gesucht: Grillfest (Muster)",
         f"{NOTE}\n\nTermin: {day:%d.%m.%Y}, 14:00 Uhr\nWir brauchen Leute am Grill und an der Kasse.\nEintragen bis {day:%d.%m.%Y}."
         + link("/dashboard?open=umfragen", "Jetzt eintragen")),
        ("Neuer Termin: Atemschutzübung (Muster)",
         f"{NOTE}\n\nTermin: {day:%d.%m.%Y}, 19:00 Uhr\nArt: Übung\nOrt: Gerätehaus" + link("/dashboard", "Direkt zum Termin")),
        ("Fristen: Fahrzeuge, Geräte & Gerätehaus (Muster)",
         f"Hallo,\n\n{NOTE}\n\nFAHRZEUGE\n  - HLF 20 (TÜV): ÜBERFÄLLIG\n  - LF 10 (Sicherheitsprüfung): fällig am {day:%d.%m.%Y}\n\n"
         f"GERÄTE & MATERIAL\n  - Hydraulikschere (Geräteprüfung): fällig am {day:%d.%m.%Y}" + link("/dashboard?open=fristen", "Fristen ansehen")),
        ("Dein Zugang zum Dienstbuch (muster)",
         f"Hallo Max Mustermann,\n\nfür dich wurde ein Zugang zum Dienstbuch der {station} eingerichtet. ({NOTE})\n\n"
         f"Dein Benutzername: muster\n(Groß-/Kleinschreibung ist beim Anmelden egal.)\n\n"
         f"Über diesen Link legst du dein eigenes Passwort fest (7 Tage gültig, nur einmal nutzbar):\n{url or 'https://beispiel.de'}/reset-password?token=MUSTER\n\n"
         f"Danach meldest du dich hier an: {url or 'https://beispiel.de'}/login"),
    ]
    subject, body = build_monthly_report(*previous_month(datetime.date.today()))
    out.append((subject + " (Muster)", body))
    return out

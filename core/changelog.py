"""Zentrale Liste der Änderungen fürs "Was ist neu"-Fenster (nur Admins, siehe
routers/users_mgr.py: /api/users/me/changelog). Neuen Eintrag ergänzen: an den ANFANG der
Liste einen Block mit einer neuen, fortlaufenden "id" einfügen - diese id (nicht das Datum)
entscheidet, ob ein Admin den Eintrag schon gesehen hat.
"""

CHANGELOG = [
    {
        "id": 7,
        "version": "2.57",
        "date": "2026-10-01",
        "items": [
            {"type": "new", "tag": "Design", "title": "Mehr Farbe und Leben",
             "text": "Jeder Bereich hat seine eigene Akzentfarbe, dazu dezente Bewegungen, ein wärmerer Hintergrund und eine persönliche Begrüßung mit dem nächsten Termin."},
        ],
    },
    {
        "id": 6,
        "version": "2.56",
        "date": "2026-10-01",
        "items": [
            {"type": "new", "tag": "Design", "title": "Ruhigeres Erscheinungsbild",
             "text": "Flachere Karten ohne Leuchteffekte, Rot nur noch für Hauptaktionen, einheitliche Buttons und klarere Schriftgrößen."},
        ],
    },
    {
        "id": 5,
        "version": "2.55",
        "date": "2026-10-01",
        "items": [
            {"type": "new", "tag": "E-Mail", "title": "E-Mail-Versand",
             "text": "Unter Verwaltung > E-Mail-Versand kann jede Wehr ihr eigenes Postfach eintragen. Damit funktionieren „Passwort vergessen“, Fristen-Erinnerungen per Mail und optional Alarme per Mail."},
            {"type": "new", "tag": "Personal", "title": "Lehrgänge in der Qualifikationsmatrix",
             "text": "Die Matrix zeigt jetzt alle Lehrgänge und den G26.3-Status mit Ampel (gültig, läuft bald ab, abgelaufen)."},
            {"type": "new", "tag": "Statistiken", "title": "Mehr Diagramme",
             "text": "Vorjahresvergleich, Einsätze nach Art und Dienststunden nach Kategorie."},
            {"type": "new", "tag": "Kleiderkammer", "title": "Lagerbestand und Rückgaben",
             "text": "Neue Kleiderkammer in der Personalverwaltung: Lagerbestand mit Mindestbestand, Ausgaben buchen automatisch ab, Rückgaben werden vermerkt statt gelöscht. Offene Rückgaben ausgeschiedener Mitglieder sind markiert."},
            {"type": "fix", "tag": "Statistiken", "title": "Jahresauswahl wirkte nicht",
             "text": "Die Einsatz-Diagramme zeigten immer alle Jahre zusammen und zählten Test-Alarme mit."},
        ],
    },
    {
        "id": 4,
        "version": "2.54",
        "date": "2026-10-01",
        "items": [
            {"type": "new", "tag": "Design", "title": "Aufgeräumte Handy-Ansicht",
             "text": "Die Listenauswahl ist auf dem Handy eingeklappt, der Dienstplaner erscheint als Karten statt als breite Tabelle."},
            {"type": "new", "tag": "Design", "title": "Ruhigere Optik",
             "text": "Dezentere Statistik-Kacheln, einheitliche Buttons und abgestimmte Farben."},
            {"type": "fix", "tag": "Login", "title": "Login-Seite blieb stehen",
             "text": "Nach erfolgreicher Anmeldung zeigte der Browser teils weiter die Login-Seite an."},
        ],
    },
    {
        "id": 2,
        "version": "2.52",
        "date": "2026-09-17",
        "items": [
            {"type": "new", "tag": "Sicherheit", "title": "Zwei-Faktor-Authentifizierung",
             "text": "Optional aktivierbar unter „Mein Profil“ - Login dann zusätzlich mit Code aus einer Authenticator-App."},
            {"type": "new", "tag": "Personalakte", "title": "Meine Daten exportieren",
             "text": "Jeder kann jetzt unter „Mein Profil“ alle eigenen gespeicherten Daten als Datei herunterladen."},
            {"type": "new", "tag": "Einsatzberichte", "title": "Ausrück- und Eintreffzeit",
             "text": "Einsätze erfassen jetzt zusätzlich zu Alarm- und Endzeit, wann ausgerückt und wann eingetroffen wurde."},
            {"type": "new", "tag": "Rollen", "title": "Atemschutzwart",
             "text": "Neue Rolle mit Zugriff auf Atemschutz-Einsatzprotokolle, ohne volle Verwaltungsrechte."},
            {"type": "fix", "tag": "Personalverwaltung", "title": "Austritt löschte Einsatzhistorie",
             "text": "Ein gelöschtes Mitglied riss bisher alle eigenen Einträge aus vergangenen Einsatz- und Atemschutzprotokollen mit heraus. Mitglieder mit Historie werden jetzt deaktiviert statt gelöscht."},
            {"type": "fix", "tag": "Jugendfeuerwehr", "title": "Derselbe Fehler bei Jugendmitgliedern",
             "text": "Betraf auch die Anwesenheitshistorie in der Jugendfeuerwehr - gleiche Korrektur."},
            {"type": "fix", "tag": "Sicherheit", "title": "Passwort-Speicherung erneuert",
             "text": "Neue Passwörter werden jetzt mit Argon2id gespeichert (aktueller Sicherheitsstandard), bestehende Konten werden beim nächsten Login automatisch umgestellt."},
        ],
    },
    {
        "id": 1,
        "version": "2.51",
        "date": "2026-09-13",
        "items": [
            {"type": "new", "tag": "Sicherung", "title": "Automatische Datensicherung",
             "text": "Läuft jetzt regelmäßig im Hintergrund von selbst. Keine manuelle Sicherung mehr nötig."},
            {"type": "new", "tag": "Personalakte", "title": "Drei neue Nachweise",
             "text": "Dienstzeitnachweis, Lehrgangsübersicht und Atemschutznachweis – direkt in der Personalakte als PDF abrufbar."},
            {"type": "new", "tag": "Hallen-Display", "title": "Wetter über den eigenen Server",
             "text": "Anzeigegeräte senden dafür keine Daten mehr an einen externen Wetterdienst."},
            {"type": "new", "tag": "Verwaltung", "title": "Sicherungen einsehen",
             "text": "Admins sehen und laden vorhandene Sicherungen jetzt direkt in der App."},
            {"type": "fix", "tag": "Geräte & Material", "title": "QR-Code-Scanner",
             "text": "Erkennt jetzt jeden gescannten Barcode zuverlässig und öffnet direkt das passende Gerät."},
            {"type": "fix", "tag": "Allgemein", "title": "Leere Listen nach dem Neuladen",
             "text": "Einträge bleiben nach F5 sichtbar, ohne dass man erst den Reiter wechseln muss."},
            {"type": "fix", "tag": "Arbeitsbescheinigung", "title": "Fehlende Bescheinigungen",
             "text": "Neu erstellte Bescheinigungen erscheinen sofort in der Liste."},
            {"type": "fix", "tag": "Login", "title": "Hängenbleiben auf dem Handy",
             "text": "Kein schwarzer Bildschirm mehr beim Anmelden."},
            {"type": "fix", "tag": "Verwaltung", "title": "Tote Schaltflächen",
             "text": "PDF-Button und beide „URL kopieren\"-Buttons funktionieren jetzt."},
            {"type": "fix", "tag": "Hallen-Display", "title": "Absturz bei Wetterausfall",
             "text": "Die Anzeige bleibt stabil, wenn der Wetterdienst mal nicht antwortet."},
            {"type": "fix", "tag": "Allgemein", "title": "Stabilität",
             "text": "Zahlreiche kleinere Verbesserungen im Hintergrund."},
        ],
    },
]


def latest_changelog_id() -> int:
    return max((e["id"] for e in CHANGELOG), default=0)

"""Zentrale Liste der Änderungen fürs "Was ist neu"-Fenster (nur Admins, siehe
routers/users_mgr.py: /api/users/me/changelog). Neuen Eintrag ergänzen: an den ANFANG der
Liste einen Block mit einer neuen, fortlaufenden "id" einfügen - diese id (nicht das Datum)
entscheidet, ob ein Admin den Eintrag schon gesehen hat.
"""

CHANGELOG = [
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

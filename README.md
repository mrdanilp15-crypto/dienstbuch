# Digitales Dienstbuch für die Feuerwehr

Ein freies, selbst betreibbares Verwaltungssystem für Freiwillige Feuerwehren: Dienstbuch, Einsatzberichte, Personal, Atemschutz, Fahrzeuge und Material in einer Web-App – ohne Cloud-Zwang, ohne Abo, auf dem eigenen Server.

Entwickelt von **Daniel Hegemann** im Ehrenamt, im Einsatz bei der Feuerwehr Buxheim.

> *Gott zur Ehr, dem Nächsten zur Wehr!*

## Was es kann

**Dienst & Einsatz**
- **Dienstbetrieb:** Übungen und Dienste erfassen, Termine planen, Zu-/Absagen der Kameraden, Jahresberichte als PDF
- **Einsatzberichte:** inklusive Alarm-, Ausrück-, Eintreff- und Endzeit, Fahrzeugbesetzung, digitale Unterschrift, PDF-Export
- **Atemschutz:** Einsatzprotokolle und Überwachung der G26.3-Fristen
- **Alarmierung:** Alarm per Webhook, Push-Benachrichtigung aufs Handy, Hallenmonitor mit Rückmeldungen, Test-Alarme
- **Kalender:** Termine als Kalender-Abo (iCal) für Handy und Outlook

**Personal & Jugend**
- **Personalakte:** Dienstgrade, Qualifikationen, Führerscheine, Lehrgänge mit Gültigkeit, Jubiläen, Verfügbarkeit/Abwesenheit
- **Qualifikationsmatrix:** wer kann was, mit Ampel für ablaufende Lehrgänge und G26.3
- **Kleiderkammer:** Lagerbestand mit Mindestbestand, Ausgabe und Rückgabe je Kamerad
- **Nachweise als PDF:** Dienstzeit, Lehrgänge, Atemschutz, Arbeitsbescheinigung für den Arbeitgeber
- **Jugendfeuerwehr:** eigene Mitglieder- und Anwesenheitsverwaltung

**Fahrzeuge, Material & Gebäude**
- **Fahrzeugpool:** Fahrtenbuch, wöchentliche Kontrollen, TÜV- und Prüffristen
- **Geräte & Material:** Prüffristen, QR-/Barcode-Scan, Mängelmeldungen, Verbrauchsmaterial mit Mindestbestand
- **First Responder / HVO:** Protokolle und Sanitätsmaterial mit Verfallsdatum
- **Lagekarte & Hydranten**, Objekte und Brandmeldeanlagen

**Verwaltung**
- **Statistiken** mit Diagrammen und Vorjahresvergleich
- **Rollen und Rechte:** Admin, Leitung, Gruppenführer, Gerätewart, Atemschutzwart, Jugendwart u. a.
- **E-Mail-Versand** mit dem eigenen Postfach der Wehr: „Passwort vergessen“, Fristen-Erinnerungen, optional Alarm-Mails
- **Automatische Datensicherung**, Aktivitäts-Log, Dokumentenarchiv, Notizbuch, Abrechnung

## Datenschutz und Sicherheit

Das Dienstbuch ist so gebaut, dass es bei der Erfüllung von DSGVO-, GoBD- und Feuerwehr-Vorgaben **unterstützt** (keine Rechtsberatung – die Verantwortung für den Betrieb liegt bei der jeweiligen Wehr):

- **Läuft auf deinem Server:** keine Daten bei Dritten, alle Bibliotheken und Schriften werden selbst ausgeliefert (kein Nachladen von CDNs oder Google Fonts)
- **Passwörter** mit Argon2id gespeichert, optional **Zwei-Faktor-Anmeldung** (Authenticator-App)
- **Auskunft und Datenübertragbarkeit:** jeder kann seine gespeicherten Daten selbst als Datei herunterladen
- **Einsatzhistorie bleibt erhalten:** ausgeschiedene Mitglieder werden deaktiviert statt gelöscht, persönliche Kontaktdaten werden dabei entfernt
- **Nachvollziehbar:** Änderungen werden im Aktivitäts-Log protokolliert

## Installation (Docker / Portainer)

1. Neuen Stack anlegen und die [`docker-compose.yml`](docker-compose.yml) aus diesem Repo verwenden.
2. Unter **Environment variables → Advanced mode** den Inhalt von [`.env.example`](.env.example) einfügen und **eigene** Werte eintragen:
   - `ROOT_PASS` und `DB_PASS`: Datenbank-Passwörter (zufällig, mind. 20 Zeichen)
   - `SECRET_KEY`: zufälliger Schlüssel (mind. 32 Zeichen), z. B. mit `openssl rand -hex 32`
   - `TOWN_NAME`: Name deiner Wehr
   - `STAPEL_PFAD`: Ordner auf dem Server für die Datenbank
3. Stack deployen, Seite öffnen – beim ersten Start wird das Admin-Konto angelegt.
4. Unter **Einstellungen → E-Mail-Versand** das Postfach der Wehr eintragen (optional).

> **Wichtig:** Passwörter und Schlüssel gehören nur in die Umgebungsvariablen des Stacks (oder eine lokale `.env`), **niemals** ins Git-Repo.

Das Image wird bei jedem Update automatisch gebaut: `ghcr.io/mrdanilp15-crypto/dienstbuch:main`. Neue Versionen zeigt das Dienstbuch selbst an, im Fenster „Was ist neu“ stehen die Änderungen.

## Technik

Python (FastAPI) · MariaDB · Vue 3 (ohne Build-Schritt) · Docker. Läuft als installierbare Web-App (PWA) im Hell- und Dunkelmodus, auf Handy und am PC.

## Hinweis zu Push-Alarmen auf Samsung Internet

Für zuverlässigen Alarmempfang (Ton und Bildschirm-Aufwecken) auf dem Handy bitte **Google Chrome** statt Samsung Internet verwenden. Samsung Internet spielt bei Web-Push-Alarmen häufig keinen Ton ab und weckt den Bildschirm nicht – das lässt sich von der App aus nicht beheben.

## Mitmachen und Kontakt

Fehler gefunden oder eine Idee? Gerne als [Issue](../../issues) melden. Du möchtest das Dienstbuch für deine Wehr nutzen und brauchst Hilfe bei Einrichtung oder Betrieb? Schreib mir: <d.hege@icloud.com>

## Lizenz

[MIT](LICENSE) – frei nutzbar und anpassbar.

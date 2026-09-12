from fastapi import APIRouter

router = APIRouter()

# Impressum & Datenschutzerklärung sind bewusst als Konstanten im Quellcode hinterlegt statt
# in der Datenbank/über die Verwaltungsoberfläche änderbar: verantwortlich für den Inhalt nach
# § 5 DDG ist eine konkrete natürliche Person (siehe Nutzer-Chat) - das darf nicht versehentlich
# von einem Admin-Account leergeräumt oder verfälscht werden. Änderungen an diesem Text bitte
# nur direkt im Code vornehmen.

IMPRESSUM_TEXT = """Angaben gemäß § 5 DDG (Digitale-Dienste-Gesetz)

Daniel Hegemann
Tannenwiesen 26
87700 Memmingen
Deutschland

Kontakt
E-Mail: d.hege@icloud.com

Verantwortlich für den Inhalt nach § 18 Abs. 2 MStV
Daniel Hegemann (Anschrift wie oben)

Hinweis zum Betrieb dieser Anwendung
Dieses Digitale Dienstbuch ist eine intern für den Feuerwehrdienst genutzte Anwendung
(Dienstplanung, Anwesenheits- und Einsatzdokumentation). Der eigentliche Funktionsumfang ist
nur nach Anmeldung für berechtigte Feuerwehrangehörige zugänglich; dieses Impressum gilt für
die gesamte technisch erreichbare Anwendung einschließlich der Anmeldeseite.

Streitschlichtung
Die Europäische Kommission stellt eine Plattform zur Online-Streitbeilegung (OS) bereit:
https://ec.europa.eu/consumers/odr/
Ich bin nicht verpflichtet und nicht bereit, an Streitbeilegungsverfahren vor einer
Verbraucherschlichtungsstelle teilzunehmen.

Haftung für Inhalte
Als Diensteanbieter bin ich gemäß § 7 Abs. 1 DDG für eigene Inhalte auf dieser Anwendung nach
den allgemeinen Gesetzen verantwortlich. Nach §§ 8 bis 10 DDG bin ich als Diensteanbieter
jedoch nicht verpflichtet, übermittelte oder gespeicherte fremde Informationen zu überwachen
oder nach Umständen zu forschen, die auf eine rechtswidrige Tätigkeit hinweisen. Verpflichtungen
zur Entfernung oder Sperrung der Nutzung von Informationen nach den allgemeinen Gesetzen bleiben
hiervon unberührt.

Haftung für Links
Diese Anwendung kann Links zu externen Webseiten Dritter enthalten, auf deren Inhalte ich
keinen Einfluss habe. Für diese fremden Inhalte übernehme ich daher keine Gewähr. Für die
Inhalte der verlinkten Seiten ist stets der jeweilige Anbieter oder Betreiber der Seiten
verantwortlich.

Urheberrecht
Die durch mich als Betreiber erstellten Inhalte und Werke auf dieser Anwendung unterliegen dem
deutschen Urheberrecht. Beiträge Dritter sind als solche gekennzeichnet."""


DATENSCHUTZ_TEXT = """Datenschutzerklärung

1. Verantwortlicher
Verantwortlicher im Sinne der Datenschutz-Grundverordnung (DSGVO) ist:

Daniel Hegemann
Tannenwiesen 26
87700 Memmingen
E-Mail: d.hege@icloud.com

2. Übersicht der Verarbeitungen
Diese Anwendung dient der internen Organisation des Feuerwehrdienstes und verarbeitet dafür
folgende Kategorien personenbezogener Daten von Feuerwehrangehörigen:
- Stammdaten (Name, Kontaktdaten, Zuordnung zu Gruppen/Einheiten)
- Zugangsdaten (Benutzername, Passwort ausschließlich als Hash-Wert gespeichert, niemals im
  Klartext)
- Dienst- und Einsatzdokumentation (Teilnahme an Diensten/Übungen/Einsätzen, Zeiten, Kategorie,
  Beschreibung, Einsatzort)
- Elektronisch geleistete Unterschriften (als Bilddatei) zur Bestätigung von Diensteinträgen,
  Einsatzberichten und Arbeitgeberbescheinigungen
- Qualifikations- und Eignungsdaten (z. B. Lehrgänge, Ablaufdaten arbeitsmedizinischer
  Vorsorgeuntersuchungen wie G26.3) zur Einsatzplanung im Atemschutz; es werden dabei nur
  Gültigkeits-/Ablaufdaten verarbeitet, keine medizinischen Befunde
- Technische Daten für Alarmierung und Push-Benachrichtigungen (Push-Abonnement-Kennungen des
  jeweiligen Endgeräts, Rückmeldestatus)
- Protokolldaten (Anmeldezeitpunkte, durchgeführte Aktionen im Revisionsprotokoll, jeweils mit
  Benutzername und Zeitstempel) zur Nachvollziehbarkeit und Absicherung gegen Missbrauch
- Technisch notwendige Cookies/Speicherung im Browser (Sitzungs-Cookie zur Anmeldung; lokale
  Zwischenspeicherung für die Offline-Nutzung als Web-App)

3. Zwecke und Rechtsgrundlagen der Verarbeitung
Die Verarbeitung erfolgt zum Zweck der Organisation, Dokumentation und Nachweisführung des
Feuerwehrdienstes (Dienstplanung, Anwesenheitskontrolle, Einsatznachweise, Qualifikationsnachweise,
Alarmierung) sowie zur Erstellung von Arbeitgeberbescheinigungen für die Freistellung von
Feuerwehrangehörigen.

Rechtsgrundlagen sind je nach Einzelfall:
- Art. 6 Abs. 1 lit. e DSGVO, soweit die Verarbeitung zur Wahrnehmung einer Aufgabe im
  öffentlichen Interesse erfolgt (Sicherstellung des Brand- und Katastrophenschutzes)
- Art. 6 Abs. 1 lit. c DSGVO, soweit eine rechtliche Verpflichtung zur Dokumentation/Aufbewahrung
  besteht (z. B. nach dem jeweiligen Landes-Feuerwehrgesetz)
- Art. 6 Abs. 1 lit. f DSGVO (berechtigtes Interesse an einer funktionierenden, nachvollziehbaren
  Einsatzorganisation), soweit keine der vorgenannten Grundlagen einschlägig ist
- Art. 6 Abs. 1 lit. a DSGVO (Einwilligung), soweit für einzelne freiwillige Funktionen (z. B.
  Push-Benachrichtigungen) eine gesonderte Einwilligung eingeholt wird; diese kann jederzeit mit
  Wirkung für die Zukunft widerrufen werden (z. B. durch Deaktivieren der Benachrichtigungen im
  Browser/Gerät)

4. Empfänger
Eine Weitergabe der Daten an Dritte außerhalb der Feuerwehrorganisation erfolgt nicht, außer
soweit dies gesetzlich vorgeschrieben ist oder für die Erstellung von Arbeitgeberbescheinigungen
durch die betroffene Person selbst an deren Arbeitgeber weitergegeben wird.

5. Hosting
Diese Anwendung wird eigenverantwortlich betrieben. Sollte für Hosting/Infrastruktur ein
externer Dienstleister eingesetzt werden, wird mit diesem ein Auftragsverarbeitungsvertrag nach
Art. 28 DSGVO geschlossen.

6. Speicherdauer
Personenbezogene Daten werden nur so lange gespeichert, wie es für die genannten Zwecke
erforderlich ist oder gesetzliche Aufbewahrungsfristen (insbesondere für Einsatzdokumentationen)
dies vorschreiben. Zugangskonten und zugehörige Daten werden gelöscht, sobald die betroffene
Person aus der Einheit ausscheidet und keine Aufbewahrungspflicht mehr entgegensteht.

7. Cookies und lokale Speicherung
Diese Anwendung setzt ein technisch notwendiges Sitzungs-Cookie (session_token) zur Anmeldung
ein. Es dient ausschließlich der Erkennung eines angemeldeten Nutzers und wird nicht zu
Tracking- oder Analysezwecken verwendet. Für dieses technisch notwendige Cookie ist gemäß
§ 25 Abs. 2 TDDDG keine gesonderte Einwilligung erforderlich. Für die Offline-Nutzbarkeit als
installierbare Web-App (PWA) werden zusätzlich Daten lokal im Browser des jeweiligen Geräts
zwischengespeichert (Service Worker, lokaler Speicher); diese Daten verlassen das Gerät nicht.

Schriftarten und alle eingebundenen Programmbibliotheken werden ausschließlich lokal von diesem
Server ausgeliefert. Es findet keine Verbindung zu Google Fonts oder anderen externen
Content-Delivery-Netzwerken statt, wodurch beim Aufruf dieser Anwendung keine Nutzungsdaten an
Dritte übertragen werden.

8. Rechte der betroffenen Personen
Betroffene Personen haben nach Maßgabe der DSGVO das Recht auf Auskunft (Art. 15), Berichtigung
(Art. 16), Löschung (Art. 17), Einschränkung der Verarbeitung (Art. 18), Datenübertragbarkeit
(Art. 20) sowie Widerspruch gegen die Verarbeitung (Art. 21). Zur Ausübung dieser Rechte genügt
eine formlose Mitteilung an die oben genannte Kontaktadresse.

Zudem besteht ein Beschwerderecht bei einer Datenschutz-Aufsichtsbehörde, insbesondere in dem
Mitgliedstaat des gewöhnlichen Aufenthalts, des Arbeitsplatzes oder des Orts des mutmaßlichen
Verstoßes.

9. Datensicherheit
Passwörter werden ausschließlich als gesalzener Hash-Wert gespeichert und sind auch für die
Betreiber der Anwendung nicht im Klartext einsehbar. Die Übertragung sollte, sofern von der
verwendeten Infrastruktur unterstützt, ausschließlich verschlüsselt (HTTPS/TLS) erfolgen."""


@router.get("/api/legal")
def get_legal_texts():
    """Öffentlich ohne Anmeldung erreichbar - das Impressum muss laut § 5 DDG jederzeit und
    ohne vorherige Anmeldung leicht auffindbar sein, u.a. auch schon auf der Login-Seite."""
    return {"impressum": IMPRESSUM_TEXT, "datenschutz": DATENSCHUTZ_TEXT}

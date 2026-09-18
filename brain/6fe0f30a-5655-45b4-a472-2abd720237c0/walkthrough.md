# Walkthrough - Release der Feuerwehr-Einsatzsuite (v3.2)

Wir haben das Portal zu einer vollumfänglichen **Feuerwehr-Einsatzsuite** aufgerüstet, das Kontrast-Design für optimale Lesbarkeit verfeinert, QR-Code Inventarisierung und vollwertiges Fuhrpark- und Loginmanagement implementiert.

---

## 1. Design-, Kontrast- & Icon-Optimierungen (v3.2)
Um die Lesbarkeit auf Tablets im Einsatz und Hallendisplays zu maximieren, wurden folgende Anpassungen vorgenommen:
- **Ausklapp-Menüs (Dropdown-Selects):** Die Optionen in allen `<select>`-Menüs (`dashboard.html`, `editor.html`, `personnel.html`) wurden mit einem festen dunklen Hintergrund (`#1c1414`) und weißer Schrift formatiert.
- **Schriftkontrast:** Der Farbwert für sekundäre Schriften (`--text-secondary`) wurde von einem transparenten Weiß auf ein deckendes, helles Silbergrau (`#e0d8d8`) geändert. Zudem wurde Bootstrap's `.text-muted` global auf ein helleres Grau (`#e0d8d8 !important`) angehoben, und `small` Elemente wurden aufgehellt (`#e8e2e2 !important`), um absolute Lesbarkeit auf den dunklen Oberflächen zu garantieren.
- **Fahrzeugpool-Icons:** Fehlende Icons wurden durch standardisierte, freie FontAwesome-Klassen (`fa-truck`) ersetzt.

---

## 2. Fuhrpark- & Fahrzeugmanagement (Fahrzeugpool)
- **Fahrzeug erstellen & bearbeiten:** Im Fahrzeugpool-Reiter befindet sich nun ein Button **Fahrzeug anlegen** (für Leitungs- & Admin-Rollen).
- **Fahrzeugakte-Modal (`vehicleFormModal`):** Ermöglicht das Bearbeiten von Name, Funkrufname, TÜV-Fristen, SP-Fristen, aktuellem Kilometerstand, Serviceterminen und BOS-Status.
- **Fahrzeug löschen:** Berechtigte Benutzer können Fahrzeuge direkt löschen, wodurch die Fahrzeugliste und die Status-Verteilung aktualisiert werden.

---

## 3. QR-Code Generierung & Barcode-Suchlauf (Geräte & Material)
- **QR-Code Anzeige:** Im Geräte-Bearbeitungsmodal wird nun automatisch ein QR-Code für den hinterlegten Barcode gezeichnet (Verwendung der leichtgewichtigen `QRious` JS Bibliothek).
- **QR-Code drucken:** Ein Klick auf "QR-Code drucken" öffnet ein sauberes, CSS-formatiertes Print-Popup mit dem Gerätenamen und dem Code für den Etikettendruck.
- **Auto-Scan & Navigation:** Scannt man ein Etikett, leitet der QR-Code auf `http://<host>/dashboard?eq_barcode=<barcode>`. Beim Laden prüft das System den Query-Parameter, wechselt automatisch auf den Material-Reiter und öffnet die Geräteakte zum Prüfen oder Bearbeiten.

---

## 4. System-Logins & Profil-Verknüpfung
- **Login-Verwaltungs-Dashboard:** Unter *Verwaltung & VB* steht Admins nun eine komplette Benutzerverwaltung zur Verfügung. Hier können System-Logins erstellt, Passwörter zurückgesetzt, Rollen zugewiesen und Konten gelöscht werden.
- **Profil-Selbstverknüpfung:** Benutzer, deren System-Login noch mit keinem Kameraden der Personalakte verknüpft ist, erhalten im Profil-Modal einen Warnhinweis und können ihr Konto direkt mit ihrem Personalnamen verknüpfen. Danach werden die persönlichen Dienststunden und Dienststatistiken live geladen.
- **Namens-Fallback:** Die API versucht nun automatisch Konten zu verknüpfen, bei denen der Benutzername mit dem echten Namen des Kameraden übereinstimmt.

### 7. Werkstatt Tab Merged into Geräte & Material
* **Files Modified**: [dashboard.html](file:///c:/Users/Daniel%20Hegemann/Documents/antigravity/static/dashboard.html)
* **Description**: Moved all workshop-related features (PSA checkups schedule, barcode scanning simulator, TETRA radios, defect logs) inside the **Geräte & Material** tab container as subtabs. Deleted the obsolete `activeTab === 'werkstatt'` block and sidebar button, decluttering the workspace.

### 8. Profile & Hours Screen Enhancements
* **Files Modified**: [dashboard.html](file:///c:/Users/Daniel%20Hegemann/Documents/antigravity/static/dashboard.html) and [main.py](file:///c:/Users/Daniel%20Hegemann/Documents/antigravity/main.py)
* **Description**: Made major readability and feature enhancements to the user profile modal:
  - **High-contrast warning banner**: Redesigned the "Konto nicht verknüpft" banner using dark text on a solid amber-yellow block for 100% contrast and readability.
  - **Detailed service list**: Added a scrollable list table inside the profile showing the date, category description, and duration of all services attended by the logged-in user for the selected year.
  - **New backend route**: Added `/api/users/me/sessions` to fetch individual service records.

### 9. Dynamic Fire Department Names
* **Files Modified**: [main.py](file:///c:/Users/Daniel%20Hegemann/Documents/antigravity/main.py) and [dashboard.html](file:///c:/Users/Daniel%20Hegemann/Documents/antigravity/static/dashboard.html)
* **Description**: Eliminated hardcoded 'Feuerwehr Neustadt' strings:
  - Dynamically inject the configured `TOWN_NAME` environment variable into the station settings database seeder and fallback settings API route.
  - Dynamically replace "Neustadt" in mock radio names and Leaflet live-GPS vehicle markers with the active station settings name.

### 10. Leaflet Map Initialization Fix
* **Files Modified**: [dashboard.html](file:///c:/Users/Daniel%20Hegemann/Documents/antigravity/static/dashboard.html)
* **Description**: Fixed a Leaflet mapping bug where the tactical map did not render correctly. Discovered and removed a duplicate element with the ID `hydrantMap` inside the obsolete POI subtab under Verwaltung, allowing the map to initialize properly.

---

## Verification Plan

### Manual Verification
1. Open **Geräte & Material** in the sidebar. Click on the new **PSA & Prüffristen** and **Funkgeräte (TETRA)** tabs to check list schedules and scanner simulator functions.
2. Click **Profil & Dienststunden** in the sidebar. Verify the high-contrast unlinked account block, link your account, and confirm that the attended services list table appears.
3. Edit the Station name settings in settings, and verify that the radio call signs and simulated map markers dynamically update to your custom fire department name.
4. Go to the **Lagekarte & Hydranten** tab and verify that the Leaflet tactical map loads, displays hydrants/BMAs, and responds to click events perfectly.
5. Open `/static/alarmdisplay.html` in the browser to view the Command Center view containing weather widgets, AAO routing guides, alarm stopwatch timers, and qualifications statistics.
6. Try typing an incorrect password on the login screen 5 times to confirm the 15-minute lockout and view the audit logs for `KONTO_GESPERRT` und `LOGIN_FEHLVERSUCH`.

---

## 5. Command Center Hallenmonitor (Alarmdisplay)
- **BOS Fahrzeug-Statusboard:** Der Monitor (`static/alarmdisplay.html`) zeigt nun in Echtzeit den aktuellen FMS-Status (S1-S6) aller Einsatzfahrzeuge der Wache an.
- **POI & Hydrantenkarte:** Eine interaktive Leaflet-Karte visualisiert das Gerätehaus und alle POIs/Wasserentnahmestellen im Einsatzgebiet.
- **Sprachalarmierung (Speech Synthesis):** Geht ein frischer aPagerPro-Alarm ein, ertönt ein Gongschlag-Warnsignal über die Hallenlautsprecher, gefolgt von einer computergenerierten Stimme, die das Einsatzstichwort und den Einsatzort laut vorliest.

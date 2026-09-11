# 🚒 Digitales Dienstbuch – 

Entwickelt von **Daniel Hegemann** (<d.hege@icloud.com>)

Dieses System ermöglicht eine rechtssichere und digitale Erfassung von Übungen und Einsätzen.

## 🚀 Installation via Portainer

1. Erstelle einen neuen Stack.
2. Nutze die `docker-compose.yml` aus diesem Repo.
3. **WICHTIG:** Lade die ENV Runter und füge sie ein bevor du den Stack Deployst!
4. 
   - `ADMIN_PIN`: Dein Admin-Code (Buchstaben & Zahlen erlaubt)
   - `USER_PIN`: Dein Mannschafts-Code
   - `TOWN_NAME`: Name deiner Wehr/Stadt
   - `ROOT_PASS` & `DB_PASS`: Datenbank-Passwörter

## 🔄 Updates
Einmal installiert, kann das System bequem anzeigen wenn ein Update vergübar ist!

## ⚠️ Bekannte Einschränkung: Push-Alarme auf Samsung Internet
Für zuverlässigen Alarmempfang (Ton + Bildschirm-Aufwecken bei Push-Benachrichtigungen) auf dem Handy bitte **Google Chrome** statt **Samsung Internet** verwenden. Samsung Internet hat eine bekannte, browserseitige Einschränkung bei der Web-Notification-API: Alarme kommen dort zwar an, aber ohne Ton und ohne den Bildschirm zu wecken – unabhängig davon, wie die Benachrichtigungs-Einstellungen konfiguriert sind. Das lässt sich von der App-Seite aus nicht beheben. In Chrome funktioniert es zuverlässig.

---
*Gott zur Ehr, dem Nächsten zur Wehr!*

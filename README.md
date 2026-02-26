# 🚒 Digitales Dienstbuch – 

Entwickelt von **Daniel Hegemann** (<d.hege@icloud.com>)

Dieses System ermöglicht eine rechtssichere und digitale Erfassung von Übungen und Einsätzen.

## 🚀 Installation via Portainer

1. Erstelle einen neuen Stack.
2. Nutze die `docker-compose.yml` aus diesem Repo.
3. **WICHTIG:** Du musst folgende Umgebungsvariablen (Environment Variables) händisch setzen, da keine Standardwerte vergeben sind (Sicherheitsgrund):
   - `ADMIN_PIN`: Dein Admin-Code (Buchstaben & Zahlen erlaubt)
   - `USER_PIN`: Dein Mannschafts-Code
   - `TOWN_NAME`: Name deiner Wehr/Stadt
   - `ROOT_PASS` & `DB_PASS`: Datenbank-Passwörter

## 🔄 Updates
Einmal installiert, kann das System bequem über den **System Update** Button im Dashboard aktuell gehalten werden.

---
*Gott zur Ehr, dem Nächsten zur Wehr!*

# 🚒 Digitales Dienstbuch – 

Entwickelt von **Daniel Hegemann** (<d.hege@icloud.com>)

Dieses System ermöglicht eine rechtssichere und digitale Erfassung von Übungen und Einsätzen.

## 🚀 Installation via Portainer

1. Erstelle einen neuen Stack und nutze die `docker-compose.yml` aus diesem Repo.
2. Öffne unter dem Editor **Environment variables → Advanced mode** und füge den Inhalt von [`.env.example`](.env.example) ein. Trage **eigene** Werte ein:
   - `ROOT_PASS` & `DB_PASS`: Datenbank-Passwörter (zufällig, mind. 20 Zeichen)
   - `SECRET_KEY`: zufälliger Schlüssel (mind. 32 Zeichen), z. B. mit `openssl rand -hex 32`
   - `TOWN_NAME`: Name deiner Wehr/Stadt
   - `STAPEL_PFAD`: Ordner auf dem Server für die Datenbank
3. Stack deployen und die Seite öffnen. Beim ersten Start wird das Admin-Konto angelegt.
4. Unter **Verwaltung → E-Mail-Versand** kannst du danach das Postfach deiner Wehr eintragen (für „Passwort vergessen“ und Erinnerungen).

> **Wichtig:** Passwörter und Schlüssel gehören nur in die Umgebungsvariablen des Stacks (oder in eine lokale `.env`), **niemals** ins Git-Repo. Die `.env.example` enthält absichtlich keine echten Werte.

## 🔄 Updates
Einmal installiert, kann das System bequem anzeigen wenn ein Update vergübar ist!

## ⚠️ Bekannte Einschränkung: Push-Alarme auf Samsung Internet
Für zuverlässigen Alarmempfang (Ton + Bildschirm-Aufwecken bei Push-Benachrichtigungen) auf dem Handy bitte **Google Chrome** statt **Samsung Internet** verwenden. Samsung Internet hat eine bekannte, browserseitige Einschränkung bei der Web-Notification-API: Alarme kommen dort zwar an, aber ohne Ton und ohne den Bildschirm zu wecken – unabhängig davon, wie die Benachrichtigungs-Einstellungen konfiguriert sind. Das lässt sich von der App-Seite aus nicht beheben. In Chrome funktioniert es zuverlässig.

---
*Gott zur Ehr, dem Nächsten zur Wehr!*

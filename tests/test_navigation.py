"""Seitenleiste und Reiter je Rolle: sieht jede Rolle genau die passenden Einträge, zeigt jeder Reiter Inhalt, und fragt die Seite
nichts ab, was die Rolle gar nicht darf (403/401). Läuft mit Playwright; ohne Playwright wird übersprungen."""
import sys

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("SKIP   Playwright nicht installiert")
    sys.exit(0)

from tests.ui_helpers import ROLES, browser_page, close_modals, ensure_demo_logbook, make_role_users, sidebar_items

fails = []


def check(name, cond, extra=""):
    print(("OK    " if cond else "FEHLER"), name, extra)
    if not cond:
        fails.append(name)


def items_for(*, personal, youth):
    """Basis für alle; Personalverwaltung nur für Rollen mit Aufgabe dort, Jugendfeuerwehr (Daten Minderjähriger) nur für Jugendwarte."""
    return ["Dienstbetrieb", "Kalender", "Fristen"] + (["Personalverwaltung"] if personal else []) + (["Jugendfeuerwehr"] if youth else []) + [
        "Digitales Notizbuch", "Alarmierung", "Einsatzberichte", "Lagekarte & Hydranten", "Objekte & BMA", "First Responder",
        "Fahrzeugpool", "Geräte & Material", "Dokumente & Archiv"]


MANAGER_ITEMS = ["Dienstbetrieb", "Kalender", "Fristen", "Personalverwaltung", "Jugendfeuerwehr", "Digitales Notizbuch", "Statistiken",
                 "Alarmierung", "Einsatzberichte", "Lagekarte & Hydranten", "Objekte & BMA", "First Responder", "Logbücher",
                 "Fahrzeugpool", "Geräte & Material", "Abrechnung", "Arbeitsbescheinigungen", "Vereinskasse & Inventar", "Dokumente & Archiv",
                 "Einstellungen"]
EXPECT = {"admin": MANAGER_ITEMS, "leitung": MANAGER_ITEMS,
          "mannschaft": items_for(personal=False, youth=False),
          "jugendwarte": items_for(personal=False, youth=True),
          "gruppenfuehrer": items_for(personal=True, youth=False),
          "geratewart": items_for(personal=True, youth=False),     # Kleiderkammer
          "atemschutzwart": items_for(personal=True, youth=False)}  # G26.3-Überwachung
# Reiter innerhalb von Einstellungen / Geräte & Material je Rolle
SETTINGS_TABS = {"admin": ["System-Logins", "Aktivitäts-Log", "Sicherheit & Datenschutz", "Systemzustand & Sicherung", "Wache & Fristen", "E-Mail-Versand"],
                 "leitung": ["Wache & Fristen"]}
MATERIAL_HAS_GERAETEHAUS = {"admin", "leitung", "geratewart"}

make_role_users()
ensure_demo_logbook()
with sync_playwright() as pw:
    for role in ["demo"] + ROLES:
        label = "admin" if role == "demo" else role
        user = role if role == "demo" else f"r_{role}"
        browser, page = browser_page(pw, user)
        denied = []
        page.on("response", lambda r, d=denied: d.append(f"{r.status} {r.url.replace('http://127.0.0.1:8801', '')}") if r.status in (401, 403) and "/api/" in r.url else None)
        items = sidebar_items(page)
        check(f"{label}: Seitenleiste zeigt genau die passenden Einträge", items == EXPECT[label], f"{set(items) ^ set(EXPECT[label])}")
        empty = []
        for i in range(len(items)):
            close_modals(page)
            btn = page.locator(".sidebar .sidebar-btn:visible").nth(i)
            name = btn.inner_text().strip().replace("\n", " ")
            btn.click()
            page.wait_for_timeout(900)
            text = page.evaluate("() => (document.querySelector('.main-content') ? [...document.querySelectorAll('.main-content > div')].filter(d => d.offsetParent && !d.classList.contains('broadcast-card')).map(d => d.innerText).join(' ') : '').replace(/\\s+/g, ' ').trim()")
            if len(text) < 40:
                empty.append(name)
            subs = page.locator(".main-content .nav-link:visible, .main-content .settings-nav button:visible")
            labels = [subs.nth(k).inner_text().strip() for k in range(subs.count())]
            if name == "Einstellungen":
                check(f"{label}: Einstellungen zeigt die passenden Reiter", labels == SETTINGS_TABS.get(label, []), str(labels))
                active = page.evaluate("() => document.querySelector('.settings-nav .nav-link.active')?.innerText.trim() || ''")
                check(f"{label}: Einstellungen startet auf einem Reiter (keine leere Seite)", active != "", active)
            if name == "Geräte & Material":
                check(f"{label}: 'Gerätehaus' nur für Berechtigte unter Geräte & Material", ("Gerätehaus" in labels) == (label in MATERIAL_HAS_GERAETEHAUS), str(labels))
            for k in range(subs.count()):
                try:
                    subs.nth(k).click(timeout=3000)
                except Exception:
                    continue
                page.wait_for_timeout(500)
        check(f"{label}: kein Reiter ist leer", not empty, str(empty))
        uniq = sorted(set(denied))
        check(f"{label}: die Seite fragt nichts ab, was die Rolle nicht darf", not uniq, str(uniq[:6]))
        browser.close()

print("\n==> ", "ALLES OK" if not fails else f"{len(fails)} FEHLER")
sys.exit(1 if fails else 0)

"""Logbücher in der Oberfläche: Eintrag per Formular erfassen, Auswertung, Logbuch im Editor aus der Vorlage anlegen. Läuft mit Playwright."""
import sys

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("SKIP   Playwright nicht installiert")
    sys.exit(0)

from tests.helpers import sql
from tests.ui_helpers import browser_page, close_modals, ensure_demo_logbook, make_role_users, vm_eval

fails = []


def check(name, cond, extra=""):
    print(("OK    " if cond else "FEHLER"), name, extra)
    if not cond:
        fails.append(name)


make_role_users()
for t in ("logbook_entries", "logbook_fields", "logbook_members", "logbooks"):
    sql(f"delete from {t}")
ensure_demo_logbook()

with sync_playwright() as pw:
    browser, page = browser_page(pw, "demo")
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)))
    page.evaluate("() => { const vm = document.querySelector('#app').__vue_app__._container._vnode.component.proxy; vm.loadLogbooks(); }")
    page.wait_for_timeout(800)
    check("Menü zeigt 'Logbücher'", "Logbücher" in page.evaluate("() => [...document.querySelectorAll('.sidebar .sidebar-btn')].map(b => b.innerText)").__str__())
    page.locator(".sidebar .sidebar-btn", has_text="Logbücher").click()
    page.wait_for_timeout(1200)
    close_modals(page)
    check("Die drei Demo-Einträge stehen in der Liste", vm_eval(page, "vm.lbEntries.length") == 3 and "Übungsplatz Nord" in page.inner_text(".main-content"))

    # Eintrag über das Formular erfassen
    page.select_option("select >> nth=1", index=1) if False else None
    form = page.locator(".main-content .card:has(h5:has-text('Neuer Eintrag'))")
    form.locator("select").nth(0).select_option(index=1)   # Person
    form.locator("select").nth(1).select_option(index=1)   # Gerät
    form.locator("input[type=time]").nth(0).fill("14:00")
    form.locator("input[type=time]").nth(1).fill("14:50")
    form.locator("select").nth(2).select_option(label="Einsatz")
    page.wait_for_timeout(300)
    check("Dauer wird beim Ausfüllen live berechnet", "0:50" in form.inner_text(), form.inner_text()[-120:])
    form.get_by_role("button", name="Eintrag speichern").click()
    page.wait_for_timeout(1200)
    check("Neuer Eintrag erscheint in der Liste", vm_eval(page, "vm.lbEntries.length") == 4, str(vm_eval(page, "vm.lbEntries.length")))
    check("Formular ist danach wieder leer", vm_eval(page, "vm.lbEntryForm.id === null && Object.keys(vm.lbEntryForm.values).length === 0"))

    # Auswertung
    page.get_by_role("button", name="Auswertung").click()
    page.wait_for_timeout(600)
    txt = page.inner_text(".main-content")
    check("Auswertung zeigt Summe, Personen und Gruppierung nach Art", "Gesamt" in txt and "Je Person" in txt and "Nach „Art“" in txt and "Flugdauer" in txt and "2:40" in txt, txt[:300].replace("\n", " "))

    # Logbuch im Editor aus der Vorlage anlegen
    page.evaluate("() => { const vm = document.querySelector('#app').__vue_app__._container._vnode.component.proxy; vm.activeTab = 'verwaltung'; vm.openSettingsTab('wache'); }")
    page.wait_for_timeout(900)
    page.get_by_role("button", name="Flugbuch aus Vorlage").click()
    page.wait_for_timeout(500)
    check("Vorlage bringt die Flugbuch-Felder mit", vm_eval(page, "vm.lbForm.fields.map(f => f.label).join(',')") == "Start,Landung,Flugdauer,Art,Ort,Akku,Vorfall / Störung,Bemerkung")
    page.locator("input[placeholder='z. B. Flugbuch']").fill("Drohnen-Flugbuch")
    page.get_by_role("button", name="Logbuch speichern").click()
    page.wait_for_timeout(1200)
    check("Neues Logbuch ist gespeichert und in der Liste", vm_eval(page, "vm.logbooks.some(b => b.name === 'Drohnen-Flugbuch')") and vm_eval(page, "vm.lbForm === null"))
    check("Beide Logbücher auswählbar", vm_eval(page, "vm.logbooks.length") == 2)
    # Feld mit fehlender Dauer-Zuordnung wird abgelehnt (Meldung statt Absturz)
    page.evaluate("() => { const vm = document.querySelector('#app').__vue_app__._container._vnode.component.proxy; vm.newLogbookForm(false); vm.lbForm.name = 'X'; vm.lbForm.fields[0].label = 'D'; vm.lbForm.fields[0].ftype = 'duration'; vm.saveLogbook(); }")
    page.wait_for_timeout(800)
    check("Ungültige Felddefinition: Meldung, nichts gespeichert", vm_eval(page, "vm.logbooks.length") == 2 and page.locator(".modal.show").count() >= 1)
    close_modals(page)
    check("Keine JavaScript-Fehler", not errors, str(errors[:2]))
    browser.close()

for t in ("logbook_entries", "logbook_fields", "logbook_members", "logbooks"):
    sql(f"delete from {t}")
print("\n==> ", "ALLES OK" if not fails else f"{len(fails)} FEHLER: {fails}")
sys.exit(1 if fails else 0)

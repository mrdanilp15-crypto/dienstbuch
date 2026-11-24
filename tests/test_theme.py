"""Hell/Dunkel: Standard, sichtbare Kästchen, einheitliche Felder, Editor, Zurück-Knopf, Rechte-Klick."""
import sys

from playwright.sync_api import sync_playwright

from tests.ui_helpers import BASE, browser_page, close_modals, set_theme, vm_eval

fails = []


def check(name, cond, extra=""):
    print(("OK    " if cond else "FEHLER"), name, extra)
    if not cond:
        fails.append(name)


BOXES = "table.table-dark input.form-check-input[type=checkbox]"
BG = "(sel) => getComputedStyle(document.querySelector(sel)).backgroundColor"

with sync_playwright() as pw:
    browser, page = browser_page(pw, "demo")
    check("Standard ist hell", page.evaluate("() => document.documentElement.getAttribute('data-theme')") == "light")

    # Rechte-Matrix: Kästchen sichtbar und klickbar
    vm_eval(page, "(vm.activeTab = 'verwaltung', vm.openSettingsTab('rechte'), 1)")
    page.wait_for_selector(BOXES, timeout=8000)
    page.wait_for_timeout(500)
    checked_bg = page.evaluate("() => { const e = document.querySelector('table.table-dark input.form-check-input:checked'); return e ? getComputedStyle(e).backgroundColor : null }")
    unchecked_bg = page.evaluate("() => { const e = document.querySelector('table.table-dark input.form-check-input:not(:checked)'); return e ? getComputedStyle(e).backgroundColor : null }")
    check("Hell: angehaktes Kästchen ist rot gefüllt", checked_bg == "rgb(183, 28, 28)", str(checked_bg))
    check("Hell: leeres Kästchen ist weiß", unchecked_bg == "rgb(255, 255, 255)", str(unchecked_bg))

    target = page.locator(BOXES + ":not(:checked)").first
    target.scroll_into_view_if_needed()
    n_before = page.locator(BOXES + ":checked").count()
    target.click()
    page.wait_for_timeout(1500)
    n_after = page.locator(BOXES + ":checked").count()
    check("Klick auf Kästchen setzt den Haken und bleibt", n_after == n_before + 1, f"{n_before} -> {n_after}")
    page.evaluate("async () => { await fetch('/api/admin/permissions/reset', {method: 'POST'}); }")

    set_theme(page, True)
    vm_eval(page, "(vm.loadPermissionMatrix(), 1)")
    page.wait_for_timeout(1200)
    d_unchecked = page.evaluate("() => { const e = document.querySelector('table.table-dark input.form-check-input:not(:checked)'); return e ? getComputedStyle(e).backgroundColor : null }")
    d_checked = page.evaluate("() => { const e = document.querySelector('table.table-dark input.form-check-input:checked'); return e ? getComputedStyle(e).backgroundColor : null }")
    check("Dunkel: leeres Kästchen nicht weiß", d_unchecked is not None and d_unchecked != "rgb(255, 255, 255)", str(d_unchecked))
    check("Dunkel: angehaktes Kästchen ist rot gefüllt", d_checked == "rgb(183, 28, 28)", str(d_checked))
    set_theme(page, False)

    # Einheitliche Eingabefelder im hellen Modus
    grey = []
    for tab in ("abrechnung", "verein", "archiv", "apager", "hvo"):
        vm_eval(page, f"(vm.activeTab = '{tab}', 1)")
        page.wait_for_timeout(900)
        for r in page.evaluate("""() => [...document.querySelectorAll('.main-content input.form-control, .main-content select.form-select, .main-content textarea.form-control')]
            .filter(e => e.offsetParent && !e.disabled && !e.readOnly).map(e => getComputedStyle(e).backgroundColor)"""):
            if r != "rgb(255, 255, 255)":
                grey.append((tab, r))
    check("Hell: alle Eingabefelder weiß (keine grauen Altlasten)", not grey, str(grey[:4]))

    # Zurück-Knopf
    vm_eval(page, "(vm.activeTab = 'kalender', 1)"); page.wait_for_timeout(300)
    vm_eval(page, "(vm.activeTab = 'fristen', 1)"); page.wait_for_timeout(300)
    vm_eval(page, "(vm.activeTab = 'einsaetze', 1)"); page.wait_for_timeout(300)
    page.go_back(); page.wait_for_timeout(500)
    check("Zurück geht zum vorherigen Reiter", vm_eval(page, "vm.activeTab") == "fristen", str(vm_eval(page, "vm.activeTab")))
    check("Adresse bleibt im Dashboard", page.url.split("#")[0].endswith("/dashboard"), page.url)
    page.go_back(); page.wait_for_timeout(500)
    check("Zweimal zurück", vm_eval(page, "vm.activeTab") == "kalender")
    page.go_forward(); page.wait_for_timeout(500)
    check("Vor geht wieder zum nächsten", vm_eval(page, "vm.activeTab") == "fristen")

    # Übersicht: neue Karten zur Auswahl
    page.evaluate("async () => { await fetch('/api/users/me/prefs/dashboard_layout', {method: 'PUT', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({value: {order: ['mitteilungen', 'berichte', 'termine', 'benachrichtigungen', 'fristen', 'maengel'], hidden: []}}) }); }")
    page.reload(); page.wait_for_timeout(3500); close_modals(page)
    vm_eval(page, "(vm.activeTab = 'uebersicht', 1)"); page.wait_for_timeout(2500)
    n_default = vm_eval(page, "vm.overviewWidgets.length")
    check("Übersicht: bei bestehender Anordnung bleiben die neuen Karten zunächst ausgeblendet", n_default == 6, str(n_default))
    vm_eval(page, "(vm.overviewLayout = { order: vm.overviewLayout.order, hidden: [] }, vm.ensureOverviewData(), 1)")
    page.wait_for_timeout(2500)
    titles = page.evaluate("() => [...document.querySelectorAll('.overview-grid .premium-card h6')].map(h => h.innerText.trim())")
    check("Übersicht: alle fünfzehn Karten lassen sich einblenden", len(titles) == 15, str(titles))
    for want in ("Meine Stunden", "Meine Rückmeldungen", "Nächste Termine", "Letzte Einsätze", "Offene Berichte", "Fahrzeugstatus", "Mannschaft", "Pinnwand", "Schnellzugriff"):
        check(f"Übersicht: Karte {want}", want in titles)
    bad = vm_eval(page, "['javascript:alert(1)', '//evil.example', 'https://ok.example', '/dashboard#kalender'].map(u => vm.safeLinkUrl(u))")
    check("Schnellzugriff: nur https-Adressen und App-Seiten erlaubt", bad == ["", "", "https://ok.example", "/dashboard#kalender"], str(bad))
    page.evaluate("async () => { await fetch('/api/users/me/prefs/dashboard_layout', {method: 'PUT', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({value: null}) }); }")

    # Favoriten im Menü und Leiste unten (Handy)
    page.evaluate("async () => { await fetch('/api/users/me/prefs/menu_favorites', {method: 'PUT', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({value: ['maengel', 'fristen']}) }); }")
    vm_eval(page, "(vm.menuFavorites = ['maengel', 'fristen'], 1)")
    page.wait_for_timeout(500)
    fav_items = page.evaluate("() => [...document.querySelectorAll('.sidebar .sidebar-btn')].map(b => b.innerText.replace(/\s+/g, ' ').trim())")
    check("Favoriten stehen im Menü", fav_items[:3] == ["Übersicht", "Mängel", "Fristen"] or ("Mängel" in fav_items[:4] and "Fristen" in fav_items[:4]), str(fav_items[:5]))
    page.evaluate("async () => { await fetch('/api/users/me/prefs/menu_favorites', {method: 'PUT', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({value: []}) }); }")
    vm_eval(page, "(vm.menuFavorites = [], 1)")
    ctx2 = browser.new_context(viewport={"width": 390, "height": 844}, locale="de-DE", is_mobile=True, has_touch=True)
    m = ctx2.new_page()
    m.goto(BASE + "/login")
    m.evaluate("""async () => { await fetch('/api/login', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({username: 'demo', password: 'demo1234'})}); }""")
    m.goto(BASE + "/dashboard")
    m.wait_for_timeout(3500)
    close_modals(m)
    nav = m.evaluate("() => [...document.querySelectorAll('.mobile-bottom-nav button')].filter(b => b.offsetParent).map(b => b.innerText.replace(/\s+/g, ' ').trim())")
    check("Handy: Leiste unten zeigt Start, Dienste, Einsätze, Fristen, Menü", len(nav) == 5 and nav[0] == "Start" and nav[-1] == "Menü", str(nav))
    m.evaluate("() => document.querySelectorAll('.mobile-bottom-nav button')[2].click()")
    m.wait_for_timeout(600)
    check("Handy: Leiste wechselt den Bereich", vm_eval(m, "vm.activeTab") == "einsaetze")
    check("Handy: Leiste ist nicht auf dem Desktop sichtbar", not page.evaluate("() => [...document.querySelectorAll('.mobile-bottom-nav')].some(n => n.offsetParent)"))
    ctx2.close()

    # Editor im hellen Modus
    mid = page.evaluate("async () => { const r = await (await fetch('/api/missions')).json(); return r.length ? r[0].id : null }")
    page.goto(BASE + (f"/editor?mission_id={mid}" if mid else "/editor"))
    page.wait_for_timeout(2500)
    body_bg = page.evaluate("() => getComputedStyle(document.body).backgroundColor")
    check("Editor folgt dem hellen Modus", body_bg == "rgb(244, 246, 249)", body_bg)
    browser.close()

print("\n==> ", "ALLES OK" if not fails else f"{len(fails)} FEHLER: {fails}")
sys.exit(1 if fails else 0)

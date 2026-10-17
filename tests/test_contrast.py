"""Lesbarkeit: jeder sichtbare Text auf jeder Seite, in Dunkel UND Hell, mindestens 4,5:1 (große Schrift 3:1) und >= 12 px.
Läuft mit Playwright (pip install playwright && python -m playwright install chromium); ohne Playwright wird übersprungen."""
import os
import sys

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("SKIP   Playwright nicht installiert")
    sys.exit(0)

from tests.ui_helpers import browser_page, close_modals, set_theme, sidebar_items, vm_eval

AUDIT_JS = open(os.path.join(os.path.dirname(__file__), "contrast_audit.js"), encoding="utf-8").read()
# Bewusst zugelassene Ausnahmen (Schlüssel wie in der Ausgabe) - hier möglichst nichts eintragen, sondern die Farbe korrigieren.
ALLOW = set()

offenders = {}
checked_total = 0
pages = 0


def audit(page, label, root=None):
    global checked_total, pages
    page.wait_for_timeout(900)
    page.evaluate(AUDIT_JS)
    res = page.evaluate("(r) => window.__audit(r)", root)
    pages += 1
    checked_total += res["checked"]
    for b in res["bad"]:
        if b["key"] in ALLOW:
            continue
        e = offenders.setdefault(b["key"], {"ratio": b["ratio"], "text": b["text"], "pages": []})
        e["pages"].append(label)


with sync_playwright() as pw:
    browser, page = browser_page(pw, "demo")
    for dark in (True, False):
        mode = "dunkel" if dark else "hell"
        set_theme(page, dark)
        count = len(sidebar_items(page))
        for i in range(count):
            close_modals(page)
            btn = page.locator(".sidebar .sidebar-btn:visible").nth(i)
            name = btn.inner_text().strip().replace("\n", " ")
            if "Unterstützen" in name or "Theme" in name or "Logout" in name or "Push" in name or "Was ist neu" in name or "Hallen" in name:
                continue
            btn.click()
            audit(page, f"{mode}: {name}")
            subs = page.locator(".main-content .nav-link:visible, .main-content .settings-nav button:visible")
            for j in range(subs.count()):
                s = subs.nth(j)
                sub_name = s.inner_text().strip().replace("\n", " ")
                try:
                    s.click(timeout=3000)
                except Exception:
                    continue
                audit(page, f"{mode}: {name} > {sub_name}")
        # Fenster und Panels
        page.evaluate("() => { const vm = document.querySelector('#app').__vue_app__._container._vnode.component.proxy; vm.activeTab = 'dienste'; }")
        page.evaluate("() => document.querySelector('#notifBell button').click()")
        audit(page, f"{mode}: Glocke", "#notifBell")
        page.evaluate("() => document.querySelector('#notifBell button').click()")
        page.evaluate("() => { const vm = document.querySelector('#app').__vue_app__._container._vnode.component.proxy; vm.openSelfPasswordModal(); }")
        audit(page, f"{mode}: Profil-Fenster", "#selfPasswordModal")
        close_modals(page)
        page.evaluate("() => { const vm = document.querySelector('#app').__vue_app__._container._vnode.component.proxy; vm.resetUserPassword({id: 1, username: 'demo'}); }")
        audit(page, f"{mode}: Admin-Passwort-Fenster", "#adminPwModal")
        close_modals(page)
    browser.close()

print(f"{pages} Seiten/Fenster, {checked_total} Textstellen geprüft")
if offenders:
    for key, e in sorted(offenders.items(), key=lambda x: x[1]["ratio"]):
        print(f"FEHLER {e['ratio']:>5} | {key} | \"{e['text']}\" | {', '.join(e['pages'][:3])}{' (+%d)' % (len(e['pages']) - 3) if len(e['pages']) > 3 else ''}")
    print(f"\n==>  {len(offenders)} Problemstellen")
    sys.exit(1)
print("OK     alle Texte lesbar (Kontrast und Größe)")
print("\n==>  ALLES OK")

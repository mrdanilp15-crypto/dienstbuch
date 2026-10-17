"""Gemeinsame Hilfen für Browser-Tests (Playwright)."""
import os

from tests.helpers import sql

BASE = "http://127.0.0.1:8801"
PASSWORD = "demo1234"
ROLES = ["mannschaft", "geratewart", "gruppenfuehrer", "jugendwarte", "atemschutzwart", "leitung"]


def make_role_users():
    """Je Rolle ein Login (Passwort wie das des Demo-Admins), ohne Erst-Anmelde-Zwang."""
    for role in ROLES:
        sql(f"delete from users where username = 'r_{role}'")
        sql(f"insert into users (username, password_hash, role, is_first_login) select 'r_{role}', password_hash, '{role}', 0 from users where username = 'demo'")


def browser_page(playwright, username, width=1366, height=900):
    browser = playwright.chromium.launch()
    ctx = browser.new_context(viewport={"width": width, "height": height}, locale="de-DE")
    page = ctx.new_page()
    page.goto(BASE + "/login")
    status = page.evaluate("""async ([u, p]) => (await fetch('/api/login', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({username: u, password: p})})).status""", [username, PASSWORD])
    assert status == 200, (username, status)
    page.goto(BASE + "/dashboard")
    page.wait_for_function("() => { const a = document.querySelector('#app'); return a && a.__vue_app__ && a.__vue_app__._container._vnode.component.proxy.ready }", timeout=20000)
    page.wait_for_timeout(2500)  # die "Was ist neu"-Meldung erscheint erst kurz nach dem Laden
    close_modals(page)
    return browser, page


def close_modals(page):
    for _ in range(3):
        page.evaluate("() => document.querySelectorAll('.modal.show .btn-close, .modal.show [data-bs-dismiss]').forEach(b => b.click())")
        page.wait_for_timeout(450)
        if not page.evaluate("() => !!document.querySelector('.modal.show')"):
            break


def vm_eval(page, expr):
    return page.evaluate("() => { const vm = document.querySelector('#app').__vue_app__._container._vnode.component.proxy; return (" + expr + "); }")


def set_theme(page, dark: bool):
    page.evaluate("(dark) => { const vm = document.querySelector('#app').__vue_app__._container._vnode.component.proxy; if (vm.isDarkMode !== dark) vm.toggleTheme(); }", dark)
    page.wait_for_timeout(500)


def sidebar_items(page):
    return page.evaluate("() => [...document.querySelectorAll('.sidebar .sidebar-btn')].filter(b => b.offsetParent).map(b => b.innerText.replace(/\\s+/g, ' ').trim())")

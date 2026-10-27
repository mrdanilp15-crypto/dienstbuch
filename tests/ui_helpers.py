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
    """Beschriftungen der sichtbaren Seitenleisten-Einträge (ohne Zähler-Badges wie bei "Fristen")."""
    return page.evaluate(r"() => [...document.querySelectorAll('.sidebar .sidebar-btn')].filter(b => b.offsetParent).map(b => { const c = b.cloneNode(true); c.querySelectorAll('.badge').forEach(x => x.remove()); return c.innerText.replace(/\s+/g, ' ').trim(); })")


def ensure_demo_logbook():
    """Legt (falls nötig) ein Demo-Flugbuch mit Einträgen an, damit die Oberflächen-Tests auch die Seite "Logbücher" prüfen.
    Mitglied ist nur Laura Schmitt (mit der kein Test-Login verknüpft ist) - sichtbar ist es also nur für Admin/Leitung."""
    import httpx
    c = httpx.Client(base_url=BASE, timeout=30)
    assert c.post("/api/login", json={"username": "demo", "password": PASSWORD}).status_code == 200
    if any(b["name"] == "Flugbuch" for b in c.get("/api/logbooks").json()):
        return
    laura = int(sql("select id from personnel where name = 'Laura Schmitt'"))
    eq = int(sql("select id from equipment order by id limit 1"))
    book = {"name": "Flugbuch", "subject": "equipment", "responsible_role": "", "responsible_pid": None, "open_to_all": False, "member_ids": [laura],
            "fields": [{"uid": "a", "label": "Start", "ftype": "time", "required": True}, {"uid": "b", "label": "Landung", "ftype": "time", "required": True},
                       {"uid": "c", "label": "Flugdauer", "ftype": "duration", "evaluate": "sum", "config": {"start": "a", "end": "b"}},
                       {"uid": "d", "label": "Art", "ftype": "select", "options": ["Einsatz", "Übung", "Training", "Test"], "required": True, "evaluate": "group"},
                       {"uid": "e", "label": "Ort", "ftype": "text"}, {"uid": "f", "label": "Vorfall / Störung", "ftype": "bool", "evaluate": "group"}]}
    lid = c.post("/api/logbooks", json=book).json()["id"]
    f = {x["label"]: str(x["id"]) for x in c.get("/api/logbooks").json()[0]["fields"]}
    import datetime
    for days, start, end, art in ((3, "09:00", "09:40", "Übung"), (10, "18:30", "19:15", "Einsatz"), (20, "10:00", "10:25", "Training")):
        day = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()
        assert c.post(f"/api/logbooks/{lid}/entries", json={"entry_date": day, "personnel_id": laura, "subject_id": eq,
                      "values": {f["Start"]: start, f["Landung"]: end, f["Art"]: art, f["Ort"]: "Übungsplatz Nord", f["Vorfall / Störung"]: False}}).status_code == 200

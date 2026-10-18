"""Handy-Layout (390 px breit): nichts läuft rechts aus dem Bild, Reiter und Knöpfe brechen um. Läuft mit Playwright."""
import sys

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("SKIP   Playwright nicht installiert")
    sys.exit(0)

from tests.ui_helpers import browser_page, close_modals, make_role_users, vm_eval

OVERFLOW_JS = """() => {
  // Sichtbar abgeschnitten = ein Element ragt über den Rand des Bereichs hinaus, der es beschneidet (oder des Bildschirms).
  // Bewusst scrollbare Bereiche (Tabellen, Karten, Fenster) und Texte mit Ellipse zählen nicht.
  const vw = window.innerWidth, bad = [];
  const skip = el => el.closest('.table-responsive, .modal, canvas, .leaflet-container, pre, code, .notif-panel, .sidebar, select, .no-clip-check');
  document.querySelectorAll('.main-content *').forEach(el => {
    const r = el.getBoundingClientRect();
    if (r.width < 2 || r.height < 2 || !el.offsetParent || skip(el)) return;
    let clip = vw;
    for (let e = el.parentElement; e && e !== document.documentElement; e = e.parentElement) {
      const cs = getComputedStyle(e);
      if (cs.overflowX !== 'visible') { clip = Math.min(clip, e.getBoundingClientRect().right); }
    }
    if (r.right > clip + 6) bad.push(el.tagName.toLowerCase() + '.' + [...el.classList].slice(0, 3).join('.') + ' ' + Math.round(r.right) + '>' + Math.round(clip) + ' | ' + (el.innerText || '').trim().slice(0, 28));
  });
  return { page: document.documentElement.scrollWidth > vw + 1 ? document.documentElement.scrollWidth : 0, bad: [...new Set(bad)].slice(0, 6) };
}"""

fails = []


def check(name, cond, extra=""):
    print(("OK    " if cond else "FEHLER"), name, extra)
    if not cond:
        fails.append(name)


make_role_users()
with sync_playwright() as pw:
    for user, label in (("demo", "admin"), ("r_mannschaft", "mannschaft")):
        browser, page = browser_page(pw, user, 390, 844)
        tabs = vm_eval(page, "[...document.querySelectorAll('.sidebar .sidebar-btn')].filter(b => !b.className.includes('d-lg-none')).length")
        names = page.evaluate(r"() => [...document.querySelectorAll('.sidebar .sidebar-btn')].filter(b => !b.className.includes('d-lg-none') && b.tagName === 'BUTTON').map(b => b.innerText.replace(/\s+/g, ' ').trim())")
        problems = []
        for i, name in enumerate(names):
            close_modals(page)
            page.evaluate(r"(n) => { const b = [...document.querySelectorAll('.sidebar .sidebar-btn')].find(x => x.innerText.replace(/\s+/g, ' ').trim() === n); b && b.click(); }", name)
            page.wait_for_timeout(900)
            subs = page.locator(".main-content .nav-link:visible, .main-content .settings-nav button:visible")
            labels = [subs.nth(k).inner_text().strip() for k in range(subs.count())]
            res = page.evaluate(OVERFLOW_JS)
            if res["page"] or res["bad"]:
                problems.append(f"{name}: Seite {res['page']}px, {res['bad']}")
            for k in range(subs.count()):
                try:
                    subs.nth(k).click(timeout=3000)
                except Exception:
                    continue
                page.wait_for_timeout(600)
                res = page.evaluate(OVERFLOW_JS)
                if res["page"] or res["bad"]:
                    problems.append(f"{name} > {labels[k]}: Seite {res['page']}px, {res['bad']}")
        check(f"{label}: auf dem Handy läuft nichts rechts aus dem Bild", not problems, "\n      " + "\n      ".join(problems[:8]))
        browser.close()

print("\n==> ", "ALLES OK" if not fails else f"{len(fails)} FEHLER")
sys.exit(1 if fails else 0)

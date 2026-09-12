#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Prüfwerkzeug für das Digitale Dienstbuch.

    python tools/check.py

Fängt genau die Fehlerklassen ab, die in diesem Projekt wiederholt aufgetreten sind und
die weder der Python-Compiler noch der Browser von sich aus meldet:

  1. Verdeckte Routen      - eine Route wird von einer frueher registrierten, generischeren
                             abgefangen (so entstand der 422-Fehler bei employer-certificates)
  2. Frontend-Vertrag      - fetch()/window.open() zeigen auf eine Route, die es nicht gibt
  3. Vorlagen-Globals      - Vue-Vorlagen nutzen Browser-Globals, die dort NICHT verfuegbar
                             sind (window/navigator/localStorage) -> Klick wirft Fehler
  4. Vorlagen-Verweise     - {{ x }} / v-if="x" auf etwas, das die JS-Datei nicht kennt
  5. this.X()-Aufrufe      - Aufruf einer Methode, die nirgends definiert ist
  6. Reiter-Lader          - ein Menuepunkt laedt Daten, die nach einem Neuladen der Seite
                             niemand holt (Reiter steht dann leer da)
  7. Schreiben ohne commit - INSERT/UPDATE/DELETE ohne conn.commit()
  8. HTML-Struktur         - unbalancierte Tags

Exit-Code 1, sobald etwas gefunden wurde.
"""
import ast
import glob
import os
import re
import sys
import warnings
from html.parser import HTMLParser

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)

findings = []


def report(check, message, location=""):
    findings.append((check, message, location))


def rel(path):
    return path.replace("\\", "/")


# --------------------------------------------------------------------------------------
# Routen einsammeln (echte Starlette-Tabelle, keine Regex-Schaetzung)
# --------------------------------------------------------------------------------------
def collect_routes():
    warnings.filterwarnings("ignore")
    import main

    routes = []

    def walk(route_list):
        for r in route_list:
            # FastAPI haelt eingebundene Router als _IncludedRouter-Huelle vor; die echten
            # APIRoutes haengen an .original_router.routes (bereits mit vollem Pfad).
            if type(r).__name__ == "_IncludedRouter":
                orig = getattr(r, "original_router", None)
                if orig is not None:
                    walk(orig.routes)
                continue
            path = getattr(r, "path", None)
            regex = getattr(r, "path_regex", None)
            if path is None or regex is None:
                continue
            routes.append({
                "path": path,
                "regex": regex,
                "methods": set(getattr(r, "methods", None) or {"GET"}),
                "name": getattr(r, "name", "?"),
                "is_mount": type(r).__name__ == "Mount",
            })

    walk(main.app.routes)
    return routes


def first_match(routes, concrete_path, method):
    for r in routes:
        if r["regex"].fullmatch(concrete_path) and method in r["methods"]:
            return r
    return None


# 1) Verdeckte Routen ---------------------------------------------------------------------
def check_shadowed_routes(routes):
    for r in routes:
        # Mounts matchen nur MIT Unterpfad, ihr eigener Praefix passt nie auf sich selbst -
        # das erzeugt sonst Fehlalarme.
        if r["is_mount"] or "{" in r["path"]:
            continue
        for method in sorted(r["methods"]):
            if method in ("HEAD", "OPTIONS"):
                continue
            winner = first_match(routes, r["path"], method)
            if winner is not None and winner["path"] != r["path"]:
                report(
                    "Verdeckte Route",
                    f"{method} {r['path']} wird von {winner['path']} abgefangen "
                    f"(Funktion {winner['name']}) - diese Route zuerst registrieren",
                )


# 2) Frontend-Vertrag ---------------------------------------------------------------------
FRONTEND_FILES = lambda: [
    f for f in glob.glob("static/**/*.js", recursive=True) + glob.glob("static/**/*.html", recursive=True)
    if "vendor" not in rel(f)
]

FETCH_RE = re.compile(r"fetch\(\s*([^,;]+?)\s*(?:,|\))")
OPEN_RE = re.compile(r"window\.open\(\s*([^,]+?)\s*(?:,|\))")


def _path_candidates(expr):
    """Macht aus dem ersten Argument moegliche Pfade.

    Beide im Projekt genutzten Schreibweisen muessen abgedeckt sein, sonst meldet das
    Werkzeug Fehlalarme:
        `/api/missions/${id}/pdf`            -> /api/missions/1/pdf
        '/api/missions/' + m.id + '/pdf'     -> /api/missions/1/pdf

    Endet der Ausdruck mit einer offenen Verkettung, ist nicht entscheidbar, ob dort noch
    ein Pfadteil ('/groups/' + id) oder nur eine Query ('/api/x' + cacheBuster) folgt -
    dann werden BEIDE Varianten zurueckgegeben und es wird nur gemeldet, wenn keine passt.
    """
    expr = expr.strip()
    if expr.startswith("`") and expr.endswith("`"):
        return [re.sub(r"\$\{[^}]*\}", "1", expr.strip("`")).split("?")[0]]

    literals = [a or b for a, b in re.findall(r"'([^']*)'|\"([^\"]*)\"", expr)]
    if not literals:
        return []
    base = "1".join(literals).split("?")[0]
    if "+" in expr and not expr.rstrip().endswith(("'", '"', "`")):
        return [base, base + "1"]
    return [base]


def check_frontend_contract(routes):
    for f in FRONTEND_FILES():
        try:
            src = open(f, encoding="utf-8").read()
        except OSError:
            continue
        for rx, forced_method in ((FETCH_RE, None), (OPEN_RE, "GET")):
            for m in rx.finditer(src):
                candidates = [p for p in _path_candidates(m.group(1))
                              if p.startswith("/") and not p.startswith("//")]
                if not candidates:
                    continue
                if forced_method:
                    method = "GET"
                else:
                    window = src[m.end(): m.end() + 260]
                    mm = re.search(r"method\s*:\s*['\"](GET|POST|PUT|DELETE|PATCH)['\"]", window)
                    method = mm.group(1) if mm else "GET"
                if any(first_match(routes, p, method) for p in candidates):
                    continue
                line = src[: m.start()].count("\n") + 1
                report("Frontend-Vertrag",
                       f"{method} {candidates[0]} - keine passende Route",
                       f"{rel(f)}:{line}")


# 3+4) Vorlagen ---------------------------------------------------------------------------
# Empirisch mit dem ausgelieferten Vue-Build gemessen: NUR diese Globals sind in Vorlagen
# verfuegbar. window/navigator/localStorage sind dort undefined.
VUE_TEMPLATE_GLOBALS = set(
    "Infinity undefined NaN isFinite isNaN parseFloat parseInt decodeURI decodeURIComponent "
    "encodeURI encodeURIComponent Math Number Date Array Object Boolean String RegExp Map Set "
    "JSON Intl BigInt console Error Symbol".split()
)
FORBIDDEN_IN_TEMPLATE = [
    "window", "navigator", "localStorage", "sessionStorage", "document",
    "location", "alert", "fetch", "setTimeout", "setInterval",
]

BINDING_RE = re.compile(
    r'(?:v-if|v-else-if|v-show|v-model(?:\.\w+)?|v-for|v-html|v-text|:[\w:-]+|@[\w.-]+)\s*=\s*"([^"]*)"'
)
MUSTACHE_RE = re.compile(r"\{\{(.*?)\}\}", re.S)

VUE_PAGES = [
    ("static/dashboard.html", "static/js/dashboard.js"),
    ("static/alarmdisplay.html", "static/alarmdisplay.html"),
    ("static/personnel.html", "static/personnel.html"),
    ("static/editor.html", "static/editor.html"),
]

IGNORE_IDENTS = set(
    """true false null undefined this new typeof instanceof in of return if else for while
    function let const var item index key value event""".split()
)


def _template_expressions(html):
    for m in BINDING_RE.finditer(html):
        yield m.group(1), html[: m.start()].count("\n") + 1
    for m in MUSTACHE_RE.finditer(html):
        yield m.group(1), html[: m.start()].count("\n") + 1


def check_templates():
    for html_file, js_file in VUE_PAGES:
        try:
            html = open(html_file, encoding="utf-8").read()
            js = open(js_file, encoding="utf-8").read()
        except OSError:
            continue

        v_for_locals = set()
        for m in re.finditer(r'v-for\s*=\s*"\s*\(?([^)"]*?)\)?\s+(?:in|of)\s', html):
            for part in m.group(1).split(","):
                v_for_locals.add(part.strip())

        for expr, line in _template_expressions(html):
            clean = re.sub(r"'[^']*'|\"[^\"]*\"|`[^`]*`", " ", expr)

            for name in FORBIDDEN_IN_TEMPLATE:
                if re.search(r"\b" + name + r"\b", clean) and name not in VUE_TEMPLATE_GLOBALS:
                    report("Vorlagen-Global",
                           f"'{name}' ist in Vue-Vorlagen undefined -> Klick wirft einen Fehler. "
                           f"Aufruf in eine Methode verschieben: {' '.join(expr.split())[:70]}",
                           f"{rel(html_file)}:{line}")

            for m in re.finditer(r"\b([A-Za-z_$][A-Za-z0-9_$]*)\b", clean):
                name = m.group(1)
                if m.start() > 0 and clean[m.start() - 1] in ".$":
                    continue
                if (name in IGNORE_IDENTS or name in v_for_locals or name in VUE_TEMPLATE_GLOBALS
                        or len(name) <= 2 or name[0].isupper()):
                    continue
                if re.search(r"\b" + re.escape(name) + r"\s*[:(]", js) or ("this." + name) in js:
                    continue
                report("Vorlagen-Verweis",
                       f"'{name}' kommt in {rel(js_file)} nicht vor",
                       f"{rel(html_file)}:{line}")


# 5) this.X() ------------------------------------------------------------------------------
VUE_BUILTINS = {"$nextTick", "$refs", "$emit", "$el", "$forceUpdate", "$watch", "$set",
                "$data", "$props", "$options", "$parent", "$root", "$attrs", "$slots"}
THIS_CALL_RE = re.compile(r"\bthis\.([A-Za-z_$][\w$]*)\s*\(")


def check_this_calls():
    for _, js_file in VUE_PAGES:
        try:
            src = open(js_file, encoding="utf-8").read()
        except OSError:
            continue
        seen = {}
        for m in THIS_CALL_RE.finditer(src):
            seen.setdefault(m.group(1), src[: m.start()].count("\n") + 1)
        for name, line in sorted(seen.items()):
            if name in VUE_BUILTINS:
                continue
            defined = False
            for dm in re.finditer(r"\b" + re.escape(name) + r"\s*[:(]", src):
                if not src[max(0, dm.start() - 5): dm.start()].endswith("this."):
                    defined = True
                    break
            if not defined:
                report("Aufruf ins Leere", f"this.{name}() ist nirgends definiert",
                       f"{rel(js_file)}:{line}")


# 6) Reiter-Lader --------------------------------------------------------------------------
def check_tab_loaders():
    """Das Nachladen beim Reiterwechsel haengt an genau einer Stelle (loadDataForActiveTab),
    aufgerufen aus dem activeTab-Watcher und aus mounted(). Faellt eine der beiden Verdrahtungen
    weg, steht der betroffene Reiter wieder leer da - genau der Fehler, der lange unentdeckt war.
    """
    try:
        html = open("static/dashboard.html", encoding="utf-8").read()
        js = open("static/js/dashboard.js", encoding="utf-8").read()
    except OSError:
        return

    block = re.search(r"loadDataForActiveTab\((\w*)\)\s*\{(.*?)\n                \},", js, re.S)
    if not block:
        report("Reiter-Lader", "loadDataForActiveTab() nicht gefunden - die Zuordnung fehlt",
               "static/js/dashboard.js")
        return

    mapped = {}
    for m in re.finditer(r"(\w+):\s*\(\)\s*=>\s*\{?([^\n]*)", block.group(2)):
        mapped.setdefault(m.group(1), set()).update(re.findall(r"this\.(\w+)\(", m.group(2)))

    # a) Verdrahtung: Watcher
    watcher = re.search(r"activeTab\((\w+)\)\s*\{(.*?)\n                \}", js, re.S)
    if not watcher or "loadDataForActiveTab" not in watcher.group(2):
        report("Reiter-Lader",
               "Der activeTab-Watcher ruft loadDataForActiveTab() nicht auf - ein Reiterwechsel "
               "laedt dann keine Daten mehr", "static/js/dashboard.js")

    # b) Verdrahtung: Seitenstart
    if "loadDataForActiveTab()" not in js.replace(block.group(0), ""):
        report("Reiter-Lader",
               "mounted() ruft loadDataForActiveTab() nicht auf - nach einem Neuladen bleibt "
               "der wiederhergestellte Reiter leer", "static/js/dashboard.js")

    # c) Zuordnung zeigt auf echte Reiter (Tippfehler im Schluessel faellt sonst nie auf)
    known_tabs = set(re.findall(r"activeTab\s*===?\s*'([^']+)'", html))
    known_tabs |= set(re.findall(r"activeTab\s*=\s*'([^']+)'", html))
    known_tabs |= set(re.findall(r"this\.activeTab\s*=\s*'([^']+)'", js))
    for tab in sorted(mapped):
        if known_tabs and tab not in known_tabs:
            report("Reiter-Lader",
                   f"loadDataForActiveTab() kennt den Reiter '{tab}', den es in der Oberflaeche "
                   f"nicht gibt (Tippfehler?)", "static/js/dashboard.js")

    # d) Lader direkt in einer Menue-Schaltflaeche sind wieder die alte Doppelpflege
    for m in re.finditer(r"""@click="activeTab\s*=\s*'([^']+)';([^"]*)\"""", html):
        calls = [c for c in re.findall(r"([A-Za-z_$][\w$]*)\s*\(", m.group(2))]
        for call in calls:
            report("Reiter-Lader",
                   f"Reiter '{m.group(1)}': {call}() wird direkt in der Schaltflaeche aufgerufen. "
                   f"Gehoert nach loadDataForActiveTab(), sonst laufen die beiden Stellen "
                   f"auseinander.",
                   f"static/dashboard.html:{html[: m.start()].count(chr(10)) + 1}")


# 7) Schreiben ohne commit -----------------------------------------------------------------
WRITE_SQL_RE = re.compile(r"\b(INSERT\s+INTO|UPDATE\s+\w|DELETE\s+FROM|REPLACE\s+INTO)\b", re.I)


def check_missing_commits():
    files = sorted(glob.glob("routers/*.py")) + ["main.py", "core/utils.py"]
    for f in files:
        if not os.path.exists(f):
            continue
        try:
            tree = ast.parse(open(f, encoding="utf-8").read())
        except SyntaxError as e:
            report("Syntaxfehler", str(e), rel(f))
            continue
        for fn in [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]:
            writes, commits = [], False
            for node in ast.walk(fn):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                    if node.func.attr == "commit":
                        commits = True
                    elif node.func.attr in ("execute", "executemany") and node.args:
                        a = node.args[0]
                        txt = None
                        if isinstance(a, ast.Constant) and isinstance(a.value, str):
                            txt = a.value
                        elif isinstance(a, ast.JoinedStr):
                            txt = "".join(v.value for v in a.values
                                          if isinstance(v, ast.Constant) and isinstance(v.value, str))
                        if txt and WRITE_SQL_RE.search(txt):
                            writes.append(node.lineno)
            if writes and not commits:
                report("Kein commit()",
                       f"{fn.name}() schreibt in die DB (Zeile {writes[0]}), ruft aber nie commit() "
                       f"- die Aenderung wird beim Schliessen verworfen",
                       f"{rel(f)}:{fn.lineno}")


# 8) HTML-Struktur --------------------------------------------------------------------------
VOID_TAGS = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta",
             "param", "source", "track", "wbr"}


class BalanceChecker(HTMLParser):
    def __init__(self):
        super().__init__()
        self.stack = []
        self.errors = []

    def handle_starttag(self, tag, attrs):
        if tag not in VOID_TAGS:
            self.stack.append((tag, self.getpos()))

    def handle_startendtag(self, tag, attrs):
        pass

    def handle_endtag(self, tag):
        if not self.stack:
            self.errors.append(f"</{tag}> bei {self.getpos()} ohne oeffnendes Tag")
            return
        top, pos = self.stack[-1]
        if top == tag:
            self.stack.pop()
            return
        for i in range(len(self.stack) - 1, -1, -1):
            if self.stack[i][0] == tag:
                self.errors.append(
                    f"</{tag}> bei {self.getpos()}, erwartet </{top}> (geoeffnet {pos})")
                self.stack = self.stack[:i]
                return
        self.errors.append(f"</{tag}> bei {self.getpos()} ohne passendes oeffnendes Tag")


def check_html_balance():
    for f in sorted(glob.glob("static/*.html")):
        src = open(f, encoding="utf-8").read()
        src = re.sub(r"(<script[^>]*>)(.*?)(</script>)", lambda m: m.group(1) + m.group(3),
                     src, flags=re.S | re.I)
        src = re.sub(r"(<style[^>]*>)(.*?)(</style>)", lambda m: m.group(1) + m.group(3),
                     src, flags=re.S | re.I)
        c = BalanceChecker()
        c.feed(src)
        for e in c.errors:
            report("HTML-Struktur", e, rel(f))
        for tag, pos in c.stack:
            report("HTML-Struktur", f"<{tag}> bei {pos} nie geschlossen", rel(f))


# --------------------------------------------------------------------------------------
def main_():
    print("Digitales Dienstbuch - Pruefung\n" + "=" * 60)

    try:
        routes = collect_routes()
        print(f"  Routen geladen: {len(routes)}")
        check_shadowed_routes(routes)
        check_frontend_contract(routes)
    except Exception as e:
        report("Start", f"App konnte nicht importiert werden: {e}", "main.py")

    check_templates()
    check_this_calls()
    check_tab_loaders()
    check_missing_commits()
    check_html_balance()

    print()
    if not findings:
        print("Keine Auffaelligkeiten.")
        return 0

    by_check = {}
    for check, message, location in findings:
        by_check.setdefault(check, []).append((message, location))
    for check in sorted(by_check):
        print(f"\n[{check}]")
        for message, location in by_check[check]:
            print(f"  - {message}")
            if location:
                print(f"      {location}")
    print(f"\n{len(findings)} Auffaelligkeit(en).")
    return 1


if __name__ == "__main__":
    sys.exit(main_())

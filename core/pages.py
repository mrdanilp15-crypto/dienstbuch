"""Setzt das Dashboard aus seinen Abschnitten zusammen.

templates/dashboard/index.html ist das Gerüst; jede Zeile `<!-- @teil name -->` wird durch
templates/dashboard/name.html ersetzt. Der Browser bekommt eine einzige fertige Seite, wie vorher.
Die Skripte zu den Bereichen liegen in static/js/dashboard/ (siehe dashboard.js).
"""
import os
import re

TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates", "dashboard")
_PART = re.compile(r"<!-- @teil ([\w-]+) -->\r?\n")
_cache = {"stamp": None, "html": ""}


def _read(name: str) -> str:
    # newline="" : Zeilenenden unverändert lassen
    with open(os.path.join(TEMPLATE_DIR, f"{name}.html"), encoding="utf-8", newline="") as f:
        return f.read()


def dashboard_html() -> str:
    """Neu zusammengesetzt wird nur, wenn sich eine der Dateien geändert hat."""
    stamp = tuple(sorted((e.name, e.stat().st_mtime_ns) for e in os.scandir(TEMPLATE_DIR) if e.name.endswith(".html")))
    if stamp != _cache["stamp"]:
        _cache["html"] = _PART.sub(lambda m: _read(m.group(1)), _read("index"))  # fehlt ein Teil: lauter Fehler statt Lücke
        _cache["stamp"] = stamp
    return _cache["html"]

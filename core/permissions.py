"""Einstellbare Rechte pro Rolle.

Bisher standen die erlaubten Rollen fest im Code. Hier steht pro Recht, wer es standardmäßig hat; der Admin kann
das im Bereich "Rechte" für jede Rolle ändern. Nur Abweichungen vom Standard werden in der Tabelle
role_permissions gespeichert. Admins haben immer alle Rechte (so kann man sich nicht aussperren).

Neues Recht: unten in PERMISSIONS eintragen und an der Stelle, die es schützt, check_auth(request, permission="...")
bzw. has_permission(user, "...") verwenden - und im Frontend (dashboard.js, hasPerm) abfragen.
"""
import time

from database import get_db_connection

ROLES = ("mannschaft", "geratewart", "jugendwarte", "atemschutzwart", "gruppenfuehrer", "leitung", "admin")
ROLE_LABELS = {
    "mannschaft": "Mannschaft", "geratewart": "Gerätewart", "jugendwarte": "Jugendwart",
    "atemschutzwart": "Atemschutzwart", "gruppenfuehrer": "Gruppenführer", "leitung": "Leitung", "admin": "Admin",
}

PERMISSIONS = [
    {"key": "personnel.manage", "group": "Personal", "label": "Personalakten bearbeiten",
     "hint": "Mitglieder anlegen und ändern, Lehrgänge, Beförderungen und Ehrungen eintragen",
     "roles": ("admin", "leitung")},
    {"key": "youth.manage", "group": "Personal", "label": "Jugendfeuerwehr verwalten",
     "hint": "Jugendmitglieder, Jugenddienste und Abzeichen",
     "roles": ("admin", "leitung", "jugendwarte")},
    {"key": "missions.manage", "group": "Einsatz", "label": "Einsatzberichte bearbeiten und freigeben",
     "hint": "Einsätze anlegen, ändern, löschen und unterschreiben, Schnellfreigabe",
     "roles": ("admin", "leitung", "gruppenfuehrer")},
    {"key": "billing.manage", "group": "Einsatz", "label": "Abrechnung und Entschädigung",
     "hint": "Rechnungen an Verursacher, Arbeitgeberbescheinigungen, Entschädigungsliste",
     "roles": ("admin", "leitung", "geratewart")},
    {"key": "material.manage", "group": "Material", "label": "Material, Fahrzeuge und Mängel verwalten",
     "hint": "Geräte anlegen, prüfen, archivieren, Mängel bearbeiten, Fahrzeuge pflegen",
     "roles": ("admin", "leitung", "geratewart")},
]

_BY_KEY = {p["key"]: p for p in PERMISSIONS}
_TTL = 15
_cache = {"data": None, "ts": 0.0}


def valid_permission(key: str) -> bool:
    return key in _BY_KEY


def invalidate_permissions_cache():
    _cache["data"] = None


def _overrides() -> dict:
    now = time.time()
    if _cache["data"] is not None and now - _cache["ts"] < _TTL:
        return _cache["data"]
    data = {}
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT role, permission, allowed FROM role_permissions")
        for r, p, a in cur.fetchall():
            data[(r, p)] = bool(a)
        cur.close(); conn.close()
    except Exception as e:
        print(f"Rechte-Abweichungen nicht lesbar, Standardrechte gelten: {e}")
        return _cache["data"] or {}
    _cache["data"], _cache["ts"] = data, now
    return data


def default_allowed(role: str, key: str) -> bool:
    p = _BY_KEY.get(key)
    return bool(p) and role in p["roles"]


def has_permission(user_or_role, key: str) -> bool:
    role = user_or_role.get("role") if isinstance(user_or_role, dict) else user_or_role
    if role == "admin":
        return True
    if key not in _BY_KEY:
        return False
    ov = _overrides().get((role, key))
    return default_allowed(role, key) if ov is None else ov


def permissions_for(role: str) -> list:
    return [p["key"] for p in PERMISSIONS if has_permission(role, p["key"])]


def matrix() -> dict:
    ov = _overrides()
    roles = [r for r in ROLES]
    out = []
    for p in PERMISSIONS:
        out.append({
            "key": p["key"], "group": p["group"], "label": p["label"], "hint": p["hint"],
            "roles": {r: has_permission(r, p["key"]) for r in roles},
            "defaults": {r: default_allowed(r, p["key"]) for r in roles},
            "changed": {r: ((r, p["key"]) in ov) for r in roles},
        })
    return {"roles": [{"key": r, "label": ROLE_LABELS[r]} for r in roles], "permissions": out}

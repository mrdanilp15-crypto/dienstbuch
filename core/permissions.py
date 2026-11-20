"""Einstellbare Rechte pro Rolle - auch für selbst angelegte Rollen.

Bisher standen die erlaubten Rollen fest im Code. Hier steht pro Recht, wer es standardmäßig hat; der Admin kann
das im Bereich "Rechte" für jede Rolle ändern. Nur Abweichungen vom Standard werden in der Tabelle
role_permissions gespeichert. Admins haben immer alle Rechte (so kann man sich nicht aussperren).

Eigene Rollen (Tabelle custom_roles) bauen auf einer Basisrolle auf: users.role bleibt die Basisrolle (dadurch
gelten alle Ansichtsregeln der Basisrolle unverändert), users.custom_role wählt zusätzlich die eigene Rechte-Matrix.
Ohne Abweichung hat eine eigene Rolle genau die Rechte ihrer Basisrolle.

Neues Recht: unten in PERMISSIONS eintragen und an der Stelle, die es schützt, check_auth(request, permission="...")
bzw. has_permission(user, "...") verwenden - und im Frontend (dashboard.js, hasPerm) abfragen.
"""
import time

from database import get_db_connection

ROLES = ("mannschaft", "geratewart", "jugendwarte", "atemschutzwart", "gruppenfuehrer", "leitung", "admin")
BASE_ROLES = tuple(r for r in ROLES if r != "admin")
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
_cache = {"ov": None, "roles": None, "ts": 0.0}


def valid_permission(key: str) -> bool:
    return key in _BY_KEY


def invalidate_permissions_cache():
    _cache["ov"] = None
    _cache["roles"] = None


def _load():
    now = time.time()
    if _cache["ov"] is not None and now - _cache["ts"] < _TTL:
        return
    ov, roles = {}, {}
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT role_key, label, base_role FROM custom_roles ORDER BY label")
        for k, label, base in cur.fetchall():
            roles[k] = {"label": label, "base": base}
        cur.execute("SELECT role, permission, allowed FROM role_permissions")
        for r, p, a in cur.fetchall():
            ov[(r, p)] = bool(a)
        cur.close(); conn.close()
    except Exception as e:
        print(f"Rechte-Abweichungen nicht lesbar, Standardrechte gelten: {e}")
        if _cache["ov"] is None:
            return
        return
    _cache["ov"], _cache["roles"], _cache["ts"] = ov, roles, now


def _overrides() -> dict:
    _load()
    return _cache["ov"] or {}


def custom_roles() -> dict:
    """key -> {"label", "base"}"""
    _load()
    return _cache["roles"] or {}


def role_key_valid(key: str) -> bool:
    return key in ROLES or key in custom_roles()


def default_allowed(role: str, key: str) -> bool:
    p = _BY_KEY.get(key)
    if not p:
        return False
    base = custom_roles().get(role, {}).get("base", role)
    return base in p["roles"]


def has_permission(user_or_role, key: str) -> bool:
    if isinstance(user_or_role, dict):
        role, custom = user_or_role.get("role"), user_or_role.get("custom_role")
    else:
        role, custom = user_or_role, None
    if role == "admin":
        return True
    if key not in _BY_KEY:
        return False
    effective = custom if (custom and custom in custom_roles()) else role
    ov = _overrides().get((effective, key))
    return default_allowed(effective, key) if ov is None else ov


def users_with(key: str) -> list:
    """Benutzernamen aller Konten, die ein Recht haben (für Benachrichtigungen an die Zuständigen)."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT username, role, custom_role FROM users")
        rows = cur.fetchall()
        cur.close(); conn.close()
    except Exception as e:
        print(f"Konten nicht lesbar: {e}")
        return []
    return [u for u, r, c in rows if has_permission({"role": r, "custom_role": c}, key)]


def roles_with(key: str) -> list:
    return [r for r in ROLES if has_permission(r, key)]


def permissions_for(user_or_role) -> list:
    return [p["key"] for p in PERMISSIONS if has_permission(user_or_role, p["key"])]


def matrix() -> dict:
    ov = _overrides()
    cr = custom_roles()
    roles = [{"key": r, "label": ROLE_LABELS[r], "custom": False, "base": r} for r in ROLES]
    roles += [{"key": k, "label": v["label"], "custom": True, "base": v["base"], "base_label": ROLE_LABELS.get(v["base"], v["base"])}
              for k, v in cr.items()]
    out = []
    for p in PERMISSIONS:
        out.append({
            "key": p["key"], "group": p["group"], "label": p["label"], "hint": p["hint"],
            "roles": {r["key"]: has_permission(r["key"], p["key"]) for r in roles},
            "defaults": {r["key"]: default_allowed(r["key"], p["key"]) for r in roles},
            "changed": {r["key"]: ((r["key"], p["key"]) in ov) for r in roles},
        })
    return {"roles": roles, "permissions": out}

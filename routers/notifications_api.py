from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from core.notify import ALWAYS_PUSH, CATEGORIES
from core.utils import get_current_user
from database import get_db_connection

router = APIRouter(prefix="/api/notifications", tags=["notifications"])

PAGE = 8


def _user(request: Request):
    u = get_current_user(request)
    if not u:
        raise HTTPException(status_code=401, detail="Nicht angemeldet")
    return u["username"]


def _fmt(r):
    return {"id": r["id"], "title": r["title"], "body": r["body"], "link": r["link"],
            "created_at": r["created_at"].strftime("%d.%m.%Y"), "age_min": int(r["age_min"] or 0), "read": r["read_at"] is not None}


_COLS = "id, title, body, link, created_at, read_at, TIMESTAMPDIFF(MINUTE, created_at, NOW()) AS age_min"


@router.get("")
def overview(request: Request):
    """Nur Kategorien mit Eintraegen; je Kategorie die neuesten PAGE Stueck."""
    me = _user(request)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT category, COUNT(*) AS total, SUM(read_at IS NULL) AS unread FROM notifications WHERE username=%s GROUP BY category", (me,))
    stats = {r["category"]: r for r in cur.fetchall()}
    cats = []
    for key, (label, icon) in CATEGORIES.items():
        st = stats.get(key)
        if not st:
            continue
        cur.execute(f"SELECT {_COLS} FROM notifications WHERE username=%s AND category=%s ORDER BY id DESC LIMIT {PAGE}", (me, key))
        cats.append({"key": key, "label": label, "icon": icon, "total": int(st["total"]), "unread": int(st["unread"] or 0),
                     "items": [_fmt(r) for r in cur.fetchall()]})
    cur.close(); conn.close()
    return {"unread": sum(c["unread"] for c in cats), "categories": cats}


@router.get("/count")
def count(request: Request):
    me = _user(request)
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM notifications WHERE username=%s AND read_at IS NULL", (me,))
    n = cur.fetchone()[0]
    cur.close(); conn.close()
    return {"unread": n}


@router.get("/category/{key}")
def more(key: str, request: Request, before_id: Optional[int] = None):
    me = _user(request)
    if key not in CATEGORIES:
        raise HTTPException(status_code=404, detail="Unbekannte Kategorie")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute(f"SELECT {_COLS} FROM notifications WHERE username=%s AND category=%s AND id < %s ORDER BY id DESC LIMIT {PAGE}",
                (me, key, before_id or 2**31 - 1))
    rows = [_fmt(r) for r in cur.fetchall()]
    cur.close(); conn.close()
    return rows


@router.post("/read-all")
def read_all(request: Request, category: Optional[str] = None):
    me = _user(request)
    conn = get_db_connection(); cur = conn.cursor()
    if category:
        cur.execute("UPDATE notifications SET read_at=NOW() WHERE username=%s AND category=%s AND read_at IS NULL", (me, category))
    else:
        cur.execute("UPDATE notifications SET read_at=NOW() WHERE username=%s AND read_at IS NULL", (me,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}


@router.delete("/category/{key}")
def clear_category(key: str, request: Request, only_read: bool = True):
    me = _user(request)
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM notifications WHERE username=%s AND category=%s" + (" AND read_at IS NOT NULL" if only_read else ""), (me, key))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}


@router.get("/prefs")
def get_prefs(request: Request):
    me = _user(request)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT category, push FROM notification_prefs WHERE username=%s", (me,))
    saved = {r["category"]: bool(r["push"]) for r in cur.fetchall()}
    cur.close(); conn.close()
    return [{"key": k, "label": v[0], "push": True if k in ALWAYS_PUSH else saved.get(k, True), "locked": k in ALWAYS_PUSH}
            for k, v in CATEGORIES.items()]


class PrefIn(BaseModel):
    push: bool


@router.put("/prefs/{key}")
def set_pref(key: str, data: PrefIn, request: Request):
    me = _user(request)
    if key not in CATEGORIES or key in ALWAYS_PUSH:
        raise HTTPException(status_code=400, detail="Diese Kategorie lässt sich nicht abschalten.")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO notification_prefs (username, category, push) VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE push=%s",
                (me, key, 1 if data.push else 0, 1 if data.push else 0))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}


@router.post("/{nid}/read")
def read_one(nid: int, request: Request):
    me = _user(request)
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("UPDATE notifications SET read_at=NOW() WHERE id=%s AND username=%s AND read_at IS NULL", (nid, me))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

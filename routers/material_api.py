from fastapi import APIRouter, HTTPException, Request

from database import get_db_connection
from core.utils import get_current_user, log_audit_action
from core.permissions import has_permission, users_with

router = APIRouter()

DEFECT_STATES = ("Offen", "In Bearbeitung", "Erledigt")


def _notify_defect(title, body, report_id, usernames=None, roles=None):
    """Meldung in der Glocke (Kategorie Mängel); Fehler beim Benachrichtigen dürfen die Aktion nie abbrechen."""
    try:
        from core.notify import notify
        notify("maengel", title, body, f"/dashboard?open=maengel:{report_id}", usernames=usernames, roles=roles)
    except Exception as e:
        print(f"Mangel-Benachrichtigung fehlgeschlagen: {e}")


# --- MÄNGELMELDER (GERÄTEWART - ERWEITERT) ---
@router.post("/api/material/defect-reports")
def create_defect_report(data: dict, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    eq_id = data.get("equipment_id")
    desc = data.get("description", "").strip()
    severity = data.get("severity", "Mittel")
    image_url = data.get("image_url")
    assigned_to = data.get("assigned_to")
    priority = data.get("priority", "Mittel")
    if not eq_id or not desc:
        raise HTTPException(status_code=400, detail="Gerät und Beschreibung erforderlich.")
    reporter = data.get("reporter_name") or user["username"]
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO equipment_defect_reports (equipment_id, reporter_name, description, severity, image_url, assigned_to, priority) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (eq_id, reporter, desc, severity, image_url, assigned_to, priority)
    )
    new_id = cur.lastrowid
    cur.execute("SELECT name FROM equipment WHERE id = %s", (eq_id,))
    eq_row = cur.fetchone()
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "MANGEL_MELDEN", f"Mangel für Gerät-ID {eq_id} gemeldet: {desc[:60]}")
    _notify_defect("Neuer Mangel: " + (eq_row[0] if eq_row else f"Gerät {eq_id}"), desc[:200], new_id,
                   usernames=users_with("material.manage"))
    return {"status": "success"}

@router.get("/api/material/defect-reports")
def get_defect_reports(request: Request, status: str = "Offen"):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    if status == "alle":
        cur.execute("""
            SELECT dr.*, COALESCE(e.name, CONCAT('Fahrzeug: ', v.name)) as equipment_name, e.barcode,
                   (SELECT COUNT(*) FROM defect_comments c WHERE c.report_id = dr.id) AS comment_count
            FROM equipment_defect_reports dr
            LEFT JOIN equipment e ON dr.equipment_id = e.id
            LEFT JOIN vehicles v ON v.id = dr.vehicle_id
            ORDER BY dr.created_at DESC LIMIT 100
        """)
    elif status == "Offen":
        # "Offen" heißt hier "noch nicht erledigt" und umfasst bewusst auch "In Bearbeitung" -
        # sonst verschwand ein Mangel beim Setzen auf "In Bearbeitung" sofort aus der
        # Offen-Ansicht und war dort nicht mehr auffindbar, um ihn später als erledigt zu markieren.
        cur.execute("""
            SELECT dr.*, COALESCE(e.name, CONCAT('Fahrzeug: ', v.name)) as equipment_name, e.barcode,
                   (SELECT COUNT(*) FROM defect_comments c WHERE c.report_id = dr.id) AS comment_count
            FROM equipment_defect_reports dr
            LEFT JOIN equipment e ON dr.equipment_id = e.id
            LEFT JOIN vehicles v ON v.id = dr.vehicle_id
            WHERE dr.status != 'Erledigt'
            ORDER BY dr.created_at DESC LIMIT 100
        """)
    else:
        cur.execute("""
            SELECT dr.*, COALESCE(e.name, CONCAT('Fahrzeug: ', v.name)) as equipment_name, e.barcode,
                   (SELECT COUNT(*) FROM defect_comments c WHERE c.report_id = dr.id) AS comment_count
            FROM equipment_defect_reports dr
            LEFT JOIN equipment e ON dr.equipment_id = e.id
            LEFT JOIN vehicles v ON v.id = dr.vehicle_id
            WHERE dr.status = %s
            ORDER BY dr.created_at DESC LIMIT 100
        """, (status,))
    rows = cur.fetchall(); cur.close(); conn.close()
    for r in rows:
        for k in ["created_at", "resolved_at"]:
            if r.get(k): r[k] = str(r[k])
    return rows

@router.put("/api/material/defect-reports/{report_id}")
def resolve_defect_report(report_id: int, data: dict, request: Request):
    user = get_current_user(request)
    if not user or not has_permission(user, "material.manage"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    new_status = data.get("status", "Erledigt")
    if new_status not in DEFECT_STATES:
        raise HTTPException(status_code=400, detail="Ungültiger Status.")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT status, assigned_to, reporter_name FROM equipment_defect_reports WHERE id = %s", (report_id,))
    old = cur.fetchone()
    if not old:
        cur.close(); conn.close()
        raise HTTPException(status_code=404, detail="Mangel-Meldung nicht gefunden")
    # Wer den Status aendert und noch niemand zugewiesen ist, uebernimmt den Mangel
    assigned = data.get("assigned_to") if "assigned_to" in data else old.get("assigned_to")
    if "assigned_to" not in data and new_status == "In Bearbeitung" and not assigned:
        assigned = user["username"]
    assigned = (assigned or "").strip()[:255] or None
    if new_status == "Erledigt":
        cur.execute(
            "UPDATE equipment_defect_reports SET status = %s, assigned_to = %s, resolved_by = %s, resolved_at = NOW() WHERE id = %s",
            (new_status, assigned, user["username"], report_id)
        )
    else:
        # Wieder geöffnet oder in Bearbeitung: Erledigt-Vermerk entfernen
        cur.execute(
            "UPDATE equipment_defect_reports SET status = %s, assigned_to = %s, resolved_by = NULL, resolved_at = NULL WHERE id = %s",
            (new_status, assigned, report_id)
        )
    if old["status"] != new_status:
        cur.execute("INSERT INTO defect_comments (report_id, author, text) VALUES (%s, %s, %s)",
                    (report_id, user["username"], f"Status: {old['status']} → {new_status}"))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "MANGEL_ERLEDIGT", f"Mangel-Meldung ID {report_id} als '{new_status}' markiert.")
    if old["status"] != new_status and old.get("reporter_name") and old["reporter_name"] != user["username"]:
        _notify_defect(f"Mangel ist jetzt: {new_status}", f"Deine Mangelmeldung wurde von {user['username']} auf „{new_status}“ gesetzt.",
                       report_id, usernames=[old["reporter_name"]])
    return {"status": "success"}

@router.get("/api/material/defect-reports/{report_id}/comments")
def list_defect_comments(report_id: int, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, author, text, created_at FROM defect_comments WHERE report_id = %s ORDER BY id ASC", (report_id,))
    rows = cur.fetchall(); cur.close(); conn.close()
    for r in rows:
        r["created_at"] = str(r["created_at"])
    return rows

@router.post("/api/material/defect-reports/{report_id}/comments")
def add_defect_comment(report_id: int, data: dict, request: Request):
    """Rückfrage oder Antwort zu einem Mangel: Gerätewart/Leitung/Admin und wer den Mangel gemeldet hat."""
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    text = (data.get("text") or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="Bitte einen Text eingeben.")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT reporter_name FROM equipment_defect_reports WHERE id = %s", (report_id,))
    rep = cur.fetchone()
    if not rep:
        cur.close(); conn.close()
        raise HTTPException(status_code=404, detail="Mangel-Meldung nicht gefunden")
    is_staff = has_permission(user, "material.manage")
    if not is_staff and (rep["reporter_name"] or "").strip().lower() != user["username"].strip().lower():
        cur.close(); conn.close()
        raise HTTPException(status_code=403, detail="Nur Gerätewart, Leitung und die meldende Person dürfen hier schreiben.")
    cur.execute("INSERT INTO defect_comments (report_id, author, text) VALUES (%s, %s, %s)", (report_id, user["username"], text[:2000]))
    conn.commit(); cur.close(); conn.close()
    if rep["reporter_name"] and rep["reporter_name"].strip().lower() == user["username"].strip().lower():
        _notify_defect("Antwort zu einem Mangel", f"{user['username']}: {text[:160]}", report_id, usernames=users_with("material.manage"))
    elif rep["reporter_name"]:
        _notify_defect("Mangel wurde beantwortet", f"{user['username']}: {text[:160]}", report_id, usernames=[rep["reporter_name"]])
    return {"status": "success"}

@router.delete("/api/material/defect-reports/{report_id}")
def delete_defect_report(report_id: int, request: Request):
    user = get_current_user(request)
    if not user or not has_permission(user, "material.manage"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM defect_comments WHERE report_id = %s", (report_id,))
    cur.execute("DELETE FROM equipment_defect_reports WHERE id = %s", (report_id,))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "MANGEL_GELOESCHT", f"Mangel-Meldung ID {report_id} gelöscht.")
    return {"status": "success"}

# --- DRONE MAP IMAGES ---
@router.get("/api/material/drone-images")
def get_drone_images(request: Request):
    check_auth = get_current_user(request)
    if not check_auth: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM drone_images ORDER BY id DESC")
    res = cur.fetchall(); cur.close(); conn.close()
    return res

@router.post("/api/material/drone-images")
def add_drone_image(data: dict, request: Request):
    user = get_current_user(request)
    if not user or not has_permission(user, "material.manage"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    url = data.get("url")
    if not url: raise HTTPException(status_code=400, detail="URL erforderlich")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO drone_images (url) VALUES (%s)", (url,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

@router.delete("/api/material/drone-images/{img_id}")
def delete_drone_image(img_id: int, request: Request):
    user = get_current_user(request)
    if not user or not has_permission(user, "material.manage"):
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM drone_images WHERE id = %s", (img_id,))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}


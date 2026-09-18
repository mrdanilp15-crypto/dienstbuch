from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, date
import json
import mysql.connector

from database import get_db_connection
from core.utils import log_audit_action, hash_password, get_current_user, invalidate_role_cache

router = APIRouter()


def _jsonable(row):
    """Wandelt date/datetime-Werte in einem dictionary()-Cursor-Ergebnis in Strings um,
    damit json.dumps() nicht mit einem TypeError abbricht."""
    return {k: (str(v) if isinstance(v, (date, datetime)) else v) for k, v in row.items()}

class UserCreateRequest(BaseModel):
    username: str
    password: str
    role: str
    personnel_id: Optional[int] = None

@router.get("/api/users/list")
def list_users(request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, username, role, is_first_login, personnel_id, last_login, failed_logins, lockout_until FROM users ORDER BY username ASC")
    users = cur.fetchall(); cur.close(); conn.close()
    for u in users:
        u["last_login"] = str(u["last_login"]) if u["last_login"] else None
        u["is_locked"] = bool(u["lockout_until"] and u["lockout_until"] > datetime.now())
        u["lockout_until"] = str(u["lockout_until"]) if u["lockout_until"] else None
    return users

@router.put("/api/users/{user_id}/unlock")
def unlock_user_account(user_id: int, request: Request):
    """Hebt die automatische Konto-Sperre (nach 5 Fehlversuchen, siehe auth_mgr.py) vorzeitig
    auf. Ohne das musste ein ausgesperrter Nutzer bisher die volle Sperrzeit (15 Min.) abwarten,
    selbst wenn ein Admin sofort daneben stand und die Ursache (z.B. Zahlendreher) klären konnte."""
    user = get_current_user(request)
    if not user or user["role"] != "admin": raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT username FROM users WHERE id = %s", (user_id,))
    target_user = cur.fetchone()
    if not target_user: cur.close(); conn.close(); raise HTTPException(status_code=404, detail="Benutzer nicht gefunden")

    cur.execute("UPDATE users SET failed_logins = 0, lockout_until = NULL WHERE id = %s", (user_id,))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "KONTO_ENTSPERRT", f"Login-Sperre für '{target_user['username']}' manuell aufgehoben.")
    return {"status": "success", "message": f"Sperre für '{target_user['username']}' aufgehoben."}


@router.post("/api/users/add")
def add_user(data: UserCreateRequest, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": raise HTTPException(status_code=403, detail="Keine Berechtigung")
    username_clean = data.username.strip()
    if not username_clean:
        raise HTTPException(status_code=400, detail="Benutzername darf nicht leer sein!")
    if len(data.password) < 8:
        raise HTTPException(status_code=400, detail="Passwort muss mindestens 8 Zeichen lang sein!")
    conn = get_db_connection(); cur = conn.cursor()
    try:
        p_hash = hash_password(data.password)
        cur.execute("INSERT INTO users (username, password_hash, role, is_first_login, personnel_id) VALUES (%s, %s, %s, 1, %s)", (username_clean, p_hash, data.role, data.personnel_id or None))
        conn.commit()
        log_audit_action(user["username"], "NUTZER_ANLEGEN", f"Konto für '{username_clean}' verknüpft mit Personal-ID {data.personnel_id} erstellt.")
    except mysql.connector.Error as err:
        if err.errno == 1062: raise HTTPException(status_code=400, detail="Benutzername existiert bereits!")
        raise HTTPException(status_code=500, detail=str(err))
    finally: cur.close(); conn.close()
    return {"status": "success"}

@router.put("/api/users/{user_id}/role")
def update_user_role(user_id: int, data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": raise HTTPException(status_code=403, detail="Keine Berechtigung")
    new_role = data.get("role")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("SELECT username FROM users WHERE id = %s", (user_id,))
    row = cur.fetchone()
    cur.execute("UPDATE users SET role = %s WHERE id = %s", (new_role, user_id))
    conn.commit(); cur.close(); conn.close()
    if row:
        invalidate_role_cache(row[0])
        log_audit_action(user["username"], "ROLLE_GEÄNDERT", f"Rolle von '{row[0]}' auf '{new_role}' geändert.")
    return {"status": "success"}

@router.put("/api/users/{user_id}/personnel")
def update_user_personnel_relation(user_id: int, data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": 
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    new_pid = data.get("personnel_id")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("UPDATE users SET personnel_id = %s WHERE id = %s", (new_pid, user_id))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "VERKNÜPFUNG_ÄNDERN", f"User-ID {user_id} wurde mit Personal-ID {new_pid} verknüpft.")
    return {"status": "success"}

@router.put("/api/users/{user_id}/password")
def change_user_password(user_id: int, data: dict, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": raise HTTPException(status_code=403, detail="Keine Berechtigung")
    new_pw = (data.get("password") or "").strip()
    if len(new_pw) < 8:
        raise HTTPException(status_code=400, detail="Passwort muss mindestens 8 Zeichen lang sein!")
    p_hash = hash_password(new_pw)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT username FROM users WHERE id = %s", (user_id,))
    target_user = cur.fetchone()
    cur.execute("UPDATE users SET password_hash = %s, is_first_login = 1 WHERE id = %s", (p_hash, user_id))
    conn.commit(); cur.close(); conn.close()
    if target_user:
        invalidate_role_cache(target_user["username"])
        log_audit_action(user["username"], "PASSWORT_GESETZT", f"Passwort für '{target_user['username']}' durch Admin gesetzt.")
    return {"status": "success"}

@router.delete("/api/users/{user_id}")
def delete_user(user_id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("SELECT username FROM users WHERE id = %s", (user_id,))
    row = cur.fetchone()
    cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
    conn.commit(); cur.close(); conn.close()
    if row:
        invalidate_role_cache(row[0])
        log_audit_action(user["username"], "KONTO_GELÖSCHT", f"System-Login '{row[0]}' gelöscht.")
    return {"status": "success"}

@router.get("/api/users/me/stats")
def get_my_global_fire_stats(year: int, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT p.name, u.personnel_id FROM users u LEFT JOIN personnel p ON u.personnel_id = p.id WHERE u.username = %s", (user["username"],))
    res = cur.fetchone()
    
    klarnat_name = None
    if res and res["name"]:
        klarnat_name = res["name"]
    else:
        cur.execute("SELECT id, name FROM personnel WHERE LOWER(name) LIKE %s", (f"%{user['username'].lower()}%",))
        fallback = cur.fetchone()
        if fallback:
            cur.execute("UPDATE users SET personnel_id = %s WHERE username = %s", (fallback["id"], user["username"]))
            conn.commit()
            klarnat_name = fallback["name"]
            
    if not klarnat_name:
        cur.close(); conn.close()
        return {"hours": 0, "count": 0, "unlinked": True}
        
    # WICHTIG: exakter Namensabgleich, kein LIKE '%Name%'-Fallback - sonst würden z.B.
    # "Max" und "Maximilian" fälschlich zusammengezählt.
    query = """
        SELECT
            COALESCE((
                SELECT SUM(s.duration)
                FROM attendance a
                JOIN sessions s ON a.session_id = s.id
                JOIN persons p ON a.person_id = p.id
                WHERE p.name = %s AND YEAR(s.date) = %s AND a.is_present = 1
            ), 0) as session_hours,
            COALESCE((
                SELECT COUNT(DISTINCT s.id)
                FROM attendance a
                JOIN sessions s ON a.session_id = s.id
                JOIN persons p ON a.person_id = p.id
                WHERE p.name = %s AND YEAR(s.date) = %s AND a.is_present = 1
            ), 0) as session_count,
            COALESCE((
                SELECT SUM(m.duration)
                FROM mission_attendance ma
                JOIN missions m ON ma.mission_id = m.id
                JOIN personnel pl ON ma.personnel_id = pl.id
                WHERE LOWER(TRIM(pl.name)) = LOWER(TRIM(%s)) AND YEAR(m.date) = %s AND ma.is_present NOT IN ('Nein', '0', 'false', 'False', '') AND ma.is_present IS NOT NULL
            ), 0) as mission_hours,
            COALESCE((
                SELECT COUNT(DISTINCT m.id)
                FROM mission_attendance ma
                JOIN missions m ON ma.mission_id = m.id
                JOIN personnel pl ON ma.personnel_id = pl.id
                WHERE LOWER(TRIM(pl.name)) = LOWER(TRIM(%s)) AND YEAR(m.date) = %s AND ma.is_present NOT IN ('Nein', '0', 'false', 'False', '') AND ma.is_present IS NOT NULL
            ), 0) as mission_count
    """
    cur.execute(query, (klarnat_name, year, klarnat_name, year, klarnat_name, year, klarnat_name, year))
    stats = cur.fetchone(); cur.close(); conn.close()
    
    total_hours = float(stats["session_hours"] or 0) + float(stats["mission_hours"] or 0)
    total_count = (stats["session_count"] or 0) + (stats["mission_count"] or 0)

    return {
        "hours": round(total_hours, 1),
        "count": total_count,
        "session_hours": float(stats["session_hours"] or 0),
        "mission_hours": float(stats["mission_hours"] or 0),
        "unlinked": False,
        "name": klarnat_name
    }

@router.get("/api/users/me/sessions")
def get_my_sessions(year: int, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT p.name FROM users u LEFT JOIN personnel p ON u.personnel_id = p.id WHERE u.username = %s", (user["username"],))
    res = cur.fetchone()
    if not res or not res["name"]:
        cur.close(); conn.close()
        return []
    
    klarnat_name = res["name"]
    query = """
        SELECT s.date, s.category, s.description, s.duration
        FROM attendance a 
        JOIN sessions s ON a.session_id = s.id 
        JOIN persons p ON a.person_id = p.id
        WHERE p.name = %s AND YEAR(s.date) = %s AND a.is_present = 1
    """
    cur.execute(query, (klarnat_name, year))
    sessions = cur.fetchall()
    
    try:
        cur.execute("""
            SELECT m.date, 'Einsatz' as category, CONCAT(m.stichwort, ': ', m.meldung, ' (', m.adresse, ')') as description, m.duration
            FROM mission_attendance ma
            JOIN missions m ON ma.mission_id = m.id
            JOIN personnel pl ON ma.personnel_id = pl.id
            WHERE LOWER(TRIM(pl.name)) = LOWER(TRIM(%s)) AND YEAR(m.date) = %s AND ma.is_present NOT IN ('Nein', '0', 'false', 'False', '') AND ma.is_present IS NOT NULL
        """, (klarnat_name, year))
        m_sessions = cur.fetchall()
        sessions.extend(m_sessions)
    except Exception as e:
        print(f"My mission sessions fetch error: {e}")
        
    cur.close(); conn.close()
    
    from datetime import date
    sessions.sort(key=lambda x: str(x['date']), reverse=True)
    for s in sessions:
        if isinstance(s['date'], date):
            s['date'] = str(s['date'])
        s['duration'] = float(s['duration'] or 2.0)
    return sessions

@router.put("/api/users/me/bind-personnel")
def bind_self_personnel(data: dict, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    pid = data.get("personnel_id")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    # Ohne diese beiden Prüfungen konnte sich jeder angemeldete Nutzer (auch niedrigste Rolle)
    # mit einer BELIEBIGEN personnel_id verknüpfen - auch mit der eines anderen Kameraden. Die
    # "Meine Stunden/Statistik"-Ansicht löst die Identität über personnel_id auf, das hätte also
    # fremde Dienststunden/Historie unter dem eigenen Account sichtbar gemacht (Identitätsverwechslung).
    cur.execute("SELECT id FROM personnel WHERE id = %s", (pid,))
    if not cur.fetchone():
        cur.close(); conn.close()
        raise HTTPException(status_code=404, detail="Person nicht gefunden")
    cur.execute("SELECT username FROM users WHERE personnel_id = %s AND username != %s", (pid, user["username"]))
    conflict = cur.fetchone()
    if conflict:
        cur.close(); conn.close()
        raise HTTPException(status_code=409, detail="Diese Person ist bereits mit einem anderen Login verknüpft.")
    cur.execute("UPDATE users SET personnel_id = %s WHERE username = %s", (pid, user["username"]))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "SELBST_VERKNÜPFUNG", f"Eigenes Konto mit Personal-ID {pid} verknüpft.")
    return {"status": "success"}

# --- DSGVO-SELBSTBEDIENUNGS-EXPORT (Art. 15 Auskunft / Art. 20 Datenübertragbarkeit) -----
# Bewusst für JEDEN angemeldeten Nutzer (nicht nur Admins): jedes Mitglied hat das Recht,
# seine eigenen gespeicherten Daten in einem gängigen, maschinenlesbaren Format zu bekommen,
# ohne dafür einen Admin bitten zu müssen. Fasst alles zusammen, was an personenbezogenen
# Daten unter dem eigenen Konto bzw. der verknüpften Personalakte gespeichert ist.
@router.get("/api/users/me/data-export")
def export_my_data(request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht angemeldet")

    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    export = {"exportiert_am": datetime.now().isoformat(), "hinweis":
              "Vollständiger Export aller unter diesem Konto gespeicherten personenbezogenen "
              "Daten (Art. 15/20 DSGVO). Einträge aus Einsatz-/Atemschutzhistorie bleiben "
              "aus Dokumentationspflicht auch nach einem Austritt bestehen."}

    cur.execute("SELECT username, role, personnel_id, last_login, is_first_login, "
               "failed_logins FROM users WHERE username = %s", (user["username"],))
    account = cur.fetchone()
    export["konto"] = _jsonable(account) if account else None
    personnel_id = account.get("personnel_id") if account else None
    personnel_name = None

    if personnel_id:
        cur.execute("SELECT * FROM personnel WHERE id = %s", (personnel_id,))
        p = cur.fetchone()
        if p:
            p.pop("id", None)
            export["personalakte"] = _jsonable(p)
            personnel_name = p.get("name")

        cur.execute("""
            SELECT m.date, m.stichwort, ma.is_present, ma.vehicle
            FROM mission_attendance ma JOIN missions m ON ma.mission_id = m.id
            WHERE ma.personnel_id = %s ORDER BY m.date DESC
        """, (personnel_id,))
        export["einsatz_teilnahmen"] = [_jsonable(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT m.date, m.stichwort, r.druck_start, r.druck_10, r.druck_20, r.druck_ende,
                   r.dauer, r.fit_ok
            FROM respiration_log r JOIN missions m ON r.mission_id = m.id
            WHERE r.personnel_id = %s ORDER BY m.date DESC
        """, (personnel_id,))
        export["atemschutz_einsaetze"] = [dict(_jsonable(r), fit_ok=bool(r["fit_ok"])) for r in cur.fetchall()]

        cur.execute("SELECT course_name, date, valid_until, certificate_url FROM lehrgaenge "
                   "WHERE personnel_id = %s ORDER BY date DESC", (personnel_id,))
        export["lehrgaenge"] = [_jsonable(r) for r in cur.fetchall()]

        cur.execute("SELECT item_name, size, issue_date, return_date FROM personal_inventar "
                   "WHERE personnel_id = %s", (personnel_id,))
        export["ausgegebene_ausruestung"] = [_jsonable(r) for r in cur.fetchall()]

    if personnel_name:
        cur.execute("""
            SELECT s.date, s.category, s.description, a.is_present, a.vehicle, a.note
            FROM attendance a JOIN sessions s ON a.session_id = s.id
            JOIN persons p ON a.person_id = p.id
            WHERE p.name = %s ORDER BY s.date DESC
        """, (personnel_name,))
        export["dienst_teilnahmen"] = [_jsonable(r) for r in cur.fetchall()]

    cur.execute("SELECT title, content, visibility, created_at FROM notes "
               "WHERE username = %s ORDER BY created_at DESC", (user["username"],))
    export["eigene_notizen"] = [_jsonable(r) for r in cur.fetchall()]

    # Nur die letzten 500 Protokolleinträge - der Audit-Log selbst bleibt als
    # Nachweispflicht bestehen, hier geht es um Auskunft an die betroffene Person.
    cur.execute("SELECT created_at, action, details FROM audit_log "
               "WHERE username = %s ORDER BY created_at DESC LIMIT 500", (user["username"],))
    export["protokollierte_eigene_aktionen"] = [_jsonable(r) for r in cur.fetchall()]

    cur.close(); conn.close()
    log_audit_action(user["username"], "DATENAUSKUNFT_EXPORT", "Eigene Daten exportiert (Art. 15/20 DSGVO).")

    filename = f"meine_daten_{user['username']}_{datetime.now().strftime('%Y-%m-%d')}.json"
    return Response(
        content=json.dumps(export, ensure_ascii=False, indent=2, default=str),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

# --- "WAS IST NEU" -----------------------------------------------------------------------
# Bewusst nur für Admins: die Änderungen betreffen überwiegend Verwaltungsfunktionen, und ein
# ungefragtes Popup für jeden Nutzer bei jedem Update wäre für die normale Mannschaft eher
# störend als hilfreich.
@router.get("/api/users/me/changelog")
def get_changelog(request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Nur für Admins")
    from core.changelog import CHANGELOG, latest_changelog_id
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT last_seen_changelog_id FROM users WHERE username = %s", (user["username"],))
    row = cur.fetchone()
    cur.close(); conn.close()
    last_seen = (row or {}).get("last_seen_changelog_id") or 0
    return {"entries": CHANGELOG, "unseen": last_seen < latest_changelog_id()}

@router.post("/api/users/me/changelog/seen")
def mark_changelog_seen(request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Nur für Admins")
    from core.changelog import latest_changelog_id
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("UPDATE users SET last_seen_changelog_id = %s WHERE username = %s",
               (latest_changelog_id(), user["username"]))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

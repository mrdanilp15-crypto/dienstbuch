from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
import mysql.connector

from database import get_db_connection
from core.utils import log_audit_action, hash_password, get_current_user, invalidate_role_cache

router = APIRouter()

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

@router.put("/api/users/{user_id}/reset-password")
def admin_reset_user_password(user_id: int, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": raise HTTPException(status_code=403, detail="Keine Berechtigung")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT username FROM users WHERE id = %s", (user_id,))
    target_user = cur.fetchone()
    if not target_user: cur.close(); conn.close(); raise HTTPException(status_code=404, detail="Benutzer nicht gefunden")
    
    p_hash = hash_password("admin123")
    cur.execute("UPDATE users SET password_hash = %s, is_first_login = 1 WHERE id = %s", (p_hash, user_id))
    conn.commit(); cur.close(); conn.close()
    invalidate_role_cache(target_user["username"])
    log_audit_action(user["username"], "PASSWORT_RESET", f"Passwort für '{target_user['username']}' auf 'admin123' zurückgesetzt.")
    return {"status": "success", "message": "Passwort auf 'admin123' zurückgesetzt. Erstanmeldung erforderlich."}

@router.post("/api/users/add")
def add_user(data: UserCreateRequest, request: Request):
    user = get_current_user(request)
    if not user or user["role"] != "admin": raise HTTPException(status_code=403, detail="Keine Berechtigung")
    username_clean = data.username.strip()
    if not username_clean:
        raise HTTPException(status_code=400, detail="Benutzername darf nicht leer sein!")
    if len(data.password) < 6:
        raise HTTPException(status_code=400, detail="Passwort muss mindestens 6 Zeichen lang sein!")
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
    if row: invalidate_role_cache(row[0])
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
    if len(new_pw) < 6:
        raise HTTPException(status_code=400, detail="Passwort muss mindestens 6 Zeichen lang sein!")
    p_hash = hash_password(new_pw)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT username FROM users WHERE id = %s", (user_id,))
    target_user = cur.fetchone()
    cur.execute("UPDATE users SET password_hash = %s, is_first_login = 1 WHERE id = %s", (p_hash, user_id))
    conn.commit(); cur.close(); conn.close()
    if target_user: invalidate_role_cache(target_user["username"])
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
    if row: invalidate_role_cache(row[0])
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
    conn = get_db_connection(); cur = conn.cursor()
    cur.execute("UPDATE users SET personnel_id = %s WHERE username = %s", (pid, user["username"]))
    conn.commit(); cur.close(); conn.close()
    return {"status": "success"}

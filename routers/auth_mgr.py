from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import Optional

from database import get_db_connection
from core.utils import log_audit_action, verify_password, hash_password, create_session_token, get_current_user

router = APIRouter()

class LoginRequest(BaseModel):
    username: str
    password: str

@router.post("/api/login")
def api_login(data: LoginRequest, response: Response):
    username_clean = data.username.strip()
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE LOWER(username) = LOWER(%s)", (username_clean,))
    user = cur.fetchone()
    
    if user:
        if user["lockout_until"] and datetime.now() < user["lockout_until"]:
            cur.close(); conn.close()
            remaining = (user["lockout_until"] - datetime.now()).seconds // 60 + 1
            log_audit_action("SYSTEM", "LOGIN_VERSUCH_GESPERRT", f"Anmeldeversuch auf gesperrtes Konto '{username_clean}'.")
            raise HTTPException(status_code=423, detail=f"Konto gesperrt. Bitte in {remaining} Min. versuchen.")
            
        if verify_password(user['password_hash'], data.password):
            cur.execute("UPDATE users SET failed_logins = 0, lockout_until = NULL WHERE id = %s", (user["id"],))
            conn.commit(); cur.close(); conn.close()
            
            token = create_session_token(user['username'], user['role'])
            response.set_cookie(key="session_token", value=token, httponly=True, max_age=30*24*60*60, samesite="lax")
            log_audit_action(user['username'], "LOGIN", "Erfolgreich eingeloggt.")
            return {"status": "success", "username": user['username'], "role": user['role'], "is_first_login": bool(user['is_first_login']), "redirect": "/dashboard"}
        else:
            failed = user["failed_logins"] + 1
            lockout = datetime.now() + timedelta(minutes=15) if failed >= 5 else None
            cur.execute("UPDATE users SET failed_logins = %s, lockout_until = %s WHERE id = %s", (failed, lockout, user["id"],))
            conn.commit(); cur.close(); conn.close()
            if failed >= 5:
                log_audit_action("SYSTEM", "KONTO_GESPERRT", f"Konto '{username_clean}' wegen zu vieler Fehllogins für 15 Min. gesperrt.")
                raise HTTPException(status_code=423, detail="Konto wegen zu vieler Fehllogins für 15 Min. gesperrt.")
            log_audit_action("SYSTEM", "LOGIN_FEHLVERSUCH", f"Falsches Passwort für Benutzer '{username_clean}' ({failed}/5).")
            raise HTTPException(status_code=401, detail=f"Passwort falsch! ({failed}/5)")
    else:
        cur.close(); conn.close()
        log_audit_action("SYSTEM", "LOGIN_BENUTZER_UNBEKANNT", f"Anmeldeversuch mit nicht existierendem Namen '{username_clean}'.")
        raise HTTPException(status_code=401, detail="Benutzername existiert nicht!")

@router.get("/api/auth/me")
def api_auth_me(request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT u.is_first_login, u.personnel_id, p.name as personnel_name 
        FROM users u 
        LEFT JOIN personnel p ON u.personnel_id = p.id 
        WHERE u.username = %s
    """, (user["username"],))
    db_user = cur.fetchone(); cur.close(); conn.close()
    
    is_first = bool(db_user["is_first_login"]) if db_user else False
    p_id = db_user["personnel_id"] if db_user else None
    p_name = db_user["personnel_name"] if db_user else None
    return {"username": user["username"], "role": user["role"], "is_first_login": is_first, "personnel_id": p_id, "personnel_name": p_name}

@router.post("/api/logout")
def api_logout(response: Response):
    response.delete_cookie("session_token", path="/")
    return {"status": "success"}

@router.put("/api/auth/change-password")
def user_change_self_password(data: dict, request: Request):
    user = get_current_user(request)
    if not user: raise HTTPException(status_code=401, detail="Nicht angemeldet")
    old_pw = data.get("old_password")
    new_pw = data.get("new_password")
    
    if not old_pw or not new_pw or len(new_pw.strip()) < 4:
        raise HTTPException(status_code=400, detail="Eingaben ungültig oder Passwort zu kurz!")
        
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT password_hash FROM users WHERE username = %s", (user["username"],))
    db_user = cur.fetchone()
    
    if not db_user or not verify_password(db_user['password_hash'], old_pw):
        cur.close(); conn.close()
        raise HTTPException(status_code=400, detail="Das aktuelle Passwort ist nicht korrekt!")
        
    new_hash = hash_password(new_pw.strip())
    cur.execute("UPDATE users SET password_hash = %s, is_first_login = 0 WHERE username = %s", (new_hash, user["username"]))
    conn.commit(); cur.close(); conn.close()
    log_audit_action(user["username"], "PASSWORT_ÄNDERUNG", "Eigenes Passwort erfolgreich aktualisiert.")
    return {"status": "success"}

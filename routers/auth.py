import time
import hashlib
import secrets
import hmac
import base64
import json
import os
from fastapi import APIRouter, Request, Response, HTTPException
from database import get_db_connection

# Wir nutzen das Präfix /api, damit die Pfade exakt zum Dashboard passen (/api/login etc.)
router = APIRouter(prefix="/api", tags=["Authentication & Users"])

SECRET_KEY = os.getenv("SECRET_KEY", "digitales-dienstbuch-global-sovereign-key-112")

# --- MODUL-INTERNE SICHERHEITS-SCHLÜSSEL ---
def hash_password(p: str) -> str:
    s = secrets.token_hex(16)
    return f"{s}:{hashlib.pbkdf2_hmac('sha256', p.encode(), s.encode(), 100000).hex()}"

def verify_password(stored, prov) -> bool:
    try:
        s, h = stored.split(":")
        return hashlib.pbkdf2_hmac('sha256', prov.encode(), s.encode(), 100000).hex() == h
    except:
        return False

def create_token(u: str, r: str) -> str:
    p = base64.b64encode(json.dumps({"u": u, "r": r, "t": time.time()}).encode()).decode()
    return f"{p}.{hmac.new(SECRET_KEY.encode(), p.encode(), hashlib.sha256).hexdigest()}"

def get_current_user(req: Request):
    t = req.cookies.get("session_token")
    if not t:
        return None
    try:
        p, sig = t.split(".")
        if hmac.compare_digest(sig, hmac.new(SECRET_KEY.encode(), p.encode(), hashlib.sha256).hexdigest()):
            return json.loads(base64.b64decode(p).decode())
    except:
        return None

def parse_val(v):
    if v == "" or v == "null" or v is None or v == 0: 
        return None
    return v

# --- ROUTE 1: SYSTEM LOGIN ---
@router.post("/login")
async def api_login(r: Request, res: Response):
    d = await r.json()
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE username = %s", (d.get('username', '').strip(),))
    u = cur.fetchone()
    
    if not u or not verify_password(u['password_hash'], d.get('password', '')):
        cur.close()
        c.close()
        raise HTTPException(status_code=401, detail="Zugangsdaten ungültig")
        
    token = create_token(u['username'], u['role'])
    res.set_cookie(key="session_token", value=token, httponly=True, samesite="lax")
    cur.close()
    c.close()
    return {"status": "success", "redirect": "/dashboard"}

# --- ROUTE 2: SYSTEM LOGOUT ---
@router.post("/logout")
def api_logout(res: Response):
    res.delete_cookie("session_token")
    return {"status": "success"}

# --- ROUTE 3: SITZUNGS-ABFRAGE (Wer bin ich?) ---
@router.get("/auth/me")
def api_me(r: Request):
    u = get_current_user(r)
    if not u:
        raise HTTPException(status_code=401, detail="Nicht eingeloggt")
        
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    # Holt Nutzernamen, Rolle und verknüpften Realnamen aus der Personalakte
    query = """
        SELECT u.username, u.role, u.personnel_id, p.name as personnel_name, p.rank 
        FROM users u 
        LEFT JOIN personnel p ON u.personnel_id = p.id 
        WHERE u.username = %s
    """
    cur.execute(query, (u['u'],))
    res = cur.fetchone()
    cur.close()
    c.close()
    return res

# --- ROUTE 4: SYSTEM-ZUGÄNGE AUFLISTEN ---
@router.get("/users")
def list_users(r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    cur.execute("SELECT id, username, role, personnel_id FROM users ORDER BY username ASC")
    res = cur.fetchall()
    cur.close()
    c.close()
    return res

# --- ROUTE 5: LOGINS ERSTELLEN ODER BEARBEITEN ---
@router.post("/users")
async def save_user(r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401)
    d = await r.json()
    c = get_db_connection()
    cur = c.cursor()
    
    u_id = d.get('id')
    p_id = parse_val(d.get('personnel_id'))
    pw = d.get('password') or ""
    role = d.get('role', 'user')
    uname = d.get('username', '').strip()
    
    if u_id:
        if pw.strip():
            # Passwort wurde geändert
            cur.execute("UPDATE users SET role=%s, personnel_id=%s, password_hash=%s WHERE id=%s", (role, p_id, hash_password(pw), u_id))
        else:
            # Passwort bleibt unangetastet
            cur.execute("UPDATE users SET role=%s, personnel_id=%s WHERE id=%s", (role, p_id, u_id))
    else:
        # Komplett neuen User anlegen
        cur.execute("INSERT INTO users (username, password_hash, role, personnel_id) VALUES (%s,%s,%s,%s)", (uname, hash_password(pw), role, p_id))
        
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

# --- ROUTE 6: SYSTEMLOGIN ENTFERNEN ---
@router.delete("/users/{u_id}")
def del_user(u_id: int, r: Request):
    if not get_current_user(r):
        raise HTTPException(status_code=401)
    c = get_db_connection()
    cur = c.cursor()
    cur.execute("DELETE FROM users WHERE id = %s", (u_id,))
    c.commit()
    cur.close()
    c.close()
    return {"status": "success"}

@router.put("/api/users/password/self")
async def change_password_self(r: Request):
    user = get_current_user(r)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht autorisiert")
    
    d = await r.json()
    new_pw = d.get("password")
    if not new_pw or len(new_pw.strip()) < 4:
        raise HTTPException(status_code=400, detail="Passwort zu kurz!")
        
    from database import hash_password
    h = hash_password(new_pw.strip())
    
    try:
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("UPDATE users SET password_hash = %s WHERE username = %s", (h, user.get("u")))
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/auth/me")
def api_auth_me(r: Request):
    user = get_current_user(r)
    if not user:
        return None
        
    c = get_db_connection()
    cur = c.cursor(dictionary=True)
    
    # REPARATUR: Lädt lückenlos alle PSA-Größen und das Profilbild des verknüpften Kameraden
    query = """
        SELECT u.username, u.role, u.personnel_id, 
               p.name as personnel_name, p.profile_picture,
               p.size_helm, p.size_jacke, p.size_stiefel
        FROM users u
        LEFT JOIN personnel p ON u.personnel_id = p.id
        WHERE u.username = %s
    """
    cur.execute(query, (user.get("u"),))
    res = cur.fetchone()
    cur.close()
    c.close()
    return res
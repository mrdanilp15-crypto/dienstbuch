import os
import hmac
import hashlib
import json
import base64
from fastapi import APIRouter, Request, HTTPException, Response
from database import get_db_connection

SECRET_KEY = os.getenv("SECRET_KEY", "digitales-dienstbuch-global-sovereign-key-112").encode()

router = APIRouter(tags=["Authentifizierung"])

def sign_data(data: dict) -> str:
    json_str = json.dumps(data)
    encoded = base64.b64encode(json_str.encode()).decode()
    signature = hmac.new(SECRET_KEY, encoded.encode(), hashlib.sha256).hexdigest()
    return f"{encoded}.{signature}"

def verify_signature(token: str) -> dict:
    try:
        encoded, signature = token.split(".")
        expected_sig = hmac.new(SECRET_KEY, encoded.encode(), hashlib.sha256).hexdigest()
        if hmac.compare_digest(signature, expected_sig):
            json_str = base64.b64decode(encoded.encode()).decode()
            return json.loads(json_str)
    except:
        pass
    return None

def get_current_user(r: Request):
    token = r.cookies.get("session_token")
    if not token:
        return None
    return verify_signature(token)

@router.post("/api/login")
async def api_login(r: Request, response: Response):
    try:
        d = await r.json()
        username = d.get("username", "").strip()
        password = d.get("password", "").strip()
        
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        cur.execute("SELECT * FROM users WHERE username = %s", (username,))
        user_row = cur.fetchone()
        cur.close()
        c.close()
        
        if not user_row:
            raise HTTPException(status_code=401, detail="Falscher Benutzername oder Passwort")
        
        db_hash = user_row["password_hash"]
        try:
            salt, key_hex = db_hash.split(":")
            input_hash = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000).hex()
            match = hmac.compare_digest(key_hex, input_hash)
        except:
            match = False
            
        if not match:
            raise HTTPException(status_code=401, detail="Falscher Benutzername oder Passwort")
            
        token_data = {"u": user_row["username"], "r": user_row["role"]}
        token = sign_data(token_data)
        response.set_cookie(key="session_token", value=token, httponly=True, samesite="lax")
        return {"status": "success", "role": user_row["role"]}
    except Exception as e:
        if isinstance(e, HTTPException): raise e
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/api/logout")
def api_logout(response: Response):
    response.delete_cookie("session_token")
    return {"status": "success"}

@router.get("/api/auth/me")
def api_auth_me(r: Request):
    user = get_current_user(r)
    if not user:
        return None
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
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
        
        if res and res.get('profile_picture') and not res['profile_picture'].startswith('data:'):
            res['profile_picture'] = f"data:image/jpeg;base64,{res['profile_picture']}"
        return res
    except:
        return {"username": user.get("u"), "role": user.get("r"), "personnel_id": 0}

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
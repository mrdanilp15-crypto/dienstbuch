from fastapi import APIRouter, Request, HTTPException
from database import get_db_connection
from routers.auth import get_current_user

router = APIRouter(prefix="/api/users", tags=["System-Logins"])

# --- ROUTE 1: BENUTZER AUFLISTEN ---
@router.get("")
def list_users(r: Request):
    user = get_current_user(r)
    if not user or user.get("r") != "admin":
        raise HTTPException(status_code=403, detail="Nicht autorisiert")
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        cur.execute("SELECT id, username, role, personnel_id FROM users ORDER BY username ASC")
        res = cur.fetchall()
        cur.close()
        c.close()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- ROUTE 2: BENUTZER SPEICHERN (POST für Neuanlagen) ---
@router.post("")
async def save_user(r: Request):
    user = get_current_user(r)
    if not user or user.get("r") != "admin":
        raise HTTPException(status_code=403, detail="Nicht autorisiert")
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        
        u_id = d.get('id')
        p_id = d.get('personnel_id') if d.get('personnel_id') != 0 else None
        
        if u_id:
            if d.get('password'):
                from database import hash_password
                h = hash_password(d.get('password').strip())
                cur.execute("UPDATE users SET role=%s, personnel_id=%s, password_hash=%s WHERE id=%s", (d.get('role'), p_id, h, u_id))
            else:
                cur.execute("UPDATE users SET role=%s, personnel_id=%s WHERE id=%s", (d.get('role'), p_id, u_id))
        else:
            from database import hash_password
            h = hash_password(d.get('password', 'Feuerwehr112!').strip())
            cur.execute("INSERT INTO users (username, password_hash, role, personnel_id) VALUES (%s, %s, %s, %s)", (d.get('username'), h, d.get('role'), p_id))
            
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- NEUE ROUTE 2b: LOGIN BEARBEITEN VIA PUT (Löst die Dashboard-Blockade) ---
@router.put("/{user_id}")
async def update_user_put(user_id: int, r: Request):
    user = get_current_user(r)
    if not user or user.get("r") != "admin":
        raise HTTPException(status_code=403, detail="Nicht autorisiert")
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        
        p_id = d.get('personnel_id') if d.get('personnel_id') != 0 else None
        
        # Falls ein neues Passwort im Modal eingetippt wurde, wird es gehasht und überschrieben
        if d.get('password'):
            from database import hash_password
            h = hash_password(d.get('password').strip())
            cur.execute("UPDATE users SET role=%s, personnel_id=%s, password_hash=%s WHERE id=%s", (d.get('role'), p_id, h, user_id))
        else:
            # Ansonsten werden nur Rolle und die verknüpfte Personal-ID aktualisiert
            cur.execute("UPDATE users SET role=%s, personnel_id=%s WHERE id=%s", (d.get('role'), p_id, user_id))
            
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- ROUTE 3: BENUTZER LÖSCHEN ---
@router.delete("/{user_id}")
def delete_user(user_id: int, r: Request):
    user = get_current_user(r)
    if not user or user.get("r") != "admin":
        raise HTTPException(status_code=403, detail="Nicht autorisiert")
    try:
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
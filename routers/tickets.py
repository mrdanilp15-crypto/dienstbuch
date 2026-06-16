from fastapi import APIRouter, Request, HTTPException
from database import get_db_connection
from routers.auth import get_current_user

router = APIRouter(prefix="/api/tickets", tags=["Tickets & Mängelberichte"])

def parse_val(v):
    if v == "" or v == "null" or v is None or v == 0 or v == "0":
        return None
    if isinstance(v, str):
        return v.strip()
    return v

# --- ROUTE 1: MÄNGEL ABFRAGEN (Mit intelligenter Rollen-Filterung) ---
@router.get("")
def list_tickets(r: Request):
    user = get_current_user(r)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht autorisiert")
    
    role = user.get("r", "mannschaft")
    username = user.get("u")

    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        
        if role == "mannschaft":
            # Mannschaft sieht NUR die eigenen erfassten Mängel
            query = """
                SELECT t.*, v.name as vehicle_name, i.item_name 
                FROM tickets t 
                LEFT JOIN vehicles v ON t.vehicle_id = v.id 
                LEFT JOIN inventory i ON t.inventory_id = i.id 
                LEFT JOIN users u ON t.created_by = u.id
                WHERE u.username = %s
                ORDER BY t.id DESC
            """
            cur.execute(query, (username,))
        else:
            # Admins und Gerätewarte sehen lückenlos alle Meldungen
            query = """
                SELECT t.*, v.name as vehicle_name, i.item_name 
                FROM tickets t 
                LEFT JOIN vehicles v ON t.vehicle_id = v.id 
                LEFT JOIN inventory i ON t.inventory_id = i.id 
                ORDER BY t.id DESC
            """
            cur.execute(query)
            
        res = cur.fetchall()
        cur.close()
        c.close()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Laden der Tickets: {str(e)}")

# --- ROUTE 2: MANGEL ERFASSEN (Auto-Zuweisung des Erstellers) ---
@router.post("")
async def create_ticket(r: Request):
    user = get_current_user(r)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht autorisiert")
    
    username = user.get("u")
    d = await r.json()
    
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        
        # Interne ID des aktuell eingeloggten Users ermitteln
        cur.execute("SELECT id FROM users WHERE username = %s", (username,))
        u_row = cur.fetchone()
        user_id = u_row['id'] if u_row else None
        
        v_id = parse_val(d.get('vehicle_id'))
        i_id = parse_val(d.get('inventory_id'))
        
        query = """
            INSERT INTO tickets (title, content, vehicle_id, inventory_id, priority, status, created_by) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        params = (d.get('title'), d.get('content'), v_id, i_id, d.get('priority', 'normal'), d.get('status', 'neu'), user_id)
        cur.execute(query, params)
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Erstellen: {str(e)}")

# --- ROUTE 3: STATUS-SCHIEBER (Gesperrt für normale Mannschaft) ---
@router.put("/{t_id}/status")
async def update_ticket_status(t_id: int, r: Request):
    user = get_current_user(r)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht autorisiert")
    
    role = user.get("r", "mannschaft")
    if role not in ["admin", "geratewart"]:
        raise HTTPException(status_code=403, detail="Nur Gerätewarte und Admins dürfen den Status ändern.")
        
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("UPDATE tickets SET status = %s WHERE id = %s", (d.get('status'), t_id))
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- ROUTE 4: LÖSCHEN (Gesperrt für normale Mannschaft) ---
@router.delete("/{t_id}")
def del_ticket(t_id: int, r: Request):
    user = get_current_user(r)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht autorisiert")
    
    role = user.get("r", "mannschaft")
    if role not in ["admin", "geratewart"]:
        raise HTTPException(status_code=403, detail="Keine Berechtigung zum Löschen von Tickets.")
        
    try:
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("DELETE FROM tickets WHERE id = %s", (t_id,))
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
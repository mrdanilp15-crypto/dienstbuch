from fastapi import APIRouter, Request, HTTPException
from database import get_db_connection

# Das Präfix sorgt dafür, dass alle Endpunkte unter /api/tickets erreichbar sind
router = APIRouter(prefix="/api/tickets", tags=["Tickets & Mängelberichte"])

def parse_val(v):
    """Überführt ungültige Frontend-IDs (wie "" oder 0) sauber in SQL-NULL-Werte."""
    if v == "" or v == "null" or v is None or v == 0 or v == "0":
        return None
    if isinstance(v, str):
        return v.strip()
    return v

# --- ROUTE 1: MÄNGELBERICHTE AUFLISTEN (Inkl. Fahrzeug- & Material-Klarnamen) ---
@router.get("")
def list_tickets(r: Request):
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
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
        raise HTTPException(status_code=500, detail=f"Fehler beim Laden der Mängelberichte: {str(e)}")

# --- ROUTE 2: MANGEL NEU ERFASSEN ---
@router.post("")
async def create_ticket(r: Request):
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        
        # Sicherstellen, dass leere Verknüpfungen als NULL in die DB laufen
        v_id = parse_val(d.get('vehicle_id'))
        i_id = parse_val(d.get('inventory_id'))
        
        query = """
            INSERT INTO tickets (title, content, vehicle_id, inventory_id, priority, status) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = (
            d.get('title'),
            d.get('content'),
            v_id,
            i_id,
            d.get('priority', 'normal'),
            d.get('status', 'neu')
        )
        cur.execute(query, params)
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Erstellen des Mängelberichts: {str(e)}")

# --- ROUTE 3: KANBAN-STATUS AKTUALISIEREN (Verschieben in Werkstatt / Erledigt) ---
@router.put("/{t_id}/status")
async def update_ticket_status(t_id: int, r: Request):
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        
        query = "UPDATE tickets SET status = %s WHERE id = %s"
        cur.execute(query, (d.get('status'), t_id))
        
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Aktualisieren des Ticket-Status: {str(e)}")

# --- ROUTE 4: TICKET ENDGÜLTIG LÖSCHEN ---
@router.delete("/{t_id}")
def del_ticket(t_id: int, r: Request):
    try:
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("DELETE FROM tickets WHERE id = %s", (t_id,))
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Löschen des Tickets: {str(e)}")
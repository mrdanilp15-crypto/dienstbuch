from fastapi import APIRouter, Request, HTTPException
from database import get_db_connection
from routers.auth import get_current_user

router = APIRouter(prefix="/api/events", tags=["Termine & Veranstaltungen"])

def parse_val(v):
    if v == "" or v == "null" or v is None or v == 0 or v == "0":
        return None
    if isinstance(v, str):
        return v.strip()
    return v

# --- ROUTE 1: ALLE TERMINE / VERANSTALTUNGEN LADEN ---
@router.get("")
def list_events(r: Request):
    user = get_current_user(r)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht autorisiert")
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        # DATE_FORMAT sorgt für ein sauberes, browserkompatibles Datumsformat im JSON
        query = """
            SELECT id, title, description, 
                   DATE_FORMAT(date, '%Y-%m-%d') as date, 
                   location, event_type 
            FROM events 
            ORDER BY date ASC, id ASC
        """
        cur.execute(query)
        res = cur.fetchall()
        cur.close()
        c.close()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Laden der Termine: {str(e)}")

# --- ROUTE 2: NEUEN TERMIN ANLEGEN (POST) ---
@router.post("")
async def create_event(r: Request):
    user = get_current_user(r)
    if not user or user.get("r") not in ["admin", "geratewart"]:
        raise HTTPException(status_code=403, detail="Keine Berechtigung zum Erstellen von Terminen.")
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        
        params = (
            d.get('title'),
            parse_val(d.get('description')),
            d.get('date'),
            parse_val(d.get('location', 'Wache')),
            d.get('event_type', 'Übung')
        )
        
        query = """
            INSERT INTO events (title, description, date, location, event_type) 
            VALUES (%s, %s, %s, %s, %s)
        """
        cur.execute(query, params)
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Eintragen des Termins: {str(e)}")

# --- ROUTE 3: TERMIN BEARBEITEN VIA PUT (Verhindert den 405-Error auf dem Dashboard) ---
@router.put("/{event_id}")
async def update_event_put(event_id: int, r: Request):
    user = get_current_user(r)
    if not user or user.get("r") not in ["admin", "geratewart"]:
        raise HTTPException(status_code=403, detail="Keine Berechtigung zum Bearbeiten von Terminen.")
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        
        params = (
            d.get('title'),
            parse_val(d.get('description')),
            d.get('date'),
            parse_val(d.get('location')),
            d.get('event_type'),
            event_id
        )
        
        query = """
            UPDATE events 
            SET title=%s, description=%s, date=%s, location=%s, event_type=%s 
            WHERE id=%s
        """
        cur.execute(query, params)
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Aktualisieren des Termins via PUT: {str(e)}")

# --- ROUTE 4: TERMIN ABSAGEN / LÖSCHEN (DELETE) ---
@router.delete("/{event_id}")
def delete_event(event_id: int, r: Request):
    user = get_current_user(r)
    if not user or user.get("r") not in ["admin", "geratewart"]:
        raise HTTPException(status_code=403, detail="Keine Berechtigung zum Löschen von Terminen.")
    try:
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("DELETE FROM events WHERE id = %s", (event_id,))
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Entfernen des Termins: {str(e)}")
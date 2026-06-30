from fastapi import APIRouter, Request, HTTPException
from database import get_db_connection
from routers.auth import get_current_user

router = APIRouter(prefix="/api/hydranten", tags=["Wasserentnahme & Hydrantenkarte"])

def parse_val(v):
    if v == "" or v == "null" or v is None or v == 0 or v == "0":
        return None
    if isinstance(v, str):
        return v.strip()
    return v

# --- ROUTE 1: ALLE WASSERENTNAHMESTELLEN FÜR DIE KARTE LADEN ---
@router.get("")
def list_hydrants(r: Request):
    user = get_current_user(r)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht autorisiert")
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        # Holt alle Hydrantendaten inklusive geografischer GPS-Koordinaten
        cur.execute("SELECT id, lat, lon, hydrant_type, diameter FROM hydranten")
        res = cur.fetchall()
        cur.close()
        c.close()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Laden der Hydrantenkarte: {str(e)}")

# --- ROUTE 2: NEUEN HYDRANTEN PER KLICK ANLEGEN (POST) ---
@router.post("")
async def save_hydrant(r: Request):
    user = get_current_user(r)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht autorisiert")
        
    # Normale Mannschaft darf keine neuen Löschwasserpunkte in die Karte zeichnen
    role = user.get("r", "mannschaft")
    if role == "mannschaft":
        raise HTTPException(status_code=403, detail="Keine Berechtigung zum Eintragen von Hydranten.")
        
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        
        query = """
            INSERT INTO hydranten (lat, lon, hydrant_type, diameter)
            VALUES (%s, %s, %s, %s)
        """
        params = (
            float(d.get('lat')),
            float(d.get('lon')),
            d.get('hydrant_type', 'Unterflurhydrant'),
            d.get('diameter', 'DN80')
        )
        cur.execute(query, params)
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Speichern der Wasserentnahmestelle: {str(e)}")

# --- ROUTE 3: WASSERENTNAHMESTELLE AUS DER KARTE LÖSCHEN (DELETE) ---
@router.delete("/{hydrant_id}")
def delete_hydrant(hydrant_id: int, r: Request):
    user = get_current_user(r)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht autorisiert")
        
    role = user.get("r", "mannschaft")
    if role == "mannschaft":
        raise HTTPException(status_code=403, detail="Keine Berechtigung zum Löschen von Hydranten.")
        
    try:
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("DELETE FROM hydranten WHERE id = %s", (hydrant_id,))
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Entfernen des Hydranten: {str(e)}")
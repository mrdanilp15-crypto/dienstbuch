from fastapi import APIRouter, Request, HTTPException
from database import get_db_connection
from routers.auth import get_current_user

router = APIRouter(prefix="/api/inventory", tags=["Kleiderkammer & Lagerbestand"])

def parse_val(v):
    if v == "" or v == "null" or v is None or v == 0 or v == "0":
        return None
    if isinstance(v, str):
        return v.strip()
    return v

# --- ROUTE 1: LAGERBESTAND AUFLISTEN ---
@router.get("")
def list_inventory(r: Request):
    user = get_current_user(r)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht autorisiert")
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        cur.execute("SELECT * FROM inventory ORDER BY item_name ASC")
        res = cur.fetchall()
        cur.close()
        c.close()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Laden des Bestands: {str(e)}")

# --- ROUTE 2: NEUEN ARTIKEL ANLEGEN (POST) ---
@router.post("")
async def create_inventory_item(r: Request):
    user = get_current_user(r)
    if not user or user.get("r") not in ["admin", "geratewart"]:
        raise HTTPException(status_code=403, detail="Nicht autorisiert")
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        
        params = (
            d.get('item_name'),
            d.get('amount', 1),
            d.get('min_amount', 5),
            d.get('unit', 'Stück'),
            d.get('location', 'Lager'),
            parse_val(d.get('size')),
            d.get('category', 'Brandschutz'),
            parse_val(d.get('manufacturer')),
            parse_val(d.get('serial_number')),
            parse_val(d.get('qr_code_id'))
        )
        
        query = """
            INSERT INTO inventory (
                item_name, amount, min_amount, unit, location, 
                size, category, manufacturer, serial_number, qr_code_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(query, params)
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Hinzufügen des Artikels: {str(e)}")

# --- NEUE ROUTE 3: ARTIKEL BEARBEITEN VIA PUT (Löst die Dashboard-Blockade) ---
@router.put("/{item_id}")
async def update_inventory_item_put(item_id: int, r: Request):
    user = get_current_user(r)
    if not user or user.get("r") not in ["admin", "geratewart"]:
        raise HTTPException(status_code=403, detail="Nicht autorisiert")
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        
        params = (
            d.get('item_name'),
            d.get('amount', 1),
            d.get('min_amount', 5),
            d.get('unit', 'Stück'),
            d.get('location', 'Lager'),
            parse_val(d.get('size')),
            d.get('category', 'Brandschutz'),
            parse_val(d.get('manufacturer')),
            parse_val(d.get('serial_number')),
            parse_val(d.get('qr_code_id')),
            item_id
        )
        
        query = """
            UPDATE inventory SET
                item_name=%s, amount=%s, min_amount=%s, unit=%s, location=%s, 
                size=%s, category=%s, manufacturer=%s, serial_number=%s, qr_code_id=%s
            WHERE id=%s
        """
        cur.execute(query, params)
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Aktualisieren des Artikels via PUT: {str(e)}")

# --- ROUTE 4: ARTIKEL LÖSCHEN (DELETE) ---
@router.delete("/{item_id}")
def delete_inventory_item(item_id: int, r: Request):
    user = get_current_user(r)
    if not user or user.get("r") not in ["admin", "geratewart"]:
        raise HTTPException(status_code=403, detail="Nicht autorisiert")
    try:
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("DELETE FROM inventory WHERE id = %s", (item_id,))
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Löschen des Artikels: {str(e)}")
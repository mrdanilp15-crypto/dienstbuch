import secrets
from fastapi import APIRouter, Request, HTTPException
from database import get_db_connection

# Prefix sorgt dafür, dass alle Anfragen unter /api/inventory landen
router = APIRouter(prefix="/api/inventory", tags=["Inventory & Kleiderkammer"])

def parse_val(v):
    if v == "" or v == "null" or v is None:
        return None
    if isinstance(v, str):
        return v.strip()
    return v

# --- ROUTE 1: INVENTAR AUFLISTEN ---
@router.get("")
def list_inventory(r: Request):
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        query = """
            SELECT id, item_name, amount, min_amount, unit, location, qr_code_id, 
                   category, manufacturer, serial_number,
                   DATE_FORMAT(last_check, '%Y-%m-%d') as last_check, 
                   DATE_FORMAT(next_check, '%Y-%m-%d') as next_check 
            FROM inventory 
            ORDER BY item_name ASC
        """
        cur.execute(query)
        res = cur.fetchall()
        cur.close()
        c.close()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Laden des Inventars: {str(e)}")

# --- ROUTE 2: MATERIAL SPEICHERN ODER BEARBEITEN ---
@router.post("")
async def save_inventory_item(r: Request):
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        
        qr_id = parse_val(d.get('qr_code_id'))
        # Wenn kein QR-Code existiert, erzeugen wir hier direkt einen sicheren Token
        if not qr_id or len(str(qr_id).strip()) == 0:
            qr_id = f"FW-QR-{secrets.token_hex(4).upper()}"
            
        i_id = d.get('id')
        params = (
            d.get('item_name'), 
            d.get('amount', 0), 
            d.get('min_amount', 5), 
            d.get('unit', 'Stück'), 
            d.get('location', 'Lager'), 
            qr_id, 
            parse_val(d.get('last_check')), 
            parse_val(d.get('next_check')),
            d.get('category', ''), 
            d.get('manufacturer', ''), 
            d.get('serial_number', '')
        )
        
        if i_id:
            query = """
                UPDATE inventory SET 
                    item_name=%s, amount=%s, min_amount=%s, unit=%s, location=%s, 
                    qr_code_id=%s, last_check=%s, next_check=%s, category=%s, 
                    manufacturer=%s, serial_number=%s 
                WHERE id=%s
            """
            cur.execute(query, params + (i_id,))
        else:
            query = """
                INSERT INTO inventory (
                    item_name, amount, min_amount, unit, location, qr_code_id, 
                    last_check, next_check, category, manufacturer, serial_number
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            cur.execute(query, params)
            
        c.commit()
        cur.close()
        c.close()
        return {"status": "success", "qr_code_id": qr_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Sichern des Materials: {str(e)}")

# --- ROUTE 3: MATERIAL LÖSCHEN ---
@router.delete("/{i_id}")
def del_inventory_item(i_id: int, r: Request):
    try:
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("DELETE FROM inventory WHERE id = %s", (i_id,))
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Löschen des Materials: {str(e)}")
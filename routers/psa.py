from fastapi import APIRouter, Request, HTTPException
from database import get_db_connection
from routers.auth import get_current_user

router = APIRouter(prefix="/api/psa", tags=["Personenbezogene PSA & Kleiderkammer"])

def parse_val(v):
    if v == "" or v == "null" or v is None or v == 0 or v == "0":
        return None
    if isinstance(v, str):
        return v.strip()
    return v

# --- ROUTE 1: AUSGEGEBENE PSA AUFLISTEN ---
@router.get("")
def list_psa_assignments(r: Request):
    user = get_current_user(r)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht autorisiert")
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        # DATE_FORMAT verhindert, dass das JSON-Format bei leeren Prüffristen crasht
        query = """
            SELECT id, person_id, item_name, size, qr_code_id, status,
                   DATE_FORMAT(next_check, '%Y-%m-%d') as next_check 
            FROM psa 
            ORDER BY id DESC
        """
        cur.execute(query)
        res = cur.fetchall()
        cur.close()
        c.close()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Laden der PSA-Zuweisungen: {str(e)}")

# --- ROUTE 2: NEUE PSA AN KAMERADEN AUSGEBEN (POST) ---
@router.post("")
async def allocate_psa(r: Request):
    user = get_current_user(r)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht autorisiert")
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        
        p_id = parse_val(d.get('person_id'))
        
        query = """
            INSERT INTO psa (person_id, item_name, size, qr_code_id, status, next_check) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = (
            p_id,
            d.get('item_name'),
            parse_val(d.get('size')),
            d.get('qr_code_id'),
            d.get('status', 'Ausgegeben'),
            parse_val(d.get('next_check'))
        )
        cur.execute(query, params)
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler bei PSA-Zuweisung: {str(e)}")

# --- ROUTE 3: PSA-DATEN ODER PRÜFFRIST ÄNDERN (PUT - Verhindert den 405-Error) ---
@router.put("/{psa_id}")
async def update_psa_assignment(psa_id: int, r: Request):
    user = get_current_user(r)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht autorisiert")
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        
        p_id = parse_val(d.get('person_id'))
        
        query = """
            UPDATE psa SET 
                person_id=%s, item_name=%s, size=%s, qr_code_id=%s, status=%s, next_check=%s 
            WHERE id=%s
        """
        params = (
            p_id,
            d.get('item_name'),
            parse_val(d.get('size')),
            d.get('qr_code_id'),
            d.get('status'),
            parse_val(d.get('next_check')),
            psa_id
        )
        cur.execute(query, params)
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Aktualisieren der PSA-Akte: {str(e)}")

# --- ROUTE 4: PSA ZURÜCKNEHMEN / LÖSCHEN (DELETE) ---
@router.delete("/{psa_id}")
def revoke_psa(psa_id: int, r: Request):
    user = get_current_user(r)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht autorisiert")
    try:
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("DELETE FROM psa WHERE id = %s", (psa_id,))
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler bei PSA-Rücknahme: {str(e)}")
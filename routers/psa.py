from fastapi import APIRouter, Request, HTTPException
from database import get_db_connection
from routers.auth import get_current_user

router = APIRouter(prefix="/api/psa", tags=["PSA Ausgabezentrale"])

@router.get("")
def list_psa(r: Request):
    user = get_current_user(r)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht autorisiert")
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        cur.execute("SELECT p.*, pers.name as person_name FROM psa p LEFT JOIN personnel pers ON p.person_id = pers.id ORDER BY p.id DESC")
        res = cur.fetchall()
        cur.close()
        c.close()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("")
async def create_psa_assignment(r: Request):
    user = get_current_user(r)
    if not user or user.get("r") not in ["admin", "geratewart"]:
        raise HTTPException(status_code=403, detail="Zugriff verweigert")
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        
        p_id = d.get('id')
        n_check = d.get('next_check') or None
        
        if p_id:
            query = "UPDATE psa SET person_id=%s, item_name=%s, size=%s, qr_code_id=%s, status=%s, next_check=%s WHERE id=%s"
            params = (d.get('person_id'), d.get('item_name'), d.get('size'), d.get('qr_code_id'), d.get('status', 'Ausgegeben'), n_check, p_id)
        else:
            query = "INSERT INTO psa (person_id, item_name, size, qr_code_id, status, next_check) VALUES (%s, %s, %s, %s, %s, %s)"
            params = (d.get('person_id'), d.get('item_name'), d.get('size'), d.get('qr_code_id'), d.get('status', 'Ausgegeben'), n_check)
            
        cur.execute(query, params)
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{psa_id}")
def delete_psa_assignment(psa_id: int, r: Request):
    user = get_current_user(r)
    if not user or user.get("r") not in ["admin", "geratewart"]:
        raise HTTPException(status_code=403, detail="Zugriff verweigert")
    try:
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("DELETE FROM psa WHERE id = %s", (psa_id,))
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
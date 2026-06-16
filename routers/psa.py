from fastapi import APIRouter, Request, HTTPException
from database import get_db_connection
from routers.auth import get_current_user

router = APIRouter(prefix="/api/psa", tags=["PSA-Zuweisung"])

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
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        cur.execute(
            "INSERT INTO psa (person_id, item_name, size, qr_code_id) VALUES (%s, %s, %s, %s)",
            (d.get('person_id'), d.get('item_name'), d.get('size'), d.get('qr_code_id'))
        )
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
        raise HTTPException(status_code=403, detail="Keine Berechtigung")
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
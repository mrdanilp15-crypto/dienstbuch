from fastapi import APIRouter, Request, HTTPException
from database import get_db_connection
from routers.auth import get_current_user

router = APIRouter(prefix="/api/personnel", tags=["Personal & Kameradenakten"])

def parse_val(v):
    if v == "" or v == "null" or v is None or v == 0 or v == "0":
        return None
    if isinstance(v, str):
        return v.strip()
    return v

# --- ROUTE 1: KAMERADEN AUFLISTEN (Wichtig: Pfad /list matched eure app.js) ---
@router.get("/list")
def list_personnel(r: Request):
    user = get_current_user(r)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht autorisiert")
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        # DATE_FORMAT fängt leere MySQL-Datumsfelder krisensicher für das JSON ab
        query = """
            SELECT id, name, rank, membership_status, is_agt, is_maschinist, is_gf,
                   DATE_FORMAT(g26_3_date, '%Y-%m-%d') as g26_3_date,
                   DATE_FORMAT(last_license_check, '%Y-%m-%d') as last_license_check,
                   mta_status, qualifications, size_helm, size_jacke, size_stiefel, profile_picture
            FROM personnel
            ORDER BY name ASC
        """
        cur.execute(query)
        res = cur.fetchall()
        cur.close()
        c.close()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Laden des Personals: {str(e)}")

# --- ROUTE 2: KAMERADEN NEU ANLEGEN (POST) ---
@router.post("")
async def create_member(r: Request):
    user = get_current_user(r)
    if not user or user.get("r") != "admin":
        raise HTTPException(status_code=403, detail="Nicht autorisiert")
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        
        # MySQL TINYINT (1/0) Konvertierung für die Checkboxen
        agt = 1 if d.get('is_agt') in [True, 1, "true"] else 0
        masch = 1 if d.get('is_maschinist') in [True, 1, "true"] else 0
        gf = 1 if d.get('is_gf') in [True, 1, "true"] else 0
        
        params = (
            d.get('name'),
            d.get('rank'),
            d.get('membership_status', 'Aktiv'),
            agt, masch, gf,
            parse_val(d.get('g26_3_date')),
            parse_val(d.get('last_license_check')),
            d.get('mta_status', 'Basis'),
            parse_val(d.get('qualifications')),
            parse_val(d.get('size_helm')),
            parse_val(d.get('size_jacke')),
            parse_val(d.get('size_stiefel')),
            d.get('profile_picture')
        )
        
        query = """
            INSERT INTO personnel (
                name, rank, membership_status, is_agt, is_maschinist, is_gf,
                g26_3_date, last_license_check, mta_status, qualifications,
                size_helm, size_jacke, size_stiefel, profile_picture
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(query, params)
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Anlegen des Kameraden: {str(e)}")

# --- NEUE ROUTE 3: KAMERADENAKTE BEARBEITEN VIA PUT (Löst die Dashboard-Blockade) ---
@router.put("/{person_id}")
async def update_member_put(person_id: int, r: Request):
    user = get_current_user(r)
    if not user or user.get("r") != "admin":
        raise HTTPException(status_code=403, detail="Nicht autorisiert")
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        
        agt = 1 if d.get('is_agt') in [True, 1, "true"] else 0
        masch = 1 if d.get('is_maschinist') in [True, 1, "true"] else 0
        gf = 1 if d.get('is_gf') in [True, 1, "true"] else 0
        
        params = (
            d.get('name'),
            d.get('rank'),
            d.get('membership_status'),
            agt, masch, gf,
            parse_val(d.get('g26_3_date')),
            parse_val(d.get('last_license_check')),
            d.get('mta_status'),
            parse_val(d.get('qualifications')),
            parse_val(d.get('size_helm')),
            parse_val(d.get('size_jacke')),
            parse_val(d.get('size_stiefel')),
            d.get('profile_picture'),
            person_id
        )
        
        query = """
            UPDATE personnel SET
                name=%s, rank=%s, membership_status=%s, is_agt=%s, is_maschinist=%s, is_gf=%s,
                g26_3_date=%s, last_license_check=%s, mta_status=%s, qualifications=%s,
                size_helm=%s, size_jacke=%s, size_stiefel=%s, profile_picture=%s
            WHERE id=%s
        """
        cur.execute(query, params)
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Aktualisieren der Kameradenakte via PUT: {str(e)}")

# --- ROUTE 4: KAMERADENAKTE LÖSCHEN (DELETE) ---
@router.delete("/{person_id}")
def delete_member(person_id: int, r: Request):
    user = get_current_user(r)
    if not user or user.get("r") != "admin":
        raise HTTPException(status_code=403, detail="Nicht autorisiert")
    try:
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("DELETE FROM personnel WHERE id = %s", (person_id,))
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Löschen des Kameraden: {str(e)}")
from fastapi import APIRouter, Request, Response, HTTPException
from database import get_db_connection

# Prefix sorgt dafür, dass alle Routen automatisch mit /api/personnel starten
router = APIRouter(prefix="/api/personnel", tags=["Personnel"])

# --- MODUL-INTERNE CLEANER (Keine Pydantic-Abstürze mehr) ---
def parse_val(v):
    if v == "" or v == "null" or v is None:
        return None
    if isinstance(v, str):
        return v.strip()
    return v

def to_int(v):
    if str(v).lower() in ['true', '1', 'yes', 't']:
        return 1
    return 0

# --- ROUTE 1: KAMERADEN-LISTE ABFRAGEN ---
@router.get("/list")
def list_personnel(r: Request):
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        # Alle Daten und sauber formatierte Datumsfelder abrufen
        query = """
            SELECT id, name, rank, membership_status, is_agt, is_maschinist, is_gf, 
                   phone, email, address, ice_contact, drive_b, drive_be, drive_c, drive_ce, 
                   profile_picture, 
                   DATE_FORMAT(g26_3_date, '%Y-%m-%d') as g26_3_date, 
                   DATE_FORMAT(birth_date, '%Y-%m-%d') as birth_date, 
                   DATE_FORMAT(entry_date, '%Y-%m-%d') as entry_date 
            FROM personnel 
            ORDER BY name ASC
        """
        cur.execute(query)
        res = cur.fetchall()
        cur.close()
        c.close()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Datenbankfehler beim Auflisten: {str(e)}")

# --- ROUTE 2: KAMERAD SPEICHERN ODER AKTUALISIEREN ---
@router.post("")
async def save_member(r: Request):
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        
        p_id = d.get('id')
        params = (
            d.get('name'), d.get('rank'), d.get('membership_status'), 
            to_int(d.get('is_agt')), to_int(d.get('is_maschinist')), to_int(d.get('is_gf')), 
            parse_val(d.get('g26_3_date')), parse_val(d.get('birth_date')), parse_val(d.get('entry_date')), 
            d.get('phone'), d.get('email'), d.get('address'), d.get('ice_contact'), 
            to_int(d.get('drive_b')), to_int(d.get('drive_be')), to_int(d.get('drive_c')), to_int(d.get('drive_ce')), 
            d.get('profile_picture')
        )
        
        if p_id:
            # Update bestehender Kamerad
            query = """
                UPDATE personnel SET 
                    name=%s, rank=%s, membership_status=%s, is_agt=%s, is_maschinist=%s, is_gf=%s, 
                    g26_3_date=%s, birth_date=%s, entry_date=%s, phone=%s, email=%s, address=%s, 
                    ice_contact=%s, drive_b=%s, drive_be=%s, drive_c=%s, drive_ce=%s, profile_picture=%s 
                WHERE id=%s
            """
            cur.execute(query, params + (p_id,))
        else:
            # Neuen Kamerad anlegen
            query = """
                INSERT INTO personnel (
                    name, rank, membership_status, is_agt, is_maschinist, is_gf, 
                    g26_3_date, birth_date, entry_date, phone, email, address, 
                    ice_contact, drive_b, drive_be, drive_c, drive_ce, profile_picture
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            cur.execute(query, params)
            
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Datenbankfehler beim Speichern: {str(e)}")

# --- ROUTE 3: KAMERAD LÖSCHEN ---
@router.delete("/{p_id}")
def delete_member(p_id: int, r: Request):
    try:
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("DELETE FROM personnel WHERE id = %s", (p_id,))
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Datenbankfehler beim Löschen: {str(e)}")
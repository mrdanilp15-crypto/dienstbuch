from fastapi import APIRouter, Request, HTTPException
from database import get_db_connection
from routers.auth import get_current_user

router = APIRouter(prefix="/api/personnel", tags=["Personalstamm & Dienstakten"])

# --- 1. KAMERADEN AUFLISTEN ---
@router.get("/list")
def list_personnel(r: Request):
    user = get_current_user(r)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht autorisiert")
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        cur.execute("SELECT * FROM personnel ORDER BY name ASC")
        res = cur.fetchall()
        cur.close()
        c.close()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- 2. KAMERAD ANLEGEN ODER AKTUALISIEREN ---
@router.post("")
async def save_member(r: Request):
    user = get_current_user(r)
    # SERVER-SIDE RECHTEPRÜFUNG: Nur Admins dürfen Akten modifizieren
    if not user or user.get("r") != "admin":
        raise HTTPException(status_code=403, detail="Berechtigung verweigert: Nur Administratoren dürfen Personalakten bearbeiten.")
        
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        
        p_id = d.get('id')
        if p_id:
            # Bestehenden Kameraden aktualisieren
            query = """
                UPDATE personnel 
                SET name=%s, rank=%s, membership_status=%s, is_agt=%s, is_maschinist=%s, is_gf=%s, 
                    g26_3_date=%s, qualifications=%s, size_helm=%s, size_jacke=%s, size_stiefel=%s, profile_picture=%s
                WHERE id=%s
            """
            params = (d.get('name'), d.get('rank'), d.get('membership_status'), int(d.get('is_agt', 0)), 
                      int(d.get('is_maschinist', 0)), int(d.get('is_gf', 0)), d.get('g26_3_date') or None, 
                      d.get('qualifications'), d.get('size_helm'), d.get('size_jacke'), d.get('size_stiefel'), 
                      d.get('profile_picture'), p_id)
        else:
            # Neuen Kameraden anlegen
            query = """
                INSERT INTO personnel (name, rank, membership_status, is_agt, is_maschinist, is_gf, g26_3_date, qualifications, size_helm, size_jacke, size_stiefel, profile_picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = (d.get('name'), d.get('rank'), d.get('membership_status'), int(d.get('is_agt', 0)), 
                      int(d.get('is_maschinist', 0)), int(d.get('is_gf', 0)), d.get('g26_3_date') or None, 
                      d.get('qualifications'), d.get('size_helm'), d.get('size_jacke'), d.get('size_stiefel'), d.get('profile_picture'))
        
        cur.execute(query, params)
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Sichern der Akte: {str(e)}")

# --- 3. KAMERADEN LÖSCHEN (REPARATUR: Gefundene Lücke geschlossen) ---
@router.delete("/{member_id}")
def delete_member(member_id: int, r: Request):
    user = get_current_user(r)
    # Nur der Admin darf Personal permanent aus dem System entfernen
    if not user or user.get("r") != "admin":
        raise HTTPException(status_code=403, detail="Berechtigung verweigert.")
        
    try:
        c = get_db_connection()
        cur = c.cursor()
        
        # 1. Optionale Sicherheitsstufe: Verknüpfte PSA-Einträge vorher entkoppeln
        cur.execute("DELETE FROM psa WHERE person_id = %s", (member_id,))
        
        # 2. Kamerad aus dem System löschen
        cur.execute("DELETE FROM personnel WHERE id = %s", (member_id,))
        
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Löschen des Kameraden: {str(e)}")
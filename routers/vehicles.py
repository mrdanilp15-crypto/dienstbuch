from fastapi import APIRouter, Request, Response, HTTPException
from database import get_db_connection

# Das Präfix sorgt dafür, dass alle Anfragen unter /api/vehicles zusammenlaufen
router = APIRouter(prefix="/api/vehicles", tags=["Vehicles & Fahrtenbuch"])

def parse_val(v):
    if v == "" or v == "null" or v is None:
        return None
    if isinstance(v, str):
        return v.strip()
    return v

# --- ROUTE 1: FAHRZEUGE AUFLISTEN ---
@router.get("")
def list_vehicles(r: Request):
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        query = """
            SELECT id, name, radio_name, status, milage, license_plate, vehicle_type,
                   DATE_FORMAT(tuv_date, '%Y-%m-%d') as tuv_date, 
                   DATE_FORMAT(sp_date, '%Y-%m-%d') as sp_date, 
                   next_oil_change_km 
            FROM vehicles 
            ORDER BY name ASC
        """
        cur.execute(query)
        res = cur.fetchall()
        cur.close()
        c.close()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Laden des Fuhrparks: {str(e)}")

# --- ROUTE 2: FAHRZEUG SPEICHERN ODER BEARBEITEN ---
@router.post("")
async def save_vehicle(r: Request):
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        
        v_id = d.get('id')
        params = (
            d.get('name'), 
            d.get('radio_name'), 
            d.get('status', 2), 
            d.get('milage', 0), 
            parse_val(d.get('tuv_date')), 
            parse_val(d.get('sp_date')), 
            d.get('next_oil_change_km', 10000),
            d.get('license_plate', ''), 
            d.get('vehicle_type', '')
        )
        
        if v_id:
            query = """
                UPDATE vehicles SET 
                    name=%s, radio_name=%s, status=%s, milage=%s, 
                    tuv_date=%s, sp_date=%s, next_oil_change_km=%s, 
                    license_plate=%s, vehicle_type=%s 
                WHERE id=%s
            """
            cur.execute(query, params + (v_id,))
        else:
            query = """
                INSERT INTO vehicles (
                    name, radio_name, status, milage, 
                    tuv_date, sp_date, next_oil_change_km, 
                    license_plate, vehicle_type
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            cur.execute(query, params)
            
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Sichern des Fahrzeugs: {str(e)}")

# --- ROUTE 3: FAHRZEUG LÖSCHEN ---
@router.delete("/{v_id}")
def del_vehicle(v_id: int, r: Request):
    try:
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("DELETE FROM vehicles WHERE id = %s", (v_id,))
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Entfernen des Fahrzeugs: {str(e)}")

# --- ROUTE 4: SCHNELLER FMS STATUS-UPDATE (Status 2/4 Tasten) ---
@router.put("/{v_id}/status")
async def vehicle_status(v_id: int, r: Request):
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("UPDATE vehicles SET status = %s WHERE id = %s", (d.get('status', 2), v_id))
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim FMS-Status-Update: {str(e)}")

# --- ROUTE 5: FAHRTENBUCH AUFLISTEN ---
@router.get("/logs")
def list_logs(r: Request):
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        query = """
            SELECT l.*, v.name as vehicle_name, 
                   DATE_FORMAT(l.date, '%Y-%m-%d') as date, 
                   DATE_FORMAT(l.date, '%d.%m.%Y') as date_formatted 
            FROM vehicle_log l 
            LEFT JOIN vehicles v ON l.vehicle_id = v.id 
            ORDER BY l.id DESC
        """
        cur.execute(query)
        res = cur.fetchall()
        cur.close()
        c.close()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Laden des Fahrtenbuchs: {str(e)}")

# --- ROUTE 6: EINTRAG LOGGEN & KILOMETERSTAND AUTO-UPDATE ---
@router.post("/logs")
async def save_log(r: Request):
    try:
        d = await r.json()
        c = get_db_connection()
        cur = c.cursor()
        l_id = d.get('id')
        params = (
            d.get('vehicle_id'), 
            parse_val(d.get('date')), 
            d.get('driver_name'), 
            d.get('purpose'), 
            d.get('km_start', 0), 
            d.get('km_end', 0), 
            d.get('fuel_liters', 0.0)
        )
        
        if l_id:
            query = """
                UPDATE vehicle_log SET 
                    vehicle_id=%s, date=%s, driver_name=%s, purpose=%s, 
                    km_start=%s, km_end=%s, fuel_liters=%s 
                WHERE id=%s
            """
            cur.execute(query, params + (l_id,))
        else:
            query = """
                INSERT INTO vehicle_log (
                    vehicle_id, date, driver_name, purpose, 
                    km_start, km_end, fuel_liters
                ) VALUES (%s,%s,%s,%s,%s,%s,%s)
            """
            cur.execute(query, params)
            # Automatisches Mitführen des Kilometerstands im Fahrzeugstamm
            query_update_veh = "UPDATE vehicles SET milage = %s WHERE id = %s"
            cur.execute(query_update_veh, (d.get('km_end', 0), d.get('vehicle_id')))
            
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Loggen der Fahrt: {str(e)}")

# --- ROUTE 7: FAHRTENBUCHEINTRAG STORNIEREN ---
@router.delete("/logs/{log_id}")
def del_log(log_id: int, r: Request):
    try:
        c = get_db_connection()
        cur = c.cursor()
        cur.execute("DELETE FROM vehicle_log WHERE id = %s", (log_id,))
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Stornieren des Eintrags: {str(e)}")
from fastapi import APIRouter, Request, HTTPException
from database import get_db_connection
from routers.auth import get_current_user

router = APIRouter(tags=["Dienstberichte & Übungsabende"])

# --- ROUTE 1: DIENSTBERICHTE AUFLISTEN ---
@router.get("/groups/{group_id}/sessions")
def list_sessions(group_id: int, r: Request):
    user = get_current_user(r)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht autorisiert")
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        cur.execute("SELECT * FROM sessions WHERE group_id = %s ORDER BY date DESC, id DESC", (group_id,))
        res = cur.fetchall()
        cur.close()
        c.close()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- NEUE ROUTE 1b: EINZELNEN BERICHT LADEN (Zwingend notwendig für den Editor!) ---
@router.get("/groups/{group_id}/sessions/{session_id}")
def get_single_session(group_id: int, session_id: int, r: Request):
    user = get_current_user(r)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht autorisiert")
    try:
        c = get_db_connection()
        cur = c.cursor(dictionary=True)
        
        # 1. Stammdaten des Berichts holen
        cur.execute("SELECT * FROM sessions WHERE id = %s AND group_id = %s", (session_id, group_id))
        session_row = cur.fetchone()
        
        if not session_row:
            cur.close()
            c.close()
            raise HTTPException(status_code=404, detail="Dienstbericht nicht gefunden.")
            
        # 2. Zugehörige Anwesenheitsliste laden
        cur.execute("SELECT * FROM attendance WHERE session_id = %s", (session_id,))
        attendance_rows = cur.fetchall()
        
        cur.close()
        c.close()
        return {"session": session_row, "attendance": attendance_rows}
    except HTTPException as http_err:
        raise http_err
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Laden des Berichts: {str(e)}")

# --- NEUE ROUTE 2: NEUEN DIENSTBERICHT ERSTELLEN (POST) ---
@router.post("/groups/{group_id}/sessions")
async def create_service_report(group_id: int, r: Request):
    user = get_current_user(r)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht autorisiert")
    try:
        d = await r.json()
        s = d.get("session", {})
        att_list = d.get("attendance", [])
        
        c = get_db_connection()
        cur = c.cursor()
        
        # 1. Bericht-Stammdaten in die 'sessions'-Tabelle jagen
        query_session = """
            INSERT INTO sessions (group_id, date, category, duration, description, content, instructors)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(query_session, (
            group_id, s.get("date"), s.get("category"), s.get("duration", 2.0),
            s.get("description"), s.get("content"), s.get("instructors")
        ))
        new_session_id = cur.lastrowid  # Generierte ID abgreifen
        
        # 2. Die gesamte Mannschafts-Anwesenheit in die 'attendance'-Tabelle schreiben
        query_attendance = """
            INSERT INTO attendance (session_id, person_id, is_present, vehicle)
            VALUES (%s, %s, %s, %s)
        """
        for a in att_list:
            cur.execute(query_attendance, (
                new_session_id, a.get("person_id"), a.get("is_present", 0), a.get("vehicle", "")
            ))
            
        c.commit()
        cur.close()
        c.close()
        return {"status": "success", "session_id": new_session_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Erstellen des Berichts: {str(e)}")

# --- NEUE ROUTE 2b: BESTEHENDEN BERICHT UPDATE (PUT - Behebt den 405-Error permanent!) ---
@router.put("/groups/{group_id}/sessions/{session_id}")
async def update_service_report(group_id: int, session_id: int, r: Request):
    user = get_current_user(r)
    if not user:
        raise HTTPException(status_code=401, detail="Nicht autorisiert")
    try:
        d = await r.json()
        s = d.get("session", {})
        att_list = d.get("attendance", [])
        
        c = get_db_connection()
        cur = c.cursor()
        
        # 1. Stammdaten aktualisieren
        query_update = """
            UPDATE sessions 
            SET date = %s, category = %s, duration = %s, description = %s, content = %s, instructors = %s
            WHERE id = %s AND group_id = %s
        """
        cur.execute(query_update, (
            s.get("date"), s.get("category"), s.get("duration", 2.0), s.get("description"),
            s.get("content"), s.get("instructors"), session_id, group_id
        ))
        
        # 2. Die alte Anwesenheitsliste für diesen Bericht radikal verwerfen
        cur.execute("DELETE FROM attendance WHERE session_id = %s", (session_id,))
        
        # 3. Die neue, korrigierte Besatzungsliste frisch einspeisen
        query_attendance = """
            INSERT INTO attendance (session_id, person_id, is_present, vehicle)
            VALUES (%s, %s, %s, %s)
        """
        for a in att_list:
            cur.execute(query_attendance, (
                session_id, a.get("person_id"), a.get("is_present", 0), a.get("vehicle", "")
            ))
            
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Aktualisieren des Berichts via PUT: {str(e)}")

# --- ROUTE 3: DIENSTBERICHT LÖSCHEN (Inkl. Anwesenheiten) ---
@router.delete("/groups/{group_id}/sessions/{session_id}")
def delete_service_report(group_id: int, session_id: int, r: Request):
    user = get_current_user(r)
    # Nur der Admin darf Berichte permanent entfernen
    if not user or user.get("r") != "admin":
        raise HTTPException(status_code=403, detail="Nur Administratoren vorbehalten.")
    try:
        c = get_db_connection()
        cur = c.cursor()
        # 1. Verknüpfte Anwesenheitsliste löschen
        cur.execute("DELETE FROM attendance WHERE session_id = %s", (session_id,))
        # 2. Den eigentlichen Bericht löschen
        cur.execute("DELETE FROM sessions WHERE id = %s AND group_id = %s", (session_id, group_id))
        c.commit()
        cur.close()
        c.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
from fastapi import APIRouter, Request, HTTPException
from database import get_db_connection
from routers.auth import get_current_user

router = APIRouter(tags=["Dienstberichte & Übungsabende"])

# --- DIENSTBERICHTE AUFLISTEN ---
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

# --- DIENSTBERICHT LÖSCHEN (Inkl. Anwesenheiten) ---
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
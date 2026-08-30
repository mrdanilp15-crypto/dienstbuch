from fastapi import APIRouter, HTTPException, Request, Response, UploadFile, File
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Optional, List, Union
from datetime import datetime, date
import json

from database import get_db_connection
from core.utils import get_current_user, log_audit_action

router = APIRouter()

# --- AI DRAFT SUMMARY GENERATOR ---
@router.post("/api/missions/ai-draft")
def get_ai_draft(data: dict, request: Request):
    if not get_current_user(request): raise HTTPException(status_code=401, detail="Nicht angemeldet")
    stichwort = data.get("stichwort", "Brandeinsatz")
    adresse = data.get("adresse", "Hauptstraße 12")
    meldung = data.get("meldung", "Rauchentwicklung")
    
    # Generiere einen ansprechenden Entwurf
    draft = f"Am Einsatzort ({adresse}) wurde nach Erkundung der Lage die Meldung '{meldung}' ({stichwort}) bestätigt. Die Mannschaft ging unter schwerem Atemschutz vor. Der Brand konnte rasch unter Kontrolle gebracht und gelöscht werden. Anschließend Belüftungsmaßnahmen durchgeführt. Übergabe an Eigentümer."
    return {"draft": draft}


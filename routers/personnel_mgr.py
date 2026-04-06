from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from database import get_db
from datetime import date, timedelta

router = APIRouter(prefix="/api/personnel", tags=["Personal"])

@router.get("/list/{group_id}")
async def get_personnel(group_id: int, db: Session = Depends(get_db)):
    """Holt alle Mitglieder einer Gruppe inklusive ihrer Qualifikations-Daten."""
    # Hier ziehen wir später die Daten aus der 'persons' Tabelle
    # Ich setze hier ein Beispiel-Skelett ein:
    return [
        {
            "id": 1, 
            "name": "Max Mustermann", 
            "is_agt": True, 
            "g26_3_expiry": "2026-12-01", 
            "last_belastung": "2025-05-20",
            "qualis": ["Sprechfunker", "Maschinist"]
        }
    ]

@router.post("/update-dates/{person_id}")
async def update_dates(person_id: int, data: dict, db: Session = Depends(get_db)):
    """Aktualisiert die ärztlichen Untersuchungen oder Übungstermine."""
    # Logik zum Speichern in der DB
    return {"status": "success", "message": "Daten aktualisiert"}

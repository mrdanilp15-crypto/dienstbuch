from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List

router = APIRouter(prefix="/api/hydranten", tags=["Wasserentnahme"])

# Temporärer Speicher für die Hydranten-Koordinaten
hydranten_storage = [
    {"id": 1, "lat": 47.9942, "lon": 10.1344, "hydrant_type": "Unterflurhydrant", "diameter": "DN80"}
]
next_id = 2

class HydrantModel(BaseModel):
    lat: float
    lon: float
    hydrant_type: str
    diameter: str

@router.get("", response_model=List[dict])
async def get_all_hydranten():
    return hydranten_storage

@router.post("")
async def add_hydrant(hydrant: HydrantModel):
    global next_id
    new_hyd = hydrant.dict()
    new_hyd["id"] = next_id
    hydranten_storage.append(new_hyd)
    next_id += 1
    return {"status": "success", "hydrant": new_hyd}

@router.delete("/{hydrant_id}")
async def delete_hydrant(hydrant_id: int):
    global hydranten_storage
    initial_length = len(hydranten_storage)
    hydranten_storage = [h for h in hydranten_storage if h["id"] != hydrant_id]
    if len(hydranten_storage) == initial_length:
        raise HTTPException(status_code=404, detail="Hydrant nicht gefunden")
    return {"status": "success", "message": "Wasserentnahmestelle entfernt"}
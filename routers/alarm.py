from fastapi import APIRouter

# Wir erstellen den Router mit dem passenden URL-Präfix
router = APIRouter(prefix="/api/alarm", tags=["Alarmierung"])

# Globaler Server-Speicher für den aktuellen Alamos-Alarm
# (Sobald Alamos einen echten Alarm sendet, überschreibt er diesen Dummy)
current_wachen_alarm = {
    "id": 1,
    "keyword": "B3 — Zimmerbrand",
    "address": "Hauptstraße 12, Wache"
}

@router.get("/active")
async def get_active_alarm():
    global current_wachen_alarm
    if current_wachen_alarm is not None:
        return current_wachen_alarm
    # Wenn kein Alarm aktiv ist, senden wir den No-Alarm-Status
    return {"status": "no_alarm"}

@router.post("/clear")
async def clear_active_alarm_globally():
    global current_wachen_alarm
    # Der Server löscht den Zimmerbrand aus dem globalen Speicher
    current_wachen_alarm = None
    return {"status": "success", "message": "Alarm auf allen Leitständen gelöscht"}
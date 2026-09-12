import hmac
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from typing import List

from core.utils import get_current_user, get_or_create_token

router = APIRouter()

def _websocket_authorized(websocket: WebSocket) -> bool:
    """Wie check_display_access() in core/utils.py, nur für WebSockets statt normaler HTTP-
    Requests (Starlette WebSocket hat kein .method, get_current_user()'s HTTPException passt
    hier auch nicht - daher eine eigene, kleine Variante statt die HTTP-Funktion wiederzuverwenden).
    Broadcasts über diesen Kanal enthalten echte Einsatz-Stichworte/Adressen - ohne diese Prüfung
    konnte bisher JEDER im Internet unangemeldet mitlesen."""
    if get_current_user(websocket):
        return True
    token = websocket.query_params.get("token")
    return bool(token) and hmac.compare_digest(token, get_or_create_token("display_token"))

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast_text(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                pass

    async def broadcast_json(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                pass

manager = ConnectionManager()

@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    if not _websocket_authorized(websocket):
        await websocket.close(code=4401)
        return
    await manager.connect(websocket)
    try:
        while True:
            # Simple keep-alive or message receive
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

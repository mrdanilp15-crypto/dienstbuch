import asyncio
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
        self.usernames = {}  # WebSocket -> Benutzername (None bei Display-Token)
        self.loop = None

    async def connect(self, websocket: WebSocket, username=None):
        await websocket.accept()
        self.active_connections.append(websocket)
        self.usernames[websocket] = username
        self.loop = asyncio.get_running_loop()

    async def _send_notif(self, users: set):
        for connection in list(self.active_connections):
            if self.usernames.get(connection) in users:
                try:
                    await connection.send_json({"type": "notif"})
                except Exception:
                    pass

    def notify_users_threadsafe(self, users):
        """Aus beliebigem Thread (auch dem normalen Request-Code) aufrufbar."""
        if self.loop and users:
            asyncio.run_coroutine_threadsafe(self._send_notif(set(users)), self.loop)

    def disconnect(self, websocket: WebSocket):
        self.usernames.pop(websocket, None)
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
    me = get_current_user(websocket)
    await manager.connect(websocket, me["username"] if me else None)
    try:
        while True:
            # Simple keep-alive or message receive
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

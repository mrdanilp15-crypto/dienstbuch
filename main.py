import os
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from database import init_db

# Alle modularisierten Router-Pakete importieren
from routers.auth import router as auth_router
from routers.personnel import router as personnel_router
from routers.vehicles import router as vehicles_router
from routers.inventory import router as inventory_router
from routers.tickets import router as tickets_router
from routers.alarm import router as alarm_router
from routers.events import router as events_router
from routers.archive import router as archive_router
from routers.system import router as system_router

app = FastAPI(title="Digitales Dienstbuch")

# Statische Verzeichnisse absichern
if not os.path.exists("static"):
    os.makedirs("static")
app.mount("/static", StaticFiles(directory="static"), name="static")

# Datenbankstruktur beim Booten prüfen und ggf. migrieren
init_db()

# Alle Funktionsmodule im API-Baum registrieren
app.include_router(auth_router)
app.include_router(personnel_router)
app.include_router(vehicles_router)
app.include_router(inventory_router)
app.include_router(tickets_router)
app.include_router(alarm_router)
app.include_router(events_router)
app.include_router(archive_router)
app.include_router(system_router)

# Routen für die Benutzeroberflächen
@app.get("/")
def route_root():
    return FileResponse("static/dashboard.html")

@app.get("/dashboard")
def route_dashboard():
    return FileResponse("static/dashboard.html")

@app.get("/login")
def route_login_page():
    return FileResponse("static/login.html")

@app.get("/editor")
def route_editor_page():
    return FileResponse("static/editor.html")
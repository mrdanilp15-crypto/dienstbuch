import os
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Importiert alle Router-Module exakt wie in image_c9c29b.png abgebildet
from routers import (
    alarm, archive, auth, events, inventory, 
    personnel, psa, sessions, system, tickets, users, vehicles
)

app = FastAPI(title="Digitales Dienstbuch-System")

# 1. Statischen Ordner für Scripte (static/js/app.js) einbinden
app.mount("/static", StaticFiles(directory="static"), name="static")

# 2. Jinja2-Engine für den templates-Ordner aktivieren
templates = Jinja2Templates(directory="templates")

# 3. Alle Backend-Schnittstellen vollautomatisch laden
app.include_router(auth.router)
app.include_router(alarm.router)
app.include_router(archive.router)
app.include_router(events.router)
app.include_router(inventory.router)
app.include_router(personnel.router)
app.include_router(psa.router)
app.include_router(sessions.router)
app.include_router(system.router)
app.include_router(tickets.router)
app.include_router(users.router)
app.include_router(vehicles.router)

# --- SEITEN-ROUTING ÜBER JINJA2-TEMPLATES ---

@app.get("/", response_class=HTMLResponse)
def root_redirect(request: Request):
    # Schickt unangemeldete Browser direkt auf die Login-Kachel
    return templates.TemplateResponse("login.html", {"request": request})

@app.get("/login", response_class=HTMLResponse)
def get_login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.get("/dashboard", response_class=HTMLResponse)
def get_dashboard_page(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/editor", response_class=HTMLResponse)
def get_editor_page(request: Request):
    return templates.TemplateResponse("editor.html", {"request": request})
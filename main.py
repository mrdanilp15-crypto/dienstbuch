import os
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Alle eure Router-Module importieren
from routers import (
    alarm, archive, auth, events, inventory, 
    personnel, psa, sessions, system, tickets, users, vehicles, hydranten
)

app = FastAPI(title="Digitales Dienstbuch-System")

app.mount("/static", StaticFiles(directory="static"), name="static")

# 1. Triebwerk normal starten
templates = Jinja2Templates(directory="templates")

# 2. DER ABSOLUTE GAMECHANGER:
# Wir biegen die Python-Variablen um. Ab jetzt ignoriert der Server alle {{ }} 
# und lässt sie unberührt für Vue.js im Browser stehen!
templates.env.variable_start_string = "{$"
templates.env.variable_end_string = "$}"

# Alle Router laden
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
app.include_router(hydranten.router)



# --- SEITEN-ROUTING ---
@app.get("/", response_class=HTMLResponse)
def root_redirect(request: Request):
    return templates.TemplateResponse(request=request, name="login.html")

@app.get("/login", response_class=HTMLResponse)
def get_login_page(request: Request):
    return templates.TemplateResponse(request=request, name="login.html")

@app.get("/dashboard", response_class=HTMLResponse)
def get_dashboard_page(request: Request):
    return templates.TemplateResponse(request=request, name="dashboard.html")

@app.get("/editor", response_class=HTMLResponse)
def get_editor_page(request: Request):
    return templates.TemplateResponse(request=request, name="editor.html")
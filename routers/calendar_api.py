from fastapi import APIRouter, HTTPException, Request, Response, UploadFile, File
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Optional, List, Union
from datetime import datetime, date
import json

from database import get_db_connection
from core.utils import get_current_user, log_audit_action

router = APIRouter()
import main


# --- ICAL / central calendar CENTRAL EXPORT ---
@router.get("/api/calendar/feed.ics", response_class=Response)
def export_calendar_ical():
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM schedules")
    schedules = cur.fetchall(); cur.close(); conn.close()
    
    import datetime
    ics = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//FF Dienstbuch//Calendar Export//DE\r\n"
    for s in schedules:
        d_val = s["date"] # date object
        t_val = s["time"] # "HH:MM" format
        try:
            h, m = map(int, t_val.split(":"))
            dt = datetime.datetime(d_val.year, d_val.month, d_val.day, h, m)
        except:
            dt = datetime.datetime(d_val.year, d_val.month, d_val.day, 19, 0)
        
        dt_str = dt.strftime("%Y%m%dT%H%M%S")
        dt_end_str = (dt + datetime.timedelta(hours=2)).strftime("%Y%m%dT%H%M%S")
        
        ics += "BEGIN:VEVENT\r\n"
        ics += f"UID:SCH{s['id']}@feuerwehr-dienstbuch.de\r\n"
        ics += f"DTSTAMP:{dt_str}\r\n"
        ics += f"DTSTART:{dt_str}\r\n"
        ics += f"DTEND:{dt_end_str}\r\n"
        ics += f"SUMMARY:{s['title']}\r\n"
        ics += f"DESCRIPTION:{s['description'] or ''} ({s['type']})\r\n"
        ics += "END:VEVENT\r\n"
    ics += "END:VCALENDAR\r\n"
    
    return Response(content=ics, media_type="text/calendar", headers={"Content-Disposition": "attachment; filename=feuerwehr_dienstplan.ics"})

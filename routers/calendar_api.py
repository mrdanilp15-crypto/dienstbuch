import hmac
from fastapi import APIRouter, Response, Request, HTTPException

from database import get_db_connection
from core.utils import get_or_create_token

router = APIRouter()


# --- ICAL / central calendar CENTRAL EXPORT ---
# Kalender-Apps (Outlook, Google Kalender, ...) können beim automatischen Abrufen keine
# Session-Cookies mitschicken - Login ist hier technisch nicht möglich. Wie bei jedem "privaten
# iCal-Link" braucht es stattdessen einen geheimen Token in der URL selbst (sonst waren alle
# geplanten Dienste/Übungen bisher komplett offen im Internet abrufbar).
@router.get("/api/calendar/feed.ics", response_class=Response)
def export_calendar_ical(request: Request, token: str = ""):
    if not token or not hmac.compare_digest(token, get_or_create_token("calendar_token")):
        raise HTTPException(status_code=401, detail="Ungültiger oder fehlender Kalender-Token.")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    # Der Abo-Link ist fuer alle gleich (kein Login) - deshalb nur Termine, die fuer alle gelten.
    cur.execute("SELECT * FROM schedules WHERE audience = 'all' OR audience IS NULL")
    schedules = cur.fetchall()
    # Helfersuchen mit Datum (ohne Namen der Eingetragenen - der Link ist für alle gleich)
    cur.execute("SELECT id, title, description, event_date, event_time FROM polls WHERE kind = 'helfer' AND event_date IS NOT NULL")
    helper_events = cur.fetchall(); cur.close(); conn.close()
    
    import datetime

    def ics_escape(text: str) -> str:
        # RFC 5545: Backslash, Semikolon, Komma und Zeilenumbrüche müssen in TEXT-Werten
        # escaped werden - sonst brechen Kalender-Clients (z.B. bei Kommas/Semikolons in
        # Titel/Beschreibung) die Zeile an der falschen Stelle um oder verwerfen sie.
        return (str(text or "")
                .replace("\\", "\\\\")
                .replace(";", "\\;")
                .replace(",", "\\,")
                .replace("\r\n", "\\n")
                .replace("\n", "\\n"))

    ics = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//FF Dienstbuch//Calendar Export//DE\r\n"
    for s in schedules:
        d_val = s["date"] # date object
        t_val = s["time"] # "HH:MM" format
        try:
            h, m = map(int, t_val.split(":"))
            dt = datetime.datetime(d_val.year, d_val.month, d_val.day, h, m)
        except Exception:
            dt = datetime.datetime(d_val.year, d_val.month, d_val.day, 19, 0)

        dt_str = dt.strftime("%Y%m%dT%H%M%S")
        dt_end_str = (dt + datetime.timedelta(hours=2)).strftime("%Y%m%dT%H%M%S")

        ics += "BEGIN:VEVENT\r\n"
        ics += f"UID:SCH{s['id']}@feuerwehr-dienstbuch.de\r\n"
        ics += f"DTSTAMP:{dt_str}\r\n"
        ics += f"DTSTART:{dt_str}\r\n"
        ics += f"DTEND:{dt_end_str}\r\n"
        ics += f"SUMMARY:{ics_escape(s['title'])}\r\n"
        ics += f"DESCRIPTION:{ics_escape(s['description'])} ({ics_escape(s['type'])})\r\n"
        ics += "END:VEVENT\r\n"
    for p in helper_events:
        day = p["event_date"]
        ics += "BEGIN:VEVENT\r\n"
        ics += f"UID:POLL{p['id']}@feuerwehr-dienstbuch.de\r\n"
        if p["event_time"]:
            h, m = map(int, p["event_time"].split(":"))
            dt = datetime.datetime(day.year, day.month, day.day, h, m)
            ics += f"DTSTAMP:{dt.strftime('%Y%m%dT%H%M%S')}\r\n"
            ics += f"DTSTART:{dt.strftime('%Y%m%dT%H%M%S')}\r\n"
            ics += f"DTEND:{(dt + datetime.timedelta(hours=2)).strftime('%Y%m%dT%H%M%S')}\r\n"
        else:  # ganztägig
            ics += f"DTSTAMP:{day.strftime('%Y%m%d')}T000000\r\n"
            ics += f"DTSTART;VALUE=DATE:{day.strftime('%Y%m%d')}\r\n"
            ics += f"DTEND;VALUE=DATE:{(day + datetime.timedelta(days=1)).strftime('%Y%m%d')}\r\n"
        ics += f"SUMMARY:{ics_escape('Helfersuche: ' + p['title'])}\r\n"
        ics += f"DESCRIPTION:{ics_escape(p['description'])}\r\n"
        ics += "END:VEVENT\r\n"
    ics += "END:VCALENDAR\r\n"
    
    return Response(content=ics, media_type="text/calendar", headers={"Content-Disposition": "attachment; filename=feuerwehr_dienstplan.ics"})

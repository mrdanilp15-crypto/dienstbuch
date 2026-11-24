"""Macht aus dem Klartext einer Mail eine gestaltete HTML-Mail (Kopfband, Absätze, Listen, Tabellen, Knöpfe).

Alle Mails entstehen weiter als einfacher Text; hier wird er nur erkannt und hübsch gesetzt. Der Klartext
geht als Alternative mit, falls ein Mailprogramm kein HTML anzeigt. Alles wird maskiert (kein Einschleusen
von HTML über Alarm-Stichworte o. Ä.). Nur Inline-Styles und Tabellen, weil Mailprogramme nichts anderes sicher können.
"""
import html
import re

_URL = re.compile(r"(https?://[^\s<>\"]+)")
_BULLET = re.compile(r"^\s*(?:[-•*])\s+(.*)$")
_KEYVAL = re.compile(r"^([A-ZÄÖÜa-zäöüß][^:\n]{0,28}):\s+(\S.*)$")
_BUTTON_LABELS = ("link", "öffnen", "zum termin", "ansehen", "anmelden", "passwort", "festlegen", "direkt")
_RED = "#b71c1c"


def _e(text: str) -> str:
    return html.escape(text or "", quote=True)


def _inline(text: str) -> str:
    """Maskieren, Adressen anklickbar, Fristen-Verzug rot."""
    out, pos = [], 0
    for m in _URL.finditer(text):
        out.append(_e(text[pos:m.start()]))
        url = m.group(1).rstrip(".,;)")
        tail = m.group(1)[len(url):]
        out.append(f'<a href="{_e(url)}" style="color:{_RED};word-break:break-all">{_e(url)}</a>{_e(tail)}')
        pos = m.end()
    out.append(_e(text[pos:]))
    s = "".join(out)
    return s.replace("ÜBERFÄLLIG", f'<strong style="color:{_RED}">ÜBERFÄLLIG</strong>')


def _is_heading(line: str) -> bool:
    t = line.strip()
    letters = [c for c in t if c.isalpha()]
    return len(letters) >= 3 and t == t.upper() and len(t) <= 60 and not t.endswith(":") and "http" not in t.lower()


def _button(label: str, url: str) -> str:
    return (f'<table role="presentation" cellpadding="0" cellspacing="0" style="margin:18px 0"><tr>'
            f'<td style="background:{_RED};border-radius:6px"><a href="{_e(url)}" '
            f'style="display:inline-block;padding:12px 22px;color:#ffffff;font-weight:600;text-decoration:none;'
            f'font-size:15px">{_e(label)}</a></td></tr></table>')


def _flush_para(buf, out):
    if buf:
        out.append(f'<p style="margin:0 0 14px;line-height:1.55">{"<br>".join(_inline(x) for x in buf)}</p>')
        buf.clear()


def _flush_list(items, out):
    if items:
        lis = "".join(f'<li style="margin:0 0 6px;line-height:1.5">{_inline(x)}</li>' for x in items)
        out.append(f'<ul style="margin:0 0 16px;padding-left:22px">{lis}</ul>')
        items.clear()


def _flush_rows(rows, out):
    if not rows:
        return
    if len(rows) == 1:  # eine einzelne "Wort: Wert"-Zeile ist eher Fließtext
        out.append(f'<p style="margin:0 0 14px;line-height:1.55">{_inline(rows[0][0] + ": " + rows[0][1])}</p>')
    else:
        trs = "".join(
            f'<tr><td style="padding:7px 12px 7px 0;color:#6b7280;vertical-align:top;white-space:nowrap">{_e(k)}</td>'
            f'<td style="padding:7px 0;font-weight:600;vertical-align:top">{_inline(v)}</td></tr>' for k, v in rows)
        out.append(f'<table role="presentation" cellpadding="0" cellspacing="0" style="margin:0 0 16px;width:100%;'
                   f'border-top:1px solid #e5e7eb;border-bottom:1px solid #e5e7eb">{trs}</table>')
    rows.clear()


def render_html(subject: str, body: str, station: str = "Dienstbuch", alarm: bool = False) -> str:
    out, para, items, rows = [], [], [], []

    def flush_all():
        _flush_para(para, out)
        _flush_list(items, out)
        _flush_rows(rows, out)

    for raw in (body or "").splitlines():
        line = raw.rstrip()
        if not line.strip():
            flush_all()
            continue
        m = _BULLET.match(line)
        if m:
            _flush_para(para, out); _flush_rows(rows, out)
            items.append(m.group(1))
            continue
        if _is_heading(line):
            flush_all()
            out.append(f'<h3 style="margin:18px 0 8px;font-size:13px;letter-spacing:.06em;color:{_RED}">{_e(line.strip())}</h3>')
            continue
        url_only = re.fullmatch(r"\s*(https?://\S+)\s*", line)
        label_url = re.fullmatch(r"\s*([^:]{2,40}):\s*(https?://\S+)\s*", line)
        if url_only or (label_url and any(w in label_url.group(1).lower() for w in _BUTTON_LABELS)):
            flush_all()
            if url_only:
                out.append(_button("Im Dienstbuch öffnen", url_only.group(1)))
            else:
                out.append(_button(label_url.group(1).strip(), label_url.group(2)))
            continue
        kv = _KEYVAL.match(line)
        if kv and "http" not in kv.group(1).lower() and len(line) < 140:
            _flush_para(para, out); _flush_list(items, out)
            rows.append((kv.group(1).strip(), kv.group(2).strip()))
            continue
        _flush_list(items, out); _flush_rows(rows, out)
        para.append(line.strip())
    flush_all()

    band = _RED if alarm else "#263238"
    content = "".join(out)
    return f"""<!doctype html>
<html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f4f6f9;font-family:-apple-system,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;color:#1f2937">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f4f6f9"><tr><td align="center" style="padding:20px 10px">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:560px;background:#ffffff;border-radius:10px;overflow:hidden;border:1px solid #e5e7eb">
<tr><td style="background:{band};padding:18px 24px"><div style="color:#ffffff;font-size:12px;letter-spacing:.08em;text-transform:uppercase;opacity:.8">{_e(station)}</div>
<div style="color:#ffffff;font-size:20px;font-weight:700;margin-top:4px;line-height:1.3">{_e(subject)}</div></td></tr>
<tr><td style="padding:24px;font-size:15px">{content}</td></tr>
<tr><td style="padding:14px 24px;background:#f9fafb;border-top:1px solid #e5e7eb;font-size:12px;color:#6b7280">Gesendet vom Digitalen Dienstbuch · {_e(station)}</td></tr>
</table></td></tr></table></body></html>"""

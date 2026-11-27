"""Macht aus dem Klartext einer Mail eine gestaltete HTML-Mail (Kopf, Infokasten, Listen, Knöpfe).

Alle Mails entstehen weiter als einfacher Text; hier wird er nur erkannt und hübsch gesetzt. Der Klartext
geht als Alternative mit, falls ein Mailprogramm kein HTML anzeigt. Alles wird maskiert (kein Einschleusen
von HTML über Alarm-Stichworte o. Ä.). Nur Inline-Styles und Tabellen, weil Mailprogramme nichts anderes sicher können.
"""
import html
import re

_URL = re.compile(r"(https?://[^\s<>\"]+)")
_BULLET = re.compile(r"^\s*(?:[-•*])\s+(.*)$")
_KEYVAL = re.compile(r"^([A-ZÄÖÜa-zäöüß][^:\n]{0,28}):\s+(\S.*)$")
_NUMBER = re.compile(r"^[\d.,]+( ?(h|Std\.?|%))?$")
_BUTTON_LABELS = ("link", "öffnen", "zum termin", "ansehen", "anmelden", "passwort", "festlegen", "direkt", "eintragen")
_RED = "#b71c1c"
_TEXT = "#1f2937"
_MUTED = "#6b7280"
_LINE = "#e5e7eb"
_BOX = "#f5f6f8"
_FONT = "-apple-system,'Segoe UI',Roboto,Helvetica,Arial,sans-serif"


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
    """GROSS geschriebene Zeile, gern mit kleiner Erklärung in Klammern: "ERSTKONTROLLE OFFEN (noch nie dokumentiert)"."""
    t = line.strip()
    head = t.split("(")[0].strip()
    letters = [c for c in head if c.isalpha()]
    return len(letters) >= 3 and head == head.upper() and len(t) <= 70 and not t.endswith(":") and "http" not in t.lower()


def _button_label(url: str) -> str:
    if "reset-password" in url:
        return "Passwort festlegen"
    if url.rstrip("/").endswith("/login"):
        return "Zur Anmeldung"
    return "Im Dienstbuch öffnen"


def _button(label: str, url: str) -> str:
    return (f'<table role="presentation" cellpadding="0" cellspacing="0" style="margin:6px 0 22px"><tr>'
            f'<td style="background:{_RED};border-radius:8px"><a href="{_e(url)}" '
            f'style="display:inline-block;padding:13px 26px;color:#ffffff;font-weight:700;text-decoration:none;'
            f'font-size:15px;font-family:{_FONT}">{_e(label)} &rarr;</a></td></tr></table>')


def _flush_para(buf, out):
    if buf:
        out.append(f'<p style="margin:0 0 16px;line-height:1.6">{"<br>".join(_inline(x) for x in buf)}</p>')
        buf.clear()


def _flush_list(items, out):
    if items:
        lis = "".join(
            f'<tr><td style="width:18px;vertical-align:top;padding:5px 0;color:{_RED};font-weight:700">&bull;</td>'
            f'<td style="padding:5px 0;line-height:1.5">{_inline(x)}</td></tr>' for x in items)
        out.append(f'<table role="presentation" cellpadding="0" cellspacing="0" style="width:100%;margin:0 0 18px">{lis}</table>')
        items.clear()


def _flush_rows(rows, out):
    """"Wort: Wert"-Zeilen als grauer Infokasten. Reine Zahlen (Statistik) rechtsbündig, damit sie untereinander stehen."""
    if not rows:
        return
    numeric = all(_NUMBER.match(v) for _, v in rows)
    trs = []
    for i, (k, v) in enumerate(rows):
        border = f"border-top:1px solid {_LINE};" if i else ""
        if numeric:
            trs.append(f'<tr><td style="{border}padding:9px 16px;color:{_MUTED}">{_e(k)}</td>'
                       f'<td style="{border}padding:9px 16px;font-weight:700;text-align:right;white-space:nowrap">{_inline(v)}</td></tr>')
        else:
            trs.append(f'<tr><td style="{border}padding:9px 16px;color:{_MUTED};vertical-align:top;white-space:nowrap;width:1%">{_e(k)}</td>'
                       f'<td style="{border}padding:9px 16px 9px 0;font-weight:700;vertical-align:top">{_inline(v)}</td></tr>')
    out.append(f'<table role="presentation" cellpadding="0" cellspacing="0" style="width:100%;margin:0 0 18px;'
               f'background:{_BOX};border-radius:8px">{"".join(trs)}</table>')
    rows.clear()


def _split_subject(subject: str):
    """"Helfer gesucht: Grillfest" -> ("Helfer gesucht", "Grillfest"); ohne kurzen Vorsatz nur der Titel."""
    m = re.fullmatch(r"([^:]{3,30}):\s+(.+)", subject.strip())
    if m and not re.search(r"\d", m.group(1)):
        return m.group(1), m.group(2)
    return "", subject.strip()


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
            t = line.strip()
            head, _, note = t.partition("(")
            note_html = f' <span style="color:{_MUTED};font-weight:400;letter-spacing:0;text-transform:none">({_e(note)}</span>' if note else ""
            out.append(f'<div style="margin:24px 0 10px;padding-bottom:6px;border-bottom:2px solid {_RED};font-size:12px;'
                       f'font-weight:700;letter-spacing:.08em;color:{_RED}">{_e(head.strip())}{note_html}</div>')
            continue
        url_only = re.fullmatch(r"\s*(https?://\S+)\s*", line)
        label_url = re.fullmatch(r"\s*([^:]{2,40}):\s*(https?://\S+)\s*", line)
        if url_only or (label_url and any(w in label_url.group(1).lower() for w in _BUTTON_LABELS)):
            flush_all()
            if url_only:
                out.append(_button(_button_label(url_only.group(1)), url_only.group(1)))
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

    kicker, title = _split_subject(subject)
    if alarm:
        head = (f'<tr><td style="background:{_RED};padding:22px 28px">'
                f'<div style="color:#ffffff;font-size:12px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;opacity:.85">'
                f'&#128680; {_e(kicker or "Alarm")} &middot; {_e(station)}</div>'
                f'<div style="color:#ffffff;font-size:24px;font-weight:800;margin-top:6px;line-height:1.25">{_e(title)}</div></td></tr>')
    else:
        kick = f'<div style="color:{_RED};font-size:12px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;margin-bottom:6px">{_e(kicker)}</div>' if kicker else ""
        head = (f'<tr><td style="height:6px;background:{_RED};font-size:0;line-height:0">&nbsp;</td></tr>'
                f'<tr><td style="padding:18px 28px 0;font-size:13px;color:{_MUTED};font-weight:600">&#128658; {_e(station)}</td></tr>'
                f'<tr><td style="padding:14px 28px 4px">{kick}'
                f'<div style="color:{_TEXT};font-size:22px;font-weight:800;line-height:1.3">{_e(title)}</div></td></tr>')
    # Vorschautext in der Postfachliste: erste richtige Textzeile statt "Hallo," oder einer Adresse
    lines = [x.strip() for x in (body or "").splitlines() if x.strip() and not x.strip().lower().startswith(("hallo", "http"))]
    preview = _e((lines[0] if lines else "")[:140])
    content = "".join(out)
    return f"""<!doctype html>
<html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light only"><meta name="supported-color-schemes" content="light"><title>{_e(subject)}</title></head>
<body style="margin:0;padding:0;background:#eef1f5;font-family:{_FONT};color:{_TEXT}">
<div style="display:none;max-height:0;overflow:hidden;opacity:0;color:transparent">{preview}&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#eef1f5"><tr><td align="center" style="padding:24px 12px">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:580px;background:#ffffff;border-radius:12px;overflow:hidden;border:1px solid #dfe3e8">
{head}
<tr><td style="padding:18px 28px 10px;font-size:15px;line-height:1.6;color:{_TEXT}">{content}</td></tr>
</table>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:580px"><tr><td style="padding:16px 28px;font-size:12px;line-height:1.5;color:#8a94a3;text-align:center">
Automatisch verschickt vom Digitalen Dienstbuch &middot; {_e(station)}
</td></tr></table>
</td></tr></table></body></html>"""

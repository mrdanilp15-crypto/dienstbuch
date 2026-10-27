"""Frei definierbare Logbücher (z.B. Flugbuch): Felder prüfen, Dauer berechnen, auswerten, als CSV ausgeben.

Die Einträge speichern ihre Werte je Feld-ID. Die Dauer-Felder werden nie gespeichert, sondern aus Start und Ende
berechnet - so bleibt sie auch nach einer Korrektur der Zeiten richtig."""
import csv
import datetime
import io
import json
import re

FIELD_TYPES = ("text", "number", "date", "time", "bool", "select", "duration")
EVALUATE = ("none", "sum", "group")
_TIME = re.compile(r"^([01]?\d|2[0-3]):[0-5]\d$")


class FieldError(ValueError):
    pass


def parse_json(raw, default):
    try:
        return json.loads(raw) if raw else default
    except ValueError:
        return default


def public_field(row) -> dict:
    return {"id": row["id"], "label": row["label"], "ftype": row["ftype"], "unit": row["unit"] or "",
            "options": parse_json(row["options"], []), "required": bool(row["required"]), "evaluate": row["evaluate"],
            "config": parse_json(row["config"], {}), "sort_order": row["sort_order"]}


def check_field_defs(fields: list):
    """Prüft die Felddefinitionen eines Logbuchs (vom Editor). Wirft FieldError mit deutscher Meldung."""
    if not fields:
        raise FieldError("Bitte mindestens ein Feld anlegen.")
    if len(fields) > 40:
        raise FieldError("Höchstens 40 Felder je Logbuch.")
    labels = set()
    for f in fields:
        label = (f.get("label") or "").strip()
        if not label or len(label) > 80:
            raise FieldError("Jedes Feld braucht eine Bezeichnung (höchstens 80 Zeichen).")
        if label.lower() in labels:
            raise FieldError(f"Die Bezeichnung „{label}“ kommt doppelt vor.")
        labels.add(label.lower())
        if f.get("ftype") not in FIELD_TYPES:
            raise FieldError(f"Unbekannter Feldtyp bei „{label}“.")
        if f.get("evaluate", "none") not in EVALUATE:
            raise FieldError(f"Unbekannte Auswertung bei „{label}“.")
        if f["ftype"] == "select" and not [o for o in (f.get("options") or []) if str(o).strip()]:
            raise FieldError(f"Bei „{label}“ (Auswahl) fehlen die Auswahlwerte.")
        if f["ftype"] == "duration":
            cfg = f.get("config") or {}
            if not cfg.get("start") or not cfg.get("end"):
                raise FieldError(f"Bei „{label}“ (Dauer) bitte Start- und Endfeld wählen.")


def _minutes(hm):
    h, m = hm.split(":")
    return int(h) * 60 + int(m)


def duration_minutes(field, values: dict):
    """Dauer in Minuten aus Start- und Endzeit (über Mitternacht gerechnet), None wenn eine Zeit fehlt."""
    cfg = field.get("config") or {}
    a, b = values.get(str(cfg.get("start"))), values.get(str(cfg.get("end")))
    if not a or not b or not _TIME.match(str(a)) or not _TIME.match(str(b)):
        return None
    d = _minutes(b) - _minutes(a)
    return d + 1440 if d < 0 else d


def clean_values(fields: list, raw: dict) -> dict:
    """Prüft die Eingaben eines Eintrags und liefert die zu speichernden Werte. Wirft FieldError."""
    out = {}
    for f in fields:
        if f["ftype"] == "duration":
            continue
        key = str(f["id"])
        v = raw.get(key, raw.get(f["id"]))
        empty = v is None or (isinstance(v, str) and not v.strip())
        if f["ftype"] == "bool":
            out[key] = bool(v) if v not in ("false", "0", "") else False
            continue
        if empty:
            if f["required"]:
                raise FieldError(f"„{f['label']}“ ist ein Pflichtfeld.")
            continue
        try:
            if f["ftype"] == "text":
                out[key] = str(v).strip()[:500]
            elif f["ftype"] == "number":
                n = float(str(v).replace(",", "."))
                out[key] = int(n) if n == int(n) else n
            elif f["ftype"] == "date":
                out[key] = datetime.date.fromisoformat(str(v)).isoformat()
            elif f["ftype"] == "time":
                if not _TIME.match(str(v)):
                    raise ValueError
                out[key] = str(v).zfill(5)
            elif f["ftype"] == "select":
                if str(v) not in f["options"]:
                    raise ValueError
                out[key] = str(v)
        except (ValueError, OverflowError):
            raise FieldError(f"Ungültiger Wert bei „{f['label']}“.")
    for f in fields:
        if f["ftype"] == "duration" and f["required"] and duration_minutes(f, out) is None:
            raise FieldError(f"„{f['label']}“: bitte Start- und Endzeit eintragen.")
    return out


def value_of(field, values: dict):
    """Anzeige-/Rechenwert eines Feldes: bei Dauer die Minuten, sonst der gespeicherte Wert."""
    if field["ftype"] == "duration":
        return duration_minutes(field, values)
    return values.get(str(field["id"]))


def fmt_minutes(m) -> str:
    return "" if m is None else f"{m // 60}:{m % 60:02d}"


def _fmt_total(field, value) -> str:
    if field["ftype"] == "duration":
        return fmt_minutes(int(value)) + " h"
    num = f"{value:g}" if isinstance(value, float) else str(value)
    return f"{num} {field['unit']}".strip()


def evaluate(fields: list, entries: list) -> dict:
    """Auswertung: Summen (Zahl/Dauer mit Auswertung 'sum'), Gruppierung nach Auswahl-/Ja-Nein-Feldern ('group'),
    je Person und je Bezug. entries: [{entry_date, person, subject, values}]."""
    sums = [f for f in fields if f["evaluate"] == "sum" and f["ftype"] in ("number", "duration")]
    groups = [f for f in fields if f["evaluate"] == "group" and f["ftype"] in ("select", "bool", "text")]

    def bucket(rows):
        tot = {}
        for f in sums:
            vals = [value_of(f, r["values"]) for r in rows]
            vals = [v for v in vals if isinstance(v, (int, float))]
            tot[f["label"]] = _fmt_total(f, sum(vals)) if vals else _fmt_total(f, 0)
        return tot

    def by(keyfn):
        acc = {}
        for r in entries:
            acc.setdefault(keyfn(r), []).append(r)
        return acc

    people = []
    for name, rows in sorted(by(lambda r: r["person"] or "(ohne Person)").items()):
        people.append({"name": name, "count": len(rows), "sums": bucket(rows), "last_date": max(r["entry_date"] for r in rows)})
    subjects = [{"name": name, "count": len(rows), "sums": bucket(rows)} for name, rows in sorted(by(lambda r: r["subject"] or "(ohne Bezug)").items())]
    grouped = []
    for f in groups:
        def val(r, f=f):
            v = r["values"].get(str(f["id"]))
            return ("Ja" if v else "Nein") if f["ftype"] == "bool" else (v or "(leer)")
        grouped.append({"label": f["label"], "rows": [{"value": k, "count": len(rows), "sums": bucket(rows)} for k, rows in sorted(by(val).items())]})
    return {"count": len(entries), "totals": bucket(entries), "by_person": people, "by_subject": subjects, "by_field": grouped}


def _safe(v):
    """Text, der in Excel mit = + - @ beginnt, würde als Formel ausgeführt - mit ' davor bleibt er Text."""
    return "'" + v if isinstance(v, str) and v[:1] in ("=", "+", "-", "@", "\t", "\r") else v


def to_csv(fields: list, entries: list, subject_label: str) -> str:
    """CSV für Excel/LibreOffice: Semikolon, UTF-8 mit BOM, Dauer als h:mm."""
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";")
    w.writerow(["Datum", "Person", subject_label or "Bezug"] + [f["label"] + (f" ({f['unit']})" if f["unit"] else "") for f in fields])
    for e in entries:
        row = [datetime.date.fromisoformat(e["entry_date"]).strftime("%d.%m.%Y"), _safe(e["person"] or ""), _safe(e["subject"] or "")]
        for f in fields:
            v = value_of(f, e["values"])
            if f["ftype"] == "duration":
                v = fmt_minutes(v)
            elif f["ftype"] == "bool":
                v = "Ja" if v else "Nein"
            elif f["ftype"] == "date" and v:
                v = datetime.date.fromisoformat(v).strftime("%d.%m.%Y")
            elif f["ftype"] == "number" and v is not None:
                v = str(v).replace(".", ",")
            row.append("" if v is None else (_safe(v) if f["ftype"] in ("text", "select") else v))
        w.writerow(row)
    return "﻿" + buf.getvalue()

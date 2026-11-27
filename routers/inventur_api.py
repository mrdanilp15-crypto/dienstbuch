"""Inventur: Geräte (und auf Wunsch Verbrauchsmaterial) am Handy durchgehen und abhaken.

Beim Start wird eine Liste eingefroren (Name, Kategorie, Barcode), damit spätere Änderungen an den Geräten das Protokoll
nicht verfälschen. Beim Abschluss wird aus „fehlt“/„defekt“ je ein Mangel, gezählte Verbrauchsmengen werden übernommen,
und es gibt ein PDF-Protokoll.
"""
import datetime
import io
from html import escape
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel

from core.utils import check_auth, get_station_name, log_audit_action
from database import get_db_connection

router = APIRouter(prefix="/api/inventur", tags=["Inventur"])
PERM = "material.manage"
RESULTS = ("ok", "fehlt", "defekt")
RESULT_LABEL = {"ok": "Vorhanden", "fehlt": "Fehlt", "defekt": "Defekt", None: "Nicht geprüft"}


def ensure_tables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inventories (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(150) NOT NULL,
            category VARCHAR(255) NOT NULL DEFAULT '',
            started_by VARCHAR(100) NOT NULL,
            started_at DATETIME NOT NULL,
            finished_by VARCHAR(100) NULL,
            finished_at DATETIME NULL,
            note TEXT NULL
        ) ENGINE=InnoDB
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inventory_items (
            id INT AUTO_INCREMENT PRIMARY KEY,
            inventory_id INT NOT NULL,
            kind VARCHAR(20) NOT NULL DEFAULT 'equipment',
            ref_id INT NULL,
            name VARCHAR(255) NOT NULL,
            category VARCHAR(255) NOT NULL DEFAULT '',
            barcode VARCHAR(255) NOT NULL DEFAULT '',
            expected DECIMAL(10,2) NULL,
            unit VARCHAR(50) NOT NULL DEFAULT '',
            result VARCHAR(20) NULL,
            counted DECIMAL(10,2) NULL,
            note VARCHAR(500) NOT NULL DEFAULT '',
            checked_by VARCHAR(100) NULL,
            checked_at DATETIME NULL,
            defect_id INT NULL,
            INDEX (inventory_id),
            FOREIGN KEY (inventory_id) REFERENCES inventories(id) ON DELETE CASCADE
        ) ENGINE=InnoDB
    """)


class InventoryStart(BaseModel):
    title: Optional[str] = ""
    category: Optional[str] = ""
    include_consumables: bool = False


class ItemUpdate(BaseModel):
    result: Optional[str] = None
    counted: Optional[float] = None
    note: Optional[str] = ""


def _num(v):
    return float(v) if v is not None else None


def _ts(v):
    return v.strftime("%Y-%m-%d %H:%M") if v else None


def _load(cur, inv_id: int):
    cur.execute("SELECT * FROM inventories WHERE id = %s", (inv_id,))
    inv = cur.fetchone()
    if not inv:
        raise HTTPException(status_code=404, detail="Inventur nicht gefunden")
    return inv


def _summary(items):
    out = {"total": len(items), "ok": 0, "fehlt": 0, "defekt": 0, "open": 0}
    for i in items:
        if i["kind"] == "consumable":
            # Verbrauchsmaterial gilt als erledigt, sobald eine Menge gezählt ist
            out["ok" if i["counted"] is not None else "open"] += 1
        else:
            out[i["result"] or "open"] += 1
    return out


@router.get("")
def list_inventories(request: Request):
    check_auth(request, permission=PERM)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM inventories ORDER BY started_at DESC, id DESC LIMIT 50")
    invs = cur.fetchall()
    cur.execute("SELECT inventory_id, kind, result, counted FROM inventory_items")
    by_inv = {}
    for r in cur.fetchall():
        by_inv.setdefault(r["inventory_id"], []).append(r)
    cur.execute("SELECT DISTINCT category FROM equipment WHERE archived = 0 AND category <> '' ORDER BY category")
    categories = [r["category"] for r in cur.fetchall()]
    cur.close(); conn.close()
    return {"inventories": [{"id": i["id"], "title": i["title"], "category": i["category"], "started_by": i["started_by"],
                             "started_at": _ts(i["started_at"]), "finished_by": i["finished_by"], "finished_at": _ts(i["finished_at"]),
                             **_summary(by_inv.get(i["id"], []))} for i in invs],
            "categories": categories}


@router.post("")
def start_inventory(data: InventoryStart, request: Request):
    user = check_auth(request, permission=PERM)
    category = (data.category or "").strip()[:255]
    title = (data.title or "").strip()[:150] or ("Inventur " + (category + " " if category else "") + datetime.date.today().strftime("%d.%m.%Y"))
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT id FROM inventories WHERE finished_at IS NULL LIMIT 1")
        if cur.fetchone():
            raise HTTPException(status_code=409, detail="Es läuft schon eine Inventur. Bitte erst abschließen oder löschen.")
        if category:
            cur.execute("SELECT id, name, category, barcode FROM equipment WHERE archived = 0 AND category = %s ORDER BY name", (category,))
        else:
            cur.execute("SELECT id, name, category, barcode FROM equipment WHERE archived = 0 ORDER BY category, name")
        equipment = cur.fetchall()
        consumables = []
        if data.include_consumables:
            cur.execute("SELECT id, name, unit, current_stock FROM consumables ORDER BY name")
            consumables = cur.fetchall()
        if not equipment and not consumables:
            raise HTTPException(status_code=400, detail="Für diese Auswahl gibt es keine Geräte.")
        cur.execute("INSERT INTO inventories (title, category, started_by, started_at) VALUES (%s, %s, %s, NOW())",
                    (title, category, user["username"]))
        inv_id = cur.lastrowid
        cur.executemany("INSERT INTO inventory_items (inventory_id, kind, ref_id, name, category, barcode) VALUES (%s, 'equipment', %s, %s, %s, %s)",
                        [(inv_id, e["id"], e["name"], e["category"] or "", e["barcode"] or "") for e in equipment])
        if consumables:
            cur.executemany("INSERT INTO inventory_items (inventory_id, kind, ref_id, name, category, expected, unit) "
                            "VALUES (%s, 'consumable', %s, %s, 'Verbrauchsmaterial', %s, %s)",
                            [(inv_id, c["id"], c["name"], c["current_stock"], c["unit"] or "") for c in consumables])
        conn.commit()
    finally:
        cur.close(); conn.close()
    log_audit_action(user["username"], "INVENTUR_GESTARTET", f"{title}: {len(equipment)} Geräte, {len(consumables)} Verbrauchsartikel")
    return {"id": inv_id}


@router.get("/{inv_id}")
def get_inventory(inv_id: int, request: Request):
    check_auth(request, permission=PERM)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    try:
        inv = _load(cur, inv_id)
        cur.execute("SELECT * FROM inventory_items WHERE inventory_id = %s ORDER BY kind DESC, category, name", (inv_id,))
        items = cur.fetchall()
    finally:
        cur.close(); conn.close()
    for i in items:
        i["expected"], i["counted"] = _num(i["expected"]), _num(i["counted"])
        i["checked_at"] = _ts(i["checked_at"])
    return {"id": inv["id"], "title": inv["title"], "category": inv["category"], "started_by": inv["started_by"],
            "started_at": _ts(inv["started_at"]), "finished_by": inv["finished_by"], "finished_at": _ts(inv["finished_at"]),
            "note": inv["note"] or "", "items": items, "summary": _summary(items)}


@router.put("/{inv_id}/items/{item_id}")
def update_item(inv_id: int, item_id: int, data: ItemUpdate, request: Request):
    user = check_auth(request, permission=PERM)
    if data.result is not None and data.result not in RESULTS and data.result != "":
        raise HTTPException(status_code=400, detail="Ergebnis muss vorhanden, fehlt oder defekt sein.")
    if data.counted is not None and not (0 <= data.counted <= 1_000_000):
        raise HTTPException(status_code=400, detail="Ungültige Menge")
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    try:
        inv = _load(cur, inv_id)
        if inv["finished_at"]:
            raise HTTPException(status_code=409, detail="Diese Inventur ist schon abgeschlossen.")
        cur.execute("SELECT id, kind FROM inventory_items WHERE id = %s AND inventory_id = %s", (item_id, inv_id))
        item = cur.fetchone()
        if not item:
            raise HTTPException(status_code=404, detail="Eintrag nicht gefunden")
        result = (data.result or None) if item["kind"] == "equipment" else None
        counted = data.counted if item["kind"] == "consumable" else None
        done = result is not None or counted is not None
        cur.execute("UPDATE inventory_items SET result=%s, counted=%s, note=%s, checked_by=%s, checked_at=%s WHERE id=%s",
                    (result, counted, (data.note or "").strip()[:500], user["username"] if done else None,
                     datetime.datetime.now() if done else None, item_id))
        conn.commit()
    finally:
        cur.close(); conn.close()
    return {"status": "success"}


@router.post("/{inv_id}/finish")
def finish_inventory(inv_id: int, data: dict, request: Request):
    """Abschließen: aus „fehlt“/„defekt“ wird je ein Mangel (Gerätewarte werden benachrichtigt), gezählte Mengen werden Bestand."""
    from core.notify import notify
    from core.permissions import users_with
    user = check_auth(request, permission=PERM)
    note = (data.get("note") or "").strip()[:2000]
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    try:
        inv = _load(cur, inv_id)
        if inv["finished_at"]:
            raise HTTPException(status_code=409, detail="Diese Inventur ist schon abgeschlossen.")
        cur.execute("SELECT * FROM inventory_items WHERE inventory_id = %s", (inv_id,))
        items = cur.fetchall()
        summary = _summary(items)
        if summary["open"] and not data.get("allow_open"):
            raise HTTPException(status_code=409, detail=f"Noch {summary['open']} Einträge nicht geprüft.")
        defects = 0
        for i in items:
            if i["kind"] == "equipment" and i["result"] in ("fehlt", "defekt") and i["ref_id"]:
                text = f"Inventur „{inv['title']}“: {'fehlt' if i['result'] == 'fehlt' else 'defekt'}" + (f"\n{i['note']}" if i["note"] else "")
                cur.execute("SELECT id FROM equipment WHERE id = %s", (i["ref_id"],))
                if not cur.fetchone():
                    continue  # Gerät inzwischen gelöscht
                cur.execute("INSERT INTO equipment_defect_reports (equipment_id, reporter_name, description, severity, priority) "
                            "VALUES (%s, %s, %s, %s, %s)", (i["ref_id"], user["username"], text,
                                                            "Hoch" if i["result"] == "fehlt" else "Mittel", "Hoch" if i["result"] == "fehlt" else "Mittel"))
                cur.execute("UPDATE inventory_items SET defect_id = %s WHERE id = %s", (cur.lastrowid, i["id"]))
                defects += 1
            elif i["kind"] == "consumable" and i["counted"] is not None and i["ref_id"]:
                cur.execute("UPDATE consumables SET current_stock = %s WHERE id = %s", (i["counted"], i["ref_id"]))
        cur.execute("UPDATE inventories SET finished_at = NOW(), finished_by = %s, note = %s WHERE id = %s", (user["username"], note, inv_id))
        conn.commit()
    finally:
        cur.close(); conn.close()
    log_audit_action(user["username"], "INVENTUR_ABGESCHLOSSEN",
                     f"{inv['title']}: {summary['ok']} vorhanden, {summary['fehlt']} fehlen, {summary['defekt']} defekt, {summary['open']} nicht geprüft")
    if defects:
        notify("maengel", f"Inventur: {defects} {'Mangel' if defects == 1 else 'Mängel'}", f"{inv['title']}: {summary['fehlt']} fehlen, {summary['defekt']} defekt.",
               "/dashboard?open=maengel", usernames=users_with(PERM))
    return {"status": "success", "defects": defects, "summary": summary}


@router.delete("/{inv_id}")
def delete_inventory(inv_id: int, request: Request):
    user = check_auth(request, permission=PERM)
    conn = get_db_connection(); cur = conn.cursor(dictionary=True)
    try:
        inv = _load(cur, inv_id)
        if inv["finished_at"]:
            raise HTTPException(status_code=409, detail="Abgeschlossene Inventuren bleiben als Nachweis erhalten.")
        cur.execute("DELETE FROM inventories WHERE id = %s", (inv_id,))
        conn.commit()
    finally:
        cur.close(); conn.close()
    log_audit_action(user["username"], "INVENTUR_GELOESCHT", inv["title"])
    return {"status": "success"}


@router.get("/{inv_id}/pdf")
def inventory_pdf(inv_id: int, request: Request):
    check_auth(request, permission=PERM)
    data = get_inventory(inv_id, request)
    esc = lambda v: escape(str(v)) if v not in (None, "") else ""
    de = lambda ts: datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M").strftime("%d.%m.%Y, %H:%M Uhr") if ts else "–"
    num = lambda v: (f"{v:g}".replace(".", ",") if v is not None else "–")
    s = data["summary"]
    colors = {"ok": "#15803d", "fehlt": "#b91c1c", "defekt": "#c2410c", None: "#6b7280"}
    rows_eq, rows_co = "", ""
    for i in data["items"]:
        if i["kind"] == "equipment":
            rows_eq += (f"<tr><td>{esc(i['name'])}</td><td>{esc(i['category'])}</td><td>{esc(i['barcode'])}</td>"
                        f"<td style='color:{colors[i['result']]}; font-weight:bold;'>{RESULT_LABEL[i['result']]}</td><td>{esc(i['note'])}</td></tr>")
        else:
            diff = "" if i["counted"] is None or i["expected"] is None else num(i["counted"] - i["expected"])
            if diff and not diff.startswith("-") and diff != "0":
                diff = "+" + diff
            rows_co += (f"<tr><td>{esc(i['name'])}</td><td>{num(i['expected'])} {esc(i['unit'])}</td><td>{num(i['counted'])} {esc(i['unit'])}</td>"
                        f"<td>{diff}</td><td>{esc(i['note'])}</td></tr>")
    co_block = (f"<h3>Verbrauchsmaterial</h3><table class='list'><thead><tr><th>Artikel</th><th>Soll</th><th>Gezählt</th><th>Differenz</th><th>Notiz</th></tr></thead>"
                f"<tbody>{rows_co}</tbody></table>") if rows_co else ""
    eq_block = (f"<h3>Geräte</h3><table class='list'><thead><tr><th>Gerät</th><th>Kategorie</th><th>Barcode</th><th>Ergebnis</th><th>Notiz</th></tr></thead>"
                f"<tbody>{rows_eq}</tbody></table>") if rows_eq else ""
    note_row = f"<tr><td><strong>Bemerkung:</strong></td><td>{esc(data['note'])}</td></tr>" if data["note"] else ""
    status = f"Abgeschlossen am {de(data['finished_at'])} von {esc(data['finished_by'])}" if data["finished_at"] else "Noch nicht abgeschlossen (Zwischenstand)"
    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8"><style>
        @page {{ size: a4 portrait; margin: 1.8cm; }}
        body {{ font-family: Helvetica, Arial, sans-serif; font-size: 10pt; color: #1f2937; }}
        .head {{ width: 100%; border-bottom: 2px solid #b91c1c; padding-bottom: 10px; margin-bottom: 16px; }}
        .station {{ font-size: 15pt; font-weight: bold; color: #b91c1c; }}
        .title {{ font-size: 14pt; font-weight: bold; margin: 10px 0; }}
        .meta td {{ padding: 3px 10px 3px 0; vertical-align: top; }}
        .sum {{ width: 100%; margin: 14px 0; border-collapse: collapse; }}
        .sum td {{ border: 1px solid #e5e7eb; padding: 8px; text-align: center; }}
        .sum .n {{ font-size: 15pt; font-weight: bold; }}
        h3 {{ font-size: 11.5pt; color: #b91c1c; margin: 18px 0 6px; }}
        table.list {{ width: 100%; border-collapse: collapse; }}
        table.list th {{ background: #f3f4f6; border: 1px solid #d1d5db; padding: 5px 6px; text-align: left; font-size: 8.5pt; }}
        table.list td {{ border: 1px solid #e5e7eb; padding: 5px 6px; font-size: 9pt; vertical-align: top; }}
        .sig td {{ padding-top: 40px; width: 50%; font-size: 9pt; color: #4b5563; }}
    </style></head><body>
    <table class="head"><tr><td><div class="station">{esc(get_station_name())}</div></td>
        <td style="text-align:right; vertical-align:bottom; font-size:9pt; color:#4b5563;">Erstellt am {datetime.date.today():%d.%m.%Y}</td></tr></table>
    <div class="title">Inventurprotokoll: {esc(data['title'])}</div>
    <table class="meta">
        <tr><td><strong>Umfang:</strong></td><td>{esc(data['category']) or 'Alle Geräte'}</td></tr>
        <tr><td><strong>Begonnen:</strong></td><td>{de(data['started_at'])} von {esc(data['started_by'])}</td></tr>
        <tr><td><strong>Stand:</strong></td><td>{status}</td></tr>
        {note_row}
    </table>
    <table class="sum"><tr>
        <td><div class="n">{s['total']}</div>Einträge</td><td><div class="n" style="color:#15803d">{s['ok']}</div>in Ordnung</td>
        <td><div class="n" style="color:#b91c1c">{s['fehlt']}</div>fehlen</td><td><div class="n" style="color:#c2410c">{s['defekt']}</div>defekt</td>
        <td><div class="n" style="color:#6b7280">{s['open']}</div>nicht geprüft</td></tr></table>
    {eq_block}{co_block}
    <table class="sig" style="width:100%"><tr><td>______________________________<br>Ort, Datum</td><td>______________________________<br>Unterschrift Gerätewart</td></tr></table>
    </body></html>"""
    import xhtml2pdf.pisa as pisa
    buf = io.BytesIO()
    pisa.CreatePDF(html, dest=buf)
    safe = "".join(ch if ch.isalnum() else "_" for ch in data["title"])[:60]
    return Response(content=buf.getvalue(), media_type="application/pdf", headers={"Content-Disposition": f'inline; filename="Inventur_{safe}.pdf"'})

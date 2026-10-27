import datetime

from database import get_db_connection

# Zeitfenster, innerhalb dessen eine Frist als "bald fällig" gemeldet wird.
REMINDER_WINDOW_DAYS = 14
# Nicht bei jedem Check (läuft alle 24h) neu melden, sonst bekommt man dieselbe
# Fälligkeit tagelang wiederholt gemeldet - stattdessen höchstens einmal pro Woche
# zusammenfassen. Der Zeitpunkt des letzten echten Laufs wird in der settings-Tabelle
# gemerkt (setting_value ist dort INT, daher als YYYYMMDD-Zahl statt als Datumstext).
REMINDER_CHECK_INTERVAL_DAYS = 7

# Fristen gehören zu dem Bereich, in dem sie bearbeitet werden - und nur dessen Zuständige sehen sie.
AREA_OF_TYPE = {
    "equipment": "material", "low_stock": "material",
    "vehicle": "fahrzeuge", "vehicle_check": "fahrzeuge",
    "building": "gebaeude",
    "hvo_material": "hvo",
    "g26": "personal", "lehrgang": "personal", "license": "personal", "prospect": "personal",
    "custom": "sonstige",  # eigene Fristarten: der Bereich folgt dem Bezug (siehe CUSTOM_AREA)
}
# Bezug einer eigenen Fristart -> Bereich
CUSTOM_AREA = {"person": "personal", "vehicle": "fahrzeuge", "equipment": "material", "general": "sonstige"}
AREA_LABEL = {"fahrzeuge": "Fahrzeuge", "material": "Geräte & Material", "gebaeude": "Gerätehaus", "hvo": "Sanitätsmaterial", "personal": "Personen", "sonstige": "Sonstige Fristen"}
AREA_ORDER = ["fahrzeuge", "material", "gebaeude", "hvo", "personal", "sonstige"]
TECHNIK_AREAS = ("fahrzeuge", "material", "gebaeude", "hvo")
# Eigene persönliche Fristen sieht jede Person selbst (G26.3, Lehrgänge, Führerscheinkontrolle)
OWN_TYPES = ("g26", "lehrgang", "license")


def get_due_items(window_days: int = None):
    """Liefert alle Fristen (Geräte, Fahrzeuge, Gerätehaus, Personen, Sanitätsmaterial, Bestände), die innerhalb
    der nächsten window_days Tage fällig oder bereits überfällig sind - als strukturierte Liste.
    Jede Frist trägt ihren Bereich ("area") und, soweit bekannt, die Verweise ref_id/personnel_id; "first_check"
    markiert eine Erstkontrolle (es gab noch nie eine) - das ist keine Fälligkeit im eigentlichen Sinn."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)

    if window_days is None:
        cur.execute("SELECT setting_value FROM settings WHERE setting_key = 'reminder_window_days'")
        row = cur.fetchone()
        window_days = row["setting_value"] if row and row["setting_value"] else REMINDER_WINDOW_DAYS

    today = datetime.date.today()
    window_end = today + datetime.timedelta(days=window_days)
    items = []

    def add_item(item_type, name, detail, due_date, ref_id=None, personnel_id=None, first_check=False, atemschutz=False, area=None, **extra):
        items.append({
            **extra,
            "type": item_type,
            "area": area or AREA_OF_TYPE[item_type],
            "name": name,
            "detail": detail,
            "due_date": due_date.isoformat(),
            "overdue": due_date < today,
            "days": (due_date - today).days,
            "first_check": first_check,
            "atemschutz": atemschutz,
            "ref_id": ref_id,
            "personnel_id": personnel_id,
        })

    # Geräteprüfungen
    cur.execute(
        "SELECT id, name, barcode, next_inspection FROM equipment "
        "WHERE next_inspection IS NOT NULL AND next_inspection <= %s ORDER BY next_inspection ASC",
        (window_end,)
    )
    for e in cur.fetchall():
        add_item("equipment", e["name"], f"Barcode {e['barcode']} - Geräteprüfung", e["next_inspection"], ref_id=e["id"])

    # Fahrzeuge: TÜV & Sicherheitsprüfung
    cur.execute(
        "SELECT id, name, tuv_date, sp_date FROM vehicles "
        "WHERE (tuv_date IS NOT NULL AND tuv_date <= %s) OR (sp_date IS NOT NULL AND sp_date <= %s)",
        (window_end, window_end)
    )
    for v in cur.fetchall():
        if v.get("tuv_date") and v["tuv_date"] <= window_end:
            add_item("vehicle", v["name"], "TÜV", v["tuv_date"], ref_id=v["id"])
        if v.get("sp_date") and v["sp_date"] <= window_end:
            add_item("vehicle", v["name"], "Sicherheitsprüfung", v["sp_date"], ref_id=v["id"])

    # G26.3-Atemschutzuntersuchung (gleiche Berechnung wie in personnel_mgr.py get_all_personnel)
    cur.execute("SELECT setting_value FROM settings WHERE setting_key = 'int_g26'")
    g26_row = cur.fetchone()
    g26_months = g26_row["setting_value"] if g26_row else 36
    cur.execute("SELECT id, name, g26_3_date FROM personnel WHERE is_agt = 1 AND g26_3_date IS NOT NULL")
    for p in cur.fetchall():
        due_date = p["g26_3_date"] + datetime.timedelta(days=round(g26_months * 30.44))
        if due_date <= window_end:
            add_item("g26", p["name"], "G26.3-Untersuchung", due_date, personnel_id=p["id"], atemschutz=True)

    # Führerscheinkontrolle: wer Einsatzfahrzeuge fährt, muss laut Halterpflicht regelmäßig (üblich: jährlich)
    # den Führerschein vorzeigen. Wer noch nie kontrolliert wurde, ist eine Erstkontrolle (kein Verzug).
    cur.execute(
        "SELECT id, name, license_checked_date FROM personnel "
        "WHERE (membership_status IS NULL OR membership_status = 'Aktiv') "
        "AND (is_maschinist = 1 OR lic_be = 1 OR lic_c = 1 OR lic_ce = 1)"
    )
    for p in cur.fetchall():
        checked = p["license_checked_date"]
        due_date = (checked + datetime.timedelta(days=365)) if checked else today
        if due_date <= window_end:
            add_item("license", p["name"], "Führerscheinkontrolle" + ("" if checked else " (noch nie erfasst)"), due_date,
                     personnel_id=p["id"], first_check=not checked)

    # Gerätehaus-Prüfungen (Feuerlöscher, E-Check, Blitzschutz, ...)
    cur.execute("SELECT id, name, next_check FROM building_checks WHERE next_check IS NOT NULL AND next_check <= %s", (window_end,))
    for b in cur.fetchall():
        add_item("building", b["name"], "Gerätehaus-Prüfung", b["next_check"], ref_id=b["id"])

    # Nachwuchs: Interessenten/Probemitglieder nachfassen bzw. Probezeit-Ende
    cur.execute("SELECT id, name, status, first_contact, last_contact, trial_end FROM prospects WHERE status IN ('Interessent', 'Probe')")
    for pr in cur.fetchall():
        last = pr["last_contact"] or pr["first_contact"]
        if last and last + datetime.timedelta(days=21) <= window_end:
            add_item("prospect", pr["name"], "Interessent: seit 3 Wochen kein Kontakt - nachfassen", last + datetime.timedelta(days=21), ref_id=pr["id"])
        if pr["trial_end"] and pr["trial_end"] <= window_end:
            add_item("prospect", pr["name"], "Probezeit endet - Entscheidung treffen", pr["trial_end"], ref_id=pr["id"])

    # Lehrgänge mit Ablaufdatum (z.B. Gruppenführer-Fortbildung, Atemschutz-Lehrgang, ...)
    cur.execute("""
        SELECT l.course_name, l.valid_until, p.name, p.id AS pid FROM lehrgaenge l
        JOIN personnel p ON l.personnel_id = p.id
        WHERE l.valid_until IS NOT NULL AND l.valid_until <= %s
    """, (window_end,))
    for l in cur.fetchall():
        add_item("lehrgang", l["name"], f"{l['course_name']} - Gültigkeit läuft ab", l["valid_until"], personnel_id=l["pid"],
                 atemschutz="atemschutz" in (l["course_name"] or "").lower())

    # Sanitätsmaterial (HVO/First Responder) mit Verfallsdatum
    cur.execute("SELECT id, name, quantity, expiry_date FROM hvo_material WHERE expiry_date IS NOT NULL AND expiry_date <= %s", (window_end,))
    for h in cur.fetchall():
        add_item("hvo_material", h["name"], f"{h['quantity']}x - Verfallsdatum Sanitätsmaterial", h["expiry_date"], ref_id=h["id"])

    # Fahrzeug-Wochenkontrolle: ältere als 7 Tage melden; die erste Kontrolle je Fahrzeug hat noch kein Datum
    # und gilt deshalb als Erstkontrolle ab heute.
    cur.execute("""
        SELECT v.id, v.name, MAX(c.date) as last_check FROM vehicles v
        LEFT JOIN vehicle_checks c ON c.vehicle_id = v.id
        GROUP BY v.id, v.name
    """)
    for v in cur.fetchall():
        due_date = (v["last_check"] + datetime.timedelta(days=7)) if v["last_check"] else today
        if due_date <= window_end:
            add_item("vehicle_check", v["name"], "Wöchentliche Fahrzeugkontrolle", due_date, ref_id=v["id"], first_check=not v["last_check"])

    # Verbrauchsmaterial unter Mindestbestand - kein Datum im eigentlichen Sinn, wird deshalb als "ab heute fällig" gemeldet.
    cur.execute("SELECT id, name, unit, current_stock, min_stock FROM consumables WHERE min_stock > 0 AND current_stock <= min_stock")
    for c in cur.fetchall():
        add_item("low_stock", c["name"], f"Bestand {c['current_stock']:g}/{c['min_stock']:g} {c['unit']}", today, ref_id=c["id"])

    cur.execute("SELECT item_name, size, quantity, min_quantity FROM clothing_stock WHERE min_quantity > 0 AND quantity <= min_quantity")
    for c in cur.fetchall():
        add_item("low_stock", f"{c['item_name']} ({c['size']})", f"Kleiderkammer: Bestand {c['quantity']}/{c['min_quantity']}", today)

    # Eigene Fristarten (z.B. Drohnenpilot): jede Frist hat ihr Datum, ihren Vorlauf und ihre Zuständigen
    cur.execute("""
        SELECT e.id, e.due_date, e.note, e.personnel_id, e.vehicle_id, e.equipment_id,
               t.name AS tname, t.subject, t.lead_days, t.responsible_role, t.responsible_pid,
               p.name AS pname, v.name AS vname, q.name AS qname
        FROM due_entries e JOIN due_types t ON t.id = e.type_id
        LEFT JOIN personnel p ON p.id = e.personnel_id
        LEFT JOIN vehicles v ON v.id = e.vehicle_id
        LEFT JOIN equipment q ON q.id = e.equipment_id
        WHERE e.due_date IS NOT NULL AND (p.id IS NULL OR p.exit_date IS NULL OR p.exit_date > CURDATE())
    """)
    for c in cur.fetchall():
        end = today + datetime.timedelta(days=c["lead_days"]) if c["lead_days"] else window_end
        subject_name = {"person": c["pname"], "vehicle": c["vname"], "equipment": c["qname"], "general": c["tname"]}.get(c["subject"])
        if c["due_date"] > end or not subject_name:
            continue  # nicht im Vorlauf, oder die Person/das Fahrzeug/das Gerät gibt es nicht mehr
        detail = c["tname"] if c["subject"] != "general" else (c["note"] or "Frist")
        if c["subject"] != "general" and c["note"]:
            detail += f" - {c['note']}"
        add_item("custom", subject_name, detail, c["due_date"], ref_id=c["id"], personnel_id=c["personnel_id"],
                 area=CUSTOM_AREA.get(c["subject"], "sonstige"),
                 responsible_role=c["responsible_role"] or "", responsible_pid=c["responsible_pid"])

    # Fristarten, die sich aus einem Logbuch ergeben (z.B. Pflicht-Übungsflug): je Mitglied des Logbuchs gilt der letzte Eintrag
    # plus Intervall; wer noch nie einen Eintrag hat, ist eine Erstkontrolle (kein Verzug).
    cur.execute("""
        SELECT t.id, t.name, t.interval_months, t.lead_days, t.responsible_role, t.responsible_pid, t.logbook_id
        FROM due_types t JOIN logbooks b ON b.id = t.logbook_id
        WHERE t.subject = 'person' AND t.interval_months IS NOT NULL
    """)
    for t in cur.fetchall():
        end = today + datetime.timedelta(days=t["lead_days"]) if t["lead_days"] else window_end
        cur.execute("""
            SELECT p.id, p.name, (SELECT MAX(e.entry_date) FROM logbook_entries e WHERE e.logbook_id = m.logbook_id AND e.personnel_id = p.id) AS last_entry
            FROM logbook_members m JOIN personnel p ON p.id = m.personnel_id
            WHERE m.logbook_id = %s AND (p.exit_date IS NULL OR p.exit_date > CURDATE())
        """, (t["logbook_id"],))
        for p in cur.fetchall():
            last = p["last_entry"]
            due_date = _add_months(last, t["interval_months"]) if last else today
            if due_date > end:
                continue
            detail = f"{t['name']} (letzter Eintrag {last.strftime('%d.%m.%Y')})" if last else f"{t['name']} (noch kein Eintrag)"
            add_item("custom", p["name"], detail, due_date, personnel_id=p["id"], first_check=not last, area="personal",
                     responsible_role=t["responsible_role"] or "", responsible_pid=t["responsible_pid"], auto=True, logbook_id=t["logbook_id"])

    cur.close(); conn.close()
    items.sort(key=lambda x: x["due_date"])
    return items


def _add_months(d: datetime.date, n: int) -> datetime.date:
    import calendar
    y, m = divmod(d.month - 1 + n, 12)
    return datetime.date(d.year + y, m + 1, min(d.day, calendar.monthrange(d.year + y, m + 1)[1]))


def visible_items(items, role, own_pid=None):
    """Welche Fristen sieht diese Rolle? Admin/Leitung alles; Gerätewart Technik (Fahrzeuge, Geräte, Material, Gerätehaus);
    Atemschutzwart G26.3 und Atemschutz-Lehrgänge; jede Person zusätzlich ihre eigenen persönlichen Fristen.
    Eigene Fristarten sieht, wer dafür zuständig ist (Rolle oder Person) - und die betroffene Person selbst;
    "can_done" sagt, wer sie abhaken darf (die Zuständigen, nicht die betroffene Person)."""
    out = []
    for it in items:
        if it["type"] == "custom":
            manages = (role in ("admin", "leitung") or (bool(it.get("responsible_role")) and it["responsible_role"] == role)
                       or (own_pid is not None and it.get("responsible_pid") == own_pid))
            if manages or (own_pid is not None and it.get("personnel_id") == own_pid):
                out.append({**it, "can_done": manages and not it.get("auto")})  # aus dem Logbuch abgeleitete Fristen erledigt der nächste Eintrag
            continue
        mine = own_pid is not None and it.get("personnel_id") == own_pid and it["type"] in OWN_TYPES
        if role in ("admin", "leitung"):
            ok = True
        elif role == "geratewart":
            ok = it["area"] in ("fahrzeuge", "material", "gebaeude")
        elif role == "atemschutzwart":
            ok = it["type"] == "g26" or (it["type"] == "lehrgang" and it.get("atemschutz"))
        else:
            ok = False
        if ok or mine:
            out.append(it)
    return out


def summarize(items) -> str:
    """Kurztext: "3 überfällig, 4 bald fällig"."""
    real = [i for i in items if not i.get("first_check")]
    over = sum(1 for i in real if i["overdue"])
    soon = len(real) - over
    parts = []
    if over:
        parts.append(f"{over} überfällig")
    if soon:
        parts.append(f"{soon} bald fällig")
    first = len(items) - len(real)
    if first:
        parts.append(f"{first} Erstkontrolle(n) offen")
    return ", ".join(parts)


def _area_breakdown(items) -> str:
    counts = {}
    for it in items:
        counts[it["area"]] = counts.get(it["area"], 0) + 1
    return " · ".join(f"{AREA_LABEL[a]}: {counts[a]}" for a in AREA_ORDER if a in counts)


def check_due_reminders():
    """Prüft die anstehenden Fristen und meldet sie in der Glocke (und per Mail) - jeweils nur den Zuständigen und nur
    für deren Bereich. Läuft höchstens einmal pro Woche. Wird von main.py periodisch im Hintergrund aufgerufen."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        today_int = int(datetime.date.today().strftime("%Y%m%d"))

        cur.execute("SELECT setting_value FROM settings WHERE setting_key = 'last_reminder_check'")
        row = cur.fetchone()
        last_check = row["setting_value"] if row else 0
        if last_check and today_int - last_check < REMINDER_CHECK_INTERVAL_DAYS:
            cur.close(); conn.close()
            return

        items = get_due_items()
        if items:
            try:
                from core.notify import notify
                # Pro Rolle nur der eigene Zuständigkeitsbereich, getrennt nach Technik und Personen
                standard = [i for i in items if i["type"] != "custom"]
                for role in ("admin", "leitung", "geratewart", "atemschutzwart"):
                    mine = visible_items(standard, role)
                    technik = [i for i in mine if i["area"] in TECHNIK_AREAS]
                    personen = [i for i in mine if i["area"] == "personal"]
                    if technik:
                        notify("fristen_technik", "Fristen: Fahrzeuge, Geräte & Gerätehaus", f"{summarize(technik)}. {_area_breakdown(technik)}",
                               "/dashboard?open=fristen", roles=[role])
                    if personen:
                        notify("fristen_personen", "Fristen: Personen", f"{summarize(personen)}. {_area_breakdown(personen)}",
                               "/dashboard?open=fristen", roles=[role])
                # Persönliche Fristen (G26.3, Lehrgänge, Führerschein) zusätzlich an die betroffene Person selbst
                cur.execute("SELECT p.id, u.username FROM personnel p JOIN users u ON u.personnel_id = p.id")
                user_by_pid = {r["id"]: r["username"] for r in cur.fetchall()}
                mine_by_user = {}
                for it in items:
                    if it["type"] in OWN_TYPES and not it["first_check"] and it.get("personnel_id") in user_by_pid:
                        mine_by_user.setdefault(user_by_pid[it["personnel_id"]], []).append(it["detail"])
                for uname, details in mine_by_user.items():
                    notify("fristen_personen", "Deine Frist läuft ab", "; ".join(details)[:300], "/dashboard?open=fristen", usernames=[uname])
                # Eigene Fristarten: jeder Zuständige bzw. jede betroffene Person mit Konto bekommt ihre Teilmenge
                custom = [i for i in items if i["type"] == "custom"]
                if custom:
                    cur.execute("SELECT username, role, personnel_id FROM users")
                    for u in cur.fetchall():
                        part = visible_items(custom, u["role"], u["personnel_id"])
                        if part:
                            names = ", ".join(dict.fromkeys(f"{i['name']} ({i['detail']})" for i in part))
                            notify("fristen_eigene", "Eigene Fristen", f"{summarize(part)}. {names}"[:300], "/dashboard?open=fristen", usernames=[u["username"]])
            except Exception as push_err:
                print(f"Fristen-Meldung fehlgeschlagen: {push_err}")
            from core.mailer import send_reminder_mails
            send_reminder_mails(items)

        cur.execute(
            "INSERT INTO settings (setting_key, setting_value) VALUES ('last_reminder_check', %s) "
            "ON DUPLICATE KEY UPDATE setting_value=%s",
            (today_int, today_int)
        )
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        print(f"Erinnerungs-Check fehlgeschlagen: {e}")

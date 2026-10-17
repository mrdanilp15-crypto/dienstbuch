import datetime

from database import get_db_connection

# Zeitfenster, innerhalb dessen eine Frist als "bald fällig" gemeldet wird.
REMINDER_WINDOW_DAYS = 14
# Nicht bei jedem Check (läuft alle 24h) neu melden, sonst bekommt man dieselbe
# Fälligkeit tagelang wiederholt gemeldet - stattdessen höchstens einmal pro Woche
# zusammenfassen. Der Zeitpunkt des letzten echten Laufs wird in der settings-Tabelle
# gemerkt (setting_value ist dort INT, daher als YYYYMMDD-Zahl statt als Datumstext).
REMINDER_CHECK_INTERVAL_DAYS = 7


def get_due_items(window_days: int = None):
    """Liefert alle Geräte-, Fahrzeug- (TÜV/SP) und G26.3-Fristen, die innerhalb der
    nächsten window_days Tage fällig sind oder bereits überfällig sind - als strukturierte
    Liste. Wird sowohl von check_due_reminders() (Broadcast-Text) als auch vom
    /api/admin/stats/due-soon Endpunkt (Dashboard-Karte) genutzt, damit beide immer
    dieselben Fristen zeigen."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)

    if window_days is None:
        # Kein fester Wert übergeben (der Normalfall) - die in den Einstellungen
        # konfigurierte Vorlaufzeit verwenden statt des früher fest einprogrammierten
        # REMINDER_WINDOW_DAYS-Werts.
        cur.execute("SELECT setting_value FROM settings WHERE setting_key = 'reminder_window_days'")
        row = cur.fetchone()
        window_days = row["setting_value"] if row and row["setting_value"] else REMINDER_WINDOW_DAYS

    today = datetime.date.today()
    window_end = today + datetime.timedelta(days=window_days)
    items = []

    def add_item(item_type, name, detail, due_date):
        items.append({
            "type": item_type,
            "name": name,
            "detail": detail,
            "due_date": due_date.isoformat(),
            "overdue": due_date < today
        })

    # Geräteprüfungen
    cur.execute(
        "SELECT name, barcode, next_inspection FROM equipment "
        "WHERE next_inspection IS NOT NULL AND next_inspection <= %s ORDER BY next_inspection ASC",
        (window_end,)
    )
    for e in cur.fetchall():
        add_item("equipment", e["name"], f"Barcode {e['barcode']} - Geräteprüfung", e["next_inspection"])

    # Fahrzeuge: TÜV & Sicherheitsprüfung
    cur.execute(
        "SELECT name, tuv_date, sp_date FROM vehicles "
        "WHERE (tuv_date IS NOT NULL AND tuv_date <= %s) OR (sp_date IS NOT NULL AND sp_date <= %s)",
        (window_end, window_end)
    )
    for v in cur.fetchall():
        if v.get("tuv_date") and v["tuv_date"] <= window_end:
            add_item("vehicle", v["name"], "TÜV", v["tuv_date"])
        if v.get("sp_date") and v["sp_date"] <= window_end:
            add_item("vehicle", v["name"], "Sicherheitsprüfung", v["sp_date"])

    # G26.3-Atemschutzuntersuchung (gleiche Berechnung wie in personnel_mgr.py get_all_personnel)
    cur.execute("SELECT setting_value FROM settings WHERE setting_key = 'int_g26'")
    g26_row = cur.fetchone()
    g26_months = g26_row["setting_value"] if g26_row else 36
    cur.execute("SELECT name, g26_3_date FROM personnel WHERE is_agt = 1 AND g26_3_date IS NOT NULL")
    for p in cur.fetchall():
        due_date = p["g26_3_date"] + datetime.timedelta(days=round(g26_months * 30.44))
        if due_date <= window_end:
            add_item("g26", p["name"], "G26.3-Untersuchung", due_date)

    # Führerscheinkontrolle: wer Einsatzfahrzeuge fährt, muss laut Halterpflicht regelmäßig (üblich: jährlich)
    # den Führerschein vorzeigen. Wer noch nie kontrolliert wurde, gilt ab heute als fällig.
    cur.execute(
        "SELECT name, license_checked_date FROM personnel "
        "WHERE (membership_status IS NULL OR membership_status = 'Aktiv') "
        "AND (is_maschinist = 1 OR lic_be = 1 OR lic_c = 1 OR lic_ce = 1)"
    )
    for p in cur.fetchall():
        checked = p["license_checked_date"]
        due_date = (checked + datetime.timedelta(days=365)) if checked else today
        if due_date <= window_end:
            add_item("license", p["name"], "Führerscheinkontrolle" + ("" if checked else " (noch nie erfasst)"), due_date)

    # Gerätehaus-Prüfungen (Feuerlöscher, E-Check, Blitzschutz, ...)
    cur.execute("SELECT name, next_check FROM building_checks WHERE next_check IS NOT NULL AND next_check <= %s", (window_end,))
    for b in cur.fetchall():
        add_item("building", b["name"], "Gerätehaus-Prüfung", b["next_check"])

    # Nachwuchs: Interessenten/Probemitglieder nachfassen bzw. Probezeit-Ende
    cur.execute("SELECT name, status, first_contact, last_contact, trial_end FROM prospects WHERE status IN ('Interessent', 'Probe')")
    for pr in cur.fetchall():
        last = pr["last_contact"] or pr["first_contact"]
        if last and last + datetime.timedelta(days=21) <= window_end:
            add_item("prospect", pr["name"], "Interessent: seit 3 Wochen kein Kontakt - nachfassen", last + datetime.timedelta(days=21))
        if pr["trial_end"] and pr["trial_end"] <= window_end:
            add_item("prospect", pr["name"], "Probezeit endet - Entscheidung treffen", pr["trial_end"])

    # Lehrgänge mit Ablaufdatum (z.B. Gruppenführer-Fortbildung, Atemschutz-Lehrgang, ...)
    cur.execute("""
        SELECT l.course_name, l.valid_until, p.name FROM lehrgaenge l
        JOIN personnel p ON l.personnel_id = p.id
        WHERE l.valid_until IS NOT NULL AND l.valid_until <= %s
    """, (window_end,))
    for l in cur.fetchall():
        add_item("lehrgang", l["name"], f"{l['course_name']} - Gültigkeit läuft ab", l["valid_until"])

    # Sanitätsmaterial (HVO/First Responder) mit Verfallsdatum
    cur.execute("SELECT name, quantity, expiry_date FROM hvo_material WHERE expiry_date IS NOT NULL AND expiry_date <= %s", (window_end,))
    for h in cur.fetchall():
        add_item("hvo_material", h["name"], f"{h['quantity']}x - Verfallsdatum Sanitätsmaterial", h["expiry_date"])

    # Fahrzeug-Wochenkontrolle: älteste noch nicht seit >7 Tagen kontrollierte Fahrzeuge
    # als "fällig" melden - erste Kontrolle je Fahrzeug hat noch kein Datum, wird deshalb
    # als "überfällig ab heute" behandelt (kein Datum, an dem man sich orientieren könnte).
    cur.execute("""
        SELECT v.id, v.name, MAX(c.date) as last_check FROM vehicles v
        LEFT JOIN vehicle_checks c ON c.vehicle_id = v.id
        GROUP BY v.id, v.name
    """)
    for v in cur.fetchall():
        due_date = (v["last_check"] + datetime.timedelta(days=7)) if v["last_check"] else today
        if due_date <= window_end:
            add_item("vehicle_check", v["name"], "Wöchentliche Fahrzeugkontrolle", due_date)

    # Verbrauchsmaterial unter Mindestbestand - kein Datum im eigentlichen Sinn, wird deshalb
    # wie die Fahrzeugkontrolle als "ab heute fällig" gemeldet statt mit einer Frist.
    cur.execute("SELECT name, unit, current_stock, min_stock FROM consumables WHERE min_stock > 0 AND current_stock <= min_stock")
    for c in cur.fetchall():
        add_item("low_stock", c["name"], f"Bestand {c['current_stock']:g}/{c['min_stock']:g} {c['unit']}", today)

    cur.execute("SELECT item_name, size, quantity, min_quantity FROM clothing_stock WHERE min_quantity > 0 AND quantity <= min_quantity")
    for c in cur.fetchall():
        add_item("low_stock", f"{c['item_name']} ({c['size']})", f"Kleiderkammer: Bestand {c['quantity']}/{c['min_quantity']}", today)

    cur.close(); conn.close()
    items.sort(key=lambda x: x["due_date"])
    return items


def check_due_reminders():
    """Prüft die anstehenden Fristen (get_due_items) und legt bei Bedarf eine
    System-Mitteilung (system_broadcasts) an sowie verschickt eine Push-Benachrichtigung an
    alle abonnierten Geräte. Wird von main.py periodisch im Hintergrund aufgerufen."""
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
            icons = {"equipment": "🔧", "vehicle": "🚒", "g26": "🫁", "lehrgang": "🎓", "vehicle_check": "📋", "hvo_material": "🩹", "low_stock": "📦", "license": "🪪", "building": "🏠", "prospect": "🤝"}
            lines = []
            for it in items:
                due = datetime.date.fromisoformat(it["due_date"])
                status = "ÜBERFÄLLIG" if it["overdue"] else f"fällig am {due.strftime('%d.%m.%Y')}"
                lines.append(f"{icons.get(it['type'], '•')} {it['name']} ({it['detail']}): {status}")

            # Nur eine kurze Zusammenfassung: bei 50 fälligen Fristen soll das Banner nicht den
            # ganzen Bildschirm füllen. Die Einzelheiten stehen im Reiter Material (Karte "Bald fällig").
            labels = {"equipment": "Geräte", "vehicle": "Fahrzeuge", "g26": "G26.3", "lehrgang": "Lehrgänge", "vehicle_check": "Fahrzeugkontrollen",
                      "hvo_material": "Verbandmaterial", "low_stock": "Bestände", "license": "Führerscheine", "building": "Gerätehaus", "prospect": "Sponsoren"}
            per_type = {}
            for it in items:
                per_type[labels.get(it["type"], "Sonstiges")] = per_type.get(labels.get(it["type"], "Sonstiges"), 0) + 1
            n_over = sum(1 for it in items if it["overdue"])
            content = (
                f"{len(items)} Frist(en): {n_over} überfällig, {len(items) - n_over} in Kürze fällig.\n"
                + " · ".join(f"{k}: {v}" for k, v in sorted(per_type.items(), key=lambda x: -x[1]))
                + "\nDetails: Reiter Material → „Bald fällig / überfällig“."
            )
            cur.execute(
                "INSERT INTO system_broadcasts (username, title, content, role_target, is_mandatory) "
                "VALUES (%s, %s, %s, %s, %s)",
                ("System", "⏰ Anstehende Prüf-/Fälligkeitsfristen", content, "all", False)
            )
            conn.commit()
            try:
                from core.notify import notify
                overdue = sum(1 for it in items if it["overdue"])
                notify("fristen", "Anstehende Prüffristen",
                       f"{len(items)} Frist(en) laufen bald ab" + (f", davon {overdue} überfällig." if overdue else "."),
                       "/dashboard?open=fristen", roles=["admin", "leitung", "geratewart"])
                # Persönliche Fristen (G26.3, Lehrgänge) zusätzlich an die betroffene Person
                cur.execute("SELECT p.name, u.username FROM personnel p JOIN users u ON u.personnel_id = p.id")
                user_by_name = {r["name"]: r["username"] for r in cur.fetchall()}
                mine = {}
                for it in items:
                    if it["type"] in ("g26", "lehrgang") and it["name"] in user_by_name:
                        mine.setdefault(user_by_name[it["name"]], []).append(it["detail"])
                for uname, details in mine.items():
                    notify("fristen", "Deine Frist läuft ab", "; ".join(details)[:300], "/dashboard", usernames=[uname])
            except Exception as push_err:
                print(f"Reminder-Push fehlgeschlagen: {push_err}")
            from core.mailer import send_reminder_mails
            send_reminder_mails(items, lines)

        cur.execute(
            "INSERT INTO settings (setting_key, setting_value) VALUES ('last_reminder_check', %s) "
            "ON DUPLICATE KEY UPDATE setting_value=%s",
            (today_int, today_int)
        )
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        print(f"Fehler bei check_due_reminders: {e}")

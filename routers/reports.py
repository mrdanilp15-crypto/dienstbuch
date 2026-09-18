from datetime import datetime

def get_report_styles():
    # WICHTIG: xhtml2pdf (erzeugt die PDFs) unterstützt kein CSS-Flexbox/Grid - display:flex
    # wird stillschweigend ignoriert und alles fällt auf normales Block-Layout zurück
    # (untereinander statt nebeneinander). Nebeneinander liegende Elemente hier deshalb
    # bewusst mit float (von xhtml2pdf unterstützt) statt flex umgesetzt.
    return """
    @page { size: A4; margin: 10mm; }
    @media print { body { -webkit-print-color-adjust: exact; } }
    body { font-family: 'Segoe UI', Arial, sans-serif; font-size: 11px; margin: 0; padding: 0; color: #333; line-height: 1.4; }
    .session-page { page-break-after: always; padding: 10px; position: relative; }
    .session-page:last-child { page-break-after: auto; }
    .clearfix:after { content: ""; display: table; clear: both; }
    .main-title { text-align: center; font-size: 20px; font-weight: bold; color: #b71c1c; text-transform: uppercase; border-bottom: 3px solid #b71c1c; padding-bottom: 10px; margin-bottom: 20px; }
    .info-box { margin-bottom: 15px; font-size: 13px; }
    .info-left { float: left; }
    .info-right { float: right; text-align: right; }
    .topic-box { padding: 15px; margin-bottom: 20px; border-radius: 6px; }
    .bg-übung { background: #e3f2fd !important; border-left: 6px solid #0d6efd; color: #0d47a1; }
    .bg-einsatz { background: #ffebee !important; border-left: 6px solid #dc3545; color: #b71c1c; }
    .bg-sonstiges { background: #fffde7 !important; border-left: 6px solid #fbc02d; color: #827717; }
    table { width: 100%; border-collapse: collapse; margin-top: 10px; }
    th { background: #f8f9fa; border: 1px solid #ddd; padding: 8px; text-align: left; font-size: 10px; text-transform: uppercase; }
    td { border: 1px solid #ddd; padding: 6px 8px; text-align: left; vertical-align: middle; }
    .sig-img { height: 45px; max-width: 140px; object-fit: contain; mix-blend-mode: multiply; }
    .sig-table { margin-top: 30px; width: 100%; }
    .sig-table td { border: none; padding: 0 20px; text-align: center; width: 50%; vertical-align: bottom; }
    .sig-line-text { border-top: 1.5px solid #333; padding-top: 6px; font-size: 10px; font-weight: bold; margin-top: 5px; }
    .leader-sig-img { height: 75px; max-width: 220px; object-fit: contain; margin-bottom: -15px; mix-blend-mode: multiply; }
    .stat-table { width: 100%; margin-bottom: 30px; margin-top: 20px; border-collapse: separate; border-spacing: 8px 0; }
    .stat-box { background: #f8f9fa; padding: 15px; border-radius: 10px; border: 1px solid #eee; text-align: center; width: 25%; }
    .total-val { font-size: 20px; font-weight: bold; color: #b71c1c; display: block; margin-top: 5px; }
    """

def generate_single_report(s, persons, town_name):
    # WICHTIG: town_name muss hier als 3. Argument stehen!
    sign_date = s['date'].strftime('%d.%m.%Y') if hasattr(s['date'], 'strftime') else str(s['date'])
    l_sig_data = s.get('leader_signature')
    l_sig_html = f"<img src='{l_sig_data}' class='leader-sig-img'>" if l_sig_data and len(str(l_sig_data)) > 100 else "<div style='height:75px;'></div>"
    cat = s.get('category', 'Übung'); cat_class = f"bg-{cat.lower()}"
    # Ausrück-/Eintreffzeit gibt es nur bei Einsätzen (FwDV 100) und nur, wenn tatsächlich
    # erfasst - ein leeres "Ausgerückt: Uhr" bei Dienstberichten oder alten Einsätzen ohne
    # diese Angabe wäre nur verwirrend.
    zeiten_zeile = ""
    if cat == 'Einsatz' and (s.get('ausrueck_zeit') or s.get('eintreff_zeit')):
        teile = []
        if s.get('ausrueck_zeit'):
            teile.append(f"Ausgerückt: {s['ausrueck_zeit']} Uhr")
        if s.get('eintreff_zeit'):
            teile.append(f"Eingetroffen: {s['eintreff_zeit']} Uhr")
        zeiten_zeile = f"<br>{' &bull; '.join(teile)}"
    html = f"""
    <div class="session-page">
        <div class="main-title">Dienstbericht</div>
        <div class="info-box clearfix">
            <div class="info-left"><strong>Einheit:</strong> {s.get('gname', '---')}<br><strong>Leitung:</strong> {s.get('instructors') or '---'}</div>
            <div class="info-right"><strong>Datum:</strong> {sign_date}<br><strong>Dauer:</strong> {float(s['duration']):g} h{zeiten_zeile}</div>
        </div>
        <div class="topic-box {cat_class}"><span style="font-size:10px; font-weight:bold; text-transform:uppercase; opacity:0.8;">{cat}</span><div style="font-size:16px; font-weight:bold;">{s['description']}</div></div>
        <table>
            <thead><tr><th>Name</th><th style="width:100px;">Status</th><th>Notiz</th><th style="width:150px; text-align:center;">Unterschrift</th></tr></thead>
            <tbody>"""
    for p in persons:
        sig = f"<img src='{p['signature']}' class='sig-img'>" if p.get('signature') and len(str(p['signature'])) > 100 else ""
        status, color = ("Anwesend", "#198754") if p.get('is_present') == 1 else ("Abwesend", "#dc3545")
        html += f"<tr><td style='font-weight:bold;'>{p['name']}</td><td style='color:{color}; font-weight:bold;'>{status}</td><td><small>{p.get('vehicle') or ''} {p.get('note') or ''}</small></td><td style='text-align:center;'>{sig}</td></tr>"
    html += f"""
            </tbody>
        </table>
        <table class="sig-table">
            <tr>
                <td>{town_name}, den {sign_date}<div class="sig-line-text">Ort / Datum</div></td>
                <td>{l_sig_html}<div class="sig-line-text">Unterschrift Leitung</div></td>
            </tr>
        </table>
    </div>"""
    return html

def generate_year_report(gname, year, p_stats, cat_sums, town_name):
    today = datetime.now().strftime('%d.%m.%Y')
    html = f"""
    <div class="session-page">
        <div class="main-title">Jahresstatistik {year}</div>
        <div style="font-size:14px; margin-bottom:20px;"><strong>Einheit:</strong> {gname}</div>
        <table class="stat-table">
            <tr>
                <td class="stat-box">GESAMT<span class="total-val">{sum(cat_sums.values()):g} h</span></td>
                <td class="stat-box" style="border-top:4px solid #0d6efd;">ÜBUNGEN<span class="total-val" style="color:#0d6efd;">{cat_sums['Übung']:g} h</span></td>
                <td class="stat-box" style="border-top:4px solid #dc3545;">EINSÄTZE<span class="total-val" style="color:#dc3545;">{cat_sums['Einsatz']:g} h</span></td>
                <td class="stat-box" style="border-top:4px solid #fbc02d;">SONSTIGES<span class="total-val" style="color:#fbc02d;">{cat_sums['Sonstiges']:g} h</span></td>
            </tr>
        </table>
        <table>
            <thead><tr style="background:#f4f4f4;"><th>Name</th><th>Übung</th><th>Einsatz</th><th>Sonst.</th><th>Gesamt</th><th style="text-align:center;">Dienste</th><th style="text-align:center;">Quote</th></tr></thead>
            <tbody>"""
    for n, d in sorted(p_stats.items(), key=lambda x: x[1]['total_h'], reverse=True):
        html += f"<tr><td style='font-weight:bold;'>{n}</td><td style='color:#0d6efd;'>{d['Übung']:g}h</td><td style='color:#dc3545;'>{d['Einsatz']:g}h</td><td style='color:#827717;'>{d['Sonstiges']:g}h</td><td style='background:#f9f9f9; font-weight:bold;'>{d['total_h']:g}h</td><td style='text-align:center;'>{d['p']}</td><td style='text-align:center; font-weight:bold;'>{d['q']}%</td></tr>"
    html += f"""
            </tbody>
        </table>
        <table class="sig-table">
            <tr>
                <td>{town_name}, den {today}<div class="sig-line-text">Ort / Datum</div></td>
                <td><div style="height:75px;"></div><div class="sig-line-text">Unterschrift Wehrführung</div></td>
            </tr>
        </table>
    </div>"""
    return html

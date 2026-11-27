// Arbeitsbescheinigungen und Abrechnung.
// Teil der Dashboard-App (Vue-Mixin): Daten stehen in ../dashboard.js, "this" ist die ganze App.
window.DB_MIXINS = window.DB_MIXINS || [];
window.DB_MIXINS.push({
            computed: {
                billsFiltered() {
                    const f = this.billFilter;
                    return (this.bills || []).filter(b => f === 'alle' ? true : f === 'bezahlt' ? !!b.paid_at : f === 'ueberfaellig' ? this.billOverdue(b) : !b.paid_at);
                },
                billStats() {
                    const list = this.bills || [];
                    const open = list.filter(b => !b.paid_at);
                    const sum = a => a.reduce((t, b) => t + (parseFloat(b.amount) || 0), 0);
                    return { open: open.length, openSum: sum(open), overdue: list.filter(b => this.billOverdue(b)).length, paid: list.length - open.length };
                },
            },
            methods: {
                async loadEmployerCertificates() {
                    try {
                        const res = await fetch('/api/missions/employer-certificates', { credentials: 'include' });
                        this.employerCertificates = res.ok ? await res.json() : [];
                    } catch(e) { this.employerCertificates = []; }
                },
                async loadCertMissionAttendees() {
                    this.newCertPersonnelId = null;
                    this.certMissionAttendees = [];
                    if (!this.newCertMissionId) return;
                    try {
                        const res = await fetch(`/api/missions/${this.newCertMissionId}`, { credentials: 'include' });
                        if (!res.ok) return;
                        const detail = await res.json();
                        const present = (detail.attendance || []).filter(a => a.is_present && !['Nein','Entschuldigt','0','false','False'].includes(String(a.is_present)));
                        this.certMissionAttendees = present.map(a => {
                            const p = this.personnel.find(p => p.id === a.personnel_id);
                            return { personnel_id: a.personnel_id, name: p ? p.name : `ID ${a.personnel_id}` };
                        }).sort((a, b) => a.name.localeCompare(b.name));
                    } catch(e) { this.certMissionAttendees = []; }
                },
                openCertSigModal() {
                    certSigModal.show();
                    setTimeout(() => { initResponsiveCanvas('certSigCanvas', certSigPad, this.newCertSignature); }, 250);
                },
                clearCertSig() { if (certSigPad) certSigPad.clear(); },
                saveCertSig() {
                    if (certSigPad && !certSigPad.isEmpty()) { this.newCertSignature = trimSignatureDataUrl(certSigPad); }
                    certSigModal.hide();
                },
                async submitEmployerCertificate() {
                    if (!this.newCertMissionId || !this.newCertPersonnelId) return;
                    const res = await fetch(`/api/missions/${this.newCertMissionId}/employer-certificate/${this.newCertPersonnelId}`, {
                        method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'include',
                        body: JSON.stringify({ signature: this.newCertSignature })
                    });
                    if (res.ok) {
                        const result = await res.json();
                        window.open(`/api/missions/${this.newCertMissionId}/employer-certificate/${this.newCertPersonnelId}?cert_id=${result.id}`, '_blank');
                        this.newCertMissionId = null; this.newCertPersonnelId = null; this.newCertSignature = null; this.certMissionAttendees = [];
                        await this.loadEmployerCertificates();
                    } else {
                        const err = await res.json().catch(() => ({}));
                        await appAlert(err.detail || "Bescheinigung konnte nicht erstellt werden.");
                    }
                },
                viewEmployerCertificate(c) {
                    window.open(`/api/missions/${c.mission_id}/employer-certificate/${c.personnel_id}?cert_id=${c.id}`, '_blank');
                },
                async deleteEmployerCertificate(id) {
                    if (!await appConfirm("Bescheinigung aus der Liste löschen? Das erstellte PDF bleibt davon unberührt, sofern es bereits heruntergeladen wurde.")) return;
                    await fetch(`/api/missions/employer-certificates/${id}`, { method: 'DELETE', credentials: 'include' });
                    await this.loadEmployerCertificates();
                },

                // Billing / Abrechnung
                async loadBills() {
                    try {
                        const [res, mRes] = await Promise.all([
                            fetch('/api/missions/billing/list', { credentials: 'include' }),
                            fetch('/api/missions', { credentials: 'include' })
                        ]);
                        if(res.ok) this.bills = await res.json();
                        if(mRes.ok) this.missions = await mRes.json();
                        this.$nextTick(() => {
                            const avail = this.availableMissionsForBilling;
                            if (avail && avail.length > 0 && (!this.newBill || !this.newBill.mission_id)) {
                                if (!this.newBill) this.newBill = { mission_id: null, recipient_name: '', address: '', amount: 150.00, details: '' };
                                this.newBill.mission_id = avail[0].id;
                            }
                        });
                    } catch(e) { console.error("Error loading bills", e); }
                },
                async submitBill() {
                    if(!this.newBill.mission_id) return await appAlert("Bitte einen Einsatz auswählen!");
                    const res = await fetch(`/api/missions/billing/${this.newBill.mission_id}`, { method: 'POST', headers: {'Content-Type':'application/json'}, credentials: 'include', body: JSON.stringify(this.newBill) });
                    if(res.ok) {
                        await appAlert("Kostenbescheid erfolgreich buchen!");
                        this.newBill = { mission_id: null, recipient_name: '', address: '', amount: 150.00, details: '' };
                        await this.loadBills();
                    } else {
                        const err = await res.json();
                        await appAlert("Fehler beim Buchen: " + (err.detail || "Keine Berechtigung"));
                    }
                },
                async deleteBill(id) {
                    if(await appConfirm("Rechnung löschen?")) {
                        await fetch(`/api/missions/billing/${id}`, { method: 'DELETE', credentials: 'include' });
                        await this.loadBills();
                    }
                },
                async payBill(id) {
                    await fetch(`/api/missions/billing/${id}/pay`, { method: 'POST', credentials: 'include' });
                    await this.loadBills();
                },
                printBill(b) {
                    const printWindow = window.open('', '_blank');
                    const html = `
                        <!DOCTYPE html>
                        <html>
                        <head>
                            <title>Kostenbescheid - ${b.recipient_name}</title>
                            <style>
                                body { font-family: Arial, sans-serif; padding: 40px; color: #333; line-height: 1.6; }
                                .header { text-align: center; border-bottom: 2px solid #dc3545; padding-bottom: 15px; margin-bottom: 30px; }
                                .header h1 { margin: 0; color: #dc3545; font-size: 24px; text-transform: uppercase; }
                                .header p { margin: 5px 0 0; color: #666; font-size: 13px; }
                                .meta-table { width: 100%; margin-bottom: 30px; }
                                .meta-table td { vertical-align: top; padding: 5px 0; }
                                .box { background: #f8f9fa; border: 1px solid #e9ecef; border-radius: 8px; padding: 20px; margin-bottom: 25px; }
                                .table { width: 100%; border-collapse: collapse; margin-top: 15px; }
                                .table th, .table td { border: 1px solid #dee2e6; padding: 10px; text-align: left; }
                                .table th { background: #e9ecef; }
                                .total { text-align: right; font-size: 18px; font-weight: bold; margin-top: 20px; }
                                .footer { margin-top: 50px; font-size: 12px; color: #777; border-top: 1px solid #ddd; padding-top: 15px; text-align: center; }
                                @media print { .no-print { display: none; } }
                            </style>
                        </head>
                        <body>
                            <div class="no-print" style="margin-bottom: 20px;">
                                <button onclick="window.print()" style="padding: 10px 20px; background: #dc3545; color: white; border: none; border-radius: 5px; cursor: pointer; font-weight: bold;">🖨️ Kostenbescheid drucken / Als PDF speichern</button>
                            </div>
                            <div class="header">
                                <h1>FEUERWEHR KOSTENBESCHEID</h1>
                                <p>Bescheid über den Ersatz von Aufwendungen für Einsätze der Feuerwehr</p>
                            </div>
                            <table class="meta-table">
                                <tr>
                                    <td style="width: 60%;">
                                        <strong>Empfänger / Verursacher:</strong><br>
                                        ${b.recipient_name}<br>
                                        ${(b.address || '').replace(/\n/g, '<br>')}
                                    </td>
                                    <td style="width: 40%; text-align: right;">
                                        <strong>Bescheid-Nr.:</strong> KB-${b.id}<br>
                                        <strong>Datum:</strong> ${b.sent_at ? b.sent_at.split(' ')[0] : new Date().toLocaleDateString('de-DE')}<br>
                                        <strong>Status:</strong> ${b.paid_at ? 'BEZAHLT' : 'OFFEN / AUSSTEHEND'}
                                    </td>
                                </tr>
                            </table>
                            <div class="box">
                                <strong>Einsatzdaten:</strong><br>
                                Einsatz-Stichwort: <strong>${b.stichwort || 'Feuerwehreinsatz'}</strong><br>
                                Einsatzdatum: ${b.date || '-'}<br>
                                Einsatzort: ${b.adresse || 'Siehe Einsatzbericht'}
                            </div>
                            <h3>Kostenaufstellung:</h3>
                            <table class="table">
                                <thead>
                                    <tr>
                                        <th>Leistungsbeschreibung / Grund</th>
                                        <th style="text-align: right;">Betrag (€)</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <tr>
                                        <td>${b.details || 'Aufwendungsersatz für Regeleinsatz gemäß Feuerwehr-Kostenverordnung'}</td>
                                        <td style="text-align: right;">${parseFloat(b.amount).toFixed(2)} €</td>
                                    </tr>
                                </tbody>
                            </table>
                            <div class="total">
                                Gesamtbetrag: ${parseFloat(b.amount).toFixed(2)} €
                            </div>
                            <div style="margin-top: 30px;">
                                <p><strong>Zahlungshinweis:</strong><br>
                                Bitte überweisen Sie den Gesamtbetrag von <strong>${parseFloat(b.amount).toFixed(2)} €</strong> innerhalb von 14 Tagen unter Angabe der Bescheid-Nummer <strong>KB-${b.id}</strong> auf folgendes Gemeindekonto:</p>
                                <p style="margin-left: 20px; font-family: monospace;">
                                    <strong>IBAN:</strong> ${this.stationConfig?.iban || 'IBAN noch nicht in den Einstellungen hinterlegt'}<br>
                                    <strong>BIC:</strong> ${this.stationConfig?.bic || '-'}
                                </p>
                            </div>
                            <div style="margin-top: 30px; font-size: 11px; color: #555; background: #f8f9fa; border: 1px solid #ddd; padding: 15px; border-radius: 5px;">
                                <h4 style="margin-top: 0; color: #333;">Rechtsbehelfsbelehrung</h4>
                                <p style="margin-bottom: 5px;">Gegen diesen Bescheid kann innerhalb eines Monats nach Bekanntgabe Widerspruch erhoben werden. Der Widerspruch ist schriftlich oder zur Niederschrift bei der ausstellenden Behörde / Gemeindeverwaltung einzulegen.</p>
                                <p style="margin-bottom: 0;"><strong>Hinweis:</strong> Die Einlegung eines Rechtsbehelfs hat keine aufschiebende Wirkung (§ 80 Abs. 2 Nr. 1 VwGO). Der Rechnungsbetrag ist ungeachtet eines etwaigen Widerspruchs fristgerecht zu zahlen.</p>
                            </div>
                            <div class="footer">
                                Dies ist ein maschinell erstellter Bescheid und ohne Unterschrift gültig. Generiert am ${new Date().toLocaleDateString('de-DE')} über Feuerwehrverwaltung.
                            </div>
                        </body>
                        </html>
                    `;
                    printWindow.document.write(html);
                    printWindow.document.close();
                },
                async calculateCompensations() {
                    const res = await fetch(`/api/missions/billing/compensations/list?year=${this.sepaYear}&hourly_rate=${this.sepaRate}`, { credentials: 'include' });
                    if(res.ok) this.compensations = await res.json();
                },
                async downloadSepa() {
                    const iban = await appPrompt("Sender IBAN eingeben:", this.stationConfig?.iban || '');
                    const bic = await appPrompt("Sender BIC eingeben:", this.stationConfig?.bic || '');
                    if(iban && bic) {
                        window.open(`/api/missions/billing/export/sepa?year=${this.sepaYear}&hourly_rate=${this.sepaRate}&sender_iban=${iban}&sender_bic=${bic}`, '_blank');
                    }
                },
            }
});

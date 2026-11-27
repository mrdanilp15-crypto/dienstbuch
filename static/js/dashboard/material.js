// Geräte, Material und Mängel.
// Teil der Dashboard-App (Vue-Mixin): Daten stehen in ../dashboard.js, "this" ist die ganze App.
window.DB_MIXINS = window.DB_MIXINS || [];
window.DB_MIXINS.push({
            computed: {
                inventurItems() {
                    if (!this.inventur) return [];
                    const q = (this.inventurSearch || '').toLowerCase().trim();
                    return this.inventur.items.filter(i => {
                        const done = i.kind === 'consumable' ? i.counted !== null : !!i.result;
                        if (this.inventurFilter === 'offen' && done && !(this.inventurTouched[i.id] && (i.result === 'fehlt' || i.result === 'defekt'))) return false;  // Fehlt/Defekt bleibt kurz stehen, damit man die Notiz eintragen kann
                        if (this.inventurFilter === 'probleme' && !(i.result === 'fehlt' || i.result === 'defekt' || (i.kind === 'consumable' && i.counted !== null && i.counted !== i.expected))) return false;
                        return !q || (i.name + ' ' + i.category + ' ' + i.barcode).toLowerCase().includes(q);
                    });
                },
                inventurProgress() {
                    const s = this.inventur ? this.inventur.summary : null;
                    return s && s.total ? Math.round((s.total - s.open) / s.total * 100) : 0;
                },
                inventurRunning() { return this.inventurList.find(i => !i.finished_at) || null; },

                lowStockConsumables() {
                    return this.consumables.filter(c => c.min_stock > 0 && c.current_stock <= c.min_stock);
                },
                clothingOpenReturns() { return this.clothingIssued.filter(g => g.membership_status === 'Ausgeschieden').length; },
                equipInspectSchedule() {
                    if (!this.equipment) return [];
                    return this.equipment.filter(eq => eq && eq.interval_months > 0 && eq.category !== 'Funk' && eq.category !== 'Funkgerät' && eq.category !== 'TETRA').map(eq => {
                        let status = 'OK';
                        if (eq.next_inspection) {
                            const nextDate = new Date(eq.next_inspection);
                            const today = new Date();
                            if (nextDate < today) {
                                status = 'Prüfung fällig';
                            }
                        }
                        return {
                            id: eq.id,
                            barcode: eq.barcode,
                            name: eq.name,
                            category: eq.category,
                            lastTest: eq.last_inspection || 'Nie',
                            interval: eq.interval_months,
                            status: eq.current_status !== 'Bestanden' ? eq.current_status : status,
                            rawEq: eq
                        };
                    });
                },
                funkDevices() {
                    // Nur echte, in der Geräteverwaltung angelegte Funkgeräte - keine
                    // erfundenen Werte (Akkustand/Ladezyklen wurden vorher aus dem Barcode-Text
                    // berechnet und hatten keinerlei Bezug zum tatsächlichen Gerätezustand).
                    if (!this.equipment) return [];
                    return this.equipment.filter(eq => eq && (eq.category === 'Funk' || eq.category === 'Funkgerät' || eq.category === 'TETRA')).map(eq => {
                        let type = 'HRT (Handfunkgerät)';
                        const name = eq.name || '';
                        if (name.toLowerCase().includes('mrt') || name.toLowerCase().includes('fahrzeug')) {
                            type = 'MRT (Fahrzeugfunk)';
                        }
                        return {
                            id: eq.id,
                            barcode: eq.barcode,
                            name: eq.name,
                            type: type,
                            lastInspection: eq.last_inspection || 'Nie geprüft',
                            currentStatus: eq.current_status || 'Bestanden',
                            rawEq: eq
                        };
                    });
                },
                filteredEquipment() {
                    // eq &&/optional chaining: ein einzelner unerwartet unvollständiger
                    // Datensatz (z.B. name/barcode fehlt) durfte diesen ganzen computed vorher
                    // mit einem Fehler abbrechen lassen - dann rendert Vue die Liste gar nicht,
                    // was wie "alle Geräte sind verschwunden" wirkt statt nur den einen Eintrag.
                    return this.equipment.filter(eq => {
                        if (!eq) return false;
                        const name = eq.name || '';
                        const barcode = eq.barcode || '';
                        const mSearch = name.toLowerCase().includes(this.eqSearch.toLowerCase()) || barcode.toLowerCase().includes(this.eqSearch.toLowerCase());
                        const mCat = !this.eqCategory || eq.category === this.eqCategory;
                        const mDefect = !this.filterDefectiveOnly || eq.current_status === 'Mangel' || eq.current_status === 'Defekt';
                        return mSearch && mCat && mDefect;
                    });
                },
            },
            methods: {

                // ---- Inventur
                async loadInventories() {
                    try {
                        const res = await fetch('/api/inventur', { credentials: 'include' });
                        if (!res.ok) return;
                        const d = await res.json();
                        this.inventurList = d.inventories; this.inventurCategories = d.categories;
                    } catch (e) { console.error(e); }
                },
                async startInventur() {
                    const res = await fetch('/api/inventur', { method: 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(this.inventurNew) });
                    const d = await res.json().catch(() => ({}));
                    if (!res.ok) { await appAlert(d.detail || 'Inventur konnte nicht gestartet werden.'); return; }
                    this.inventurNew = { title: '', category: '', include_consumables: false };
                    await this.openInventur(d.id);
                    this.loadInventories();
                },
                async openInventur(id) {
                    const res = await fetch(`/api/inventur/${id}`, { credentials: 'include' });
                    if (!res.ok) { await appAlert('Inventur nicht gefunden.'); return; }
                    this.inventur = await res.json();
                    this.inventurFilter = this.inventur.finished_at ? 'alle' : 'offen';
                    this.inventurSearch = '';
                    this.inventurTouched = {};
                },
                closeInventur() { this.inventur = null; this.loadInventories(); },
                _inventurRecount() {
                    const s = { total: 0, ok: 0, fehlt: 0, defekt: 0, open: 0 };
                    for (const i of this.inventur.items) {
                        s.total++;
                        if (i.kind === 'consumable') s[i.counted !== null ? 'ok' : 'open']++;
                        else s[i.result || 'open']++;
                    }
                    this.inventur.summary = s;
                },
                async setInventurItem(item, changes) {
                    const before = { result: item.result, counted: item.counted, note: item.note };
                    if (!this.inventurScanMode) this.inventurTouched = { ...this.inventurTouched, [item.id]: true };
                    Object.assign(item, changes);
                    if (item.counted === '') item.counted = null;
                    this._inventurRecount();
                    const res = await fetch(`/api/inventur/${this.inventur.id}/items/${item.id}`, {
                        method: 'PUT', credentials: 'include', headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ result: item.result || '', counted: item.counted, note: item.note || '' })
                    });
                    if (!res.ok) {
                        Object.assign(item, before); this._inventurRecount();
                        const d = await res.json().catch(() => ({}));
                        await appAlert(d.detail || 'Konnte nicht gespeichert werden.');
                    }
                },
                async finishInventur() {
                    const s = this.inventur.summary;
                    const problems = s.fehlt + s.defekt;
                    const msg = (s.open ? `${s.open} Einträge sind noch nicht geprüft und stehen dann als „nicht geprüft“ im Protokoll. ` : '')
                        + (problems ? `${problems} ${problems === 1 ? 'Eintrag wird' : 'Einträge werden'} als Mangel gemeldet. ` : '')
                        + 'Danach lässt sich die Inventur nicht mehr ändern.';
                    if (!await appConfirm(msg, 'Inventur abschließen')) return;
                    const note = await appPrompt('Bemerkung fürs Protokoll (optional):', '', 'Inventur abschließen');
                    if (note === null) return;
                    const res = await fetch(`/api/inventur/${this.inventur.id}/finish`, { method: 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ note, allow_open: true }) });
                    const d = await res.json().catch(() => ({}));
                    if (!res.ok) { await appAlert(d.detail || 'Abschließen fehlgeschlagen.'); return; }
                    await this.openInventur(this.inventur.id);
                    this.loadInventories();
                    if (d.defects && this.loadDefectReports) this.loadDefectReports();
                    await appAlert(d.defects ? `Abgeschlossen. ${d.defects} ${d.defects === 1 ? 'Mangel wurde' : 'Mängel wurden'} angelegt.` : 'Abgeschlossen.');
                },
                async deleteInventur(inv) {
                    if (!await appConfirm(`Laufende Inventur „${inv.title}“ löschen? Alle Haken gehen verloren.`, 'Inventur löschen')) return;
                    const res = await fetch(`/api/inventur/${inv.id}`, { method: 'DELETE', credentials: 'include' });
                    if (!res.ok) { const d = await res.json().catch(() => ({})); await appAlert(d.detail || 'Löschen fehlgeschlagen.'); return; }
                    if (this.inventur && this.inventur.id === inv.id) this.inventur = null;
                    this.loadInventories();
                },
                inventurPdf(id) { window.open(`/api/inventur/${id}/pdf`, '_blank'); },
                inventurDone(i) { return i.kind === 'consumable' ? i.counted !== null : !!i.result; },
                startInventurScan() { this.inventurScanMode = true; this.inventurLastScan = ''; this.startQrScanner(); },
                async handleInventurScan(code) {
                    // Gescanntes Gerät gilt als vorhanden; der Scanner bleibt offen für das nächste Gerät
                    if (code === this.inventurLastScan) return;
                    this.inventurLastScan = code;
                    const item = this.inventur && this.inventur.items.find(i => i.kind === 'equipment' && (i.barcode === code || String(i.ref_id) === code));
                    const box = document.getElementById('qr-reader-result');
                    if (!item) { if (box) box.textContent = `„${code}“ gehört nicht zu dieser Inventur.`; return; }
                    if (navigator.vibrate) navigator.vibrate(80);
                    if (box) box.textContent = `✔ ${item.name}: vorhanden`;
                    if (item.result !== 'ok') await this.setInventurItem(item, { result: 'ok' });
                },

                async startQrScanner() {
                    if (typeof Html5Qrcode === 'undefined') { await appAlert('Scanner-Bibliothek nicht geladen'); return; }
                    this.html5Qrcode = new Html5Qrcode("qr-reader");
                    const modal = new bootstrap.Modal(document.getElementById('qrScannerModal'));
                    modal.show();
                    this.html5Qrcode.start(
                        { facingMode: "environment" },
                        { fps: 10, qrbox: { width: 250, height: 250 } },
                        async (decodedText, decodedResult) => {
                            // Der auf dem Geräte-Label ausgedruckte QR-Code kodiert bewusst eine volle
                            // URL (.../dashboard?eq_barcode=112), damit auch eine normale Handy-Kamera-App
                            // (ohne unseren In-App-Scanner) direkt zum Gerät springt. Der In-App-Scanner
                            // bekommt beim Scannen denselben Text zurück - verglich den bisher aber 1:1 mit
                            // dem gespeicherten (nackten) Barcode, weshalb JEDER Scan hier fehlschlug,
                            // obwohl das Gerät korrekt erfasst war. Erst den eq_barcode-Parameter aus der
                            // URL extrahieren, falls der Scan wie eine URL aussieht.
                            let scannedValue = decodedText;
                            try {
                                const url = new URL(decodedText);
                                const param = url.searchParams.get('eq_barcode');
                                if (param) scannedValue = param;
                            } catch (e) { /* kein URL-Format, decodedText bleibt wie es ist */ }
                            if (this.inventurScanMode) { await this.handleInventurScan(scannedValue); return; }
                            const eq = this.equipment.find(e => e.barcode === scannedValue || String(e.id) === scannedValue);
                            this.stopQrScanner();
                            const qrEl = document.getElementById('qrScannerModal');
                            const modalEl = qrEl ? bootstrap.Modal.getInstance(qrEl) : null;
                            if (eq) {
                                // Das Prüf-Fenster darf erst aufgehen, NACHDEM das Scanner-Fenster
                                // fertig ausgeblendet ist. Öffnet man es sofort, entfernt die noch
                                // laufende Schliess-Animation danach die "modal-open"-Markierung vom
                                // body - die Seite ist dann nach dem Schliessen nicht mehr scrollbar.
                                if (modalEl && qrEl) {
                                    qrEl.addEventListener('hidden.bs.modal', () => this.openAddInspectionModal(eq), { once: true });
                                    modalEl.hide();
                                } else {
                                    this.openAddInspectionModal(eq);
                                }
                            } else {
                                if (modalEl) modalEl.hide();
                                await appAlert("Kein Gerät mit Barcode '" + scannedValue + "' gefunden.");
                            }
                        },
                        (err) => { /* ignore */ }
                    ).catch(async err => {
                        console.error(err);
                        await appAlert("Kamera-Zugriff fehlgeschlagen. Ist HTTPS aktiv?");
                    });
                },
                stopQrScanner() {
                    this.inventurScanMode = false;
                    if (this.html5Qrcode) {
                        const instance = this.html5Qrcode;
                        try {
                            instance.stop().then(() => instance.clear()).catch(e => console.error(e));
                        } catch(e) {}
                        this.html5Qrcode = null;
                    }
                },

                // Equipment
                async loadEquipment() {
                    // Reihenfolge-Schutz: loadEquipment() wird von vielen Stellen aus aufgerufen
                    // (Sidebar-Klick, nach Speichern/Löschen, nach Sammelprüfung, ...). Wird es
                    // zweimal kurz hintereinander aufgerufen (z.B. Doppelklick auf den
                    // Sidebar-Button), kann die Antwort des ERSTEN (älteren) Aufrufs durch
                    // Netzwerk-Schwankungen NACH der Antwort des zweiten (neueren) ankommen und
                    // die frisch geladene Liste mit veralteten Daten überschreiben - dadurch
                    // wirkten gerade erst gespeicherte Geräte "verschwunden", bis man neu lud.
                    // Nur die Antwort des zuletzt gestarteten Aufrufs wird übernommen.
                    const requestSeq = (this._equipmentLoadSeq = (this._equipmentLoadSeq || 0) + 1);
                    const res = await fetch('/api/material/equipment?archived=' + (this.showArchivedEquipment ? 1 : 0), { credentials: 'include' });
                    if(res.ok) {
                        const data = await res.json();
                        if (requestSeq === this._equipmentLoadSeq) {
                            this.equipment = data;
                        }
                    }
                    await this.loadDefectReports();
                },
                openEquipmentReportPdf(eq) {
                    window.open('/api/material/equipment/' + eq.id + '/report/pdf', '_blank');
                },
                openEquipmentModal() {
                    this.activeEquipment = { id: null, name: '', barcode: '', category: 'Schläuche', interval_months: 12, last_inspection: null, next_inspection: null, purchase_value: null, insurance_policy: '' };
                    new bootstrap.Modal(document.getElementById('equipmentModal')).show();
                },
                editEquipment(eq) {
                    this.activeEquipment = {...eq};
                    new bootstrap.Modal(document.getElementById('equipmentModal')).show();
                    setTimeout(() => {
                        const canvas = document.getElementById('eqQrCanvas');
                        if (canvas) {
                            const url = `${window.location.protocol}//${window.location.host}/dashboard?eq_barcode=${eq.barcode}`;
                            new QRious({
                                element: canvas,
                                value: url,
                                size: 200
                            });
                        }
                    }, 350);
                },
                printQrCode() {
                    const canvas = document.getElementById('eqQrCanvas');
                    const imgData = canvas.toDataURL('image/png');
                    const win = window.open('', '_blank');
                    win.document.write(`
                        <html>
                        <head>
                            <title>QR-Code Drucken - ${this.activeEquipment.name}</title>
                            <style>
                                body { font-family: sans-serif; text-align: center; padding: 40px; }
                                h2 { margin-bottom: 20px; }
                                img { border: 1px solid #ccc; padding: 10px; }
                            </style>
                        </head>
                        <body onload="window.print(); window.close();">
                            <h2>${this.activeEquipment.name}</h2>
                            <h3>Barcode: ${this.activeEquipment.barcode}</h3>
                            <img src="${imgData}" width="200" height="200" />
                        </body>
                        </html>
                    `);
                    win.document.close();
                },
                async saveEquipment() {
                    const isEdit = this.activeEquipment.id !== null;
                    const url = isEdit ? `/api/material/equipment/${this.activeEquipment.id}` : '/api/material/equipment';
                    const method = isEdit ? 'PUT' : 'POST';
                    const res = await fetch(url, { method: method, headers: {'Content-Type':'application/json'}, credentials: 'include', body: JSON.stringify(this.activeEquipment) });
                    if(res.ok) {
                        bootstrap.Modal.getInstance(document.getElementById('equipmentModal')).hide();
                        await this.loadEquipment();
                    } else {
                        const err = await res.json().catch(() => ({}));
                        await appAlert(err.detail || "Gerät konnte nicht gespeichert werden.");
                    }
                },
                async openInspectionsHistoryModal(eq) {
                    this.activeEquipment = eq;
                    const res = await fetch(`/api/material/equipment/${eq.id}/inspections`, { credentials: 'include' });
                    if(res.ok) this.inspectionsHistory = await res.json();
                    new bootstrap.Modal(document.getElementById('inspectionsHistoryModal')).show();
                },
                openAddInspectionModal(eq) {
                    this.activeEquipment = eq;
                    this.newInspection = { date: localISODate(), inspector: '', status: 'Bestanden', note: '' };
                    new bootstrap.Modal(document.getElementById('addInspectionModal')).show();
                },
                async submitInspection() {
                    const res = await fetch(`/api/material/equipment/${this.activeEquipment.id}/inspections`, { method: 'POST', headers: {'Content-Type':'application/json'}, credentials: 'include', body: JSON.stringify(this.newInspection) });
                    if(res.ok) {
                        bootstrap.Modal.getInstance(document.getElementById('addInspectionModal')).hide();
                        await this.loadEquipment();
                    } else {
                        await appAlert("Prüfung konnte nicht gespeichert werden.");
                    }
                },
                openLoanModal(eq) {
                    this.activeEquipment = eq;
                    this.newLoan = { borrower_name: '', note: '' };
                    new bootstrap.Modal(document.getElementById('loanEquipmentModal')).show();
                },
                async submitLoan() {
                    const res = await fetch(`/api/material/equipment/${this.activeEquipment.id}/loans`, { method: 'POST', headers: {'Content-Type':'application/json'}, credentials: 'include', body: JSON.stringify(this.newLoan) });
                    if (res.ok) {
                        bootstrap.Modal.getInstance(document.getElementById('loanEquipmentModal')).hide();
                        await this.loadEquipment();
                    } else {
                        const err = await res.json().catch(() => ({}));
                        await appAlert(err.detail || "Ausleihe konnte nicht gespeichert werden.");
                    }
                },
                async returnEquipmentLoan(eq) {
                    if (!await appConfirm(`${eq.name} als zurückgegeben markieren?`)) return;
                    try {
                        const listRes = await fetch(`/api/material/equipment/${eq.id}/loans`, { credentials: 'include' });
                        const loans = listRes.ok ? await listRes.json() : [];
                        const openLoan = loans.find(l => !l.returned_at);
                        if (!openLoan) { await this.loadEquipment(); return; }
                        const res = await fetch(`/api/material/equipment/loans/${openLoan.id}/return`, { method: 'PUT', credentials: 'include' });
                        if (res.ok) { await this.loadEquipment(); }
                        else { await appAlert("Rückgabe konnte nicht gespeichert werden."); }
                    } catch(e) { await appAlert("Verbindung fehlgeschlagen."); }
                },
                openBatchInspectModal() {
                    this.batchInspect = { rawBarcodes: '', inspector: '', status: 'Bestanden', note: '' };
                    new bootstrap.Modal(document.getElementById('batchInspectModal')).show();
                },
                async submitBatchInspect() {
                    const barcodes = this.batchInspect.rawBarcodes.split('\n').map(x => x.trim()).filter(x => x.length > 0);
                    const payload = { barcodes: barcodes, inspector: this.batchInspect.inspector, status: this.batchInspect.status, note: this.batchInspect.note };
                    const res = await fetch('/api/material/equipment/batch-inspect', { method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify(payload) });
                    if(res.ok) {
                        bootstrap.Modal.getInstance(document.getElementById('batchInspectModal')).hide();
                        await this.loadEquipment();
                    } else {
                        await appAlert("Sammelprüfung konnte nicht gespeichert werden.");
                    }
                },
                async loadConsumables() {
                    try {
                        const res = await fetch('/api/material/consumables', { credentials: 'include' });
                        this.consumables = res.ok ? await res.json() : [];
                    } catch(e) { this.consumables = []; }
                },
                async submitConsumable() {
                    if (!this.newConsumable.name.trim()) return;
                    const res = await fetch('/api/material/consumables', {
                        method: 'POST', headers: {'Content-Type':'application/json'}, credentials: 'include',
                        body: JSON.stringify(this.newConsumable)
                    });
                    if (res.ok) {
                        this.newConsumable = { name: '', unit: 'Stk', current_stock: 0, min_stock: 0, note: '' };
                        await this.loadConsumables();
                    }
                },
                async saveConsumable(c) {
                    await fetch(`/api/material/consumables/${c.id}`, {
                        method: 'PUT', headers: {'Content-Type':'application/json'}, credentials: 'include',
                        body: JSON.stringify(c)
                    });
                },
                async deleteConsumable(id) {
                    if (!await appConfirm("Eintrag löschen?")) return;
                    await fetch(`/api/material/consumables/${id}`, { method: 'DELETE', credentials: 'include' });
                    await this.loadConsumables();
                },

                addPsaEquipment() {
                    this.activeEquipment = { id: null, name: '', barcode: '', category: 'Atemschutz', image_url: '', manual_url: '', interval_months: 12, last_inspection: null, next_inspection: null };
                    new bootstrap.Modal(document.getElementById('equipmentModal')).show();
                },
                addFunkEquipment() {
                    this.activeEquipment = { id: null, name: '', barcode: '', category: 'Funkgerät', image_url: '', manual_url: '', interval_months: 0, last_inspection: null, next_inspection: null };
                    new bootstrap.Modal(document.getElementById('equipmentModal')).show();
                },
                async archiveEquipment(eq, archived) {
                    if (archived && !await appConfirm(`„${eq.name}“ archivieren? Das Gerät verschwindet aus der Liste und löst keine Prüf-Erinnerungen mehr aus. Der Verlauf bleibt erhalten, Wiederherstellen ist jederzeit möglich.`)) return;
                    const res = await fetch(`/api/material/equipment/${eq.id}/archive`, { method: 'POST', headers: {'Content-Type':'application/json'}, credentials: 'include', body: JSON.stringify({ archived }) });
                    if (res.ok) await this.loadEquipment();
                    else await appAlert((await res.json().catch(() => ({}))).detail || "Aktion nicht möglich.");
                },
                async deleteEquipmentFromSubtab(id) {
                    if (await appConfirm("Gerät permanent aus der Datenbank löschen? All seine Prüf- und Mängeldaten gehen verloren!")) {
                        const res = await fetch(`/api/material/equipment/${id}`, { method: 'DELETE', credentials: 'include' });
                        if(res.ok) {
                            await this.loadEquipment();
                        } else {
                            const err = await res.json();
                            await appAlert("Fehler beim Löschen: " + (err.detail || "Keine Berechtigung"));
                        }
                    }
                },
                async deleteEquipmentInModal() {
                    if (!this.activeEquipment || !this.activeEquipment.id) return;
                    if (await appConfirm(`Gerät "${this.activeEquipment.name}" wirklich permanent löschen?`)) {
                        const res = await fetch(`/api/material/equipment/${this.activeEquipment.id}`, { method: 'DELETE', credentials: 'include' });
                        if (res.ok) {
                            const modalEl = document.getElementById('equipmentModal');
                            const m = bootstrap.Modal.getInstance(modalEl);
                            if (m) m.hide();
                            await this.loadEquipment();
                        } else {
                            const err = await res.json();
                            await appAlert("Fehler beim Löschen: " + (err.detail || "Keine Berechtigung"));
                        }
                    }
                },
                
                // === MÄNGELMELDER METHODS ===
                async loadDefectReports() {
                    try {
                        const res = await fetch(`/api/material/defect-reports?status=${encodeURIComponent(this.defectFilter)}`, { credentials: 'include' });
                        if (res.ok) this.defectReports = await res.json();
                    } catch(e) { console.error('Defect reports error:', e); }
                },
                openDefectReportModal(eq) {
                    this.newDefect = { equipment_id: eq.id, description: '', severity: 'Mittel' };
                    this.materialSubTab = 'maengel';
                    this.loadDefectReports();
                },
                async submitDefectReport() {
                    if (!this.newDefect.equipment_id || !this.newDefect.description.trim()) {
                        await appAlert('Bitte Gerät und Beschreibung angeben!'); return;
                    }
                    const res = await fetch('/api/material/defect-reports', {
                        method: 'POST', headers: {'Content-Type': 'application/json'},
                        credentials: 'include', body: JSON.stringify(this.newDefect)
                    });
                    if (res.ok) {
                        await appAlert('✅ Mangel wurde gemeldet!');
                        this.newDefect = { equipment_id: null, description: '', severity: 'Mittel' };
                        await this.loadDefectReports();
                    } else { const d = await res.json(); await appAlert(d.detail || 'Fehler beim Melden.'); }
                },
                async resolveDefect(id, status) {
                    const res = await fetch(`/api/material/defect-reports/${id}`, {
                        method: 'PUT', headers: {'Content-Type': 'application/json'},
                        credentials: 'include', body: JSON.stringify({ status: status })
                    });
                    if (res.ok) { await this.loadDefectReports(); if (this.openDefectId === id) await this.loadDefectComments(id); }
                    else await appAlert((await res.json().catch(() => ({}))).detail || 'Status konnte nicht geändert werden.');
                },
                async saveDefectAssignee(dr) {
                    const res = await fetch(`/api/material/defect-reports/${dr.id}`, {
                        method: 'PUT', headers: {'Content-Type': 'application/json'},
                        credentials: 'include', body: JSON.stringify({ status: dr.status, assigned_to: dr.assigned_to || '' })
                    });
                    if (!res.ok) await appAlert((await res.json().catch(() => ({}))).detail || 'Zuweisung konnte nicht gespeichert werden.');
                },
                async toggleDefectComments(dr) {
                    if (this.openDefectId === dr.id) { this.openDefectId = null; return; }
                    this.openDefectId = dr.id; this.defectComments = []; this.newDefectComment = '';
                    await this.loadDefectComments(dr.id);
                },
                async loadDefectComments(id) {
                    const res = await fetch(`/api/material/defect-reports/${id}/comments`, { credentials: 'include' });
                    if (res.ok && this.openDefectId === id) this.defectComments = await res.json();
                },
                async sendDefectComment(dr) {
                    const text = (this.newDefectComment || '').trim();
                    if (!text) return;
                    const res = await fetch(`/api/material/defect-reports/${dr.id}/comments`, {
                        method: 'POST', headers: {'Content-Type': 'application/json'},
                        credentials: 'include', body: JSON.stringify({ text })
                    });
                    if (res.ok) { this.newDefectComment = ''; await this.loadDefectComments(dr.id); await this.loadDefectReports(); }
                    else await appAlert((await res.json().catch(() => ({}))).detail || 'Nachricht konnte nicht gesendet werden.');
                },
                async deleteDefectReport(id) {
                    if (!await appConfirm("Mangelmeldung unwiderruflich löschen?")) return;
                    const res = await fetch(`/api/material/defect-reports/${id}`, {
                        method: 'DELETE',
                        credentials: 'include'
                    });
                    if (res.ok) {
                        await this.loadDefectReports();
                    } else {
                        const d = await res.json();
                        await appAlert(d.detail || "Fehler beim Löschen.");
                    }
                },
            }
});

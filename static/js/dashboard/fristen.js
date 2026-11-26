// Fristen, Logbücher und Gerätehaus.
// Teil der Dashboard-App (Vue-Mixin): Daten stehen in ../dashboard.js, "this" ist die ganze App.
window.DB_MIXINS = window.DB_MIXINS || [];
window.DB_MIXINS.push({
            computed: {
                dueBadge() { return this.dueItems.filter(i => !i.first_check).length; },
                dueItemsFiltered() { return this.dueOnlyOverdue ? this.dueItems.filter(i => i.overdue) : this.dueItems; },
                dueAreas() {
                    const labels = { fahrzeuge: 'Fahrzeuge', material: 'Geräte & Material', gebaeude: 'Gerätehaus', hvo: 'Sanitätsmaterial', personal: 'Personen', sonstige: 'Sonstige Fristen' };
                    return Object.keys(labels).filter(k => this.dueItemsFiltered.some(i => i.area === k)).map(k => ({ key: k, label: labels[k] }));
                },
                activeLogbook() { return this.logbooks.find(b => b.id === this.activeLogbookId) || null; },
                logbookYears() { const y = new Date().getFullYear(); return [y, y - 1, y - 2, y - 3, y - 4]; },
                dueManualTypes() { return this.dueTypes.filter(t => !t.logbook_id); },
            },
            methods: {
                async loadDueItems() {
                    try {
                        const res = await fetch('/api/due-items', { credentials: 'include' });
                        this.dueItems = res.ok ? await res.json() : [];
                    } catch(e) { this.dueItems = []; }
                },
                goToDue(it) {
                    const t = it.type;
                    if (t === 'equipment') { this.activeTab = 'material'; this.materialSubTab = 'geraete'; this.eqSearch = it.name; }
                    else if (t === 'low_stock') { this.activeTab = 'material'; this.materialSubTab = 'verbrauch'; this.loadConsumables(); }
                    else if (t === 'vehicle' || t === 'vehicle_check') { this.activeTab = 'fahrzeuge'; }
                    else if (t === 'building') { this.activeTab = 'material'; this.materialSubTab = 'gebaeude'; this.loadBuilding(); }
                    else if (t === 'hvo_material') { this.activeTab = 'hvo'; }
                    else if (t === 'prospect') { this.activeTab = 'personal'; this.openProspectsModal(); }
                    else if (t === 'custom') {
                        if (it.auto && this.logbooks.some(b => b.id === it.logbook_id)) { this.activeLogbookId = it.logbook_id; this.logbookView = 'entries'; this.activeTab = 'logbuch'; }
                        else this.activeTab = 'fristen';
                    }
                    else { this.activeTab = 'personal'; this.personnelSearch = it.name; }
                },
                async doneDue(it) {
                    // Direkt als erledigt eintragen, wo es dafür eine einfache Aktion gibt
                    if (it.type === 'custom' && it.ref_id) {
                        const res = await fetch(`/api/due/entries/${it.ref_id}/done`, { method: 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({}) });
                        if (res.ok) { await this.loadDueItems(); if (this.dueTypes.length) await this.loadDueManager(); }
                    } else if (it.type === 'vehicle' && it.ref_id && it.field) {
                        const isTuv = it.field === 'tuv_date', d = new Date();
                        d.setMonth(d.getMonth() + (isTuv ? 24 : 12));
                        const val = await appPrompt(`${it.detail} von „${it.name}“ bestanden. Wann ist die nächste fällig?`, localISODate(d), 'Neues Fälligkeitsdatum', 'date');
                        if (!val) return;
                        const res = await fetch(`/api/vehicles/${it.ref_id}/inspection`, { method: 'PUT', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ field: it.field, date: val }) });
                        if (res.ok) { await this.loadDueItems(); this.loadVehicles && this.loadVehicles(); }
                        else { const e = await res.json().catch(() => ({})); await appAlert(typeof e.detail === 'string' ? e.detail : 'Konnte nicht gespeichert werden.'); }
                    } else if (it.type === 'building' && it.ref_id) {
                        const res = await fetch(`/api/building/checks/${it.ref_id}/done`, { method: 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({}) });
                        if (res.ok) await this.loadDueItems();
                    } else if (it.type === 'license' && it.personnel_id) {
                        if (!await appConfirm(`Führerscheinkontrolle für „${it.name}“ als heute durchgeführt eintragen?`)) return;
                        const res = await fetch('/api/personnel/license-checked', { method: 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ids: [it.personnel_id] }) });
                        if (res.ok) await this.loadDueItems();
                    }
                },
                async doneDueBulk(items) {
                    const ids = items.filter(i => i.type === 'license' && i.personnel_id).map(i => i.personnel_id);
                    if (!ids.length) return;
                    if (!await appConfirm(`Führerscheinkontrolle für ${ids.length} Person(en) als heute durchgeführt eintragen?`)) return;
                    const res = await fetch('/api/personnel/license-checked', { method: 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ids }) });
                    if (res.ok) await this.loadDueItems();
                },
                // ---- Eigene Fristarten und ihre Fristen (Einstellungen → Wache & Fristen, Fristen-Seite)
                async loadDueManager() {
                    try {
                        const [t, e] = await Promise.all([fetch('/api/due/types', { credentials: 'include' }), fetch('/api/due/entries', { credentials: 'include' })]);
                        if (t.ok) { const d = await t.json(); this.dueTypes = d.types; this.dueCanCreate = d.can_create; }
                        if (e.ok) this.dueEntries = await e.json();
                    } catch (err) {}
                    if (this.dueCanCreate) this.loadDueSubjects('person');
                    if (!this.dueEntryForm.type_id && this.dueManualTypes.length === 1) { this.dueEntryForm.type_id = this.dueManualTypes[0].id; this.onDueEntryTypeChange(); }
                },
                dueSubjectLabel(s) { return ({ person: 'Person', vehicle: 'Fahrzeug', equipment: 'Gerät', general: 'Allgemein (ohne Bezug)' })[s] || s; },
                dueRoleLabel(r) { return ({ geratewart: 'Gerätewart', atemschutzwart: 'Atemschutzwart', gruppenfuehrer: 'Gruppenführer', jugendwarte: 'Jugendwart' })[r] || ''; },
                dueResponsible(t) { return [this.dueRoleLabel(t.responsible_role), t.responsible_name].filter(Boolean).join(' + ') || 'nur Admin/Leitung'; },
                async loadDueSubjects(kind) {
                    if (this.dueSubjects[kind]) return;
                    try {
                        const res = await fetch('/api/due/subjects?kind=' + kind, { credentials: 'include' });
                        if (res.ok) this.dueSubjects = { ...this.dueSubjects, [kind]: await res.json() };
                    } catch (err) {}
                },
                resetDueTypeForm() { this.dueTypeForm = { id: null, name: '', subject: 'person', interval_months: null, lead_days: null, responsible_role: '', responsible_pid: null, logbook_id: null }; },
                editDueType(t) { this.dueTypeForm = { id: t.id, name: t.name, subject: t.subject, interval_months: t.interval_months, lead_days: t.lead_days, responsible_role: t.responsible_role || '', responsible_pid: t.responsible_pid, logbook_id: t.logbook_id || null }; },
                async saveDueType() {
                    const f = this.dueTypeForm;
                    const body = { name: f.name, subject: f.subject, interval_months: f.interval_months || null, lead_days: f.lead_days || null, responsible_role: f.responsible_role || '', responsible_pid: f.responsible_pid || null, logbook_id: f.logbook_id || null };
                    const res = await fetch(f.id ? `/api/due/types/${f.id}` : '/api/due/types', { method: f.id ? 'PUT' : 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
                    if (!res.ok) { const d = await res.json().catch(() => ({})); await appAlert(typeof d.detail === 'string' ? d.detail : 'Bitte die Angaben prüfen.'); return; }
                    this.resetDueTypeForm();
                    await this.loadDueManager(); await this.loadDueItems();
                },
                async deleteDueType(t) {
                    const n = this.dueEntries.filter(e => e.type_id === t.id).length;
                    if (!await appConfirm(`Fristart „${t.name}“ löschen?` + (n ? ` Die ${n} eingetragene(n) Frist(en) dazu werden mitgelöscht.` : ''))) return;
                    const res = await fetch(`/api/due/types/${t.id}`, { method: 'DELETE', credentials: 'include' });
                    if (res.ok) { await this.loadDueManager(); await this.loadDueItems(); }
                },
                dueEntryType() { return this.dueTypes.find(t => t.id === this.dueEntryForm.type_id) || null; },
                async onDueEntryTypeChange() {
                    this.dueEntryForm.subject_id = null;
                    const t = this.dueEntryType();
                    if (t && t.subject !== 'general') await this.loadDueSubjects(t.subject);
                },
                resetDueEntryForm() { this.dueEntryForm = { id: null, type_id: this.dueManualTypes.length === 1 ? this.dueManualTypes[0].id : null, subject_id: null, due_date: '', note: '' }; if (this.dueEntryForm.type_id) this.onDueEntryTypeChange(); },
                async editDueEntry(e) {
                    this.dueEntryForm = { id: e.id, type_id: e.type_id, subject_id: e.personnel_id || e.vehicle_id || e.equipment_id || null, due_date: e.due_date || '', note: e.note || '' };
                    const t = this.dueEntryType();
                    if (t && t.subject !== 'general') await this.loadDueSubjects(t.subject);
                },
                async saveDueEntry() {
                    const f = this.dueEntryForm;
                    const res = await fetch(f.id ? `/api/due/entries/${f.id}` : '/api/due/entries', { method: f.id ? 'PUT' : 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ type_id: f.type_id, subject_id: f.subject_id, due_date: f.due_date || null, note: f.note }) });
                    if (!res.ok) { const d = await res.json().catch(() => ({})); await appAlert(typeof d.detail === 'string' ? d.detail : 'Bitte die Angaben prüfen.'); return; }
                    this.resetDueEntryForm();
                    await this.loadDueManager(); await this.loadDueItems();
                },
                async doneDueEntry(e) {
                    const res = await fetch(`/api/due/entries/${e.id}/done`, { method: 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({}) });
                    if (res.ok) { await this.loadDueManager(); await this.loadDueItems(); }
                },
                async deleteDueEntry(e) {
                    if (!await appConfirm(`Frist „${e.type_name}“ für ${e.subject_name || 'alle'} löschen?`)) return;
                    const res = await fetch(`/api/due/entries/${e.id}`, { method: 'DELETE', credentials: 'include' });
                    if (res.ok) { await this.loadDueManager(); await this.loadDueItems(); }
                },
                // ---- Logbücher (z.B. Flugbuch): Tab "Logbücher" mit Einträgen, Auswertung und (für Admin/Leitung) dem Einrichten
                async loadLogbooks() {
                    try {
                        const res = await fetch('/api/logbooks', { credentials: 'include' });
                        if (res.ok) {
                            this.logbooks = await res.json();
                            if (!this.logbooks.some(b => b.id === this.activeLogbookId)) this.activeLogbookId = this.logbooks.length ? this.logbooks[0].id : null;
                        }
                    } catch (err) {}
                    this.logbooksLoaded = true;
                    if (this.activeTab === 'logbuch') { if (!this.logbooks.length && !this.canSeeSettings) this.activeTab = 'dienste'; else this.loadLogbookData(); }
                },
                openLogbooks() { this.activeTab = 'logbuch'; this.lbChooser = !this.logbooks.length; window.scrollTo({ top: 0 }); },
                selectLogbook(id) { this.activeLogbookId = id; this.logbookView = 'entries'; this.resetLbEntryForm(); this.loadLogbookData(); },
                async loadLogbookData() {
                    const b = this.activeLogbook;
                    if (!b) return;
                    const y = this.logbookYear || 0;
                    try {
                        const [e, v, c] = await Promise.all([fetch(`/api/logbooks/${b.id}/entries?year=${y}`, { credentials: 'include' }), fetch(`/api/logbooks/${b.id}/evaluation?year=${y}`, { credentials: 'include' }), fetch(`/api/logbooks/${b.id}/choices`, { credentials: 'include' })]);
                        this.lbEntries = e.ok ? await e.json() : [];
                        this.lbEval = v.ok ? await v.json() : null;
                        if (c.ok) this.lbChoices = await c.json();
                    } catch (err) { this.lbEntries = []; this.lbEval = null; }
                    if (!this.lbEntryForm.id) this.resetLbEntryForm();
                },
                resetLbEntryForm() {
                    const people = this.lbChoices.people || [];
                    this.lbEntryForm = { id: null, entry_date: localISODate(), personnel_id: people.length === 1 ? people[0].id : null, subject_id: null, values: {} };
                },
                printPage() { window.print(); },
                fmtMinutes(m) { return m === null || m === undefined ? '' : Math.floor(m / 60) + ':' + String(m % 60).padStart(2, '0') + ' h'; },
                lbLiveDuration(f) {
                    const v = this.lbEntryForm.values, a = v[f.config.start], z = v[f.config.end];
                    if (!a || !z) return '';
                    const m = (t) => { const [h, mi] = String(t).split(':'); return (+h) * 60 + (+mi); };
                    let d = m(z) - m(a); if (d < 0) d += 1440;
                    return this.fmtMinutes(d);
                },
                lbShow(e, f) {
                    if (f.ftype === 'duration') return this.fmtMinutes(e.durations[f.id]);
                    const v = e.values[f.id];
                    if (v === undefined || v === null || v === '') return '';
                    if (f.ftype === 'bool') return v ? 'Ja' : 'Nein';
                    if (f.ftype === 'date') return new Date(v).toLocaleDateString('de-DE');
                    return f.unit ? v + ' ' + f.unit : v;
                },
                editLbEntry(e) {
                    this.lbEntryForm = { id: e.id, entry_date: e.entry_date, personnel_id: e.personnel_id, subject_id: e.subject_id, values: { ...e.values } };
                    window.scrollTo({ top: 0, behavior: 'smooth' });
                },
                async saveLbEntry() {
                    const f = this.lbEntryForm, b = this.activeLogbook;
                    const res = await fetch(f.id ? `/api/logbooks/entries/${f.id}` : `/api/logbooks/${b.id}/entries`, { method: f.id ? 'PUT' : 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ entry_date: f.entry_date, personnel_id: f.personnel_id, subject_id: f.subject_id, values: f.values }) });
                    if (!res.ok) { const d = await res.json().catch(() => ({})); await appAlert(typeof d.detail === 'string' ? d.detail : 'Bitte die Angaben prüfen.'); return; }
                    this.resetLbEntryForm(); await this.loadLogbookData(); this.loadDueItems();
                },
                async deleteLbEntry(e) {
                    if (!await appConfirm('Diesen Eintrag löschen?')) return;
                    const res = await fetch(`/api/logbooks/entries/${e.id}`, { method: 'DELETE', credentials: 'include' });
                    if (res.ok) { await this.loadLogbookData(); this.loadDueItems(); }
                },
                // Editor (Admin/Leitung)
                lbNewUid() { return 'n' + Math.random().toString(36).slice(2, 8); },
                lbBlankField() { return { uid: this.lbNewUid(), label: '', ftype: 'text', unit: '', optionsText: '', required: false, evaluate: 'none', config: {} }; },
                lbPreset() {
                    const a = this.lbNewUid(), z = this.lbNewUid();
                    const f = (label, ftype, extra) => ({ ...this.lbBlankField(), label, ftype, ...extra });
                    return { id: null, name: 'Flugbuch', subject: 'equipment', responsible_role: '', responsible_pid: null, open_to_all: false, member_ids: [], fields: [
                        f('Start', 'time', { uid: a, required: true }), f('Landung', 'time', { uid: z, required: true }),
                        f('Flugdauer', 'duration', { evaluate: 'sum', config: { start: a, end: z } }),
                        f('Art', 'select', { optionsText: 'Einsatz, Übung, Training, Test', required: true, evaluate: 'group' }),
                        f('Ort', 'text', {}), f('Akku', 'text', {}), f('Vorfall / Störung', 'bool', { evaluate: 'group' }), f('Bemerkung', 'text', {})] };
                },
                newLogbookForm(preset) {
                    this.lbChooser = false;
                    this.lbForm = preset ? this.lbPreset() : { id: null, name: '', subject: 'none', responsible_role: '', responsible_pid: null, open_to_all: false, member_ids: [], fields: [this.lbBlankField()] };
                    this.loadDueSubjects('person');
                },
                editLogbook(b) {
                    this.lbChooser = false;
                    this.lbForm = { id: b.id, name: b.name, subject: b.subject, responsible_role: b.responsible_role || '', responsible_pid: b.responsible_pid, open_to_all: b.open_to_all, member_ids: [...b.member_ids],
                        fields: b.fields.map(f => ({ uid: String(f.id), id: f.id, label: f.label, ftype: f.ftype, unit: f.unit || '', optionsText: (f.options || []).join(', '), required: f.required, evaluate: f.evaluate,
                            config: f.ftype === 'duration' ? { start: String(f.config.start), end: String(f.config.end) } : {} })) };
                    this.loadDueSubjects('person');
                },
                lbAddField() { this.lbForm.fields.push(this.lbBlankField()); },
                lbMoveField(i, d) { const a = this.lbForm.fields, j = i + d; if (j < 0 || j >= a.length) return; [a[i], a[j]] = [a[j], a[i]]; },
                lbTimeFields() { return this.lbForm.fields.filter(f => f.ftype === 'time'); },
                lbToggleMember(id) { const m = this.lbForm.member_ids, i = m.indexOf(id); if (i >= 0) m.splice(i, 1); else m.push(id); },
                async saveLogbook() {
                    const f = this.lbForm;
                    const evalFor = (x) => (x.ftype === 'number' || x.ftype === 'duration') ? (x.evaluate === 'sum' ? 'sum' : 'none') : (['select', 'bool', 'text'].includes(x.ftype) ? (x.evaluate === 'group' ? 'group' : 'none') : 'none');
                    const fields = f.fields.map(x => ({ uid: x.uid, id: x.id || null, label: x.label, ftype: x.ftype, unit: x.unit || '', required: !!x.required, evaluate: evalFor(x),
                        options: x.ftype === 'select' ? x.optionsText.split(/[,\n]/).map(o => o.trim()).filter(Boolean) : null, config: x.ftype === 'duration' ? x.config : null }));
                    const body = { name: f.name, subject: f.subject, responsible_role: f.responsible_role || '', responsible_pid: f.responsible_pid || null, open_to_all: !!f.open_to_all, member_ids: f.member_ids, fields };
                    const res = await fetch(f.id ? `/api/logbooks/${f.id}` : '/api/logbooks', { method: f.id ? 'PUT' : 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
                    if (!res.ok) { const d = await res.json().catch(() => ({})); await appAlert(typeof d.detail === 'string' ? d.detail : 'Bitte die Angaben prüfen.'); return; }
                    const known = this.logbooks.map(x => x.id);
                    this.lbForm = null; await this.loadLogbooks();
                    const fresh = this.logbooks.find(x => !known.includes(x.id));
                    if (fresh) this.activeLogbookId = fresh.id;
                    this.logbookView = 'entries'; this.resetLbEntryForm(); this.loadLogbookData();
                },
                async deleteLogbook(b) {
                    if (!await appConfirm(`Logbuch „${b.name}“ samt allen Einträgen löschen?`)) return;
                    const res = await fetch(`/api/logbooks/${b.id}`, { method: 'DELETE', credentials: 'include' });
                    if (res.ok) { await this.loadLogbooks(); await this.loadDueManager(); await this.loadDueItems(); }
                },
                async loadBuilding() {
                    const res = await fetch('/api/building/checks', { credentials: 'include' });
                    if (!res.ok) return;
                    const data = await res.json();
                    this.buildingChecks = data.checks;
                    this.buildingSuggestions = data.suggestions.filter(s => !data.checks.some(c => c.name === s.name));
                },
                resetBuildingForm() { this.buildingForm = { id: null, name: '', interval_months: 12, last_check: '', responsible: '' }; },
                applyBuildingSuggestion(s) { this.buildingForm = { id: null, name: s.name, interval_months: s.interval_months, last_check: '', responsible: '' }; },
                editBuildingCheck(c) { this.buildingForm = { id: c.id, name: c.name, interval_months: c.interval_months, last_check: c.last_check || '', responsible: c.responsible || '' }; },
                async saveBuildingCheck() {
                    const f = this.buildingForm;
                    const res = await fetch(f.id ? `/api/building/checks/${f.id}` : '/api/building/checks', {
                        method: f.id ? 'PUT' : 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ name: f.name, interval_months: f.interval_months, last_check: f.last_check || null, responsible: f.responsible })
                    });
                    if (!res.ok) { const d = await res.json().catch(() => ({})); await appAlert(typeof d.detail === 'string' ? d.detail : 'Bitte Bezeichnung und Intervall prüfen.'); return; }
                    this.resetBuildingForm();
                    await this.loadBuilding();
                },
                async markBuildingDone(c) {
                    const res = await fetch(`/api/building/checks/${c.id}/done`, { method: 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({}) });
                    if (res.ok) await this.loadBuilding();
                },
                async deleteBuildingCheck(c) {
                    if (!await appConfirm(`Prüfung „${c.name}" löschen?`)) return;
                    const res = await fetch(`/api/building/checks/${c.id}`, { method: 'DELETE', credentials: 'include' });
                    if (res.ok) await this.loadBuilding();
                },
                buildingStatus(c) {
                    if (!c.next_check) return { cls: 'bg-secondary', label: 'offen' };
                    const days = Math.ceil((new Date(c.next_check) - new Date()) / 86400000);
                    if (days < 0) return { cls: 'bg-danger', label: `seit ${-days} Tagen überfällig` };
                    if (days <= 30) return { cls: 'bg-warning text-dark', label: `in ${days} Tagen` };
                    return { cls: 'bg-success', label: 'in Ordnung' };
                },
            }
});

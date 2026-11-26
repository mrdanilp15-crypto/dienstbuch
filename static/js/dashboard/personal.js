// Personalakten, Lehrgänge und Nachwuchs.
// Teil der Dashboard-App (Vue-Mixin): Daten stehen in ../dashboard.js, "this" ist die ganze App.
window.DB_MIXINS = window.DB_MIXINS || [];
window.DB_MIXINS.push({
            computed: {
                activePersonnelForMatrix() {
                    return this.personnel.filter(p => p.membership_status === 'Aktiv');
                },
                certYears() {
                    const now = new Date().getFullYear();
                    return [0, 1, 2, 3, 4, 5].map(offset => now - offset);
                },
                filteredPersonnel() {
                    return this.personnel.filter(p => p.name.toLowerCase().includes(this.personnelSearch.toLowerCase()));
                },
            },
            methods: {
                openPersonnelCertificate(kind) {
                    if (!this.activePersonnel || !this.activePersonnel.id) return;
                    const base = `/api/personnel/${this.activePersonnel.id}/certificate/${kind}/pdf`;
                    // Die Lehrgangsübersicht ist bewusst die vollständige Historie und kennt
                    // daher kein Berichtsjahr.
                    window.open(kind === 'lehrgaenge' ? base : `${base}?year=${this.certYear}`, '_blank');
                },

                // Personal
                async loadPersonnel() {
                    try {
                        const res = await fetch('/api/personnel/list', { credentials: 'include' });
                        if(res.ok) this.personnel = await res.json();
                    } catch(e) { console.error("Error loading personnel", e); }
                },
                openPersonnelModal() {
                    this.inviteNew = { on: false, username: '' };
                    this.activePersonnel = { id: null, name: '', rank: '', membership_status: 'Aktiv', phone: '', email: '', address: '', badge_number: '', birth_date: null, entry_date: null, honors: '', profile_picture: '', has_picture: false, courses: [], gearList: [], emergency_contact_name: '', emergency_contact_phone: '' };
                    this.qualifications.forEach(q => this.activePersonnel[q.id] = false);
                    ['lic_b', 'lic_be', 'lic_c', 'lic_ce'].forEach(k => this.activePersonnel[k] = false);
                    new bootstrap.Modal(document.getElementById('personnelModal')).show();
                },
                async editPersonnel(p) {
                    const res = await fetch(`/api/personnel/get/${p.id}`, { credentials: 'include' });
                    if(res.ok) {
                        const detail = await res.json();
                        // Load courses & gear
                        const [cRes, gRes, hRes] = await Promise.all([
                            fetch(`/api/material/personnel/${p.id}/lehrgaenge`, { credentials: 'include' }),
                            fetch(`/api/material/personnel/${p.id}/inventar`, { credentials: 'include' }),
                            fetch(`/api/material/personnel/${p.id}/honors`, { credentials: 'include' })
                        ]);
                        detail.honors = hRes.ok ? await hRes.json() : [];
                        detail.courses = cRes.ok ? await cRes.json() : [];
                        detail.gearList = gRes.ok ? await gRes.json() : [];
                        this.activePersonnel = detail;
                        new bootstrap.Modal(document.getElementById('personnelModal')).show();
                    }
                },
                async onAvatarChange(e) {
                    const file = e.target.files[0]; if (!file) return;
                    const formData = new FormData();
                    formData.append('file', file);
                    try {
                        const res = await fetch('/api/upload', {
                            method: 'POST',
                            body: formData,
                            credentials: 'include'
                        });
                        if (res.ok) {
                            const data = await res.json();
                            this.activePersonnel.profile_picture = data.url;
                            this.activePersonnel.has_picture = true;
                        } else {
                            await appAlert("Fehler beim Hochladen des Profilbilds.");
                        }
                    } catch(err) {
                        console.error(err);
                        await appAlert("Verbindung fehlgeschlagen beim Hochladen des Profilbilds.");
                    }
                },
                // "Jürgen Groß" -> "juergen.gross"
                suggestUsername(name) {
                    const map = { 'ä': 'ae', 'ö': 'oe', 'ü': 'ue', 'ß': 'ss' };
                    return String(name || '').trim().toLowerCase().replace(/[äöüß]/g, c => map[c]).normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-z0-9]+/g, '.').replace(/^\.+|\.+$/g, '');
                },
                async inviteMember(personnelId, username) {
                    if (!personnelId) return await appAlert("Das Mitglied wurde nicht angelegt, daher auch kein Konto.");
                    const res = await fetch('/api/users/add', { method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'include',
                        body: JSON.stringify({ username, password: '', role: 'mannschaft', personnel_id: personnelId, invite: true }) });
                    if (res.ok) return await appAlert(`Mitglied gespeichert. Konto "${username}" angelegt, die Einladung ist unterwegs.`);
                    const err = await res.json().catch(() => ({}));
                    await appAlert("Das Mitglied ist gespeichert, aber das Konto wurde nicht angelegt: " + (err.detail || "Unbekannter Fehler") + " (nachholen unter Einstellungen → System-Logins)");
                },
                async savePersonnel() {
                    const payload = { ...this.activePersonnel };
                    this.qualifications.forEach(q => payload[q.id] = this.activePersonnel[q.id] ? 1 : 0);
                    ['lic_b', 'lic_be', 'lic_c', 'lic_ce'].forEach(k => payload[k] = this.activePersonnel[k] ? 1 : 0);
                    const isNew = this.activePersonnel.id === null;
                    const endpoint = isNew ? '/api/personnel/add' : `/api/personnel/update/${this.activePersonnel.id}`;
                    const invite = isNew && this.role === 'admin' && this.inviteNew.on && !!(this.activePersonnel.email || '').trim();
                    const username = (this.inviteNew.username || this.suggestUsername(this.activePersonnel.name)).trim();
                    const res = await fetch(endpoint, { method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify(payload) });
                    if(res.ok) {
                        if (invite) await this.inviteMember((await res.json()).id, username);
                        this.cacheBuster = Date.now();
                        bootstrap.Modal.getInstance(document.getElementById('personnelModal')).hide();
                        await this.loadPersonnel();
                    }
                },
                async deletePersonnel() {
                    if(await appConfirm("Mitglied permanent löschen?")) {
                        await fetch(`/api/personnel/delete/${this.activePersonnel.id}`, { method: 'DELETE', credentials: 'include' });
                        bootstrap.Modal.getInstance(document.getElementById('personnelModal')).hide();
                        await this.loadPersonnel();
                    }
                },
                async submitGear() {
                    const res = await fetch(`/api/material/personnel/${this.activePersonnel.id}/inventar`, { method: 'POST', headers: {'Content-Type':'application/json'}, credentials: 'include', body: JSON.stringify(this.newGear) });
                    if(res.ok) {
                        this.newGear = { item_name: '', size: '', issue_date: localISODate() };
                        const gRes = await fetch(`/api/material/personnel/${this.activePersonnel.id}/inventar`, { credentials: 'include' });
                        this.activePersonnel.gearList = await gRes.json();
                    }
                },
                async returnGear(g) {
                    if (!await appConfirm(`Rückgabe von „${g.item_name}" (${g.size}) vermerken?`, 'Rückgabe')) return;
                    const backToStock = await appConfirm('Kommt der Artikel zurück ins Lager?\n\n(Abbrechen = nein, z. B. weil defekt oder ausgesondert)', 'Lagerbestand');
                    const res = await fetch(`/api/material/personnel/inventar/${g.id}/return`, {
                        method: 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ back_to_stock: !!backToStock })
                    });
                    if (!res.ok) { const d = await res.json().catch(() => ({})); await appAlert(d.detail || 'Rückgabe fehlgeschlagen.'); return; }
                    if (this.activePersonnel && this.activePersonnel.id) {
                        const gRes = await fetch(`/api/material/personnel/${this.activePersonnel.id}/inventar`, { credentials: 'include' });
                        if (gRes.ok) this.activePersonnel.gearList = await gRes.json();
                    }
                    if (document.getElementById('clothingModal')?.classList.contains('show')) await this.loadClothing();
                },
                async openClothingModal() {
                    await this.loadClothing();
                    bootstrap.Modal.getOrCreateInstance(document.getElementById('clothingModal')).show();
                },
                async loadClothing() {
                    const [s, i] = await Promise.all([
                        fetch('/api/material/clothing/stock', { credentials: 'include' }),
                        fetch('/api/material/clothing/issued', { credentials: 'include' })
                    ]);
                    if (s.ok) this.clothingStock = await s.json();
                    if (i.ok) this.clothingIssued = await i.json();
                },
                async saveStockItem() {
                    const res = await fetch('/api/material/clothing/stock', {
                        method: 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(this.newStock)
                    });
                    if (!res.ok) { const d = await res.json().catch(() => ({})); await appAlert(typeof d.detail === 'string' ? d.detail : 'Bitte Artikel, Größe und Mengen prüfen.'); return; }
                    this.newStock = { item_name: '', size: '', quantity: 1, min_quantity: 0 };
                    await this.loadClothing();
                },
                async changeStock(s, delta) {
                    const res = await fetch(`/api/material/clothing/stock/${s.id}`, {
                        method: 'PATCH', credentials: 'include', headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ delta })
                    });
                    if (res.ok) s.quantity = Math.max(0, s.quantity + delta);
                },
                async deleteStockItem(s) {
                    if (!await appConfirm(`„${s.item_name}" (${s.size}) aus dem Lagerbestand löschen?`)) return;
                    const res = await fetch(`/api/material/clothing/stock/${s.id}`, { method: 'DELETE', credentials: 'include' });
                    if (res.ok) await this.loadClothing();
                },
                async deleteGear(id) {
                    const res = await fetch(`/api/material/personnel/inventar/${id}`, { method: 'DELETE', credentials: 'include' });
                    if(res.ok) {
                        const gRes = await fetch(`/api/material/personnel/${this.activePersonnel.id}/inventar`, { credentials: 'include' });
                        this.activePersonnel.gearList = await gRes.json();
                    }
                },
                async loadAvailability(personnelId) {
                    if (!personnelId) { this.availabilityList = []; return; }
                    try {
                        const res = await fetch(`/api/personnel/${personnelId}/availability`, { credentials: 'include' });
                        this.availabilityList = res.ok ? await res.json() : [];
                    } catch(e) { this.availabilityList = []; }
                },
                async submitAvailability(personnelId) {
                    if (!this.newAvailability.start_date || !this.newAvailability.end_date) {
                        await appAlert("Bitte Von- und Bis-Datum angeben.");
                        return;
                    }
                    const res = await fetch(`/api/personnel/${personnelId}/availability`, { method: 'POST', headers: {'Content-Type':'application/json'}, credentials: 'include', body: JSON.stringify(this.newAvailability) });
                    if (res.ok) {
                        this.newAvailability = { start_date: '', end_date: '', reason: '' };
                        await this.loadAvailability(personnelId);
                    } else {
                        const err = await res.json().catch(() => ({}));
                        await appAlert(err.detail || "Eintrag konnte nicht gespeichert werden.");
                    }
                },
                async deleteAvailability(entryId, personnelId) {
                    if (!await appConfirm("Abwesenheitseintrag löschen?")) return;
                    const res = await fetch(`/api/personnel/availability/${entryId}`, { method: 'DELETE', credentials: 'include' });
                    if (res.ok) await this.loadAvailability(personnelId);
                },
                async submitCourse() {
                    const res = await fetch(`/api/material/personnel/${this.activePersonnel.id}/lehrgaenge`, { method: 'POST', headers: {'Content-Type':'application/json'}, credentials: 'include', body: JSON.stringify(this.newCourse) });
                    if(res.ok) {
                        this.newCourse = { course_name: '', date: localISODate(), certificate_url: '', valid_until: '' };
                        const cRes = await fetch(`/api/material/personnel/${this.activePersonnel.id}/lehrgaenge`, { credentials: 'include' });
                        this.activePersonnel.courses = await cRes.json();
                    }
                },
                async submitHonor() {
                    if (!(this.newHonor.title || '').trim()) return await appAlert("Bitte eine Bezeichnung eintragen (z. B. Oberfeuerwehrmann oder Ehrenzeichen in Silber).");
                    const res = await fetch(`/api/material/personnel/${this.activePersonnel.id}/honors`, { method: 'POST', headers: {'Content-Type':'application/json'}, credentials: 'include', body: JSON.stringify(this.newHonor) });
                    if (res.ok) {
                        this.newHonor = { kind: this.newHonor.kind, title: '', date: localISODate(), note: '' };
                        const hRes = await fetch(`/api/material/personnel/${this.activePersonnel.id}/honors`, { credentials: 'include' });
                        this.activePersonnel.honors = hRes.ok ? await hRes.json() : [];
                    } else {
                        const err = await res.json().catch(() => ({}));
                        await appAlert(err.detail || "Eintrag konnte nicht gespeichert werden.");
                    }
                },
                async deleteHonor(id) {
                    if (!await appConfirm("Eintrag löschen?")) return;
                    const res = await fetch(`/api/material/personnel/honors/${id}`, { method: 'DELETE', credentials: 'include' });
                    if (res.ok) {
                        const hRes = await fetch(`/api/material/personnel/${this.activePersonnel.id}/honors`, { credentials: 'include' });
                        this.activePersonnel.honors = hRes.ok ? await hRes.json() : [];
                    }
                },
                async deleteCourse(id) {
                    const res = await fetch(`/api/material/personnel/lehrgaenge/${id}`, { method: 'DELETE', credentials: 'include' });
                    if(res.ok) {
                        const cRes = await fetch(`/api/material/personnel/${this.activePersonnel.id}/lehrgaenge`, { credentials: 'include' });
                        this.activePersonnel.courses = await cRes.json();
                    }
                },
                async loadLehrgangTypes() {
                    try {
                        const res = await fetch('/api/material/lehrgang-types', { credentials: 'include' });
                        this.lehrgangTypes = res.ok ? await res.json() : [];
                    } catch(e) { this.lehrgangTypes = []; }
                },
                async addLehrgangType() {
                    if (!this.newLehrgangType.trim()) return;
                    const res = await fetch('/api/material/lehrgang-types', {
                        method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'include',
                        body: JSON.stringify({ name: this.newLehrgangType.trim() })
                    });
                    if (res.ok) {
                        this.newLehrgangType = '';
                        await this.loadLehrgangTypes();
                    } else {
                        const d = await res.json().catch(() => ({}));
                        await appAlert(d.detail || "Konnte nicht hinzugefügt werden.");
                    }
                },
                async deleteLehrgangType(id) {
                    await fetch(`/api/material/lehrgang-types/${id}`, { method: 'DELETE', credentials: 'include' });
                    await this.loadLehrgangTypes();
                },
                async openProspectsModal() {
                    this.resetProspectForm();
                    await this.loadProspects();
                    bootstrap.Modal.getOrCreateInstance(document.getElementById('prospectsModal')).show();
                },
                async loadProspects() {
                    const res = await fetch('/api/prospects', { credentials: 'include' });
                    if (res.ok) this.prospects = await res.json();
                },
                resetProspectForm() { this.prospectForm = { id: null, name: '', phone: '', email: '', source: '', mentor: '', status: 'Interessent', first_contact: null, last_contact: null, trial_end: '', note: '' }; },
                editProspect(p) { this.prospectForm = { ...p, trial_end: p.trial_end || '' }; },
                async saveProspect() {
                    const f = this.prospectForm;
                    const body = { name: f.name, phone: f.phone, email: f.email, source: f.source, mentor: f.mentor, status: f.status,
                                   first_contact: f.first_contact || null, last_contact: f.last_contact || null, trial_end: f.trial_end || null, note: f.note };
                    const res = await fetch(f.id ? `/api/prospects/${f.id}` : '/api/prospects', {
                        method: f.id ? 'PUT' : 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body)
                    });
                    if (!res.ok) { const d = await res.json().catch(() => ({})); await appAlert(typeof d.detail === 'string' ? d.detail : 'Bitte Eingaben prüfen.'); return; }
                    this.resetProspectForm();
                    await this.loadProspects();
                },
                async prospectContact(p) {
                    const res = await fetch(`/api/prospects/${p.id}/contact`, { method: 'POST', credentials: 'include' });
                    if (res.ok) await this.loadProspects();
                },
                async deleteProspect(p) {
                    if (!await appConfirm(`„${p.name}" endgültig löschen? Das lässt sich nicht rückgängig machen.`)) return;
                    const res = await fetch(`/api/prospects/${p.id}`, { method: 'DELETE', credentials: 'include' });
                    if (res.ok) await this.loadProspects();
                },
                async loadParticipation() {
                    try {
                        const res = await fetch(`/api/admin/stats/participation?year=${this.statsYear}`, { credentials: 'include' });
                        if (res.ok) { this.participation = await res.json(); this.participationInput = this.participation.threshold; }
                    } catch (e) { console.error(e); }
                },
                async saveParticipationThreshold() {
                    const res = await fetch('/api/admin/stats/participation-threshold', {
                        method: 'PUT', credentials: 'include', headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ value: this.participationInput })
                    });
                    if (!res.ok) { const d = await res.json().catch(() => ({})); await appAlert(typeof d.detail === 'string' ? d.detail : 'Bitte eine Zahl von 0 bis 100 eingeben.'); return; }
                    await this.loadParticipation();
                },
                lehrgangCell(p, course) {
                    const e = (this.lehrgangMatrix.entries[String(p.id)] || {})[course];
                    if (!e) return null;
                    const fmt = d => d ? d.split('-').reverse().join('.') : '';
                    let state = 'ok';
                    if (e.valid_until) {
                        const days = (new Date(e.valid_until) - new Date()) / 86400000;
                        if (days < 0) state = 'expired';
                        else if (days <= 90) state = 'soon';
                    }
                    const title = 'Abgelegt: ' + fmt(e.date) + (e.valid_until ? ' – gültig bis ' + fmt(e.valid_until) : '');
                    return { state, title };
                },
                async openQualMatrixModal() {
                    if (this.personnel.length === 0) await this.loadPersonnel();
                    try {
                        const res = await fetch('/api/personnel/lehrgaenge-matrix', { credentials: 'include' });
                        if (res.ok) this.lehrgangMatrix = await res.json();
                    } catch (e) { console.error(e); }
                    new bootstrap.Modal(document.getElementById('qualMatrixModal')).show();
                },
                async openAnniversariesModal() {
                    this.anniversariesYear = new Date().getFullYear();
                    await this.loadAnniversaries(this.anniversariesYear);
                    const modalEl = document.getElementById('anniversariesModal');
                    if (modalEl) {
                        const m = new bootstrap.Modal(modalEl);
                        m.show();
                    }
                },
                async loadAnniversaries(year) {
                    this.isLoadingAnniversaries = true;
                    try {
                        const res = await fetch(`/api/personnel/anniversaries?year=${year}`, { credentials: 'include' });
                        if (res.ok) {
                            this.anniversariesData = await res.json();
                        } else {
                            console.error("Fehler beim Laden der Jubiläen");
                        }
                    } catch (e) {
                        console.error("Netzwerkfehler Jubiläen:", e);
                    } finally {
                        this.isLoadingAnniversaries = false;
                    }
                },
            }
});

// Jugendfeuerwehr, Verein und First Responder.
// Teil der Dashboard-App (Vue-Mixin): Daten stehen in ../dashboard.js, "this" ist die ganze App.
window.DB_MIXINS = window.DB_MIXINS || [];
window.DB_MIXINS.push({
            computed: {

                filteredYouthMembers() {
                    if (!this.youthSearch) return this.youthMembers;
                    const q = this.youthSearch.toLowerCase();
                    return this.youthMembers.filter(m => 
                        (m.name && m.name.toLowerCase().includes(q)) ||
                        (m.parent_contact && m.parent_contact.toLowerCase().includes(q)) ||
                        (m.skills && m.skills.toLowerCase().includes(q)) ||
                        (m.badges && m.badges.toLowerCase().includes(q)) ||
                        (m.address && m.address.toLowerCase().includes(q))
                    );
                },
            },
            methods: {
                
                // Jugend & Verein Module
                // Jugendfeuerwehr Module
                async loadJugendData() {
                    try {
                        const [yRes, sRes] = await Promise.all([
                            fetch('/api/jugend/members', { credentials: 'include' }),
                            fetch('/api/jugend/sessions', { credentials: 'include' })
                        ]);
                        if(yRes.ok) this.youthMembers = await yRes.json();
                        if(sRes.ok) this.youthSessions = await sRes.json();
                    } catch(err) { console.error(err); }
                },
                calculateAge(dateStr) {
                    if (!dateStr) return null;
                    const birth = new Date(dateStr);
                    if (isNaN(birth.getTime())) return null;
                    const today = new Date();
                    let age = today.getFullYear() - birth.getFullYear();
                    const m = today.getMonth() - birth.getMonth();
                    if (m < 0 || (m === 0 && today.getDate() < birth.getDate())) age--;
                    return age >= 0 ? age : null;
                },
                openNewYouthModal() {
                    this.cancelYouthEdit();
                    this.youthModalTab = 'stamm';
                    new bootstrap.Modal('#youthModal').show();
                },
                editYouthMember(ym) {
                    this.editingYouthId = ym.id;
                    this.youthModalTab = 'stamm';
                    this.newYouth = {
                        name: ym.name || '',
                        parent_contact: ym.parent_contact || '',
                        badges: ym.badges || '',
                        skills: ym.skills || '',
                        birth_date: ym.birth_date || '',
                        entry_date: ym.entry_date || '',
                        phone: ym.phone || '',
                        email: ym.email || '',
                        address: ym.address || '',
                        notes: ym.notes || '',
                        profile_picture: ym.profile_picture || '',
                        lic_am: !!ym.lic_am,
                        lic_a1: !!ym.lic_a1,
                        lic_b: !!ym.lic_b,
                        lic_l: !!ym.lic_l,
                        lic_t: !!ym.lic_t,
                        has_jf1: !!ym.has_jf1,
                        has_jf2: !!ym.has_jf2,
                        has_jf3: !!ym.has_jf3,
                        has_wissentest: !!ym.has_wissentest,
                        has_leistungsspange: !!ym.has_leistungsspange,
                        has_jugendabzeichen: !!ym.has_jugendabzeichen,
                        has_mta_basis: !!ym.has_mta_basis,
                        has_erste_hilfe: !!ym.has_erste_hilfe,
                        has_funk: !!ym.has_funk
                    };
                    new bootstrap.Modal('#youthModal').show();
                },
                cancelYouthEdit() {
                    this.editingYouthId = null;
                    this.newYouth = {
                        name: '', parent_contact: '', badges: '', skills: '', birth_date: '', entry_date: '', phone: '', email: '', address: '', notes: '', profile_picture: '',
                        lic_am: false, lic_a1: false, lic_b: false, lic_l: false, lic_t: false,
                        has_jf1: false, has_jf2: false, has_jf3: false, has_wissentest: false, has_leistungsspange: false, has_jugendabzeichen: false, has_mta_basis: false, has_erste_hilfe: false, has_funk: false
                    };
                },
                onYouthAvatarChange(e) {
                    const file = e.target.files[0];
                    if (!file) return;
                    const reader = new FileReader();
                    reader.onload = (event) => {
                        this.newYouth.profile_picture = event.target.result;
                    };
                    reader.readAsDataURL(file);
                },
                async saveYouthModal() {
                    if (!this.newYouth.name || !this.newYouth.name.trim()) {
                        await appAlert("Bitte gib den Namen des Jugendlichen ein!");
                        return;
                    }
                    await this.submitYouthMember();
                    const modalEl = document.getElementById('youthModal');
                    const modal = bootstrap.Modal.getInstance(modalEl);
                    if (modal) modal.hide();
                },
                async deleteYouthMemberInModal() {
                    if (this.editingYouthId) {
                        await this.deleteYouthMember(this.editingYouthId);
                        const modalEl = document.getElementById('youthModal');
                        const modal = bootstrap.Modal.getInstance(modalEl);
                        if (modal) modal.hide();
                    }
                },
                async submitYouthMember() {
                    if (!this.newYouth.name || !this.newYouth.name.trim()) {
                        await appAlert("Bitte gib den Namen des Jugendlichen ein!");
                        return;
                    }
                    const isNew = !this.editingYouthId;
                    const endpoint = isNew ? '/api/jugend/members' : `/api/jugend/members/${this.editingYouthId}`;
                    const method = isNew ? 'POST' : 'PUT';

                    const res = await fetch(endpoint, {
                        method: method,
                        headers: {'Content-Type':'application/json'},
                        credentials: 'include',
                        body: JSON.stringify(this.newYouth)
                    });
                    if(res.ok) {
                        this.cancelYouthEdit();
                        await this.loadJugendData();
                    } else {
                        const err = await res.json();
                        await appAlert("Fehler beim Speichern: " + (err.detail || "Unbekannter Fehler"));
                    }
                },
                async deleteYouthMember(id) {
                    if (await appConfirm("Jugendfeuerwehr-Mitglied wirklich löschen?")) {
                        const res = await fetch(`/api/jugend/members/${id}`, { method: 'DELETE', credentials: 'include' });
                        if(res.ok) {
                            if (this.editingYouthId === id) this.cancelYouthEdit();
                            await this.loadJugendData();
                        }
                    }
                },
                editYouthSession(session) {
                    this.editingYouthSessionId = session.id;
                    const attObj = {};
                    if(session.attendance) {
                        session.attendance.forEach(a => { attObj[a.member_id] = a.is_present === 1; });
                    }
                    this.newYouthSession = {
                        date: session.date,
                        topic: session.topic,
                        duration: session.duration,
                        instructors: session.instructors,
                        description: session.description,
                        attendance: attObj
                    };
                    window.scrollTo({ top: 0, behavior: 'smooth' });
                },
                cancelYouthSessionEdit() {
                    this.editingYouthSessionId = null;
                    this.newYouthSession = { date: localISODate(), topic: '', duration: 2.0, instructors: '', description: '', attendance: {} };
                },
                async submitYouthSession() {
                    if (!this.newYouthSession.date || !this.newYouthSession.topic || this.newYouthSession.topic.trim() === '') {
                        await appAlert("Bitte Datum und Thema angeben!");
                        return;
                    }
                    const payload = {
                        date: this.newYouthSession.date,
                        topic: this.newYouthSession.topic,
                        duration: parseFloat(this.newYouthSession.duration) || 0,
                        instructors: this.newYouthSession.instructors,
                        description: this.newYouthSession.description,
                        attendance: this.newYouthSession.attendance
                    };
                    const url = this.editingYouthSessionId ? `/api/jugend/sessions/${this.editingYouthSessionId}` : '/api/jugend/sessions';
                    const method = this.editingYouthSessionId ? 'PUT' : 'POST';
                    const res = await fetch(url, {
                        method: method,
                        headers: {'Content-Type':'application/json'},
                        credentials: 'include',
                        body: JSON.stringify(payload)
                    });
                    if(res.ok) {
                        this.cancelYouthSessionEdit();
                        await this.loadJugendData();
                        await appAlert("Dienstbericht erfolgreich gespeichert!");
                    } else {
                        await appAlert("Fehler beim Speichern des Dienstberichts.");
                    }
                },
                async deleteYouthSession(id) {
                    if (await appConfirm("Dienstbericht wirklich löschen?")) {
                        const res = await fetch(`/api/jugend/sessions/${id}`, { method: 'DELETE', credentials: 'include' });
                        if(res.ok) await this.loadJugendData();
                    }
                },

                // Vereinsverwaltung Module (Moved to Material)
                async loadClubData() {
                    try {
                        const iRes = await fetch('/api/verein/inventory', { credentials: 'include' });
                        if(iRes.ok) this.clubInventory = await iRes.json();
                        if (this.canSeeFinance) {  // Spenden und Beiträge sehen nur Admin/Leitung (Server sperrt sonst mit 403)
                            const dRes = await fetch('/api/verein/donations', { credentials: 'include' });
                            if(dRes.ok) this.clubDonations = await dRes.json();
                        }
                    } catch(err) { console.error(err); }
                    if (this.canSeeFinance) await this.loadMembershipFees();
                },
                async loadMembershipFees() {
                    try {
                        const res = await fetch(`/api/verein/membership-fees?year=${this.feesYear}`, { credentials: 'include' });
                        this.membershipFees = res.ok ? await res.json() : [];
                    } catch(e) { this.membershipFees = []; }
                },
                async toggleMembershipFee(f, paid) {
                    const res = await fetch('/api/verein/membership-fees', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        credentials: 'include',
                        body: JSON.stringify({ personnel_id: f.personnel_id, year: this.feesYear, amount: f.amount || 0, paid: paid })
                    });
                    if (res.ok) await this.loadMembershipFees();
                    else await appAlert("Konnte nicht gespeichert werden.");
                },
                async submitClubItem() {
                    const res = await fetch('/api/verein/inventory', {
                        method: 'POST',
                        headers: {'Content-Type':'application/json'},
                        credentials: 'include',
                        body: JSON.stringify(this.newClubItem)
                    });
                    if(res.ok) {
                        this.newClubItem = { item_name: '', quantity: 1, status: 'OK' };
                        await this.loadClubData();
                    }
                },
                async deleteClubItem(id) {
                    if(await appConfirm("Vereins-Inventargegenstand löschen?")) {
                        const res = await fetch(`/api/verein/inventory/${id}`, { method: 'DELETE', credentials: 'include' });
                        if(res.ok) await this.loadClubData();
                    }
                },
                async submitDonation() {
                    const res = await fetch('/api/verein/donations', {
                        method: 'POST',
                        headers: {'Content-Type':'application/json'},
                        credentials: 'include',
                        body: JSON.stringify(this.newDonation)
                    });
                    if(res.ok) {
                        this.newDonation = { donor: '', amount: 15.00, date: localISODate() };
                        await this.loadClubData();
                    }
                },
                
                // First Responder / HvO Module
                async loadHvoData() {
                    try {
                        const [pRes, cRes] = await Promise.all([
                            fetch('/api/hvo/protocols', { credentials: 'include' }),
                            fetch('/api/hvo/checks', { credentials: 'include' })
                        ]);
                        if(pRes.ok) this.hvoProtocols = await pRes.json();
                        if(cRes.ok) this.hvoChecks = await cRes.json();
                    } catch(err) { console.error(err); }
                },
                async submitHvoProtocol() {
                    const res = await fetch('/api/hvo/protocols', {
                        method: 'POST',
                        headers: {'Content-Type':'application/json'},
                        credentials: 'include',
                        body: JSON.stringify(this.newHvoProtocol)
                    });
                    if(res.ok) {
                        this.newHvoProtocol = { date: localISODate(), symptoms: '', therapy: '', handover: '' };
                        await this.loadHvoData();
                    }
                },
                async submitHvoCheck() {
                    const res = await fetch('/api/hvo/checks', {
                        method: 'POST',
                        headers: {'Content-Type':'application/json'},
                        credentials: 'include',
                        body: JSON.stringify(this.newHvoCheck)
                    });
                    if(res.ok) {
                        this.newHvoCheck = { device_name: '' };
                        await this.loadHvoData();
                    }
                },
                async loadHvoMaterial() {
                    try {
                        const res = await fetch('/api/hvo/material', { credentials: 'include' });
                        this.hvoMaterial = res.ok ? await res.json() : [];
                    } catch(e) { this.hvoMaterial = []; }
                },
                async submitHvoMaterial() {
                    if (!this.newHvoMaterial.name.trim()) return;
                    const res = await fetch('/api/hvo/material', {
                        method: 'POST', headers: {'Content-Type':'application/json'}, credentials: 'include',
                        body: JSON.stringify(this.newHvoMaterial)
                    });
                    if (res.ok) {
                        this.newHvoMaterial = { name: '', quantity: 1, expiry_date: '', note: '' };
                        await this.loadHvoMaterial();
                    }
                },
                async deleteHvoMaterial(id) {
                    if (!await appConfirm("Eintrag löschen?")) return;
                    await fetch(`/api/hvo/material/${id}`, { method: 'DELETE', credentials: 'include' });
                    await this.loadHvoMaterial();
                },
            }
});

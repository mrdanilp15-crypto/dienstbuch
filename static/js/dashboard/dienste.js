// Dienste, Termine, Kalender und Gruppen.
// Teil der Dashboard-App (Vue-Mixin): Daten stehen in ../dashboard.js, "this" ist die ganze App.
window.DB_MIXINS = window.DB_MIXINS || [];
window.DB_MIXINS.push({
            computed: {
                detailRsvp() { return this.detailSchedule ? (this.scheduleRsvpMap[this.detailSchedule.id] || null) : null; },
                detailWhen() {
                    const s = this.detailSchedule; if (!s) return '';
                    const d = new Date(String(s.date).slice(0, 10) + 'T12:00:00');
                    return d.toLocaleDateString('de-DE', { weekday: 'long', day: '2-digit', month: 'long', year: 'numeric' }) + (s.time ? ' · ' + s.time + ' Uhr' : '');
                },
                detailInvited() {
                    const s = this.detailSchedule;
                    if (!s || s.audience !== 'selected' || !s.audience_ids) return [];
                    return (this.personnel || []).filter(p => s.audience_ids.includes(p.id)).map(p => p.name).sort((a, b) => a.localeCompare(b, 'de'));
                },
                audienceWithoutLogin() {
                    if (!this.audienceInfo) return [];
                    const chosen = new Set(this.newSchedule.audience_ids || []);
                    return (this.personnel || []).filter(p => chosen.has(p.id) && !this.audienceInfo.loginSet.has(p.id));
                },
                scheduleCandidates() {
                    const q = (this.scheduleMemberSearch || '').trim().toLowerCase();
                    return (this.personnel || [])
                        .filter(p => p.membership_status !== 'Ausgeschieden' && (!q || p.name.toLowerCase().includes(q)))
                        .sort((a, b) => a.name.localeCompare(b.name, 'de'));
                },
                yearOptions() {
                    const years = [];
                    for (let y = 2024; y <= new Date().getFullYear() + 1; y++) years.push(y);
                    return years;
                },

                calendarMonthLabel() {
                    const names = ['Januar','Februar','März','April','Mai','Juni','Juli','August','September','Oktober','November','Dezember'];
                    return `${names[this.calendarMonth - 1]} ${this.calendarYear}`;
                },
                calendarCells() {
                    const year = this.calendarYear, month = this.calendarMonth;
                    const eventsByDate = {};
                    for (const ev of this.calendarEvents) {
                        (eventsByDate[ev.date] = eventsByDate[ev.date] || []).push(ev);
                    }
                    const firstOfMonth = new Date(year, month - 1, 1);
                    const startOffset = (firstOfMonth.getDay() + 6) % 7; // Montag = 0
                    const daysInMonth = new Date(year, month, 0).getDate();
                    const daysInPrevMonth = new Date(year, month - 1, 0).getDate();
                    const todayStr = localISODate();
                    const cells = [];
                    const pad2 = n => String(n).padStart(2, '0');
                    for (let i = 0; i < startOffset; i++) {
                        const day = daysInPrevMonth - startOffset + i + 1;
                        const pk = `${month === 1 ? year - 1 : year}-${pad2(month === 1 ? 12 : month - 1)}-${pad2(day)}`;
                        cells.push({ key: pk, day, inMonth: false, isToday: pk === todayStr, events: eventsByDate[pk] || [] });
                    }
                    for (let day = 1; day <= daysInMonth; day++) {
                        const dateStr = `${year}-${String(month).padStart(2,'0')}-${String(day).padStart(2,'0')}`;
                        cells.push({ key: dateStr, day, inMonth: true, isToday: dateStr === todayStr, events: eventsByDate[dateStr] || [] });
                    }
                    let nextDay = 1;
                    while (cells.length % 7 !== 0) {
                        const nk = `${month === 12 ? year + 1 : year}-${pad2(month === 12 ? 1 : month + 1)}-${pad2(nextDay)}`;
                        cells.push({ key: nk, day: nextDay, inMonth: false, isToday: nk === todayStr, events: eventsByDate[nk] || [] });
                        nextDay++;
                    }
                    return cells;
                },
                sessions() {
                    if (!this.sessionsList) return [];
                    return this.sessionsList.filter(s => s && s.date && new Date(s.date).getFullYear() === this.selectedYear).sort((a, b) => {
                        if (!a || !a.date || !b || !b.date) return 0;
                        return new Date(b.date) - new Date(a.date);
                    });
                },
            },
            methods: {
                
                // Group management
                async loadGroups() {
                    try {
                        const res = await fetch('/groups', { credentials: 'include' }); this.groups = await res.json();
                        if (this.groups.length > 0) { const lastId = localStorage.getItem('lastSelectedGroupId'); this.selectGroup(this.groups.find(g => g.id == lastId) || this.groups[0]); }
                    } catch(e) {}
                },
                selectGroup(g) { this.selectedGroup = g; this.mobileSelectedId = g.id; localStorage.setItem('lastSelectedGroupId', g.id); this.loadData(); },
                
                async loadData() {
                    if (!this.selectedGroup) return;
                    const [res, sRes] = await Promise.all([
                        fetch(`/groups/${this.selectedGroup.id}/sessions`, { credentials: 'include' }), 
                        fetch(`/groups/${this.selectedGroup.id}/stats?year=${this.selectedYear}`, { credentials: 'include' })
                    ]);
                    this.sessionsList = await res.json(); this.ranking = (await sRes.json()).persons; this.calcStats();
                    this.fetchPersonalStats(); 
                },
                calcStats() {
                    const s = { Übung: 0, Einsatz: 0, Sonstiges: 0 };
                    this.sessions.forEach(x => { if (s[x.category] !== undefined) s[x.category] += parseFloat(x.duration); });
                    this.catStats = s;
                },
                
                // Schedules / Dienstplanung
                async loadSchedules() {
                    try {
                        const res = await fetch('/api/missions/schedules/list', { credentials: 'include' });
                        if(res.ok) this.schedules = await res.json();
                        await this.loadScheduleRsvps();
                    } catch(e) { console.error("Error loading schedules", e); }
                },
                async loadScheduleRsvps() {
                    // Eine einzige Sammelabfrage statt einer Anfrage pro Termin (bei Serien waren das Dutzende)
                    try {
                        const res = await fetch('/api/missions/schedules/rsvp-all', { credentials: 'include' });
                        this.scheduleRsvpMap = res.ok ? await res.json() : {};
                    } catch(e) { this.scheduleRsvpMap = {}; }
                },
                async submitScheduleRsvp(scheduleId, status) {
                    const res = await fetch(`/api/missions/schedules/${scheduleId}/rsvp`, {
                        method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'include',
                        body: JSON.stringify({ status })
                    });
                    if (res.ok) {
                        await this.loadScheduleRsvps();
                    } else {
                        const d = await res.json().catch(() => ({}));
                        await appAlert(d.detail || "Rückmeldung konnte nicht gespeichert werden.");
                    }
                },
                openRsvpListModal(schedule, rsvpData) {
                    this.activeRsvpSchedule = schedule;
                    this.activeRsvpResponses = rsvpData.responses || [];
                    new bootstrap.Modal(document.getElementById('rsvpListModal')).show();
                },
                async ensurePersonnelLoaded() {
                    if (!this.personnel || this.personnel.length === 0) await this.loadPersonnel();
                },
                addScheduleGroup(flag) {
                    const ids = new Set(this.newSchedule.audience_ids);
                    if (flag === 'none') { this.newSchedule.audience_ids = []; return; }
                    this.scheduleCandidates.filter(p => flag === 'all' || p[flag]).forEach(p => ids.add(p.id));
                    this.newSchedule.audience_ids = [...ids];
                },
                async openScheduleModal() {
                    await this.ensurePersonnelLoaded();
                    this.scheduleMemberSearch = '';
                    this.newSchedule = { title: '', date: localISODate(), time: '19:00', description: '', type: 'Übung', group_id: this.selectedGroup?.id, repeat: '', repeat_until: '', notify: true, audience: 'all', audience_ids: [] };
                    new bootstrap.Modal(document.getElementById('scheduleModal')).show();
                },
                async submitSchedule() {
                    const isEdit = !!this.newSchedule.id;
                    const url = isEdit ? `/api/missions/schedules/${this.newSchedule.id}` : '/api/missions/schedules';
                    const method = isEdit ? 'PUT' : 'POST';
                    const { title, date, time, description, type, group_id, repeat, repeat_until, notify, audience, audience_ids } = this.newSchedule;
                    if (audience === 'selected' && (!audience_ids || audience_ids.length === 0)) {
                        await appAlert('Bitte mindestens eine Person auswählen – oder oben „Alle aktiven Mitglieder“ wählen.');
                        return;
                    }
                    const aud = { audience, audience_ids: audience === 'selected' ? audience_ids : [] };
                    const payload = isEdit ? { title, date, time, description, type, group_id, ...aud }
                                           : { title, date, time, description, type, group_id, repeat: repeat || null, repeat_until: repeat ? repeat_until : null, notify, ...aud };
                    const res = await fetch(url, { method, headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify(payload) });
                    if(res.ok) {
                        const result = await res.json().catch(() => ({}));
                        bootstrap.Modal.getInstance(document.getElementById('scheduleModal')).hide();
                        if (result.created > 1) await appAlert(`${result.created} Termine wurden angelegt.`);
                        await this.loadSchedules();
                        if (this.activeTab === 'kalender') await this.loadCalendar();
                    } else {
                        const err = await res.json().catch(() => ({}));
                        await appAlert(typeof err.detail === 'string' ? err.detail : "Speichern fehlgeschlagen.");
                    }
                },
                async askDeleteSchedule(id) {
                    if (!await appConfirm("Termin wirklich löschen?")) return null;
                    const sch = (this.schedules || []).find(x => x.id === id) || this.newSchedule;
                    let series = false;
                    if (sch && sch.series_id) {
                        series = await appConfirm("Dieser Termin gehört zu einer Serie.\n\nAuch alle folgenden Termine der Serie löschen?\n(Abbrechen = nur diesen Termin)", 'Serie');
                    }
                    return { series };
                },
                async deleteSchedule(id) {
                    const choice = await this.askDeleteSchedule(id);
                    if (!choice) return;
                    await fetch(`/api/missions/schedules/${id}?series=${choice.series}`, { method: 'DELETE', credentials: 'include' });
                    await this.loadSchedules();
                },
                async openScheduleAttendance(s) {
                    this.activeScheduleForAttendance = s;
                    const [pRes, aRes] = await Promise.all([
                        fetch('/api/personnel/list', { credentials: 'include' }),
                        fetch(`/api/missions/schedules/${s.id}/attendance`, { credentials: 'include' })
                    ]);
                    const plist = await pRes.json();
                    const alist = await aRes.json();
                    this.schedulePersonnelList = plist.map(p => {
                        const match = alist.find(x => x.personnel_id === p.id);
                        return { personnel_id: p.id, name: p.name, status: match ? match.status : 'Nein' };
                    });
                    new bootstrap.Modal(document.getElementById('scheduleAttendanceModal')).show();
                },
                async saveScheduleAttendance() {
                    const res = await fetch(`/api/missions/schedules/${this.activeScheduleForAttendance.id}/attendance`, {
                        method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'include',
                        body: JSON.stringify(this.schedulePersonnelList)
                    });
                    if(res.ok) {
                        bootstrap.Modal.getInstance(document.getElementById('scheduleAttendanceModal')).hide();
                    }
                },
                scheduleOf(ev) { return ev && ev.schedule_id ? (this.schedules || []).find(s => s.id === ev.schedule_id) : null; },
                async openCalendarTerminDetail(ev, domEvent) {
                    if (domEvent) domEvent.stopPropagation();
                    let s = this.scheduleOf(ev);
                    if (!s) { await this.loadSchedules(); s = this.scheduleOf(ev); }
                    if (!s) { await appAlert('Dieser Termin ist nicht mehr vorhanden.'); return; }
                    await this.openScheduleDetail(s);
                },
                async loadAudienceInfo() {
                    try {
                        const res = await fetch('/api/missions/schedules/audience-info', { credentials: 'include' });
                        if (res.ok) { const d = await res.json(); this.audienceInfo = { loginSet: new Set(d.with_login), emailSet: new Set(d.with_email) }; }
                    } catch (e) {}
                },
                async openScheduleDetail(s) {
                    if (this.canManageDienste) await this.ensurePersonnelLoaded();
                    this.detailSchedule = s;
                    this.detailPending = [];
                    if (this.canManageDienste) {
                        // Einzelabfrage nur für Wehrführung/Gruppenführer: wer hat noch nicht geantwortet?
                        fetch(`/api/missions/schedules/${s.id}/rsvp`, { credentials: 'include' }).then(r => r.ok ? r.json() : null).then(d => { if (d && this.detailSchedule && this.detailSchedule.id === s.id) this.detailPending = d.pending || []; }).catch(() => {});
                    }
                    bootstrap.Modal.getOrCreateInstance(document.getElementById('scheduleDetailModal')).show();
                },
                _closeDetail() { bootstrap.Modal.getInstance(document.getElementById('scheduleDetailModal'))?.hide(); },
                editFromDetail() { const s = this.detailSchedule; if (!s) return; this._closeDetail(); this.editCalendarTermin({ schedule_id: s.id }); },
                attendanceFromDetail() { const s = this.detailSchedule; if (!s) return; this._closeDetail(); this.openScheduleAttendance(s); },
                captureFromDetail() {
                    const s = this.detailSchedule; if (!s) return;
                    const g = s.group_id || (this.selectedGroup && this.selectedGroup.id);
                    if (!g) { appAlert('Bitte zuerst im Dienstbetrieb eine Liste wählen.'); return; }
                    window.location.href = `/editor?group_id=${g}&from_schedule=${s.id}`;
                },
                exportAttendanceStatsCsv() {
                    // Semikolon als Trennzeichen UND Komma statt Punkt als Dezimaltrennzeichen -
                    // beides entspricht dem deutschen Excel-Gebietsschema. Nur eines von beidem
                    // anzupassen reicht nicht: mit Punkt-Dezimalzahlen erkennt ein deutsches
                    // Excel die Stundenwerte nicht als Zahl, sondern als Text (keine Summen-/
                    // Sortierfunktion, linksbündig statt rechtsbündig).
                    const num = n => String(n).replace('.', ',');
                    const rows = [['Name', 'Dienststunden', 'Einsatzstunden', 'Gesamtstunden']];
                    for (const p of this.statsAttendanceTop) {
                        rows.push([p.name, num(p.session_hours), num(p.mission_hours), num(p.total_hours)]);
                    }
                    const csv = rows.map(r => r.map(c => `"${String(c).replace(/"/g, '""')}"`).join(';')).join('\r\n');
                    const blob = new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8;' });
                    const url = URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = `Stundenstatistik_${this.statsYear}.csv`;
                    document.body.appendChild(a);
                    a.click();
                    document.body.removeChild(a);
                    URL.revokeObjectURL(url);
                },
                async loadCalendar() {
                    // Auch Vor- und Folgemonat laden: das Raster zeigt ja die Randtage dieser Monate mit an -
                    // vorher blieben Termine an diesen Tagen unsichtbar.
                    try {
                        const months = [-1, 0, 1].map(d => {
                            let m = this.calendarMonth + d, y = this.calendarYear;
                            if (m < 1) { m = 12; y--; } else if (m > 12) { m = 1; y++; }
                            return { y, m };
                        });
                        const parts = await Promise.all(months.map(({ y, m }) =>
                            fetch(`/api/calendar?year=${y}&month=${m}`, { credentials: 'include' }).then(r => r.ok ? r.json() : []).catch(() => [])));
                        this.calendarEvents = parts.flat();
                    } catch(e) { this.calendarEvents = []; }
                },
                openCalendarEvent(ev, domEvent) {
                    if (ev.type === 'termin') return this.openCalendarTerminDetail(ev, domEvent);
                    if (ev.type === 'helfer') { domEvent.stopPropagation(); this.activeTab = 'umfragen'; }
                },
                shiftCalendarMonth(delta) {
                    let m = this.calendarMonth + delta, y = this.calendarYear;
                    if (m < 1) { m = 12; y--; } else if (m > 12) { m = 1; y++; }
                    this.calendarMonth = m; this.calendarYear = y;
                    this.loadCalendar();
                },
                async openCalendarDayModal(dateStr) {
                    await this.ensurePersonnelLoaded();
                    this.scheduleMemberSearch = '';
                    this.newSchedule = { title: '', date: dateStr, time: '19:00', description: '', type: 'Übung', group_id: this.selectedGroup?.id || null, repeat: '', repeat_until: '', notify: true, audience: 'all', audience_ids: [] };
                    new bootstrap.Modal(document.getElementById('scheduleModal')).show();
                },
                async editCalendarTermin(ev, domEvent) {
                    if (domEvent) domEvent.stopPropagation();
                    const full = this.schedules.find(s => s.id === ev.schedule_id);
                    if (!full) return;
                    await this.ensurePersonnelLoaded();
                    this.scheduleMemberSearch = '';
                    this.newSchedule = { id: full.id, title: full.title, date: full.date, time: full.time || '19:00', description: full.description || '', type: full.type, group_id: full.group_id || null,
                                         series_id: full.series_id || null, audience: full.audience || 'all', audience_ids: [...(full.audience_ids || [])], notify: true };
                    new bootstrap.Modal(document.getElementById('scheduleModal')).show();
                },
                async deleteScheduleFromModal() {
                    if (!this.newSchedule.id) return;
                    const id = this.newSchedule.id;
                    const choice = await this.askDeleteSchedule(id);
                    if (!choice) return;
                    await fetch(`/api/missions/schedules/${id}?series=${choice.series}`, { method: 'DELETE', credentials: 'include' });
                    bootstrap.Modal.getInstance(document.getElementById('scheduleModal'))?.hide();
                    await this.loadSchedules();
                    if (this.activeTab === 'kalender') await this.loadCalendar();
                },
                
                // Group methods
                async addGroup() { const name = await appPrompt("Name der neuen Gruppe:"); if (name) { await fetch('/groups', { method: 'POST', headers: {'Content-Type':'application/json'}, credentials: 'include', body: JSON.stringify({name: name}) }); await this.loadGroups(); } },
                async editGroup(g) { const name = await appPrompt("Neuer Name:", g.name); if (name && name !== g.name) { await fetch(`/groups/${g.id}`, { method: 'PUT', headers: {'Content-Type':'application/json'}, credentials: 'include', body: JSON.stringify({name: name}) }); await this.loadGroups(); } },
                async deleteGroup(g) { if (await appConfirm(`Gruppe "${g.name}" wirklich löschen?`)) { await fetch(`/groups/${g.id}`, { method: 'DELETE', credentials: 'include' }); await this.loadGroups(); this.selectedGroup = null; if(this.groups.length > 0) this.selectGroup(this.groups[0]); } },
                async deleteSession(s) { 
                    if (s.is_mission || (typeof s.id === 'string' && s.id.startsWith('m_'))) {
                        const realId = s.real_mission_id || parseInt(String(s.id).replace('m_', ''));
                        if (await appConfirm("Einsatzbericht unwiderruflich löschen?")) {
                            const res = await fetch(`/api/missions/${realId}`, { method: 'DELETE', credentials: 'include' });
                            if (res.ok) {
                                await Promise.all([this.loadMissions(), this.loadData()]);
                            } else {
                                await appAlert("Fehler: Einsatzbericht konnte nicht gelöscht werden (Fehlende Rechte?).");
                            }
                        }
                    } else {
                        if (await appConfirm("Eintrag löschen?")) {
                            await fetch(`/sessions/${s.id}`, { method: 'DELETE', credentials: 'include' });
                            await this.loadData();
                        }
                    }
                },
                createNew() { if(!this.selectedGroup) return; window.location.href = `/editor?group_id=${this.selectedGroup.id}`; },
                loadSession(s) {
                    if (s.is_mission || (typeof s.id === 'string' && s.id.startsWith('m_'))) {
                        const realId = s.real_mission_id || parseInt(String(s.id).replace('m_', ''));
                        window.location.href = `/editor?mission_id=${realId}`;
                    } else {
                        window.location.href = `/editor?group_id=${this.selectedGroup.id}&session_id=${s.id}`;
                    }
                },
                async loadStats() {
                    // $nextTick allein reicht nicht zuverlässig: der Statistik-Tab wird per
                    // v-show ein-/ausgeblendet, und direkt im selben Frame gemessene
                    // Canvas-Maße können noch 0x0 sein (Chart.js zeichnet dann nichts,
                    // obwohl die Daten da sind - Diagramme wirkten komplett leer).
                    // requestAnimationFrame wartet zusätzlich auf den nächsten Paint-Zyklus.
                    await new Promise(resolve => this.$nextTick(() => requestAnimationFrame(resolve)));
                    applyChartTheme();
                    try {
                        const res = await fetch(`/api/admin/stats?year=${this.statsYear}`, { credentials: 'include' });
                        if (res.ok) {
                            const data = await res.json();
                            this.statsTotalMissions = data.total_missions;
                            this.statsPrevTotal = data.total_missions_prev;
                            
                                                        const ctx = document.getElementById('statsChart');
                            if (ctx) {
                                if (this.statsChartInstance) this.statsChartInstance.destroy();
                                this.statsChartInstance = new Chart(ctx, {
                                    type: 'bar',
                                    data: {
                                        labels: data.missions_by_day.map(d => d.day),
                                        datasets: [{
                                            label: 'Einsätze',
                                            data: data.missions_by_day.map(d => d.count),
                                            backgroundColor: 'rgba(220, 53, 69, 0.85)',
                                            borderColor: 'rgba(220, 53, 69, 1)',
                                            borderWidth: 1
                                        }]
                                    },
                                    options: { responsive: true, maintainAspectRatio: false, animation: false, scales: { y: { beginAtZero: true, ticks: { stepSize: 1 } } } }
                                });
                            }
                            
                            const ctxMonth = document.getElementById('statsChartMonth');
                            if (ctxMonth) {
                                if (this.statsChartMonthInstance) this.statsChartMonthInstance.destroy();
                                this.statsChartMonthInstance = new Chart(ctxMonth, {
                                    type: 'bar',
                                    data: {
                                        labels: data.missions_by_month.map(d => d.month),
                                        datasets: [{
                                            label: String(this.statsYear),
                                            data: data.missions_by_month.map(d => d.count),
                                            backgroundColor: 'rgba(13, 110, 253, 0.85)',
                                            borderColor: 'rgba(13, 110, 253, 1)',
                                            borderWidth: 1
                                        }, {
                                            label: String(this.statsYear - 1),
                                            data: data.missions_by_month.map(d => d.prev),
                                            backgroundColor: 'rgba(148, 163, 184, 0.6)',
                                            borderColor: 'rgba(148, 163, 184, 0.8)',
                                            borderWidth: 1
                                        }]
                                    },
                                    options: { responsive: true, maintainAspectRatio: false, animation: false, scales: { y: { beginAtZero: true, ticks: { stepSize: 1 } } } }
                                });
                            }

                            const ctxType = document.getElementById('statsChartType');
                            if (ctxType) {
                                if (this.statsTypeInstance) this.statsTypeInstance.destroy();
                                const typeColors = ['#dc3545', '#0d6efd', '#ffc107', '#10b981', '#a855f7', '#f97316', '#94a3b8'];
                                this.statsTypeInstance = new Chart(ctxType, {
                                    type: 'doughnut',
                                    data: {
                                        labels: data.missions_by_type.map(d => d.type),
                                        datasets: [{ data: data.missions_by_type.map(d => d.count), backgroundColor: typeColors, borderWidth: 0 }]
                                    },
                                    options: { responsive: true, maintainAspectRatio: false, animation: false, plugins: { legend: { position: 'bottom' } } }
                                });
                            }

                            const ctxHours = document.getElementById('statsChartHours');
                            if (ctxHours) {
                                if (this.statsHoursInstance) this.statsHoursInstance.destroy();
                                const catColors = { 'Übung': 'rgba(13, 110, 253, 0.6)', 'Einsatz': 'rgba(220, 53, 69, 0.6)', 'Sonstiges': 'rgba(255, 193, 7, 0.6)' };
                                this.statsHoursInstance = new Chart(ctxHours, {
                                    type: 'bar',
                                    data: {
                                        labels: data.missions_by_month.map(d => d.month),
                                        datasets: Object.entries(data.hours_by_category).map(([cat, vals]) => ({
                                            label: cat, data: vals, backgroundColor: catColors[cat] || 'rgba(148, 163, 184, 0.6)'
                                        }))
                                    },
                                    options: { responsive: true, maintainAspectRatio: false, animation: false, scales: { x: { stacked: true }, y: { stacked: true, beginAtZero: true } } }
                                });
                            }
                        }
                    } catch(err) {
                        console.error('Error loading stats:', err);
                    }

                    if (this.isAdmin || this.role === 'leitung') {
                        try {
                            const aRes = await fetch(`/api/admin/stats/attendance?year=${this.statsYear}`, { credentials: 'include' });
                            if (aRes.ok) {
                                this.statsAttendanceTop = await aRes.json();
                                const chartTop = this.statsAttendanceTop.slice(0, 10);
                                const ctxAtt = document.getElementById('statsChartAttendance');
                                if (ctxAtt) {
                                    if (this.statsAttendanceInstance) this.statsAttendanceInstance.destroy();
                                    this.statsAttendanceInstance = new Chart(ctxAtt, {
                                        type: 'bar',
                                        data: {
                                            labels: chartTop.map(p => p.name),
                                            datasets: [{
                                                label: 'Stunden ' + this.statsYear,
                                                data: chartTop.map(p => p.total_hours),
                                                backgroundColor: 'rgba(16, 185, 129, 0.85)',
                                                borderColor: 'rgba(16, 185, 129, 1)',
                                                borderWidth: 1
                                            }]
                                        },
                                        options: { indexAxis: 'y', responsive: true, maintainAspectRatio: false, animation: false, scales: { x: { beginAtZero: true } } }
                                    });
                                }
                            }
                        } catch(err) {
                            console.error('Error loading attendance stats:', err);
                        }
                        await this.loadParticipation();
                    }
                },
            }
});

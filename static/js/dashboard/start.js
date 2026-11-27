// Übersicht, Menü, Suche und alles, was mehrere Bereiche verbindet.
// Teil der Dashboard-App (Vue-Mixin): Daten stehen in ../dashboard.js, "this" ist die ganze App.
window.DB_MIXINS = window.DB_MIXINS || [];
window.DB_MIXINS.push({
            computed: {
                menuCatalog() {
                    return {
                        dienste: { label: 'Dienste', icon: 'fa-book', show: true },
                        kalender: { label: 'Kalender', icon: 'fa-calendar-days', show: true },
                        fristen: { label: 'Fristen', icon: 'fa-clock', show: true },
                        personal: { label: 'Personal', icon: 'fa-users', show: this.canSeePersonnel },
                        jugend: { label: 'Jugendfeuerwehr', icon: 'fa-graduation-cap', show: this.canSeeYouth },
                        lehrgaenge: { label: 'Lehrgänge', icon: 'fa-user-graduate', show: true },
                        umfragen: { label: 'Umfragen & Helfer', icon: 'fa-square-poll-vertical', show: true },
                        notizen: { label: 'Notizen', icon: 'fa-sticky-note', show: true },
                        stats: { label: 'Statistiken', icon: 'fa-chart-bar', show: this.canSeeStats },
                        apager: { label: 'Alarmierung', icon: 'fa-bell', show: true },
                        einsaetze: { label: 'Einsätze', icon: 'fa-fire', show: true },
                        lagekarte: { label: 'Lagekarte & Hydranten', icon: 'fa-map-location-dot', show: true },
                        bma: { label: 'Objekte & BMA', icon: 'fa-building-shield', show: true },
                        hvo: { label: 'First Responder', icon: 'fa-heart-pulse', show: true },
                        logbuch: { label: 'Logbücher', icon: 'fa-book-open', show: this.logbooks.length > 0 || this.canSeeSettings },
                        fahrzeuge: { label: 'Fahrzeuge', icon: 'fa-truck', show: true },
                        material: { label: 'Geräte & Material', icon: 'fa-screwdriver-wrench', show: true },
                        maengel: { label: 'Mängel', icon: 'fa-triangle-exclamation', show: true },
                        abrechnung: { label: 'Abrechnung', icon: 'fa-euro-sign', show: this.canSeeFinance },
                        arbeitsbescheinigung: { label: 'Arbeitsbescheinigungen', icon: 'fa-file-signature', show: this.canSeeFinance },
                        verein: { label: 'Vereinskasse & Inventar', icon: 'fa-building-columns', show: this.canSeeFinance },
                        archiv: { label: 'Dokumente & Archiv', icon: 'fa-folder-open', show: true },
                        verwaltung: { label: 'Einstellungen', icon: 'fa-gears', show: this.canSeeSettings }
                    };
                },
                mobileNavItems() {
                    const t = this.activeTab;
                    return [
                        { id: 'uebersicht', label: 'Start', icon: 'fa-gauge-high', active: t === 'uebersicht' },
                        { id: 'dienste', label: 'Dienste', icon: 'fa-book', active: t === 'dienste' },
                        { id: 'einsaetze', label: 'Einsätze', icon: 'fa-fire', active: t === 'einsaetze' },
                        { id: 'fristen', label: 'Fristen', icon: 'fa-clock', active: t === 'fristen', badge: this.dueBadge || 0 }
                    ];
                },
                menuFavoriteItems() {
                    const cat = this.menuCatalog;
                    return (this.menuFavorites || []).filter(id => cat[id] && cat[id].show).map(id => ({
                        id, ...cat[id],
                        active: id === 'maengel' ? (this.activeTab === 'material' && this.materialSubTab === 'maengel')
                              : id === 'material' ? (this.activeTab === 'material' && this.materialSubTab !== 'maengel') : this.activeTab === id
                    }));
                },
                overviewWidgetDefs() {
                    return {
                        mitteilungen: { title: 'Neueste Mitteilungen', icon: 'fa-comment-dots' },
                        berichte: { title: 'Einsatzberichte', icon: 'fa-file-lines' },
                        termine: { title: 'Termine', icon: 'fa-calendar-days' },
                        benachrichtigungen: { title: 'Benachrichtigungen', icon: 'fa-bell' },
                        fristen: { title: 'Fristen', icon: 'fa-clock' },
                        maengel: { title: 'Offene Mängel', icon: 'fa-triangle-exclamation' },
                        stunden: { title: 'Meine Stunden', icon: 'fa-stopwatch' },
                        rueckmeldung: { title: 'Meine Rückmeldungen', icon: 'fa-hand-point-up' },
                        naechsteTermine: { title: 'Nächste Termine', icon: 'fa-calendar-week' },
                        letzteEinsaetze: { title: 'Letzte Einsätze', icon: 'fa-fire' },
                        offeneBerichte: { title: 'Offene Berichte', icon: 'fa-file-circle-exclamation' },
                        fahrzeuge: { title: 'Fahrzeugstatus', icon: 'fa-truck' },
                        mannschaft: { title: 'Mannschaft', icon: 'fa-people-group' },
                        pinnwand: { title: 'Pinnwand', icon: 'fa-thumbtack' },
                        schnellzugriff: { title: 'Schnellzugriff', icon: 'fa-link' }
                    };
                },
                overviewWidgets() {
                    const defs = this.overviewWidgetDefs;
                    return this.overviewLayout.order.filter(id => defs[id] && !this.overviewLayout.hidden.includes(id)).map(id => ({ id, ...defs[id] }));
                },
                // Leere Karten rutschen nach hinten und bleiben klein (beim Bearbeiten bleibt die eigene Reihenfolge)
                overviewCards() {
                    const empty = {
                        mitteilungen: !this.activeBroadcasts.length, maengel: !(this.overviewDefects || []).length,
                        benachrichtigungen: !this.overviewNotifItems.length, pinnwand: !this.overviewPins.length,
                        termine: !this.overviewDayItems.length && !this.overviewUpcoming.length,
                        naechsteTermine: !this.overviewNextSchedules.length, letzteEinsaetze: !this.overviewLatestMissions.length
                    };
                    const list = this.overviewWidgets.map(w => ({ ...w, empty: !this.overviewEdit && this.overviewLoaded && !!empty[w.id] }));
                    return list.filter(w => !w.empty).concat(list.filter(w => w.empty));
                },
                fabItems() {
                    const items = [];
                    if (this.canManageMissions) items.push({ id: 'einsatz', label: 'Einsatz erfassen', icon: 'fa-fire' });
                    if (this.canManageDienste) {
                        if (this.selectedGroup) items.push({ id: 'dienst', label: 'Dienst erfassen', icon: 'fa-book' });
                        items.push({ id: 'termin', label: 'Termin planen', icon: 'fa-calendar-plus' });
                    }
                    items.push({ id: 'mangel', label: 'Mangel melden', icon: 'fa-triangle-exclamation' });
                    return items;
                },
                searchCommands() {
                    const q = (this.globalSearchQuery || '').trim().toLowerCase();
                    if (q.length < 2) return [];
                    const list = [];
                    const words = { dienste: 'dienstbetrieb übung anwesenheit', kalender: 'termine', lehrgaenge: 'lehrgang wunsch ausbildung kurs', umfragen: 'abstimmung helfersuche helfer eintragen', apager: 'alarm webhook apager', personal: 'kameraden mitglieder lehrgänge',
                        einsaetze: 'einsatzberichte', fahrzeuge: 'fahrtenbuch fahrzeugpool', material: 'geräte prüfung', maengel: 'defekt kaputt melden', notizen: 'notizbuch', verwaltung: 'einstellungen' };
                    Object.entries(this.menuCatalog).forEach(([id, m]) => { if (m.show) list.push({ id: 'tab:' + id, label: m.label, hint: 'Bereich', icon: m.icon, words: words[id] || '' }); });
                    const more = { mail: 'e-mail smtp versand', rechte: 'berechtigungen rollen freigabe', logins: 'benutzer konten passwort zurücksetzen', betrieb: 'sicherung backup', sicherheit: 'datenschutz zwei-faktor', wache: 'fristen logbücher standort' };
                    (this.settingsGroups || []).forEach(g => g.tabs.forEach(t => list.push({ id: 'set:' + t.key, label: t.label, hint: 'Einstellungen', icon: t.icon, words: more[t.key] || '' })));
                    list.push({ id: 'profil', label: 'Passwort & Profil', hint: 'dein Konto', icon: 'fa-user-gear', words: 'passwort ändern profil dienststunden zwei-faktor' });
                    list.push({ id: 'ausweis', label: 'Dienstausweis', hint: 'dein Konto', icon: 'fa-id-card', words: 'ausweis' });
                    return list.filter(c => (c.label + ' ' + c.words).toLowerCase().includes(q)).slice(0, 6);
                },
                overviewHiddenWidgets() {
                    const defs = this.overviewWidgetDefs;
                    return this.overviewLayout.hidden.filter(id => defs[id]).map(id => ({ id, ...defs[id] }));
                },
                overviewStats() {
                    const year = new Date().getFullYear();
                    const list = (this.missions || []).filter(m => m && m.date && String(m.date).slice(0, 4) === String(year));
                    const open = list.filter(m => !(m.leader_signature || m.status === 'Freigegeben')).length;
                    const by = { Brand: 0, Hilfeleistung: 0, Sonstige: 0 };
                    list.forEach(m => { by[this.kindOfStichwort(m.stichwort)]++; });
                    const latest = [...list].sort((a, b) => String(b.date + (b.time || '')).localeCompare(String(a.date + (a.time || '')))).slice(0, 3);
                    return { year, total: list.length, open, by, latest };
                },
                overviewUpcoming() {
                    const today = localISODate();
                    return (this.schedules || []).filter(s => String(s.date).slice(0, 10) >= today)
                        .sort((a, b) => (String(a.date).slice(0, 10) + (a.time || '')).localeCompare(String(b.date).slice(0, 10) + (b.time || '')));
                },
                overviewNextSchedules() { return this.overviewUpcoming.slice(0, 5); },
                overviewRsvpItems() {
                    const limit = localISODate(new Date(Date.now() + 21 * 86400000));
                    const mine = s => (this.scheduleRsvpMap[s.id] || {}).my_status || null;
                    return this.overviewUpcoming.filter(s => this.scheduleRsvpMap[s.id] && String(s.date).slice(0, 10) <= limit)
                        .sort((a, b) => (mine(a) ? 1 : 0) - (mine(b) ? 1 : 0)).slice(0, 4);
                },
                overviewLatestMissions() {
                    return [...(this.missions || [])].filter(m => m && m.date)
                        .sort((a, b) => String(b.date + (b.time || '')).localeCompare(String(a.date + (a.time || '')))).slice(0, 4);
                },
                overviewDrafts() {
                    const list = (this.missions || []).filter(m => m && !(m.leader_signature || m.status === 'Freigegeben'))
                        .sort((a, b) => String(b.date + (b.time || '')).localeCompare(String(a.date + (a.time || ''))));
                    return { count: list.length, top: list.slice(0, 4) };
                },
                overviewCrew() {
                    const act = (this.personnel || []).filter(p => p.membership_status === 'Aktiv');
                    return { active: act.length, agt: act.filter(p => p.is_agt).length, g26: act.filter(p => p.is_agt && p.g26_expired).length };
                },
                overviewPins() {
                    return (this.notes || []).filter(n => n.visibility === 'public').slice(0, 3);
                },
                safeQuickLinks() {
                    return (this.quickLinks || []).filter(l => l && l.label && this.safeLinkUrl(l.url));
                },
                overviewDayItems() {
                    return (this.schedules || []).filter(s => String(s.date).slice(0, 10) === this.overviewDay)
                        .sort((a, b) => String(a.time || '').localeCompare(String(b.time || '')));
                },
                overviewDayLabel() {
                    const d = new Date(this.overviewDay + 'T12:00:00');
                    const t = localISODate();
                    const lbl = d.toLocaleDateString('de-DE', { weekday: 'long', day: '2-digit', month: 'long', year: 'numeric' });
                    return this.overviewDay === t ? 'Heute, ' + lbl : lbl;
                },
                overviewNotifItems() {
                    const all = [];
                    const shown = new Set((this.activeBroadcasts || []).map(b => b.title));  // steht schon bei den Mitteilungen
                    (this.notifCats || []).forEach(c => (c.items || []).forEach(n => { if (!shown.has(n.title)) all.push({ ...n, catLabel: c.label, catIcon: c.icon }); }));
                    return all.sort((a, b) => (b.id || 0) - (a.id || 0)).slice(0, 4);
                },
                overviewDue() {
                    const items = (this.dueItems || []).filter(i => !i.first_check).slice().sort((a, b) => (a.days || 0) - (b.days || 0));
                    return { overdue: items.filter(i => i.overdue).length, soon: items.length, top: items.slice(0, 4) };
                },
                greeting() {
                    const h = new Date().getHours();
                    return h < 11 ? 'Guten Morgen' : h < 18 ? 'Hallo' : 'Guten Abend';
                },
                nextSchedule() {
                    const today = localISODate();
                    return [...(this.schedules || [])].filter(s => String(s.date).slice(0, 10) >= today && !s.done_session_id)
                        .sort((a, b) => String(a.date).localeCompare(String(b.date)) || String(a.time).localeCompare(String(b.time)))[0] || null;
                },
                globalSearchResultCount() {
                    if (!this.globalSearchResults) return 0;
                    const r = this.globalSearchResults;
                    return r.personnel.length + r.vehicles.length + r.equipment.length + r.documents.length + r.notes.length + r.missions.length + r.bmas.length;
                },
                todoItems() {
                    const items = [];
                    const add = (id, count, one, many, tone) => { if (count) items.push({ id, count, text: count === 1 ? one : many, tone }); };
                    const open = (this.missions || []).filter(m => m && !(m.leader_signature || m.status === 'Freigegeben'));
                    if (this.canApproveMissions) add('freigabe', open.filter(m => m.status === 'In Prüfung').length, 'Einsatzbericht wartet auf deine Freigabe', 'Einsatzberichte warten auf deine Freigabe', 'red');
                    if (this.canManageMissions) add('entwurf', open.filter(m => m.status === 'Entwurf').length, 'Einsatzbericht noch im Entwurf', 'Einsatzberichte noch im Entwurf', 'yellow');
                    const limit = localISODate(new Date(Date.now() + 21 * 86400000));
                    const rsvp = this.overviewUpcoming.filter(s => { const r = this.scheduleRsvpMap[s.id]; return r && !r.unlinked && !r.my_status && String(s.date).slice(0, 10) <= limit; });
                    add('rueckmeldung', rsvp.length, 'Termin ohne deine Rückmeldung', 'Termine ohne deine Rückmeldung', 'yellow');
                    add('fristen', this.overviewDue.overdue, 'überfällige Frist', 'überfällige Fristen', 'red');
                    if (this.systemAlert) add('system', this.systemAlert.count, 'Systemhinweis: ' + this.systemAlert.label, this.systemAlert.count + ' Systemhinweise, z. B. ' + this.systemAlert.label, this.systemAlert.tone);
                    add('ausfall', (this.vehicles || []).filter(v => v.status == 6).length, 'Fahrzeug außer Dienst', 'Fahrzeuge außer Dienst', 'red');
                    if (this.canManageMaterial) add('maengel', (this.overviewDefects || []).length, 'neuer Mangel', 'neue Mängel', 'red');
                    add('umfragen', this.polls.filter(p => p.open && !p.answered).length, 'Umfrage oder Helfersuche ohne deine Antwort', 'Umfragen oder Helfersuchen ohne deine Antwort', 'yellow');
                    if (this.feeOverview && this.feeOverview.overdue) add('beitraege', this.feeOverview.open_count, `Mitgliedsbeitrag ${this.feeOverview.year} offen`, `Mitgliedsbeiträge ${this.feeOverview.year} offen`, 'yellow');
                    if (this.wishData.manage) add('lehrgaenge', this.wishData.wishes.filter(w => w.status === 'Wunsch').length, 'offener Lehrgangswunsch', 'offene Lehrgangswünsche', 'yellow');
                    return items;
                },
            },
            methods: {
                incidentKey(m) { return (m.isNewMission ? 'new_' : 'old_') + m.id; },
                // Der naechste sinnvolle Schritt steht als Knopf da, alles andere im Menue
                incidentPrimary(m) {
                    const done = m.status === 'Freigegeben';
                    if (!m.isNewMission) return !done && this.canManageMissions ? 'sign' : '';
                    if (done) return 'pdf';
                    if (this.canApproveMissions) return 'approve';
                    if (this.canManageMissions && m.status === 'Entwurf') return 'submit';
                    return 'pdf';
                },
                incidentMenu(m) {
                    const primary = this.incidentPrimary(m);
                    const list = [];
                    if (m.isNewMission && primary !== 'pdf') list.push({ id: 'pdf', label: 'Als PDF', icon: 'fa-file-pdf' });
                    if (m.isNewMission && this.canApproveMissions && m.status !== 'Freigegeben') list.push({ id: 'sign', label: 'Mit Unterschrift freigeben', icon: 'fa-pen' });
                    if (!m.isNewMission && this.canManageMissions && primary !== 'sign') list.push({ id: 'sign', label: 'Unterschrift', icon: 'fa-pen' });
                    if (this.canDeleteMissions) list.push({ id: 'delete', label: 'Löschen', icon: 'fa-trash-alt' });
                    return list;
                },
                toggleIncidentMenu(m) { this.toggleAction(this.incidentKey(m)); },
                toggleAction(key) {
                    this.openIncident = this.openIncident === key ? null : key;
                    if (!this._menuCloser) {
                        this._menuCloser = (e) => { if (!e.target.closest || !e.target.closest('.action-menu')) this.openIncident = null; };
                        document.addEventListener('click', this._menuCloser, true);
                        document.addEventListener('keydown', (e) => { if (e.key === 'Escape') this.openIncident = null; });
                    }
                },
                runFab(id) {
                    if (id === 'einsatz') this.goToNewMission();
                    else if (id === 'dienst') this.createNew();
                    else if (id === 'termin') this.openScheduleModal();
                    else if (id === 'mangel') { this.goMenu('maengel'); this.loadEquipment(); this.loadDefectReports(); }
                },
                runSearchCommand(c) {
                    const [kind, key] = c.id.split(':');
                    this.globalSearchQuery = ''; this.globalSearchResults = null; this.globalSearchOpen = false; this.mobileSearchOpen = false;
                    if (kind === 'tab') this.goMenu(key);
                    else if (kind === 'set') { this.activeTab = 'verwaltung'; this.openSettingsTab(key); }
                    else if (kind === 'profil') this.openSelfPasswordModal();
                    else this.openIdCard();
                },
                // Nach dem Bearbeiten im Editor wieder an dieselbe Stelle: Filter, Liste und Scrollposition
                rememberPlace() {
                    try {
                        sessionStorage.setItem('dashboardPlace', JSON.stringify({
                            tab: this.activeTab, search: this.missionSearch, status: this.missionStatus, kind: this.missionKind, year: this.missionYear,
                            group: this.missionGroupFilter, open: this.missionFiltersOpen, list: this.selectedGroup ? this.selectedGroup.id : null,
                            y: window.scrollY, my: (document.querySelector('.main-content') || {}).scrollTop || 0
                        }));
                    } catch (e) { /* Speicher gesperrt: dann eben ohne */ }
                },
                restorePlace() {
                    let p = null;
                    try {
                        const back = (document.referrer || '').includes('/editor') || (performance.getEntriesByType('navigation')[0] || {}).type === 'back_forward';
                        p = back ? JSON.parse(sessionStorage.getItem('dashboardPlace') || 'null') : null;
                    } catch (e) { p = null; }
                    if (!p || p.tab !== this.activeTab) return;
                    this.missionSearch = p.search || ''; this.missionStatus = p.status || 'alle'; this.missionKind = p.kind || 'alle';
                    this.missionYear = p.year || 'all'; this.missionGroupFilter = p.group || 'all'; this.missionFiltersOpen = !!p.open;
                    const g = (this.groups || []).find(x => x.id === p.list);
                    if (g && (!this.selectedGroup || this.selectedGroup.id !== g.id)) this.selectGroup(g);
                    setTimeout(() => {
                        window.scrollTo(0, p.y || 0);
                        const el = document.querySelector('.main-content');
                        if (el) el.scrollTop = p.my || 0;
                    }, 700);
                },
                tabAllowed(tab) {
                    if (!this.role) return true; // Rolle noch nicht geladen
                    if (['abrechnung', 'arbeitsbescheinigung', 'verein'].includes(tab)) return this.canSeeFinance;
                    if (tab === 'stats') return this.canSeeStats;
                    if (tab === 'personal') return this.canSeePersonnel;
                    if (tab === 'jugend') return this.canSeeYouth;
                    if (tab === 'logbuch') return !this.logbooksLoaded || this.logbooks.length > 0 || this.canSeeSettings;
                    if (tab === 'verwaltung') return this.canSeeSettings;
                    return true;
                },
                // Rechnungen haben kein Fälligkeitsdatum: als überfällig gilt, was seit über 30 Tagen unbezahlt ist
                billOverdue(b) {
                    if (b.paid_at || !b.sent_at) return false;
                    const t = new Date(String(b.sent_at).replace(' ', 'T'));
                    return !isNaN(t) && (Date.now() - t.getTime()) > 30 * 86400000;
                },
                // Menü-Gruppen: einklappbar, Wahl bleibt gespeichert; die Gruppe mit dem geöffneten Reiter ist immer offen
                menuSectionOfTab(tab) {
                    const groups = {
                        org: ['dienste', 'kalender', 'fristen', 'personal', 'jugend', 'lehrgaenge', 'umfragen', 'notizen', 'stats'],
                        einsatz: ['apager', 'einsaetze', 'lagekarte', 'bma', 'hvo', 'logbuch'],
                        material: ['fahrzeuge', 'material', 'abrechnung', 'arbeitsbescheinigung', 'verein', 'archiv']
                    };
                    return Object.keys(groups).find(k => groups[k].includes(tab)) || null;
                },
                menuOpen(key) { return !this.menuCollapsed[key]; },
                isFavorite(id) { return (this.menuFavorites || []).includes(id); },
                async toggleFavorite(id) {
                    const cur = this.menuFavorites || [];
                    this.menuFavorites = cur.includes(id) ? cur.filter(x => x !== id) : [...cur, id].slice(0, 8);
                    try { await fetch('/api/users/me/prefs/menu_favorites', { method: 'PUT', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify({ value: this.menuFavorites }) }); } catch (e) { console.error(e); }
                },
                async loadFavorites() {
                    try {
                        const res = await fetch('/api/users/me/prefs/menu_favorites', { credentials: 'include' });
                        const v = res.ok ? (await res.json()).value : null;
                        if (Array.isArray(v)) this.menuFavorites = v.filter(x => typeof x === 'string').slice(0, 8);
                    } catch (e) { console.error(e); }
                },
                goMenu(id) {
                    if (id === 'maengel') { this.activeTab = 'material'; this.materialSubTab = 'maengel'; this.loadDefectReports(); }
                    else { this.activeTab = id; if (id === 'material' && this.materialSubTab === 'maengel') this.materialSubTab = 'geraete'; }
                    this.showMobileSidebar = false;
                },
                saveMenuState() { try { localStorage.setItem('menuCollapsed', JSON.stringify(this.menuCollapsed)); } catch (e) {} },
                toggleMenuSection(key) { this.menuCollapsed = { ...this.menuCollapsed, [key]: !this.menuCollapsed[key] }; this.saveMenuState(); },
                // Wer einen Reiter öffnet (Menü, Suche, Link), sieht dessen Gruppe wieder aufgeklappt
                expandMenuFor(tab) {
                    const k = this.menuSectionOfTab(tab);
                    if (k && this.menuCollapsed[k]) { this.menuCollapsed = { ...this.menuCollapsed, [k]: false }; this.saveMenuState(); }
                },
                kindOfStichwort(s) { const t = String(s || '').trim(); return /^b/i.test(t) ? 'Brand' : (/^(h|th|t)/i.test(t) ? 'Hilfeleistung' : 'Sonstige'); },
                missionYearOptions() {
                    const ys = new Set((this.missions || []).map(m => String(m.date || '').slice(0, 4)).filter(y => /^\d{4}$/.test(y)));
                    ys.add(String(new Date().getFullYear()));
                    return [...ys].sort().reverse();
                },
                // Gespeicherte Filter ("Filter speichern"): pro Konto, je Liste
                currentFilterOf(listKey) {
                    if (listKey === 'einsaetze') return { search: this.missionSearch, status: this.missionStatus, kind: this.missionKind, year: this.missionYear, group: this.missionGroupFilter };
                    return { search: this.eqSearch, category: this.eqCategory, defective: this.filterDefectiveOnly, archived: this.showArchivedEquipment };
                },
                applySavedFilter(listKey, f) {
                    const v = f.filter || {};
                    if (listKey === 'einsaetze') {
                        this.missionSearch = v.search || ''; this.missionStatus = v.status || 'alle'; this.missionKind = v.kind || 'alle';
                        this.missionYear = v.year || 'all'; this.missionGroupFilter = v.group || 'all';
                    } else {
                        this.eqSearch = v.search || ''; this.eqCategory = v.category || ''; this.filterDefectiveOnly = !!v.defective;
                        const arch = !!v.archived;
                        if (arch !== this.showArchivedEquipment) { this.showArchivedEquipment = arch; this.loadEquipment(); }
                    }
                },
                filterIsActive(listKey) {
                    const v = this.currentFilterOf(listKey);
                    return listKey === 'einsaetze'
                        ? !!(v.search || v.status !== 'alle' || v.kind !== 'alle' || v.year !== 'all' || v.group !== 'all')
                        : !!(v.search || v.category || v.defective || v.archived);
                },
                resetFilter(listKey) {
                    this.applySavedFilter(listKey, { filter: {} });
                },
                async loadSavedFilters() {
                    if (this._filtersLoaded) return;
                    this._filtersLoaded = true;
                    try {
                        const res = await fetch('/api/users/me/prefs/saved_filters', { credentials: 'include' });
                        const v = res.ok ? (await res.json()).value : null;
                        if (v && typeof v === 'object') this.savedFilters = { einsaetze: Array.isArray(v.einsaetze) ? v.einsaetze : [], material: Array.isArray(v.material) ? v.material : [] };
                    } catch (e) { console.error(e); }
                },
                async persistSavedFilters() {
                    try {
                        await fetch('/api/users/me/prefs/saved_filters', { method: 'PUT', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify({ value: this.savedFilters }) });
                    } catch (e) { console.error(e); }
                },
                async saveCurrentFilter(listKey) {
                    if (!this.filterIsActive(listKey)) { await appAlert('Stelle zuerst einen Filter ein (Suche, Status, Art, Jahr …), dann kannst du ihn speichern.'); return; }
                    const name = ((await appPrompt('Name für diesen Filter:', '', 'Filter speichern')) || '').trim();
                    if (!name) return;
                    const list = (this.savedFilters[listKey] || []).filter(f => f.name !== name);
                    list.push({ name: name.slice(0, 40), filter: this.currentFilterOf(listKey) });
                    this.savedFilters = { ...this.savedFilters, [listKey]: list.slice(0, 12) };
                    await this.persistSavedFilters();
                },
                async deleteSavedFilter(listKey, f) {
                    this.savedFilters = { ...this.savedFilters, [listKey]: (this.savedFilters[listKey] || []).filter(x => x.name !== f.name) };
                    await this.persistSavedFilters();
                },
                safeLinkUrl(u) {
                    const t = String(u || '').trim();
                    return /^https?:\/\//i.test(t) || /^\/(?!\/)/.test(t) ? t : '';
                },
                async loadQuickLinks() {
                    try {
                        const res = await fetch('/api/users/me/prefs/quick_links', { credentials: 'include' });
                        const v = res.ok ? (await res.json()).value : null;
                        if (Array.isArray(v)) this.quickLinks = v.filter(l => l && typeof l.label === 'string' && typeof l.url === 'string').slice(0, 12);
                    } catch (e) { console.error(e); }
                },
                async persistQuickLinks() {
                    try { await fetch('/api/users/me/prefs/quick_links', { method: 'PUT', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify({ value: this.quickLinks }) }); } catch (e) { console.error(e); }
                },
                async addQuickLink() {
                    const label = (this.quickLinkForm.label || '').trim().slice(0, 30);
                    let url = (this.quickLinkForm.url || '').trim();
                    if (!label || !url) { await appAlert('Bitte Name und Adresse eintragen.'); return; }
                    if (!/^(https?:\/\/|\/)/i.test(url)) url = 'https://' + url;
                    if (!this.safeLinkUrl(url)) { await appAlert('Diese Adresse ist nicht erlaubt. Erlaubt sind Internetadressen (https://…) und Seiten dieser App (/dashboard …).'); return; }
                    if (this.quickLinks.length >= 12) { await appAlert('Mehr als 12 Links sind nicht möglich.'); return; }
                    this.quickLinks = [...this.quickLinks, { label, url }];
                    this.quickLinkForm = { label: '', url: '' };
                    await this.persistQuickLinks();
                },
                async removeQuickLink(i) {
                    this.quickLinks = this.quickLinks.filter((_, ix) => ix !== i);
                    await this.persistQuickLinks();
                },
                // Daten für sichtbare Karten laden (nur was wirklich gebraucht wird)
                ensureOverviewData() {
                    const vis = new Set(this.overviewWidgets.map(w => w.id));
                    if (vis.has('stunden')) this.fetchPersonalStats();
                    if (vis.has('fahrzeuge')) this.loadVehicles();
                    if (vis.has('mannschaft') && this.canSeePersonnel) this.loadPersonnel();
                    if (vis.has('pinnwand')) this.loadNotes();
                    if (vis.has('schnellzugriff')) this.loadQuickLinks();
                },
                async loadSystemAlert() {
                    if (!this.isAdmin) { this.systemAlert = null; return; }
                    try {
                        const res = await fetch('/api/admin/health', { credentials: 'include' });
                        const h = res.ok ? await res.json() : null;
                        const bad = h ? h.checks.filter(c => c.status === 'error' || c.status === 'warn') : [];
                        bad.sort((a, b) => (a.status === 'error' ? 0 : 1) - (b.status === 'error' ? 0 : 1));
                        this.systemAlert = bad.length ? { count: bad.length, label: bad[0].label, tone: bad[0].status === 'error' ? 'red' : 'yellow' } : null;
                    } catch (e) { this.systemAlert = null; }
                },
                async loadOverview() {
                    this.loadOverviewLayout();
                    try {
                        await Promise.all([
                            this.loadMissions(), this.loadSchedules(), this.loadNotifs(), this.loadBroadcasts(),
                            this.loadDueItems(), this.loadFeeOverview(), this.loadSystemAlert(),
                            fetch('/api/material/defect-reports?status=Offen', { credentials: 'include' })
                                .then(r => r.ok ? r.json() : []).then(d => { this.overviewDefects = d; }).catch(() => {})
                        ]);
                    } catch (e) { console.error('Übersicht laden:', e); }
                    this.overviewLoaded = true;
                    this.ensureOverviewData();
                    this.$nextTick(() => this.renderOverviewChart());
                },
                async loadOverviewLayout() {
                    if (this._layoutLoaded) return;
                    this._layoutLoaded = true;
                    try {
                        const res = await fetch('/api/users/me/prefs/dashboard_layout', { credentials: 'include' });
                        const v = res.ok ? (await res.json()).value : null;
                        if (v && Array.isArray(v.order)) {
                            const known = Object.keys(this.overviewWidgetDefs);
                            const order = v.order.filter(id => known.includes(id));
                            const hidden = (v.hidden || []).filter(id => known.includes(id));
                            known.forEach(id => { if (!order.includes(id)) { order.push(id); hidden.push(id); } });
                            this.overviewLayout = { order, hidden };
                            this.ensureOverviewData();
                            this.$nextTick(() => this.renderOverviewChart());
                        }
                    } catch (e) { console.error(e); }
                },
                async saveOverviewLayout() {
                    try {
                        await fetch('/api/users/me/prefs/dashboard_layout', { method: 'PUT', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify({ value: this.overviewLayout }) });
                    } catch (e) { console.error(e); }
                    this.$nextTick(() => this.renderOverviewChart());
                },
                moveOverviewWidget(id, dir) {
                    const vis = this.overviewWidgets.map(w => w.id);
                    const i = vis.indexOf(id), j = i + dir;
                    if (i < 0 || j < 0 || j >= vis.length) return;
                    const order = this.overviewLayout.order.slice();
                    const a = order.indexOf(vis[i]), b = order.indexOf(vis[j]);
                    [order[a], order[b]] = [order[b], order[a]];
                    this.overviewLayout = { ...this.overviewLayout, order };
                    this.saveOverviewLayout();
                },
                hideOverviewWidget(id) {
                    this.overviewLayout = { ...this.overviewLayout, hidden: [...this.overviewLayout.hidden, id] };
                    this.saveOverviewLayout();
                },
                showOverviewWidget(id) {
                    this.overviewLayout = { ...this.overviewLayout, hidden: this.overviewLayout.hidden.filter(x => x !== id) };
                    this.ensureOverviewData();
                    this.saveOverviewLayout();
                },
                shiftOverviewDay(n) {
                    if (n === 0) { this.overviewDay = localISODate(); return; }
                    const d = new Date(this.overviewDay + 'T12:00:00'); d.setDate(d.getDate() + n);
                    this.overviewDay = localISODate(d);
                },
                renderOverviewChart() {
                    const el = document.getElementById('overviewChart');
                    if (this._overviewChart) { this._overviewChart.destroy(); this._overviewChart = null; }
                    if (!el || typeof Chart === 'undefined') return;
                    const s = this.overviewStats;
                    if (!s.total) return;
                    this._overviewChart = new Chart(el, {
                        type: 'doughnut',
                        data: { labels: ['Brand', 'Hilfeleistung', 'Sonstige'], datasets: [{ data: [s.by.Brand, s.by.Hilfeleistung, s.by.Sonstige], backgroundColor: ['#dc3545', '#0d6efd', '#6c757d'], borderWidth: 0 }] },
                        options: { cutout: '62%', plugins: { legend: { position: 'bottom' } }, maintainAspectRatio: false }
                    });
                },
                async openOverviewSchedule(s) {
                    this.activeTab = 'dienste';
                    await this.openScheduleDetail(s);
                },
                toggleMobileSearch() {
                    this.mobileSearchOpen = !this.mobileSearchOpen;
                    if (this.mobileSearchOpen) { this.globalSearchOpen = true; this.$nextTick(() => { const i = document.querySelector('.global-search input'); if (i) i.focus(); }); }
                    else { this.globalSearchQuery = ''; this.globalSearchResults = null; }
                },
                onGlobalSearchInput() {
                    this.globalSearchOpen = true;
                    clearTimeout(globalSearchTimer);
                    if (this.globalSearchQuery.trim().length < 2) {
                        this.globalSearchResults = null;
                        return;
                    }
                    globalSearchTimer = setTimeout(async () => {
                        try {
                            const res = await fetch(`/api/search?q=${encodeURIComponent(this.globalSearchQuery.trim())}`, { credentials: 'include' });
                            this.globalSearchResults = res.ok ? await res.json() : null;
                        } catch(e) { this.globalSearchResults = null; }
                    }, 300);
                },
                onGlobalSearchBlur() {
                    setTimeout(() => { this.globalSearchOpen = false; }, 200);
                },
                async goToSearchPersonnel(item) {
                    this.globalSearchOpen = false; this.globalSearchQuery = '';
                    this.activeTab = 'personal'; this.showMobileSidebar = false;
                    await this.editPersonnel({ id: item.id });
                },
                goToSearchVehicle(item) {
                    this.globalSearchOpen = false; this.globalSearchQuery = '';
                    this.activeTab = 'fahrzeuge'; this.showMobileSidebar = false;
                    const full = this.vehicles.find(v => v.id === item.id);
                    if (full) this.openVehicleFormModal(full);
                },
                goToSearchEquipment(item) {
                    this.globalSearchOpen = false; this.globalSearchQuery = '';
                    this.activeTab = 'material'; this.materialSubTab = 'geraete'; this.showMobileSidebar = false;
                    this.eqSearch = item.name;
                    this.loadEquipment();
                },
                goToSearchDocument(item) {
                    this.globalSearchOpen = false; this.globalSearchQuery = '';
                    window.open(item.url, '_blank');
                },
                goToSearchNote(item) {
                    this.globalSearchOpen = false; this.globalSearchQuery = '';
                    this.activeTab = 'notizen'; this.notesActiveFilter = 'all'; this.showMobileSidebar = false;
                    this.loadNotes();
                },
                goToSearchMission(item) {
                    this.globalSearchOpen = false; this.globalSearchQuery = '';
                    window.location.href = `/editor?mission_id=${item.id}`;
                },
                goToSearchBma(item) {
                    this.globalSearchOpen = false; this.globalSearchQuery = '';
                    this.activeTab = 'bma'; this.showMobileSidebar = false;
                    const full = this.bmas.find(b => b.id === item.id);
                    if (full) this.startBmaEdit(full);
                },
            }
});

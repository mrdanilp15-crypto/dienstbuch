// Einsätze, Freigabe und Unterschriften.
// Teil der Dashboard-App (Vue-Mixin): Daten stehen in ../dashboard.js, "this" ist die ganze App.
window.DB_MIXINS = window.DB_MIXINS || [];
window.DB_MIXINS.push({
            computed: {
                // Alte Einsätze (vor dem Einsatz-Editor als Logbuch-Eintrag "Einsatz" erfasst), aus allen Gruppen
                legacyIncidents() {
                    return (this.legacyMissions || []).map(s => ({
                        id: 'alt_' + s.id,
                        isNewMission: false,
                        isLegacy: true,
                        date: s.date,
                        time: s.time || '00:00',
                        stichwort: s.description || 'Einsatz',
                        adresse: s.instructors || 'Siehe Bericht',
                        meldung: s.description || '',
                        duration: s.duration,
                        status: s.signed ? 'Freigegeben' : 'Entwurf',
                        rawSession: { id: s.id, group_id: s.group_id },
                        group_id: s.group_id
                    }));
                },
                // Neue Einsatzberichte und alte Einsätze zusammen, ohne Filter (Übersicht)
                allMissionEntries() {
                    return [...(this.missions || []).map(m => ({ ...m, isNewMission: true })), ...this.legacyIncidents];
                },
                allIncidents() {
                    let list = this.missions.map(m => {
                        const isSigned = !!(m.leader_signature || m.status === 'Freigegeben');
                        return {
                            id: m.id,
                            isNewMission: true,
                            date: m.date,
                            time: m.time,
                            stichwort: m.stichwort,
                            adresse: m.adresse,
                            meldung: m.meldung,
                            duration: m.duration,
                            status: isSigned ? 'Freigegeben' : (m.status === 'In Prüfung' ? 'In Prüfung' : 'Entwurf'),
                            leader_signature: m.leader_signature,
                            approved_by: m.approved_by,
                            group_id: m.group_id
                        };
                    });
                    list.push(...this.legacyIncidents);
                    if (this.missionGroupFilter !== 'all') {
                        list = list.filter(m => m.group_id == this.missionGroupFilter || !m.group_id);
                    }
                    const q = (this.missionSearch || '').trim().toLowerCase();
                    if (q) list = list.filter(m => [m.stichwort, m.adresse, m.meldung].some(t => String(t || '').toLowerCase().includes(q)));
                    if (this.missionStatus !== 'alle') list = list.filter(m => m.status === this.missionStatus);
                    if (this.missionKind !== 'alle') list = list.filter(m => this.kindOfStichwort(m.stichwort) === this.missionKind);
                    if (this.missionYear !== 'all') list = list.filter(m => String(m.date || '').slice(0, 4) === String(this.missionYear));
                    return list.sort((a, b) => new Date(b.date) - new Date(a.date));
                },
                availableMissionsForBilling() {
                    let list = [];
                    const pushMission = (id, stichwort, date, adresse, status) => {
                        if (!id) return;
                        const cleanId = typeof id === 'string' && id.startsWith('m_') ? parseInt(id.replace('m_', '')) : parseInt(id);
                        if (isNaN(cleanId)) return;
                        const exists = list.find(x => x.id === cleanId);
                        if (!exists) {
                            list.push({
                                id: cleanId,
                                stichwort: stichwort || 'Einsatz',
                                date: date || '',
                                adresse: adresse || 'Siehe Bericht',
                                status: status === 'Freigegeben' ? 'Freigegeben' : 'Entwurf'
                            });
                        }
                    };

                    if (this.missions && this.missions.length > 0) {
                        this.missions.forEach(m => {
                            const isSigned = !!(m.leader_signature || m.status === 'Freigegeben');
                            pushMission(m.id, m.stichwort, m.date, m.adresse, isSigned ? 'Freigegeben' : 'Entwurf');
                        });
                    }

                    if (this.allIncidents && this.allIncidents.length > 0) {
                        this.allIncidents.forEach(inc => {
                            const realId = inc.real_mission_id || inc.id;
                            pushMission(realId, inc.stichwort, inc.date, inc.adresse, inc.status);
                        });
                    }
                    return list;
                },
                missionFilterCount() { return [this.missionStatus !== 'alle', this.missionKind !== 'alle', this.missionYear !== 'all'].filter(Boolean).length; },
            },
            methods: {

                handleIncidentClick(m) {
                    if (m.isNewMission) {
                        window.location.href = `/editor?mission_id=${m.id}`;
                    } else {
                        this.loadSession(m.rawSession);
                    }
                },
                // Missions
                async loadMissions() {
                    try {
                        const [res, oldRes] = await Promise.all([
                            fetch('/api/missions', { credentials: 'include' }),
                            fetch('/api/missions/legacy', { credentials: 'include' })
                        ]);
                        if(res.ok) this.missions = await res.json();
                        if(oldRes.ok) this.legacyMissions = await oldRes.json();
                    } catch(e) { console.error("Error loading missions", e); }
                },
                goToNewMission() {
                    // Einsätze werden seit der Editor-Vereinheitlichung nur noch im gemeinsamen
                    // Editor (static/editor.html, Route /editor) angelegt/bearbeitet - kein
                    // eigenständiges missionModal mehr, siehe [[editor-vereinheitlichung]].
                    const groupId = (this.missionGroupFilter !== 'all' ? parseInt(this.missionGroupFilter) : (this.selectedGroup ? this.selectedGroup.id : null));
                    window.location.href = `/editor?new_type=Einsatz${groupId ? '&group_id=' + groupId : ''}`;
                },
                async deleteMission(id) {
                    if(await appConfirm("Einsatzbericht unwiderruflich löschen?")) {
                        const res = await fetch(`/api/missions/${id}`, { method: 'DELETE', credentials: 'include' });
                        if (!res.ok) {
                            const err = await res.json().catch(() => ({}));
                            await appAlert("Löschen fehlgeschlagen: " + (err.detail || "Unbekannter Fehler."));
                            return;
                        }
                        await Promise.all([this.loadMissions(), this.loadData()]);
                    }
                },
                async releaseMission(m, action) {
                    const approve = action === 'approve';
                    const text = approve ? `Bericht "${m.stichwort}" freigeben? Danach kann ihn nur noch ein Admin ändern.` : `Bericht "${m.stichwort}" zur Freigabe einreichen?`;
                    if (!await appConfirm(text)) return;
                    const res = await fetch(approve ? `/api/missions/${m.id}/approve` : `/api/missions/${m.id}/submit`, { method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify({ approve: true }) });
                    if (!res.ok) {
                        const err = await res.json().catch(() => ({}));
                        await appAlert((approve ? 'Freigabe' : 'Einreichen') + ' fehlgeschlagen: ' + (err.detail || 'Unbekannter Fehler.'));
                    }
                    await this.loadMissions();
                    await this.loadData();
                },
                goTodo(id) {
                    if (id === 'freigabe' || id === 'entwurf') {
                        this.resetFilter('einsaetze');
                        this.missionStatus = id === 'freigabe' ? 'In Prüfung' : 'Entwurf';
                        this.activeTab = 'einsaetze';
                    }
                    else if (id === 'rueckmeldung') this.activeTab = 'kalender';
                    else if (id === 'fristen') this.activeTab = 'fristen';
                    else if (id === 'ausfall') this.activeTab = 'fahrzeuge';
                    else if (id === 'system') { this.activeTab = 'verwaltung'; this.openSettingsTab('betrieb'); }
                    else if (id === 'beitraege') { this.feesYear = this.feeOverview.year; this.feeFilter = 'offen'; this.activeTab = 'verein'; }
                    else if (id === 'umfragen' || id === 'lehrgaenge') this.activeTab = id;
                    else if (id === 'maengel') { this.activeTab = 'material'; this.materialSubTab = 'maengel'; }
                },
                incidentAction(m, action) {
                    this.openIncident = null;
                    if (action === 'pdf') this.downloadMissionPdf(m);
                    else if (action === 'sign') { if (m.isNewMission) this.openMissionSigModal(m); else this.openSig(m.rawSession); }
                    else if (m.isNewMission) this.deleteMission(m.id);
                    else this.deleteSession(m.rawSession);
                },
                openMissionSigModal(m) {
                    this.activeS = m;
                    missionSigModal.show();
                    setTimeout(() => {
                        initResponsiveCanvas('missionSigCanvas', missionSigPad, m.leader_signature);
                    }, 250);
                },
                async saveMissionSig() {
                    if(!missionSigPad || missionSigPad.isEmpty()) return await appAlert("Bitte unterschreiben!");
                    const sigData = trimSignatureDataUrl(missionSigPad);
                    let url = `/api/missions/${this.activeS.id}/signature`;
                    if (this.activeS.isNewMission === false) {
                        url = `/sessions/${this.activeS.id}/leader_signature`;
                    }
                    const res = await fetch(url, {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        credentials: 'include',
                        body: JSON.stringify({signature: sigData})
                    });
                    if(res.ok) {
                        missionSigModal.hide();
                        await this.loadMissions();
                        await this.loadData();
                    } else {
                        const err = await res.json().catch(() => ({}));
                        await appAlert("Freigabe fehlgeschlagen: " + (err.detail || "Unbekannter Fehler."));
                    }
                },
                clearMissionSig() { if(missionSigPad) missionSigPad.clear(); },
                async downloadMissionPdf(m) {
                    if (m.isNewMission) {
                        window.open('/api/missions/' + m.id + '/pdf', '_blank');
                    } else if (typeof m.id === 'string' && String(m.id).startsWith('hvo_')) {
                        window.open('/api/missions/' + String(m.id).replace('hvo_', '') + '/pdf', '_blank');
                    } else if (m.rawSession && typeof m.rawSession.id === 'string' && String(m.rawSession.id).startsWith('m_')) {
                        const mId = parseInt(String(m.rawSession.id).replace('m_', ''));
                        window.open('/api/missions/' + mId + '/pdf', '_blank');
                    } else {
                        await appAlert("PDF-Download steht nur für Einsätze im neuen Einsatz-System (Reiter 'Einsätze') zur Verfügung. Für alte Dienste verwende bitte die Druckfunktion (STRG+P) in der Bearbeiten-Ansicht.");
                    }
                },
                openReport(s) { 
                    if (s.is_mission || (typeof s.id === 'string' && s.id.startsWith('m_'))) {
                        const realId = s.real_mission_id || parseInt(String(s.id).replace('m_', ''));
                        window.open('/api/missions/' + realId + '/report', '_blank');
                    } else {
                        window.open('/sessions/' + s.id + '/report', '_blank');
                    }
                },
                
                openSig(s) { 
                    if (s.is_mission || (typeof s.id === 'string' && s.id.startsWith('m_'))) {
                        const realId = s.real_mission_id || parseInt(String(s.id).replace('m_', ''));
                        this.openMissionSigModal({ id: realId, isNewMission: true, leader_signature: s.leader_signature });
                    } else {
                        this.activeS = s; 
                        if(sigModal) sigModal.show(); 
                        setTimeout(() => {
                            initResponsiveCanvas('sigCanvas', sigPad, s.leader_signature);
                        }, 250);
                    }
                },
                async saveSig() {
                    if (!sigPad || sigPad.isEmpty()) { await appAlert("Bitte unterschreiben!"); return; }
                    const r = await fetch(`/sessions/${this.activeS.id}/leader_signature`, { method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify({signature: trimSignatureDataUrl(sigPad)}) });
                    if (r.ok) {
                        sigModal.hide();
                        await this.loadData();
                    } else {
                        await appAlert("Freigabe fehlgeschlagen. Bitte erneut versuchen.");
                    }
                },
                clearSig() { if(sigPad) sigPad.clear(); },
            }
});

const { createApp } = Vue;
        let sigPad = null, sigModal = null, selfPasswordModal = null;
        let missionSigPad = null, missionSigModal = null;
        let map = null, hydrantMarkers = [];

        function initResponsiveCanvas(canvasId, padInstance, existingSignatureDataUrl) {
            const canvas = document.getElementById(canvasId);
            if (!canvas) return;
            const ratio = Math.max(window.devicePixelRatio || 1, 1);
            const rect = canvas.getBoundingClientRect();
            if (rect.width > 0 && rect.height > 0) {
                canvas.width = rect.width * ratio;
                canvas.height = rect.height * ratio;
                const ctx = canvas.getContext("2d");
                ctx.scale(ratio, ratio);
            }
            if (padInstance) {
                padInstance.clear();
                if (existingSignatureDataUrl) {
                    padInstance.fromDataURL(existingSignatureDataUrl);
                }
            }
        }

        const app = createApp({
            data() {
                return {
                    isOnline: navigator.onLine,
                    qrScanner: null,
                    isDarkMode: true,
                    ready: false, isAdmin: false, username: '', role: '',
                    groups: [], selectedGroup: null, mobileSelectedId: null,
                    sessionsList: [], selectedYear: new Date().getFullYear(),
                    ranking: [], showRanking: false, catStats: { Übung: 0, Einsatz: 0, Sonstiges: 0 }, activeS: null,
                    vehicles: [], activeVehicleForLog: null, vehicleLogs: [],
                    activeVehicleForCheck: null, vehicleCheckHistory: [],
                    currentChecklist: { date: new Date().toISOString().split('T')[0], checker_name: '', status: 'OK', notes: '', items_checked: {'Ölstand': false, 'Beleuchtung': false, 'Funkgeräte (HRT)': false, 'Reifendruck': false} },
                    newVehicle: { name: '', radio_name: '', status: 2, tuv_date: '', sp_date: '', milage: 0, next_service: '' },
                    selfPwData: { old_password: '', new_password: '', confirm_password: '' },
                    activeBroadcasts: [],
                    newBroadcast: { title: '', content: '', is_mandatory: false },
                    isFirstLoginBlock: false,
                    personalStats: { hours: 0, count: 0 },
                    personalSessions: [],
                    activeTab: localStorage.getItem('activeDashboardTab') || 'dienste',
                    notes: [],
                    notesActiveFilter: 'all',
                    noteEditingId: null,
                    newNote: { title: '', content: '', visibility: 'private' },

                    apagerConfig: { api_key: '', active: true },
                    apagerLogs: [],
                    apagerFeedbacks: [],
                    serverHost: window.location.host,
                    
                    // Schedules / Dienstplanung
                    schedules: [],
                    newSchedule: { title: '', date: new Date().toISOString().split('T')[0], time: '19:00', description: '', type: 'Übung', group_id: null },
                    schedulePersonnelList: [],
                    activeScheduleForAttendance: null,
                    
                    // Missions / Einsatzberichte
                    missions: [],
                    missionGroupFilter: 'all',
                    activeMission: null,
                    newRespi: { personnel_id: null, druck_start: 300, druck_10: 270, druck_20: 240, druck_ende: 80, dauer: 30, fit_ok: true },
                    
                    // Equipment
                    equipment: [],
                    eqSearch: '',
                    eqCategory: '',
                    filterDefectiveOnly: false,
                    activeEquipment: null,
                    newInspection: { date: new Date().toISOString().split('T')[0], inspector: '', status: 'Bestanden', note: '' },
                    inspectionsHistory: [],
                    batchInspect: { rawBarcodes: '', inspector: '', status: 'Bestanden', note: '' },
                    
                    // Personalverwaltung
                    personnel: [],
                    personnelSearch: '',
                    activePersonnel: null,
                    newGear: { item_name: '', size: '', issue_date: new Date().toISOString().split('T')[0] },
                    newCourse: { course_name: '', date: new Date().toISOString().split('T')[0], certificate_url: '' },
                    ranks: [
                        'Feuerwehranwärter (FWA)', 'Feuerwehrfrauanwärterin (FWAin)',
                        'Feuerwehrmann (FM)', 'Feuerwehrfrau (FF)',
                        'Oberfeuerwehrmann (OFM)', 'Oberfeuerwehrfrau (OFF)',
                        'Hauptfeuerwehrmann (HFM)', 'Hauptfeuerwehrfrau (HFF)',
                        'Unterbrandmeister (UBM)', 'Unterbrandmeisterin (UBMin)',
                        'Löschmeister (LM)', 'Löschmeisterin (LMin)',
                        'Oberlöschmeister (OLM)', 'Oberlöschmeisterin (OLMin)',
                        'Hauptlöschmeister (HLM)', 'Hauptlöschmeisterin (HLMin)',
                        '1. Hauptlöschmeister (1. HLM)', '1. Hauptlöschmeisterin (1. HLMin)',
                        'Brandmeister (BM)', 'Brandmeisterin (BMin)',
                        'Oberbrandmeister (OBM)', 'Oberbrandmeisterin (OBMin)',
                        'Hauptbrandmeister (HBM)', 'Hauptbrandmeisterin (HBMin)',
                        'Hauptbrandmeister mit Zulage (HBMZ)',
                        '1. Hauptbrandmeister (1. HBM)', '1. Hauptbrandmeisterin (1. HBMin)',
                        'Brandinspektor (BI)', 'Brandinspektorin (BIn)',
                        'Brandoberinspektor (BOI)', 'Brandoberinspektorin (BOIn)',
                        'Brandamtmann (BAM)', 'Brandamtfrau (BAMf)',
                        'Brandamtsrat (BAR)', 'Brandamtsrätin (BARin)',
                        'Branddirektor (BD)', 'Branddirektorin (BDin)',
                        'Gemeindebrandmeister (GBM)', 'Stadtbrandmeister (SBM)',
                        'Stellv. Kreisbrandmeister (Stellv. KBM)', 'Kreisbrandmeister (KBM)',
                        'Landesbranddirektor (LBD)'
                    ],
                    qualifications: [
                        {id: 'is_truppmann', label: 'Truppmann (TM)'}, {id: 'is_funk', label: 'Sprechfunker'},
                        {id: 'is_agt', label: 'Atemschutzgeräteträger'}, {id: 'is_maschinist', label: 'Maschinist'},
                        {id: 'is_tf', label: 'Truppführer (TF)'}, {id: 'is_gf', label: 'Gruppenführer (GF)'}
                    ],
                    
                    // Billing / Abrechnung
                    bills: [],
                    newBill: { mission_id: null, recipient_name: '', address: '', amount: 150.00, details: '' },
                    sepaYear: new Date().getFullYear(),
                    sepaRate: 15.00,
                    compensations: [],
                    
                    // BMA & Hydrants
                    bmas: [],
                    newBma: { object_name: '', address: '', bma_number: '', key_depot: false, map_url: '' },
                    activeBmaId: null,
                    newHydrant: { lat: 0.0, lng: 0.0, type: 'Unterflur', label: 'Hydrant' },
                    newLogRide: { date: '', mileage_start: 0, mileage_end: 0, driver_name: '', purpose: '' },
                    systemUsers: [],
                    newLogin: { username: '', password: '', role: 'mannschaft', personnel_id: null },
                    activeVehicle: null,
                    selfBindPersonnelId: null,
                    
                    // Sub-tab navigation
                    materialSubTab: 'geraete',
                    verwaltungTab: 'logins',
                    
                    // Mängelmelder
                    defectReports: [],
                    defectFilter: 'Offen',
                    newDefect: { equipment_id: null, description: '', severity: 'Mittel', assigned_to: '', priority: 'Mittel', image_url: '' },
                    
                    // Test Alarm
                    testAlarm: { stichwort: '', adresse: '', meldung: '' },
                    
                    // User Edit State
                    editingUserId: null,
                    editingUsername: '',
                    editUserRole: 'mannschaft',
                    editUserPersonnelId: null,
                    showMobileSidebar: false,
                    stationConfig: { station_name: 'Feuerwehr', lat: 50.1109, lng: 8.6821, zoom: 14 },
                    uploadedMissionFiles: [],
                    archiveFiles: [],
                    newArchiveFile: { is_public: false },
                    archiveSearch: '',
                    archiveFilter: 'all',
                    cacheBuster: Date.now(),

                    // Neue Felder für Checkpoint 14 (Automatisierung, Lagekarte, Jugend, HvO, Werkstatt)
                    mapSearchQuery: '',
                    mapFilterCategory: 'all',
                    clickedLat: 0.0,
                    clickedLng: 0.0,
                    newMapObject: { category: 'hydrant', label: '', hydrantType: 'Unterflurhydrant', accessType: 'Zufahrt', bma_name: '', bma_address: '', bma_number: '', bma_fsd: false, danger_name: '', danger_level: 'Mittel' },
                    hydrantsList: [],
                    droneImages: [],
                    trackingSimActive: false,

                    
                    jugendTab: 'members',
                    youthMembers: [],
                    youthSearch: '',
                    youthModalTab: 'stamm',
                    editingYouthId: null,
                    newYouth: {
                        name: '', parent_contact: '', badges: '', skills: '', birth_date: '', entry_date: '', phone: '', email: '', address: '', notes: '',
                        lic_am: false, lic_a1: false, lic_b: false, lic_l: false, lic_t: false,
                        has_jf1: false, has_jf2: false, has_jf3: false, has_wissentest: false, has_leistungsspange: false, has_jugendabzeichen: false, has_mta_basis: false, has_erste_hilfe: false, has_funk: false
                    },
                    youthSessions: [],
                    newYouthSession: { date: new Date().toISOString().split('T')[0], topic: '', duration: 2.0, instructors: '', description: '', attendance: {} },
                    editingYouthSessionId: null,
                    
                    clubTab: 'inventory',
                    clubInventory: [],
                    newClubItem: { item_name: '', quantity: 1, status: 'OK' },
                    clubDonations: [],
                    newDonation: { donor: '', amount: 15.00, date: new Date().toISOString().split('T')[0] },
                    
                    hvoTab: 'protocols',
                    hvoProtocols: [],
                    newHvoProtocol: { date: new Date().toISOString().split('T')[0], symptoms: '', therapy: '', handover: '' },
                    hvoChecks: [],
                    newHvoCheck: { device_name: '' },
                    
                    werkstattTab: 'pruefungen',
                    qrScanInput: '',
                    scannedQrObject: null,
                    
                    statsTotalMissions: 0,
                    statsChartInstance: null, statsChartMonthInstance: null
                }
            },
            computed: {
                filteredNotes() {
                    if (this.notesActiveFilter === 'all') return this.notes;
                    return this.notes.filter(n => n.visibility === this.notesActiveFilter);
                },

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
                    if (!this.equipment) return [];
                    return this.equipment.filter(eq => eq && (eq.category === 'Funk' || eq.category === 'Funkgerät' || eq.category === 'TETRA')).map(eq => {
                        let type = 'HRT (Handfunkgerät)';
                        if (eq.name.toLowerCase().includes('mrt') || eq.name.toLowerCase().includes('fahrzeug')) {
                            type = 'MRT (Fahrzeugfunk)';
                        }
                        let rufname = `Florian ${this.getCityName()} - ${eq.name}`;
                        let akku = 100;
                        let ladezyklen = 0;
                        if (eq.barcode) {
                            let sum = 0;
                            for (let i = 0; i < eq.barcode.length; i++) sum += eq.barcode.charCodeAt(i);
                            akku = 70 + (sum % 31);
                            ladezyklen = 10 + (sum % 200);
                        }
                        return {
                            id: eq.id,
                            barcode: eq.barcode,
                            name: eq.name,
                            type: type,
                            rufname: rufname,
                            akku: akku,
                            ladezyklen: ladezyklen,
                            rawEq: eq
                        };
                    });
                },
                sessions() {
                    if (!this.sessionsList) return [];
                    return this.sessionsList.filter(s => s && s.date && new Date(s.date).getFullYear() === this.selectedYear).sort((a, b) => {
                        if (!a || !a.date || !b || !b.date) return 0;
                        return new Date(b.date) - new Date(a.date);
                    });
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
                            status: isSigned ? 'Freigegeben' : 'Entwurf',
                            leader_signature: m.leader_signature,
                            group_id: m.group_id
                        };
                    });
                    if (this.sessionsList) {
                        const past = this.sessionsList.filter(s => s && s.date && s.category === 'Einsatz' && new Date(s.date).getFullYear() === this.selectedYear).map(s => {
                            const isSigned = !!(s.leader_signature || s.is_signed || s.status === 'Freigegeben');
                            let sTime = '00:00';
                            if (s.time && s.time !== 'None' && s.time.trim() !== '') {
                                sTime = s.time.length > 5 ? s.time.substring(0, 5) : s.time;
                            }
                            return {
                                id: s.id,
                                isNewMission: false,
                                date: s.date,
                                time: sTime,
                                stichwort: s.description || 'Einsatz',
                                adresse: s.instructors || 'Siehe Bericht',
                                meldung: s.description || '',
                                duration: s.duration,
                                status: isSigned ? 'Freigegeben' : 'Entwurf',
                                rawSession: s,
                                group_id: s.group_id || (this.selectedGroup ? this.selectedGroup.id : null)
                            };
                        });
                        list.push(...past);
                    }
                    if (this.missionGroupFilter !== 'all') {
                        list = list.filter(m => m.group_id == this.missionGroupFilter || !m.group_id);
                    }
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
                visibleBroadcasts() { return this.activeBroadcasts.filter(b => b.gelesen == 0); },
                filteredEquipment() {
                    return this.equipment.filter(eq => {
                        const mSearch = eq.name.toLowerCase().includes(this.eqSearch.toLowerCase()) || eq.barcode.toLowerCase().includes(this.eqSearch.toLowerCase());
                        const mCat = !this.eqCategory || eq.category === this.eqCategory;
                        const mDefect = !this.filterDefectiveOnly || eq.current_status === 'Mangel' || eq.current_status === 'Defekt';
                        return mSearch && mCat && mDefect;
                    });
                },
                filteredPersonnel() {
                    return this.personnel.filter(p => p.name.toLowerCase().includes(this.personnelSearch.toLowerCase()));
                },
                activeFeedbacksCount() {
                    return this.apagerFeedbacks.filter(f => f.status === 'Komme').length;
                },
                filteredArchiveFiles() {
                    return this.archiveFiles.filter(f => {
                        const mSearch = f.filename.toLowerCase().includes(this.archiveSearch.toLowerCase()) || f.uploaded_by.toLowerCase().includes(this.archiveSearch.toLowerCase());
                        let mFilter = true;
                        if (this.archiveFilter === 'public') mFilter = !!f.is_public;
                        else if (this.archiveFilter === 'private') mFilter = !f.is_public;
                        return mSearch && mFilter;
                    });
                },
                filteredMapHydrants() {
                    const q = this.mapSearchQuery.toLowerCase();
                    if (this.mapFilterCategory !== 'all' && this.mapFilterCategory !== 'hydrant') return [];
                    return (this.hydrantsList || []).filter(h => h.label.toLowerCase().includes(q) || h.type.toLowerCase().includes(q));
                },
                filteredMapBmas() {
                    const q = this.mapSearchQuery.toLowerCase();
                    if (this.mapFilterCategory !== 'all' && this.mapFilterCategory !== 'bma') return [];
                    return (this.bmas || []).filter(b => b.object_name.toLowerCase().includes(q) || b.address.toLowerCase().includes(q) || b.bma_number.toLowerCase().includes(q));
                }
            },
            watch: {
                activeTab(newTab) {
                    if (newTab) {
                        localStorage.setItem('activeDashboardTab', newTab);
                    }
                    if (newTab === 'stats') {
                        this.loadStats();
                    }
                }
            },
            async mounted() {
                window.addEventListener('online', () => this.isOnline = true);
                window.addEventListener('offline', () => this.isOnline = false);
                this.setupPushNotifications();
                try {
                    const authRes = await fetch('/api/auth/me', { credentials: 'include' });
                    if (!authRes.ok) {
                        const params = new URLSearchParams(window.location.search);
                        const eqBarcode = params.get('eq_barcode');
                        if (eqBarcode) {
                            window.location.href = '/login?eq_barcode=' + eqBarcode;
                        } else {
                            window.location.href = '/login';
                        }
                        return;
                    }
                    const authData = await authRes.json();
                    this.isAdmin = (authData.role === 'admin'); this.username = authData.username; this.role = authData.role;
                    this.isFirstLoginBlock = !!authData.is_first_login;
                } catch (e) {
                    const params = new URLSearchParams(window.location.search);
                    const eqBarcode = params.get('eq_barcode');
                    if (eqBarcode) {
                        window.location.href = '/login?eq_barcode=' + eqBarcode;
                    } else {
                        window.location.href = '/login';
                    }
                    return;
                }
                
                const loadTasks = [
                    this.loadStationSettings(),
                    this.loadGroups(),
                    this.loadVehicles(),
                    this.loadBroadcasts(),
                    this.loadApagerConfig(),
                    this.loadSchedules(),
                    this.loadPersonnel(),
                    this.loadMissions(),
                    this.loadBills()
                ];
                if (this.isAdmin) {
                    loadTasks.push(this.loadSystemUsers());
                }
                await Promise.all(loadTasks);
                
                if (this.activeTab === 'stats') {
                    this.loadStats();
                }
                
                const canvas = document.getElementById('sigCanvas');
                if(canvas) { sigPad = new SignaturePad(canvas, { backgroundColor: 'white' }); }
                sigModal = new bootstrap.Modal(document.getElementById('sigModal'));
                selfPasswordModal = new bootstrap.Modal(document.getElementById('selfPasswordModal'));
                
                const mCanvas = document.getElementById('missionSigCanvas');
                if(mCanvas) { missionSigPad = new SignaturePad(mCanvas, { backgroundColor: 'white' }); }
                missionSigModal = new bootstrap.Modal(document.getElementById('missionSigModal'));
                
                // QR-Code Barcode scanner query param
                const params = new URLSearchParams(window.location.search);
                const eqBarcode = params.get('eq_barcode');
                if (eqBarcode) {
                    this.activeTab = 'material';
                    setTimeout(async () => {
                        await this.loadEquipment();
                        const match = this.equipment.find(e => e.barcode === eqBarcode);
                        if (match) {
                            this.editEquipment(match);
                        }
                    }, 800);
                }

                this.ready = true;
                if (this.isFirstLoginBlock) {
                    setTimeout(() => {
                        const el = document.getElementById('selfPasswordModal');
                        if (el) {
                            let m = bootstrap.Modal.getInstance(el);
                            if (!m) m = new bootstrap.Modal(el);
                            m.show();
                        }
                    }, 400);
                }

                // Register Service Worker for PWA
                if ('serviceWorker' in navigator) {
                    navigator.serviceWorker.register('/sw.js').then(reg => {
                        console.log('ServiceWorker registered:', reg.scope);
                    }).catch(err => {
                        console.error('ServiceWorker registration failed:', err);
                    });
                }
                
                // Initialize WebSocket for Real-Time Updates
                this.initWebSocket();
                
                // Set Theme from LocalStorage
                const savedTheme = localStorage.getItem('theme');
                if(savedTheme === 'light') {
                    this.isDarkMode = false;
                    document.documentElement.setAttribute('data-theme', 'light');
                }
            },
            methods: {
                async loadNotes() {
                    try {
                        const res = await fetch('/api/notes', { credentials: 'include' });
                        if (res.ok) { this.notes = await res.json(); }
                    } catch (e) { console.error("Fehler beim Laden", e); }
                },
                async saveNote() {
                    try {
                        const isEdit = this.noteEditingId !== null;
                        const url = isEdit ? `/api/notes/${this.noteEditingId}` : '/api/notes';
                        const method = isEdit ? 'PUT' : 'POST';
    
                        const res = await fetch(url, {
                            method: method,
                            headers: { 'Content-Type': 'application/json' },
                            credentials: 'include',
                            body: JSON.stringify(this.newNote)
                        });
    
                        if (res.ok) {
                            this.newNote.title = '';
                            this.newNote.content = '';
                            this.noteEditingId = null;
                            await this.loadNotes();
                        } else {
                            alert("Eintrag konnte nicht gespeichert werden.");
                        }
                    } catch (e) { alert("Serverfehler beim Senden."); }
                },
                startNoteEdit(note) {
                    this.noteEditingId = note.id;
                    this.newNote.title = note.title;
                    this.newNote.content = note.content;
                    this.newNote.visibility = note.visibility;
                    window.scrollTo({ top: 0, behavior: 'smooth' });
                },
                cancelNoteEdit() {
                    this.noteEditingId = null;
                    this.newNote.title = '';
                    this.newNote.content = '';
                    this.newNote.visibility = 'private';
                },
                async deleteNote(id) {
                    if (confirm("Möchtest du diesen Eintrag permanent löschen?")) {
                        try {
                            const res = await fetch(`/api/notes/${id}`, { method: 'DELETE', credentials: 'include' });
                            if (res.ok) { 
                                if (this.noteEditingId === id) this.cancelNoteEdit();
                                await this.loadNotes(); 
                            } else { 
                                alert("Löschen fehlgeschlagen."); 
                            }
                        } catch (e) { alert("Verbindungsfehler."); }
                    }
                },
                getVisLabel(vis) {
                    if (vis === 'public') return 'Pinnwand (Öffentlich)';
                    if (vis === 'private') return 'Privat';
                    if (vis === 'admin') return 'Führung Intern';
                    if (vis === 'geratewart') return 'Gerätewart Notiz';
                    return vis;
                },

                startQrScanner() {
                    if (typeof Html5Qrcode === 'undefined') { alert('Scanner-Bibliothek nicht geladen'); return; }
                    this.html5Qrcode = new Html5Qrcode("qr-reader");
                    const modal = new bootstrap.Modal(document.getElementById('qrScannerModal'));
                    modal.show();
                    this.html5Qrcode.start(
                        { facingMode: "environment" },
                        { fps: 10, qrbox: { width: 250, height: 250 } },
                        (decodedText, decodedResult) => {
                            const eq = this.equipment.find(e => e.barcode === decodedText || String(e.id) === decodedText);
                            this.stopQrScanner();
                            const modalEl = bootstrap.Modal.getInstance(document.getElementById('qrScannerModal'));
                            if (modalEl) modalEl.hide();
                            if (eq) {
                                this.addInspection(eq);
                            } else {
                                alert("Kein Gerät mit Barcode '" + decodedText + "' gefunden.");
                            }
                        },
                        (err) => { /* ignore */ }
                    ).catch(err => {
                        console.error(err);
                        alert("Kamera-Zugriff fehlgeschlagen. Ist HTTPS aktiv?");
                    });
                },
                stopQrScanner() {
                    if (this.html5Qrcode) {
                        try {
                            this.html5Qrcode.stop().then(() => this.html5Qrcode.clear()).catch(e => console.error(e));
                        } catch(e) {}
                    }
                },
                async requestPushPermission() {
                    const permission = await Notification.requestPermission();
                    if (permission === 'granted') {
                        try {
                            const reg = await navigator.serviceWorker.ready;
                            let sub = await reg.pushManager.getSubscription();
                            if (sub) await sub.unsubscribe();
                        } catch(e) {}
                        await this.setupPushNotifications(true);
                        alert("Push-Benachrichtigungen aktiviert!");
                    } else {
                        alert("Berechtigung verweigert.");
                    }
                },
                async setupPushNotifications(forceReset = false) {
                    if (!('serviceWorker' in navigator) || !('PushManager' in window)) return;
                    try {
                        const reg = await navigator.serviceWorker.ready;
                        let sub = await reg.pushManager.getSubscription();
                        if (!sub || forceReset) {
                            if (sub) await sub.unsubscribe();
                            const res = await fetch('/api/push/public-key');
                            const { public_key } = await res.json();
                            const convertedVapidKey = this.urlBase64ToUint8Array(public_key);
                            sub = await reg.pushManager.subscribe({
                                userVisibleOnly: true,
                                applicationServerKey: convertedVapidKey
                            });
                        }
                        await fetch('/api/push/subscribe', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify(sub)
                        });
                    } catch(err) { console.error('Push setup failed:', err); }
                },
                urlBase64ToUint8Array(base64String) {
                    const padding = '='.repeat((4 - base64String.length % 4) % 4);
                    const base64 = (base64String + padding).replace(/\-/g, '+').replace(/_/g, '/');
                    const rawData = window.atob(base64);
                    const outputArray = new Uint8Array(rawData.length);
                    for (let i = 0; i < rawData.length; ++i) { outputArray[i] = rawData.charCodeAt(i); }
                    return outputArray;
                },
                initWebSocket() {
                    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                    const ws = new WebSocket(`${protocol}//${window.location.host}/ws`);
                    ws.onmessage = async (event) => {
                        try {
                            const data = JSON.parse(event.data);
                            if (data.type === 'new_mission' || data.type === 'update_mission') {
                                // Live update the dashboard when a mission arrives or updates
                                await this.loadMissions();
                                if (data.type === 'new_mission') {
                                    this.playAlarmSound();
                                    this.activeTab = 'einsaetze'; // switch to missions
                                    
                                    // Speak the alarm details
                                    setTimeout(() => {
                                        if (data.mission && data.mission.stichwort) {
                                            const msg = new SpeechSynthesisUtterance(`Alarm! ${data.mission.stichwort}. ${data.mission.adresse || ''}`);
                                            msg.lang = 'de-DE';
                                            msg.rate = 0.9;
                                            window.speechSynthesis.speak(msg);
                                        }
                                    }, 2000);
                                }
                            }
                        } catch(e) {}
                    };
                    ws.onclose = () => {
                        // Reconnect after 5 seconds if connection lost
                        setTimeout(() => this.initWebSocket(), 5000);
                    };
                },
                playAlarmSound() {
                    try {
                        const AudioContext = window.AudioContext || window.webkitAudioContext;
                        if (!AudioContext) return;
                        const ctx = new AudioContext();
                        for (let i = 0; i < 5; i++) {
                            const osc = ctx.createOscillator();
                            const gain = ctx.createGain();
                            osc.type = 'square';
                            osc.frequency.value = 1000;
                            gain.gain.setValueAtTime(0, ctx.currentTime + (i * 0.6));
                            gain.gain.linearRampToValueAtTime(0.1, ctx.currentTime + (i * 0.6) + 0.1);
                            gain.gain.linearRampToValueAtTime(0, ctx.currentTime + (i * 0.6) + 0.5);
                            osc.connect(gain);
                            gain.connect(ctx.destination);
                            osc.start(ctx.currentTime + (i * 0.6));
                            osc.stop(ctx.currentTime + (i * 0.6) + 0.5);
                        }
                    } catch(e) { console.error("Audio failed", e); }
                },
                toggleTheme() {
                    this.isDarkMode = !this.isDarkMode;
                    document.documentElement.setAttribute('data-theme', this.isDarkMode ? 'dark' : 'light');
                    localStorage.setItem('theme', this.isDarkMode ? 'dark' : 'light');
                },
                startScanner() {
                    const modal = new bootstrap.Modal(document.getElementById('scannerModal'));
                    modal.show();
                    
                    if (this.qrScanner) {
                        this.qrScanner.clear();
                    }
                    
                    this.qrScanner = new Html5QrcodeScanner("qr-reader", { fps: 10, qrbox: 250 }, false);
                    this.qrScanner.render((decodedText, decodedResult) => {
                        this.qrScanner.clear();
                        bootstrap.Modal.getInstance(document.getElementById('scannerModal')).hide();
                        
                        // Try to find equipment with this barcode/QR
                        const eq = this.equipment.find(e => e.id == decodedText || e.name.includes(decodedText));
                        if(eq) {
                            this.activeEquipment = JSON.parse(JSON.stringify(eq));
                            new bootstrap.Modal(document.getElementById('equipmentModal')).show();
                        } else {
                            alert(`Gerät "${decodedText}" nicht im System gefunden.`);
                        }
                    }, (errorMessage) => {
                        // ignore errors during scanning
                    });
                },
                stopScanner() {
                    if (this.qrScanner) {
                        this.qrScanner.clear();
                    }
                    const m = bootstrap.Modal.getInstance(document.getElementById('scannerModal'));
                    if(m) m.hide();
                },
                async fullLogout() { await fetch('/api/logout', { method: 'POST', credentials: 'include' }); window.location.href = '/login'; },
                
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
                    const res = await fetch('/api/missions/schedules/list', { credentials: 'include' });
                    if(res.ok) this.schedules = await res.json();
                },
                openScheduleModal() {
                    this.newSchedule = { title: '', date: new Date().toISOString().split('T')[0], time: '19:00', description: '', type: 'Übung', group_id: this.selectedGroup?.id };
                    new bootstrap.Modal(document.getElementById('scheduleModal')).show();
                },
                async submitSchedule() {
                    const res = await fetch('/api/missions/schedules', { method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify(this.newSchedule) });
                    if(res.ok) {
                        bootstrap.Modal.getInstance(document.getElementById('scheduleModal')).hide();
                        await this.loadSchedules();
                    }
                },
                async deleteSchedule(id) {
                    if(confirm("Termin wirklich löschen?")) {
                        await fetch(`/api/missions/schedules/${id}`, { method: 'DELETE', credentials: 'include' });
                        await this.loadSchedules();
                    }
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

                // aPager Webhook Log / Config
                async loadApagerConfig() {
                    try {
                        const cacheBuster = `?_=${new Date().getTime()}`;
                        const [cRes, lRes, fRes] = await Promise.all([
                            fetch('/api/apager/config' + cacheBuster, { credentials: 'include' }),
                            fetch('/api/apager/logs' + cacheBuster, { credentials: 'include' }),
                            fetch('/api/apager/feedbacks' + cacheBuster, { credentials: 'include' })
                        ]);
                        if(cRes.ok) this.apagerConfig = await cRes.json();
                        if(lRes.ok) this.apagerLogs = await lRes.json();
                        if(fRes.ok) this.apagerFeedbacks = await fRes.json();
                    } catch(e) {}
                },
                async generateApiKey() {
                    try {
                        const res = await fetch('/api/apager/config', { method: 'POST', credentials: 'include' });
                        if(res.ok) this.apagerConfig = await res.json();
                    } catch(e) {}
                },
                async submitFeedback(status) {
                    const res = await fetch(`/api/apager/feedbacks?status=${encodeURIComponent(status)}`, { method: 'POST', credentials: 'include' });
                    if(res.ok) {
                        alert("Rückmeldung erfolgreich gesendet!");
                        await this.loadApagerConfig();
                    }
                },
                async deleteApagerLog(id) {
                    if (confirm("Alarmierungseintrag wirklich löschen?")) {
                        const res = await fetch(`/api/apager/logs/${id}`, { method: 'DELETE', credentials: 'include' });
                        if(res.ok) await this.loadApagerConfig();
                    }
                },
                async clearApagerLogs() {
                    if (confirm("Ganzes Alarmierungsprotokoll wirklich unwiderruflich leeren?")) {
                        const res = await fetch('/api/apager/logs', { method: 'DELETE', credentials: 'include' });
                        if(res.ok) await this.loadApagerConfig();
                    }
                },
                async editApagerLog(log) {
                    const stichwort = prompt("Stichwort anpassen:", log.stichwort);
                    if (stichwort === null) return;
                    const adresse = prompt("Einsatzort anpassen:", log.adresse);
                    if (adresse === null) return;
                    const meldung = prompt("Meldung anpassen:", log.meldung);
                    if (meldung === null) return;
                    const res = await fetch(`/api/apager/logs/${log.id}`, {
                        method: 'PUT',
                        headers: {'Content-Type': 'application/json'},
                        credentials: 'include',
                        body: JSON.stringify({ stichwort, adresse, meldung })
                    });
                    if(res.ok) await this.loadApagerConfig();
                },

                // Vehicles & Logs
                async loadVehicles() { const res = await fetch('/api/vehicles', { credentials: 'include' }); this.vehicles = await res.json(); },
                async quickStatusChange(v) { await fetch(`/api/vehicles/${v.id}/status`, { method: 'PUT', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify({ status: parseInt(v.status) }) }); },
                async loadVehicleLogs() {
                    if(!this.activeVehicleForLog) return;
                    const res = await fetch(`/api/material/vehicles/${this.activeVehicleForLog.id}/log`, { credentials: 'include' });
                    if(res.ok) this.vehicleLogs = await res.json();
                },
                openLogRideModal() {
                    this.newLogRide = { date: new Date().toISOString().split('T')[0], mileage_start: this.activeVehicleForLog.milage || 0, mileage_end: (this.activeVehicleForLog.milage || 0) + 10, driver_name: '', purpose: '' };
                    new bootstrap.Modal(document.getElementById('logRideModal')).show();
                },
                async submitLogRide() {
                    const res = await fetch(`/api/material/vehicles/${this.activeVehicleForLog.id}/log`, { method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify(this.newLogRide) });
                    if(res.ok) {
                        bootstrap.Modal.getInstance(document.getElementById('logRideModal')).hide();
                        await this.loadVehicles();
                        this.activeVehicleForLog = this.vehicles.find(x => x.id === this.activeVehicleForLog.id);
                        await this.loadVehicleLogs();
                    }
                },

                // Broadcasts
                async loadBroadcasts() {
                    const res = await fetch('/api/broadcasts/active', { credentials: 'include' });
                    if(res.ok) this.activeBroadcasts = await res.json();
                },
                async markBroadcastAsRead(b) {
                    const res = await fetch(`/api/broadcasts/${b.id}/read`, { method: 'POST', credentials: 'include' });
                    if(res.ok) this.activeBroadcasts = this.activeBroadcasts.filter(item => item.id !== b.id);
                },

                handleIncidentClick(m) {
                    if (m.isNewMission) {
                        this.editMission(m);
                    } else {
                        this.loadSession(m.rawSession);
                    }
                },
                // Missions
                async loadMissions() {
                    const res = await fetch('/api/missions', { credentials: 'include' });
                    if(res.ok) this.missions = await res.json();
                },
                openNewMissionModal() {
                    const now = new Date();
                    const nowStr = now.toTimeString().slice(0, 5);
                    const laterStr = new Date(now.getTime() + 2 * 3600 * 1000).toTimeString().slice(0, 5);
                    this.activeMission = { 
                        id: null, 
                        date: new Date().toISOString().split('T')[0], 
                        time: nowStr, 
                        end_time: laterStr,
                        stichwort: '', 
                        adresse: '', 
                        meldung: '', 
                        description: '', 
                        duration: 2.0, 
                        status: 'Entwurf', 
                        group_id: (this.missionGroupFilter !== 'all' ? parseInt(this.missionGroupFilter) : (this.selectedGroup ? this.selectedGroup.id : null)),
                        attList: [], 
                        respiList: [] 
                    };
                    this.uploadedMissionFiles = [];
                    this.initActiveMissionAttendance();
                    new bootstrap.Modal(document.getElementById('missionModal')).show();
                },
                calcMissionDuration() {
                    if (this.activeMission && this.activeMission.time && this.activeMission.end_time) {
                        const [h1, m1] = this.activeMission.time.split(':').map(Number);
                        const [h2, m2] = this.activeMission.end_time.split(':').map(Number);
                        let diffMins = (h2 * 60 + m2) - (h1 * 60 + m1);
                        if (diffMins < 0) diffMins += 24 * 60;
                        if (!isNaN(diffMins) && diffMins > 0) {
                            this.activeMission.duration = Math.round((diffMins / 60) * 10) / 10;
                        }
                    }
                },
                async initActiveMissionAttendance() {
                    const res = await fetch('/api/personnel/list', { credentials: 'include' });
                    const pList = await res.json();
                    this.activeMission.attList = pList.map(p => ({ personnel_id: p.id, name: p.name, is_present: 'Nein', vehicle: '', g26_expired: p.g26_expired }));
                },
                async editMission(m) {
                    const res = await fetch(`/api/missions/${m.id}`, { credentials: 'include' });
                    if(res.ok) {
                        const detail = await res.json();
                        this.uploadedMissionFiles = detail.media_files ? JSON.parse(detail.media_files) : [];
                        const pRes = await fetch('/api/personnel/list', { credentials: 'include' });
                        const pList = await pRes.json();
                        
                        detail.attList = pList.map(p => {
                            const match = detail.attendance.find(x => x.personnel_id === p.id);
                            return { personnel_id: p.id, name: p.name, is_present: match ? match.is_present : 'Nein', vehicle: match ? match.vehicle : '', g26_expired: p.g26_expired };
                        });
                        
                        // Load respiration
                        const rRes = await fetch(`/api/missions/${m.id}/respiration`, { credentials: 'include' });
                        detail.respiList = rRes.ok ? await rRes.json() : [];
                        detail.end_time = detail.end_time || '';
                        
                        this.activeMission = detail;
                        new bootstrap.Modal(document.getElementById('missionModal')).show();
                    }
                },
                async saveMission() {
                    let mStatus = this.activeMission.status;
                    if (this.activeMission.leader_signature && mStatus === 'Entwurf') {
                        mStatus = 'Freigegeben';
                    }
                    const payload = {
                        date: this.activeMission.date, time: this.activeMission.time, end_time: this.activeMission.end_time || '', stichwort: this.activeMission.stichwort, adresse: this.activeMission.adresse,
                        meldung: this.activeMission.meldung, description: this.activeMission.description, duration: parseFloat(this.activeMission.duration), status: mStatus,
                        group_id: this.activeMission.group_id ? parseInt(this.activeMission.group_id) : null,
                        media_files: JSON.stringify(this.uploadedMissionFiles),
                        attendance: this.activeMission.attList.filter(x => x.is_present !== 'Nein')
                    };
                    const isEdit = this.activeMission.id !== null;
                    const url = isEdit ? `/api/missions/${this.activeMission.id}` : '/api/missions';
                    const method = isEdit ? 'PUT' : 'POST';
                    
                    const res = await fetch(url, { method: method, headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify(payload) });
                    if(res.ok) {
                        bootstrap.Modal.getInstance(document.getElementById('missionModal')).hide();
                        await Promise.all([this.loadMissions(), this.loadData()]);
                    }
                },
                async deleteMission(id) {
                    if(confirm("Einsatzbericht unwiderruflich löschen?")) {
                        await fetch(`/api/missions/${id}`, { method: 'DELETE', credentials: 'include' });
                        await Promise.all([this.loadMissions(), this.loadData()]);
                    }
                },
                openMissionSigModal(m) {
                    this.activeS = m;
                    missionSigModal.show();
                    setTimeout(() => {
                        initResponsiveCanvas('missionSigCanvas', missionSigPad, m.leader_signature);
                    }, 250);
                },
                async saveMissionSig() {
                    if(!missionSigPad || missionSigPad.isEmpty()) return alert("Bitte unterschreiben!");
                    const sigData = missionSigPad.toDataURL();
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
                    }
                },
                clearMissionSig() { if(missionSigPad) missionSigPad.clear(); },
                
                // Respiration Atemschutz
                async submitRespi() {
                    if(!this.newRespi.personnel_id) return alert("Träger wählen!");
                    const res = await fetch(`/api/missions/${this.activeMission.id}/respiration`, { method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify(this.newRespi) });
                    if(res.ok) {
                        this.newRespi = { personnel_id: null, druck_start: 300, druck_10: 270, druck_20: 240, druck_ende: 80, dauer: 30, fit_ok: true };
                        const rRes = await fetch(`/api/missions/${this.activeMission.id}/respiration`, { credentials: 'include' });
                        this.activeMission.respiList = await rRes.json();
                    }
                },
                async deleteRespi(id) {
                    const res = await fetch(`/api/missions/respiration/${id}`, { method: 'DELETE', credentials: 'include' });
                    if(res.ok) {
                        const rRes = await fetch(`/api/missions/${this.activeMission.id}/respiration`, { credentials: 'include' });
                        this.activeMission.respiList = await rRes.json();
                    }
                },

                // Equipment
                async loadEquipment() {
                    const res = await fetch('/api/material/equipment', { credentials: 'include' });
                    if(res.ok) this.equipment = await res.json();
                    await this.loadDefectReports();
                    await this.loadClubData();
                },
                openEquipmentModal() {
                    this.activeEquipment = { id: null, name: '', barcode: '', category: 'Schläuche', interval_months: 12, last_inspection: null, next_inspection: null };
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
                    this.newInspection = { date: new Date().toISOString().split('T')[0], inspector: '', status: 'Bestanden', note: '' };
                    new bootstrap.Modal(document.getElementById('addInspectionModal')).show();
                },
                async submitInspection() {
                    const res = await fetch(`/api/material/equipment/${this.activeEquipment.id}/inspections`, { method: 'POST', headers: {'Content-Type':'application/json'}, credentials: 'include', body: JSON.stringify(this.newInspection) });
                    if(res.ok) {
                        bootstrap.Modal.getInstance(document.getElementById('addInspectionModal')).hide();
                        await this.loadEquipment();
                    }
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
                    }
                },

                // Personal
                async loadPersonnel() {
                    const res = await fetch('/api/personnel/list', { credentials: 'include' });
                    if(res.ok) this.personnel = await res.json();
                },
                openPersonnelModal() {
                    this.activePersonnel = { id: null, name: '', rank: '', membership_status: 'Aktiv', phone: '', email: '', address: '', badge_number: '', birth_date: null, entry_date: null, honors: '', profile_picture: '', has_picture: false, courses: [], gearList: [] };
                    this.qualifications.forEach(q => this.activePersonnel[q.id] = false);
                    ['lic_b', 'lic_be', 'lic_c', 'lic_ce'].forEach(k => this.activePersonnel[k] = false);
                    new bootstrap.Modal(document.getElementById('personnelModal')).show();
                },
                async editPersonnel(p) {
                    const res = await fetch(`/api/personnel/get/${p.id}`, { credentials: 'include' });
                    if(res.ok) {
                        const detail = await res.json();
                        // Load courses & gear
                        const [cRes, gRes] = await Promise.all([
                            fetch(`/api/material/personnel/${p.id}/lehrgaenge`, { credentials: 'include' }),
                            fetch(`/api/material/personnel/${p.id}/inventar`, { credentials: 'include' })
                        ]);
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
                            alert("Fehler beim Hochladen des Profilbilds.");
                        }
                    } catch(err) {
                        console.error(err);
                        alert("Verbindung fehlgeschlagen beim Hochladen des Profilbilds.");
                    }
                },
                async savePersonnel() {
                    const payload = { ...this.activePersonnel };
                    this.qualifications.forEach(q => payload[q.id] = this.activePersonnel[q.id] ? 1 : 0);
                    ['lic_b', 'lic_be', 'lic_c', 'lic_ce'].forEach(k => payload[k] = this.activePersonnel[k] ? 1 : 0);
                    const isNew = this.activePersonnel.id === null;
                    const endpoint = isNew ? '/api/personnel/add' : `/api/personnel/update/${this.activePersonnel.id}`;
                    const res = await fetch(endpoint, { method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify(payload) });
                    if(res.ok) {
                        this.cacheBuster = Date.now();
                        bootstrap.Modal.getInstance(document.getElementById('personnelModal')).hide();
                        await this.loadPersonnel();
                    }
                },
                async deletePersonnel() {
                    if(confirm("Mitglied permanent löschen?")) {
                        await fetch(`/api/personnel/delete/${this.activePersonnel.id}`, { method: 'DELETE', credentials: 'include' });
                        bootstrap.Modal.getInstance(document.getElementById('personnelModal')).hide();
                        await this.loadPersonnel();
                    }
                },
                async submitGear() {
                    const res = await fetch(`/api/material/personnel/${this.activePersonnel.id}/inventar`, { method: 'POST', headers: {'Content-Type':'application/json'}, credentials: 'include', body: JSON.stringify(this.newGear) });
                    if(res.ok) {
                        this.newGear = { item_name: '', size: '', issue_date: new Date().toISOString().split('T')[0] };
                        const gRes = await fetch(`/api/material/personnel/${this.activePersonnel.id}/inventar`, { credentials: 'include' });
                        this.activePersonnel.gearList = await gRes.json();
                    }
                },
                async deleteGear(id) {
                    const res = await fetch(`/api/material/personnel/inventar/${id}`, { method: 'DELETE', credentials: 'include' });
                    if(res.ok) {
                        const gRes = await fetch(`/api/material/personnel/${this.activePersonnel.id}/inventar`, { credentials: 'include' });
                        this.activePersonnel.gearList = await gRes.json();
                    }
                },
                async submitCourse() {
                    const res = await fetch(`/api/material/personnel/${this.activePersonnel.id}/lehrgaenge`, { method: 'POST', headers: {'Content-Type':'application/json'}, credentials: 'include', body: JSON.stringify(this.newCourse) });
                    if(res.ok) {
                        this.newCourse = { course_name: '', date: new Date().toISOString().split('T')[0], certificate_url: '' };
                        const cRes = await fetch(`/api/material/personnel/${this.activePersonnel.id}/lehrgaenge`, { credentials: 'include' });
                        this.activePersonnel.courses = await cRes.json();
                    }
                },
                async deleteCourse(id) {
                    const res = await fetch(`/api/material/personnel/lehrgaenge/${id}`, { method: 'DELETE', credentials: 'include' });
                    if(res.ok) {
                        const cRes = await fetch(`/api/material/personnel/${this.activePersonnel.id}/lehrgaenge`, { credentials: 'include' });
                        this.activePersonnel.courses = await cRes.json();
                    }
                },

                // Billing / Abrechnung
                async loadBills() {
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
                },
                async submitBill() {
                    if(!this.newBill.mission_id) return alert("Bitte einen Einsatz auswählen!");
                    const res = await fetch(`/api/missions/billing/${this.newBill.mission_id}`, { method: 'POST', headers: {'Content-Type':'application/json'}, credentials: 'include', body: JSON.stringify(this.newBill) });
                    if(res.ok) {
                        alert("Kostenbescheid erfolgreich buchen!");
                        this.newBill = { mission_id: null, recipient_name: '', address: '', amount: 150.00, details: '' };
                        await this.loadBills();
                    } else {
                        const err = await res.json();
                        alert("Fehler beim Buchen: " + (err.detail || "Keine Berechtigung"));
                    }
                },
                async deleteBill(id) {
                    if(confirm("Rechnung löschen?")) {
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
                downloadSepa() {
                    const iban = prompt("Sender IBAN eingeben:", this.stationConfig?.iban || '');
                    const bic = prompt("Sender BIC eingeben:", this.stationConfig?.bic || '');
                    if(iban && bic) {
                        window.open(`/api/missions/billing/export/sepa?year=${this.sepaYear}&hourly_rate=${this.sepaRate}&sender_iban=${iban}&sender_bic=${bic}`, '_blank');
                    }
                },

                // BMA & Hydrants
                async loadBmas() {
                    const res = await fetch('/api/material/bma', { credentials: 'include' });
                    if(res.ok) this.bmas = await res.json();
                },
                async submitBma() {
                    const isEdit = this.activeBmaId !== null;
                    const url = isEdit ? `/api/material/bma/${this.activeBmaId}` : '/api/material/bma';
                    const method = isEdit ? 'PUT' : 'POST';
                    const res = await fetch(url, { method: method, headers: {'Content-Type':'application/json'}, credentials: 'include', body: JSON.stringify(this.newBma) });
                    if(res.ok) {
                        this.cancelBmaEdit();
                        await this.loadBmas();
                    }
                },
                startBmaEdit(b) {
                    this.activeBmaId = b.id;
                    this.newBma = { ...b, key_depot: !!b.key_depot };
                },
                cancelBmaEdit() {
                    this.activeBmaId = null;
                    this.newBma = { object_name: '', address: '', bma_number: '', key_depot: false, map_url: '' };
                },
                async deleteBma(id) {
                    if(confirm("Objekt löschen?")) {
                        await fetch(`/api/material/bma/${id}`, { method: 'DELETE', credentials: 'include' });
                        await this.loadBmas();
                    }
                },
                initMap() {
                    this.$nextTick(() => {
                        setTimeout(() => {
                            if (!map) {
                                const el = document.getElementById('hydrantMap');
                                if (!el) return;
                                map = L.map('hydrantMap', { maxZoom: 22 }).setView([this.stationConfig.lat, this.stationConfig.lng], this.stationConfig.zoom);
                                L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 22, maxNativeZoom: 19 }).addTo(map);
                                
                                this.loadHydrants();
                                this.loadDroneImages();
                                
                                map.on('click', (e) => {
                                    if (this.role === 'mannschaft') return;
                                    map.invalidateSize({ animate: false });
                                    const exactLatLng = map.mouseEventToLatLng(e.originalEvent || e);
                                    this.clickedLat = exactLatLng.lat;
                                    this.clickedLng = exactLatLng.lng;
                                    
                                    this.newMapObject = {
                                        category: 'hydrant',
                                        label: '',
                                        hydrantType: 'Unterflurhydrant',
                                        accessType: 'Zufahrt',
                                        bma_name: '',
                                        bma_address: '',
                                        bma_number: '',
                                        bma_fsd: false,
                                        danger_name: '',
                                        danger_level: 'Mittel'
                                    };
                                    
                                    const elModal = document.getElementById('mapObjectModal');
                                    if (elModal) {
                                        let m = bootstrap.Modal.getInstance(elModal);
                                        if (!m) m = new bootstrap.Modal(elModal);
                                        m.show();
                                    }
                                });
                            } else {
                                map.invalidateSize();
                                setTimeout(() => { if (map) map.invalidateSize(); }, 300);
                            }
                        }, 250);
                    });
                },
                async loadHydrants() {
                    if (!map) return;
                    const [hRes, bRes] = await Promise.all([
                        fetch('/api/material/hydrants', { credentials: 'include' }),
                        fetch('/api/material/bma', { credentials: 'include' })
                    ]);
                    
                    if(hRes.ok) this.hydrantsList = await hRes.json();
                    if(bRes.ok) this.bmas = await bRes.json();
                    
                    hydrantMarkers.forEach(m => map.removeLayer(m));
                    hydrantMarkers = [];
                    
                    const getIconHtml = (bgColor, iconClass, color = 'white') => {
                        return `<div style="background-color: ${bgColor}; border: 2px solid white; border-radius: 50%; width: 26px; height: 26px; display: flex; align-items: center; justify-content: center; color: ${color}; box-shadow: 0 2px 5px rgba(0,0,0,0.4);"><i class="${iconClass}" style="font-size: 11px;"></i></div>`;
                    };
                    
                    this.hydrantsList.forEach(h => {
                        let bgColor = '#dc3545';
                        let iconClass = 'fa fa-faucet';
                        
                        if (h.type.includes('Zisterne')) {
                            bgColor = '#0d6efd';
                            iconClass = 'fa fa-droplet';
                        } else if (h.type.includes('Brunnen')) {
                            bgColor = '#0dcaf0';
                            iconClass = 'fa fa-water';
                        } else if (h.type.includes('Zufahrt') || h.type.includes('Rettungsweg')) {
                            bgColor = '#198754';
                            iconClass = 'fa fa-right-to-bracket';
                        } else if (h.type.includes('Sammelplatz')) {
                            bgColor = '#20c997';
                            iconClass = 'fa fa-users';
                        } else if (h.type.includes('Anleiterbereitschaft')) {
                            bgColor = '#6f42c1';
                            iconClass = 'fa fa-ladder';
                        } else if (h.type.includes('Gefahrstofflager')) {
                            bgColor = '#fd7e14';
                            iconClass = 'fa fa-triangle-exclamation';
                        }
                        
                        const divIcon = L.divIcon({
                            html: getIconHtml(bgColor, iconClass),
                            className: 'custom-div-icon',
                            iconSize: [26, 26],
                            iconAnchor: [13, 13]
                        });
                        
                        const marker = L.marker([h.lat, h.lng], { icon: divIcon })
                            .bindPopup(`<b>${h.label}</b><br>Typ: ${h.type}<br><button class="btn btn-xs btn-danger mt-1 text-white border-0" onclick="window.deleteHydrant(${h.id})">Löschen</button>`)
                            .addTo(map);
                        hydrantMarkers.push(marker);
                    });
                    
                    this.bmas.forEach(b => {
                        if (b.lat && b.lng) {
                            const divIcon = L.divIcon({
                                html: getIconHtml('#ffc107', 'fa fa-building-shield', 'black'),
                                className: 'custom-div-icon',
                                iconSize: [26, 26],
                                iconAnchor: [13, 13]
                            });
                            
                            const marker = L.marker([b.lat, b.lng], { icon: divIcon })
                                .bindPopup(`<b>BMA: ${b.object_name}</b><br>BMA-Nr: ${b.bma_number}<br>Schlüssel-Depot: ${b.key_depot ? 'JA' : 'NEIN'}<br><button class="btn btn-xs btn-danger mt-1 text-white border-0" onclick="window.deleteBmaOnMap(${b.id})">Entfernen</button>`)
                                .addTo(map);
                            hydrantMarkers.push(marker);
                        }
                    });
                },
                async submitMapObject() {
                    const o = this.newMapObject;
                    let url = '';
                    let payload = {};
                    
                    if (o.category === 'bma') {
                        url = '/api/material/bma';
                        payload = {
                            object_name: o.bma_name,
                            address: o.bma_address,
                            bma_number: o.bma_number,
                            key_depot: o.bma_fsd,
                            map_url: '',
                            lat: this.clickedLat,
                            lng: this.clickedLng
                        };
                    } else {
                        url = '/api/material/hydrants';
                        let label = o.label;
                        let type = '';
                        if (o.category === 'hydrant') {
                            type = o.hydrantType;
                            if(!label) label = `${type} #${Math.floor(Math.random() * 1000)}`;
                        } else if (o.category === 'access') {
                            type = o.accessType;
                            if(!label) label = type;
                        } else {
                            type = 'Gefahrstofflager';
                            if(!label) label = o.danger_name || 'Gefahrstofflager';
                        }
                        payload = {
                            lat: this.clickedLat,
                            lng: this.clickedLng,
                            type: type,
                            label: label
                        };
                    }
                    
                    const res = await fetch(url, {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        credentials: 'include',
                        body: JSON.stringify(payload)
                    });
                    
                    if (res.ok) {
                        bootstrap.Modal.getInstance(document.getElementById('mapObjectModal')).hide();
                        this.loadHydrants();
                    } else {
                        alert("Fehler beim Speichern des Objekts.");
                    }
                },
                panToCoords(lat, lng, zoom) {
                    if (map) {
                        map.setView([lat, lng], zoom);
                    }
                },
                async deleteHydrantItem(id) {
                    if (confirm("Objekt wirklich permanent von der Karte löschen?")) {
                        const res = await fetch(`/api/material/hydrants/${id}`, { method: 'DELETE', credentials: 'include' });
                        if (res.ok) this.loadHydrants();
                    }
                },
                async deleteBmaItem(id) {
                    if (confirm("BMA-Objekt wirklich permanent aus dem Verzeichnis und von der Karte löschen?")) {
                        const res = await fetch(`/api/material/bma/${id}`, { method: 'DELETE', credentials: 'include' });
                        if (res.ok) this.loadHydrants();
                    }
                },
                async loadDroneImages() {
                    const res = await fetch('/api/material/drone-images', { credentials: 'include' });
                    if(res.ok) this.droneImages = await res.json();
                },
                async uploadDroneImage(e) {
                    const file = e.target.files[0]; if (!file) return;
                    const formData = new FormData();
                    formData.append('file', file);
                    try {
                        const res = await fetch('/api/upload', { method: 'POST', body: formData, credentials: 'include' });
                        if (res.ok) {
                            const d = await res.json();
                            await fetch('/api/material/drone-images', {
                                method: 'POST',
                                headers: {'Content-Type':'application/json'},
                                credentials: 'include',
                                body: JSON.stringify({ url: d.url })
                            });
                            await this.loadDroneImages();
                        }
                    } catch(err) { console.error(err); }
                },
                async deleteDroneImage(id) {
                    if (confirm("Bild wirklich permanent löschen?")) {
                        const res = await fetch(`/api/material/drone-images/${id}`, { method: 'DELETE', credentials: 'include' });
                        if (res.ok) await this.loadDroneImages();
                    }
                },
                
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
                        alert("Bitte gib den Namen des Jugendlichen ein!");
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
                        alert("Bitte gib den Namen des Jugendlichen ein!");
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
                        alert("Fehler beim Speichern: " + (err.detail || "Unbekannter Fehler"));
                    }
                },
                async deleteYouthMember(id) {
                    if (confirm("Jugendfeuerwehr-Mitglied wirklich löschen?")) {
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
                    this.newYouthSession = { date: new Date().toISOString().split('T')[0], topic: '', duration: 2.0, instructors: '', description: '', attendance: {} };
                },
                async submitYouthSession() {
                    if (!this.newYouthSession.date || !this.newYouthSession.topic || this.newYouthSession.topic.trim() === '') {
                        alert("Bitte Datum und Thema angeben!");
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
                        alert("Dienstbericht erfolgreich gespeichert!");
                    } else {
                        alert("Fehler beim Speichern des Dienstberichts.");
                    }
                },
                async deleteYouthSession(id) {
                    if (confirm("Dienstbericht wirklich löschen?")) {
                        const res = await fetch(`/api/jugend/sessions/${id}`, { method: 'DELETE', credentials: 'include' });
                        if(res.ok) await this.loadJugendData();
                    }
                },

                // Vereinsverwaltung Module (Moved to Material)
                async loadClubData() {
                    try {
                        const [iRes, dRes] = await Promise.all([
                            fetch('/api/verein/inventory', { credentials: 'include' }),
                            fetch('/api/verein/donations', { credentials: 'include' })
                        ]);
                        if(iRes.ok) this.clubInventory = await iRes.json();
                        if(dRes.ok) this.clubDonations = await dRes.json();
                    } catch(err) { console.error(err); }
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
                    if(confirm("Vereins-Inventargegenstand löschen?")) {
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
                        this.newDonation = { donor: '', amount: 15.00, date: new Date().toISOString().split('T')[0] };
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
                        this.newHvoProtocol = { date: new Date().toISOString().split('T')[0], symptoms: '', therapy: '', handover: '' };
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
                
                // AI Mission summary generator draft trigger
                async generateAiMissionDraft() {
                    if (!this.activeMission) return;
                    try {
                        const res = await fetch('/api/missions/ai-draft', {
                            method: 'POST',
                            headers: {'Content-Type':'application/json'},
                            credentials: 'include',
                            body: JSON.stringify({
                                stichwort: this.activeMission.stichwort || 'Bandeinsatz',
                                adresse: this.activeMission.adresse || 'Musterstraße 1',
                                meldung: this.activeMission.meldung || 'Rauchentwicklung'
                            })
                        });
                        if (res.ok) {
                            const d = await res.json();
                            this.activeMission.description = d.draft;
                        }
                    } catch(err) { console.error(err); }
                },
                
                scanQrCodeSim() {
                    const code = this.qrScanInput.trim();
                    if (!code) return;
                    const match = this.equipInspectSchedule.find(x => x.barcode === code);
                    if (match) {
                        this.scannedQrObject = match;
                    } else {
                        alert("Prüfobjekt mit diesem QR-Code / Barcode nicht gefunden!");
                        this.scannedQrObject = null;
                    }
                },
                async confirmQrCheck() {
                    if (this.scannedQrObject && this.scannedQrObject.rawEq) {
                        const eq = this.scannedQrObject.rawEq;
                        const todayStr = new Date().toISOString().split('T')[0];
                        let nextDateStr = null;
                        if (eq.interval_months) {
                            const d = new Date();
                            d.setMonth(d.getMonth() + eq.interval_months);
                            nextDateStr = d.toISOString().split('T')[0];
                        }
                        const res = await fetch(`/api/material/equipment/${eq.id}/inspections`, {
                            method: 'POST',
                            headers: {'Content-Type': 'application/json'},
                            credentials: 'include',
                            body: JSON.stringify({
                                date: todayStr,
                                inspector: this.username || 'System',
                                status: 'Bestanden',
                                note: 'Automatische QR-Code Prüfung'
                            })
                        });
                        if (res.ok) {
                            const updatePayload = {
                                name: eq.name,
                                barcode: eq.barcode,
                                category: eq.category,
                                image_url: eq.image_url,
                                manual_url: eq.manual_url,
                                interval_months: eq.interval_months,
                                last_inspection: todayStr,
                                next_inspection: nextDateStr
                            };
                            await fetch(`/api/material/equipment/${eq.id}`, {
                                method: 'PUT',
                                headers: {'Content-Type': 'application/json'},
                                credentials: 'include',
                                body: JSON.stringify(updatePayload)
                            });
                            await this.loadEquipment();
                            alert(`Prüfung für ${eq.name} erfolgreich erfasst.`);
                            this.scannedQrObject = null;
                            this.qrScanInput = '';
                        } else {
                            alert("Fehler beim Übermitteln der Prüfung.");
                        }
                    }
                },
                addPsaEquipment() {
                    this.activeEquipment = { id: null, name: '', barcode: '', category: 'Atemschutz', image_url: '', manual_url: '', interval_months: 12, last_inspection: null, next_inspection: null };
                    new bootstrap.Modal(document.getElementById('equipmentModal')).show();
                },
                addFunkEquipment() {
                    this.activeEquipment = { id: null, name: '', barcode: '', category: 'Funkgerät', image_url: '', manual_url: '', interval_months: 0, last_inspection: null, next_inspection: null };
                    new bootstrap.Modal(document.getElementById('equipmentModal')).show();
                },
                async deleteEquipmentFromSubtab(id) {
                    if (confirm("Gerät permanent aus der Datenbank löschen? All seine Prüf- und Mängeldaten gehen verloren!")) {
                        const res = await fetch(`/api/material/equipment/${id}`, { method: 'DELETE', credentials: 'include' });
                        if(res.ok) {
                            await this.loadEquipment();
                        } else {
                            const err = await res.json();
                            alert("Fehler beim Löschen: " + (err.detail || "Keine Berechtigung"));
                        }
                    }
                },
                async deleteEquipmentInModal() {
                    if (!this.activeEquipment || !this.activeEquipment.id) return;
                    if (confirm(`Gerät "${this.activeEquipment.name}" wirklich permanent löschen?`)) {
                        const res = await fetch(`/api/material/equipment/${this.activeEquipment.id}`, { method: 'DELETE', credentials: 'include' });
                        if (res.ok) {
                            const modalEl = document.getElementById('equipmentModal');
                            const m = bootstrap.Modal.getInstance(modalEl);
                            if (m) m.hide();
                            await this.loadEquipment();
                        } else {
                            const err = await res.json();
                            alert("Fehler beim Löschen: " + (err.detail || "Keine Berechtigung"));
                        }
                    }
                },

                // Global Profile
                async fetchPersonalStats() {
                    try {
                        const res = await fetch('/api/users/me/stats?year=' + this.selectedYear, { credentials: 'include' });
                        if (res.ok) { this.personalStats = await res.json(); }
                        const sRes = await fetch('/api/users/me/sessions?year=' + this.selectedYear, { credentials: 'include' });
                        if (sRes.ok) { this.personalSessions = await sRes.json(); }
                    } catch(e) {}
                },
                getCityName() {
                    const name = this.stationConfig.station_name || 'Neustadt';
                    return name.replace(/feuerwehr/gi, '').trim() || 'Neustadt';
                },
                openSelfPasswordModal() {
                    this.selfPwData = { old_password: '', new_password: '', confirm_password: '' };
                    this.fetchPersonalStats();
                    const el = document.getElementById('selfPasswordModal');
                    if (el) {
                        let m = bootstrap.Modal.getInstance(el);
                        if (!m) m = new bootstrap.Modal(el);
                        m.show();
                    }
                },
                async submitSelfPassword() {
                    if (this.selfPwData.new_password !== this.selfPwData.confirm_password) { alert("Die neuen Passwörter stimmen nicht überein!"); return; }
                    const res = await fetch('/api/auth/change-password', { method: 'PUT', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify({ old_password: this.selfPwData.old_password, new_password: this.selfPwData.new_password }) });
                    if (res.ok) {
                        alert("Dein Passwort wurde erfolgreich aktualisiert!");
                        this.isFirstLoginBlock = false;
                        const el = document.getElementById('selfPasswordModal');
                        if (el) {
                            const m = bootstrap.Modal.getInstance(el);
                            if (m) m.hide();
                        }
                    }
                },
                async bindSelfAccount() {
                    if (!this.selfBindPersonnelId) return alert("Wähle einen Kameraden aus!");
                    const res = await fetch('/api/users/me/bind-personnel', { method: 'PUT', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify({ personnel_id: this.selfBindPersonnelId }) });
                    if (res.ok) {
                        alert("Konto erfolgreich verknüpft!");
                        await this.fetchPersonalStats();
                    }
                },
                
                
                async submitNewLogin() {
                    if (!this.newLogin.username || !this.newLogin.password) return alert("Benutzername und Passwort sind erforderlich!");
                    const res = await fetch('/api/users/add', { method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify(this.newLogin) });
                    if (res.ok) {
                        alert("System-Login angelegt!");
                        this.newLogin = { username: '', password: '', role: 'mannschaft', personnel_id: null };
                        await this.loadSystemUsers();
                    } else {
                        const err = await res.json();
                        alert("Fehler: " + (err.detail || "Unbekannter Fehler"));
                    }
                },
                async deleteUserAccount(id) {
                    if (confirm("System-Login permanent löschen?")) {
                        const res = await fetch(`/api/users/${id}`, { method: 'DELETE', credentials: 'include' });
                        if (res.ok) {
                            alert("Konto gelöscht.");
                            await this.loadSystemUsers();
                        }
                    }
                },
                async resetUserPassword(usr) {
                    const newPw = prompt(`Neues Passwort für Benutzer '${usr.username}':`);
                    if (newPw) {
                        const res = await fetch(`/api/users/${usr.id}/password`, { method: 'PUT', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify({ password: newPw }) });
                        if (res.ok) alert("Passwort erfolgreich zurückgesetzt!");
                    }
                },

                // Vehicles Form Actions
                async openChecklist(veh) {
                    this.activeVehicleForCheck = veh;
                    this.currentChecklist = { date: new Date().toISOString().split('T')[0], checker_name: this.username, status: 'OK', notes: '', items_checked: {'Ölstand (Motor)': false, 'Reifendruck': false, 'Beleuchtung / Blaulicht': false, 'Funkgeräte (HRT/MRT)': false, 'Erste-Hilfe-Rucksack': false, 'Atemschutzgeräte': false} };
                    await this.loadCheckHistory(veh.id);
                    new bootstrap.Modal(document.getElementById('vehicleCheckModal')).show();
                },
                async loadCheckHistory(vehId) {
                    try {
                        const res = await fetch(`/api/vehicles/${vehId}/checks`, { credentials: 'include' });
                        if(res.ok) this.vehicleCheckHistory = await res.json();
                    } catch(e) { console.error(e); }
                },
                async submitChecklist() {
                    // Check if any items are unchecked
                    const allChecked = Object.values(this.currentChecklist.items_checked).every(v => v);
                    this.currentChecklist.status = allChecked ? 'OK' : 'Mängel';
                    if (!allChecked && !this.currentChecklist.notes) {
                        alert("Da nicht alle Punkte abgehakt wurden, trage bitte die gefundenen Mängel in die Bemerkungen ein!");
                        return;
                    }
                    try {
                        const res = await fetch(`/api/vehicles/${this.activeVehicleForCheck.id}/checks`, {
                            method: 'POST', headers: {'Content-Type':'application/json'}, credentials: 'include', body: JSON.stringify(this.currentChecklist)
                        });
                        if (res.ok) {
                            bootstrap.Modal.getInstance(document.getElementById('vehicleCheckModal')).hide();
                            alert("Fahrzeug-Check erfolgreich gespeichert!");
                            await this.loadVehicles(); // update status if needed
                        }
                    } catch(e) { alert("Verbindung fehlgeschlagen."); }
                },
                openVehicleFormModal(veh) {
                    if (veh) {
                        this.activeVehicle = { ...veh };
                    } else {
                        this.activeVehicle = { id: null, name: '', radio_name: '', status: 2, tuv_date: '', sp_date: '', milage: 0, next_service: '' };
                    }
                    new bootstrap.Modal(document.getElementById('vehicleFormModal')).show();
                },
                async saveVehicle() {
                    const isEdit = this.activeVehicle.id !== null;
                    const url = isEdit ? `/api/vehicles/${this.activeVehicle.id}` : '/api/vehicles';
                    const method = isEdit ? 'PUT' : 'POST';
                    const res = await fetch(url, { method: method, headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify(this.activeVehicle) });
                    if (res.ok) {
                        bootstrap.Modal.getInstance(document.getElementById('vehicleFormModal')).hide();
                        await this.loadVehicles();
                    }
                },
                async deleteVehicle(id) {
                    if (confirm("Fahrzeug aus Fuhrpark löschen?")) {
                        const res = await fetch(`/api/vehicles/${id}`, { method: 'DELETE', credentials: 'include' });
                        if (res.ok) await this.loadVehicles();
                    }
                },
                
                formatDateDay(d) {
                    if (!d || typeof d !== 'string' || !d.includes('-')) return '';
                    return d.split('-')[2];
                },
                formatDateMonth(d) {
                    if (!d || typeof d !== 'string' || !d.includes('-')) return '';
                    return d.split('-')[1];
                },
                formatDateMonthYear(d) {
                    if (!d || typeof d !== 'string' || !d.includes('-')) return '';
                    const parts = d.split('-');
                    return `${parts[1]}.${parts[0].substring(2)}`;
                },
                isOverdue(d) { if(!d) return false; return new Date(d) < new Date(); },
                getStatusClass(status) { if(status === 'Aktiv') return 'aktiv'; if(status === 'Passiv') return 'passiv'; if(status === 'Jugend') return 'jugend'; return 'ehren'; },
                
                // Group methods
                async addGroup() { const name = prompt("Name der neuen Gruppe:"); if (name) { await fetch('/groups', { method: 'POST', headers: {'Content-Type':'application/json'}, credentials: 'include', body: JSON.stringify({name: name}) }); await this.loadGroups(); } },
                async editGroup(g) { const name = prompt("Neuer Name:", g.name); if (name && name !== g.name) { await fetch(`/groups/${g.id}`, { method: 'PUT', headers: {'Content-Type':'application/json'}, credentials: 'include', body: JSON.stringify({name: name}) }); await this.loadGroups(); } },
                async deleteGroup(g) { if (confirm(`Gruppe "${g.name}" wirklich löschen?`)) { await fetch(`/groups/${g.id}`, { method: 'DELETE', credentials: 'include' }); await this.loadGroups(); this.selectedGroup = null; if(this.groups.length > 0) this.selectGroup(this.groups[0]); } },
                async deleteSession(s) { 
                    if (s.is_mission || (typeof s.id === 'string' && s.id.startsWith('m_'))) {
                        const realId = s.real_mission_id || parseInt(String(s.id).replace('m_', ''));
                        if (confirm("Einsatzbericht unwiderruflich löschen?")) {
                            const res = await fetch(`/api/missions/${realId}`, { method: 'DELETE', credentials: 'include' });
                            if (res.ok) {
                                await Promise.all([this.loadMissions(), this.loadData()]);
                            } else {
                                alert("Fehler: Einsatzbericht konnte nicht gelöscht werden (Fehlende Rechte?).");
                            }
                        }
                    } else {
                        if (confirm("Eintrag löschen?")) {
                            await fetch(`/sessions/${s.id}`, { method: 'DELETE', credentials: 'include' });
                            await this.loadData();
                        }
                    }
                },
                createNew() { if(!this.selectedGroup) return; window.location.href = `/editor?group_id=${this.selectedGroup.id}`; },
                loadSession(s) { 
                    if (s.is_mission || (typeof s.id === 'string' && s.id.startsWith('m_'))) {
                        const realId = s.real_mission_id || parseInt(String(s.id).replace('m_', ''));
                        this.editMission({ id: realId });
                    } else {
                        window.location.href = `/editor?group_id=${this.selectedGroup.id}&session_id=${s.id}`;
                    }
                },
                downloadMissionPdf(m) {
                    if (m.isNewMission) {
                        window.open('/api/missions/' + m.id + '/pdf', '_blank');
                    } else if (typeof m.id === 'string' && String(m.id).startsWith('hvo_')) {
                        window.open('/api/missions/' + String(m.id).replace('hvo_', '') + '/pdf', '_blank');
                    } else if (m.rawSession && typeof m.rawSession.id === 'string' && String(m.rawSession.id).startsWith('m_')) {
                        const mId = parseInt(String(m.rawSession.id).replace('m_', ''));
                        window.open('/api/missions/' + mId + '/pdf', '_blank');
                    } else {
                        alert("PDF-Download steht nur für Einsätze im neuen Einsatz-System (Reiter 'Einsätze') zur Verfügung. Für alte Dienste verwende bitte die Druckfunktion (STRG+P) in der Bearbeiten-Ansicht.");
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
                    if (!sigPad || sigPad.isEmpty()) { alert("Bitte unterschreiben!"); return; }
                    const r = await fetch(`/sessions/${this.activeS.id}/leader_signature`, { method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify({signature: sigPad.toDataURL()}) });
                    if (r.ok) { sigModal.hide(); await this.loadData(); }
                },
                clearSig() { if(sigPad) sigPad.clear(); },
                
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
                        alert('Bitte Gerät und Beschreibung angeben!'); return;
                    }
                    const res = await fetch('/api/material/defect-reports', {
                        method: 'POST', headers: {'Content-Type': 'application/json'},
                        credentials: 'include', body: JSON.stringify(this.newDefect)
                    });
                    if (res.ok) {
                        alert('✅ Mangel wurde gemeldet!');
                        this.newDefect = { equipment_id: null, description: '', severity: 'Mittel' };
                        await this.loadDefectReports();
                    } else { const d = await res.json(); alert(d.detail || 'Fehler beim Melden.'); }
                },
                async resolveDefect(id, status) {
                    const res = await fetch(`/api/material/defect-reports/${id}`, {
                        method: 'PUT', headers: {'Content-Type': 'application/json'},
                        credentials: 'include', body: JSON.stringify({ status: status })
                    });
                    if (res.ok) await this.loadDefectReports();
                },
                async deleteDefectReport(id) {
                    if (!confirm("Mangelmeldung unwiderruflich löschen?")) return;
                    const res = await fetch(`/api/material/defect-reports/${id}`, {
                        method: 'DELETE',
                        credentials: 'include'
                    });
                    if (res.ok) {
                        await this.loadDefectReports();
                    } else {
                        const d = await res.json();
                        alert(d.detail || "Fehler beim Löschen.");
                    }
                },
                
                // === TEST ALARM METHODS ===
                async sendTestAlarm() {
                    if (!confirm('Test-Alarm wirklich auslösen? Dies erscheint im Protokoll.')) return;
                    const res = await fetch('/api/apager/test-alarm', {
                        method: 'POST', headers: {'Content-Type': 'application/json'},
                        credentials: 'include', body: JSON.stringify(this.testAlarm)
                    });
                    if (res.ok) {
                        alert('✅ Test-Alarm wurde im Protokoll eingetragen!');
                        this.testAlarm = { stichwort: '', adresse: '', meldung: '' };
                        await this.loadApagerConfig();
                    } else { const d = await res.json(); alert(d.detail || 'Fehler beim Test-Alarm.'); }
                },
                copyWebhookUrl() {
                    const url = `${window.location.protocol}//${this.serverHost}/api/apager/webhook?api_key=${this.apagerConfig.api_key || ''}`;
                    navigator.clipboard.writeText(url).then(() => alert('✅ Webhook-URL in Zwischenablage kopiert!'));
                },
                
                async uploadBackupFile() {
                    const fileInput = this.$refs.backupFileInput;
                    if (!fileInput || !fileInput.files || fileInput.files.length === 0) {
                        alert("Bitte wähle zuerst eine Backup-JSON-Datei aus!");
                        return;
                    }
                    if (!confirm("WARNUNG: Möchtest du diese Datenbank-Sicherung wirklich einspielen? Vorhandene Datensätze werden aktualisiert.")) {
                        return;
                    }
                    const file = fileInput.files[0];
                    const formData = new FormData();
                    formData.append('file', file);
                    try {
                        const res = await fetch('/api/admin/backup/import', {
                            method: 'POST',
                            credentials: 'include',
                            body: formData
                        });
                        const data = await res.json();
                        if (res.ok) {
                            alert(`✅ Backup erfolgreich importiert (${data.imported_rows} Datensätze aktualisiert)!`);
                            fileInput.value = '';
                            location.reload();
                        } else {
                            alert("Fehler beim Importieren: " + (data.detail || "Unbekannter Fehler"));
                        }
                    } catch (err) {
                        alert("Verbindung zum Server fehlgeschlagen!");
                    }
                },
                
                // === USER EDIT METHODS ===
                async loadSystemUsers() {
                    const res = await fetch('/api/users/list', { credentials: 'include' });
                    if (res.ok) {
                        const users = await res.json();
                        // Enrich with personnel name lookup
                        this.systemUsers = users.map(u => ({
                            ...u,
                            personnel_name: u.personnel_id ? (this.personnel.find(p => p.id === u.personnel_id)?.name || `ID ${u.personnel_id}`) : null
                        }));
                    }
                },
                startUserEdit(usr) {
                    this.editingUserId = usr.id;
                    this.editingUsername = usr.username;
                    this.editUserRole = usr.role;
                    this.editUserPersonnelId = usr.personnel_id || null;
                },
                cancelUserEdit() {
                    this.editingUserId = null;
                    this.editingUsername = '';
                    this.editUserRole = 'mannschaft';
                    this.editUserPersonnelId = null;
                },
                async saveUserEdit() {
                    if (!this.editingUserId) return;
                    const [r1, r2] = await Promise.all([
                        fetch(`/api/users/${this.editingUserId}/role`, {
                            method: 'PUT', headers: {'Content-Type': 'application/json'},
                            credentials: 'include', body: JSON.stringify({ role: this.editUserRole })
                        }),
                        fetch(`/api/users/${this.editingUserId}/personnel`, {
                            method: 'PUT', headers: {'Content-Type': 'application/json'},
                            credentials: 'include', body: JSON.stringify({ personnel_id: this.editUserPersonnelId })
                        })
                    ]);
                    if (r1.ok && r2.ok) {
                        alert('✅ Login erfolgreich aktualisiert!');
                        this.cancelUserEdit();
                        await this.loadSystemUsers();
                    } else { alert('Fehler beim Speichern. Bitte erneut versuchen.'); }
                },
                async loadStationSettings() {
                    try {
                        const res = await fetch('/api/settings/station', { credentials: 'include' });
                        if (res.ok) {
                            this.stationConfig = await res.json();
                        }
                    } catch(e) { console.error("Error loading station settings", e); }
                },
                async saveStationSettings() {
                    try {
                        const res = await fetch('/api/settings/station', {
                            method: 'PUT',
                            headers: { 'Content-Type': 'application/json' },
                            credentials: 'include',
                            body: JSON.stringify(this.stationConfig)
                        });
                        if (res.ok) {
                            alert("✅ Standort-Einstellungen erfolgreich gespeichert!");
                            await this.loadStationSettings();
                        } else {
                            const d = await res.json();
                            alert(d.detail || "Fehler beim Speichern der Wachen-Einstellungen.");
                        }
                    } catch(e) {
                        console.error(e);
                        alert("Verbindung fehlgeschlagen.");
                    }
                },
                async lookupStationCoordinates() {
                    const query = this.stationConfig.station_name.trim();
                    if (!query) {
                        alert("Bitte gib zuerst einen Wachennamen an!");
                        return;
                    }
                    try {
                        const url = `https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(query)}&limit=1`;
                        const res = await fetch(url);
                        if (res.ok) {
                            const data = await res.json();
                            if (data && data.length > 0) {
                                const lat = parseFloat(data[0].lat);
                                const lon = parseFloat(data[0].lon);
                                this.stationConfig.lat = lat;
                                this.stationConfig.lng = lon;
                                this.stationConfig.zoom = 15;
                                alert(`✅ Koordinaten gefunden:\nBreitengrad: ${lat}\nLängengrad: ${lon}`);
                            } else {
                                alert("Es konnten keine Koordinaten für diesen Namen gefunden werden. Bitte genauer angeben (z. B. 'Feuerwehrhaus Buxheim' oder inklusive Postleitzahl/Ort).");
                            }
                        } else {
                            alert("Fehler bei der Verbindung zum Geokodierungs-Dienst.");
                        }
                    } catch(e) {
                        console.error(e);
                        alert("Fehler beim Suchen der Koordinaten.");
                    }
                },
                async uploadMissionFile(event) {
                    const file = event.target.files[0];
                    if (!file) return;
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
                            this.uploadedMissionFiles.push({ name: data.filename, url: data.url });
                            this.$refs.missionFile.value = ''; // clear input
                        } else {
                            alert('Fehler beim Datei-Upload.');
                        }
                    } catch(e) {
                        console.error(e);
                        alert('Fehler beim Datei-Upload.');
                    }
                },
                 removeMissionFile(index) {
                    this.uploadedMissionFiles.splice(index, 1);
                },
                async loadArchiveFiles() {
                    try {
                        const res = await fetch('/api/archive/files', { credentials: 'include' });
                        if (res.ok) {
                            this.archiveFiles = await res.json();
                        }
                    } catch(e) { console.error("Error loading archive files", e); }
                },
                async uploadArchiveFile(event) {
                    const file = event.target.files[0];
                    if (!file) return;
                    const formData = new FormData();
                    formData.append('file', file);
                    try {
                        const url = `/api/archive/upload?is_public=${this.newArchiveFile.is_public}`;
                        const res = await fetch(url, {
                            method: 'POST',
                            body: formData,
                            credentials: 'include'
                        });
                        if (res.ok) {
                            alert("✅ Datei erfolgreich ins Archiv hochgeladen!");
                            this.$refs.archiveFileInput.value = '';
                            await this.loadArchiveFiles();
                        } else {
                            alert("Fehler beim Hochladen ins Archiv.");
                        }
                    } catch(e) {
                        console.error(e);
                        alert("Verbindung fehlgeschlagen.");
                    }
                },
                async deleteArchiveFile(id) {
                    if (!confirm("Datei wirklich permanent aus dem Archiv löschen?")) return;
                    try {
                        const res = await fetch(`/api/archive/files/${id}`, {
                            method: 'DELETE',
                            credentials: 'include'
                        });
                        if (res.ok) {
                            await this.loadArchiveFiles();
                        } else {
                            const d = await res.json();
                            alert(d.detail || "Fehler beim Löschen.");
                        }
                    } catch(e) {
                        console.error(e);
                        alert("Verbindung fehlgeschlagen.");
                    }
                },
                async loadStats() {
                    this.$nextTick(async () => {
                    try {
                        const res = await fetch('/api/admin/stats', { credentials: 'include' });
                        if (res.ok) {
                            const data = await res.json();
                            this.statsTotalMissions = data.total_missions;
                            
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
                                            backgroundColor: 'rgba(220, 53, 69, 0.5)',
                                            borderColor: 'rgba(220, 53, 69, 1)',
                                            borderWidth: 1
                                        }]
                                    },
                                    options: { responsive: true, maintainAspectRatio: false, scales: { y: { beginAtZero: true, ticks: { stepSize: 1 } } } }
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
                                            label: 'Einsätze',
                                            data: data.missions_by_month.map(d => d.count),
                                            backgroundColor: 'rgba(13, 110, 253, 0.5)',
                                            borderColor: 'rgba(13, 110, 253, 1)',
                                            borderWidth: 1
                                        }]
                                    },
                                    options: { responsive: true, maintainAspectRatio: false, scales: { y: { beginAtZero: true, ticks: { stepSize: 1 } } } }
                                });
                            }

                        }
                    } catch(err) {
                        console.error('Error loading stats:', err);
                    }
                    });
                }
            }
        });
        window.vueApp = app.mount('#app');

        // Global function for map popup delete button
        window.deleteHydrant = async (id) => {
            if(confirm("Hydrant permanent löschen?")) {
                const res = await fetch(`/api/material/hydrants/${id}`, { method: 'DELETE' });
                if(res.ok) {
                    if (window.vueApp && window.vueApp.loadHydrants) {
                        window.vueApp.loadHydrants();
                    } else {
                        location.reload();
                    }
                }
            }
        };

        window.deleteBmaOnMap = async (id) => {
            if(confirm("BMA-Objekt von der Karte entfernen? (Das Objekt bleibt in der Liste vorhanden)")) {
                if (window.vueApp) {
                    const match = window.vueApp.bmas.find(b => b.id === id);
                    if (match) {
                        const payload = { ...match, lat: null, lng: null };
                        const res = await fetch(`/api/material/bma/${id}`, {
                            method: 'PUT',
                            headers: {'Content-Type':'application/json'},
                            credentials: 'include',
                            body: JSON.stringify(payload)
                        });
                        if (res.ok) {
                            window.vueApp.loadHydrants();
                        }
                    }
                }
            }
        };

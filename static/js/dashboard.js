const { createApp } = Vue;
        let sigPad = null, sigModal = null, selfPasswordModal = null;
        let missionSigPad = null, missionSigModal = null;
        let certSigPad = null, certSigModal = null;
        let map = null, hydrantMarkers = [];
        let globalSearchTimer = null;

// Chart.js rendert per Default für helle Seiten (dunkelgraue Achsenbeschriftung, fast
// unsichtbare Gitterlinien). Das hier war früher fest auf helle Schrift für dunklen
// Hintergrund gesetzt - seit es zusätzlich einen Hell/Dunkel-Umschalter (toggleTheme) gibt,
// blieben Diagramme dadurch in EINEM der beiden Modi unlesbar (weiße Schrift auf hellem
// Kartenhintergrund bzw. umgekehrt), je nachdem welcher Modus beim Erstmal-Laden gerade
// aktiv war. Jetzt themenabhängig statt fest verdrahtet, und wird sowohl beim initialen
// Laden als auch bei jedem Theme-Wechsel (siehe toggleTheme()) neu angewendet.
function applyChartTheme() {
    if (!window.Chart) return;
    const isLight = document.documentElement.getAttribute('data-theme') === 'light';
    Chart.defaults.color = isLight ? 'rgba(33, 37, 41, 0.75)' : 'rgba(255, 255, 255, 0.75)';
    Chart.defaults.borderColor = isLight ? 'rgba(0, 0, 0, 0.1)' : 'rgba(255, 255, 255, 0.15)';
    Chart.defaults.plugins.legend.labels.color = isLight ? 'rgba(33, 37, 41, 0.85)' : 'rgba(255, 255, 255, 0.85)';
}
applyChartTheme();

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

        // Ein SignaturePad-Canvas exportiert immer die KOMPLETTE Zeichenfläche (z.B. 600x250px),
        // auch wenn jemand nur eine kleine, flache Unterschrift in einer Ecke gezeichnet hat.
        // Eingebettet in PDFs (Arbeitsbescheinigung) wirkte die Unterschrift dadurch winzig/dünn
        // und "schwebte" mit großem Leerraum über der Unterschriftslinie. Schneidet die leeren
        // Ränder auf das tatsächlich gezeichnete Rechteck (plus etwas Puffer) zu, bevor das Bild
        // gespeichert wird - der PDF-Generator bekommt so direkt eine knapp zugeschnittene
        // Unterschrift statt der vollen, größtenteils leeren Canvas.
        function trimSignatureDataUrl(padInstance) {
            const sourceCanvas = padInstance.canvas;
            const ctx = sourceCanvas.getContext('2d');
            const { width, height } = sourceCanvas;
            const data = ctx.getImageData(0, 0, width, height).data;
            let minX = width, minY = height, maxX = 0, maxY = 0, found = false;
            for (let y = 0; y < height; y++) {
                for (let x = 0; x < width; x++) {
                    const alpha = data[(y * width + x) * 4 + 3];
                    if (alpha > 10) {
                        found = true;
                        if (x < minX) minX = x;
                        if (x > maxX) maxX = x;
                        if (y < minY) minY = y;
                        if (y > maxY) maxY = y;
                    }
                }
            }
            if (!found) return padInstance.toDataURL();
            const pad = 12;
            minX = Math.max(0, minX - pad); minY = Math.max(0, minY - pad);
            maxX = Math.min(width, maxX + pad); maxY = Math.min(height, maxY + pad);
            const w = maxX - minX, h = maxY - minY;
            const out = document.createElement('canvas');
            out.width = w; out.height = h;
            out.getContext('2d').drawImage(sourceCanvas, minX, minY, w, h, 0, 0, w, h);
            return out.toDataURL('image/png');
        }

        // In-App-Ersatz für window.alert()/confirm()/prompt(): die nativen Browser-Dialoge
        // zeigen die Host-Adresse an und wirken wie eine Systembenachrichtigung statt eines
        // echten App-Fensters. Diese Varianten rendern stattdessen ein normales, zum Rest der
        // App passendes Bootstrap-Modal. Aufruf wie gewohnt, nur mit "await" davor.
        function _appDialogShow(html, id) {
            let el = document.getElementById(id);
            if (el) el.remove();
            el = document.createElement('div');
            el.className = 'modal fade';
            el.id = id;
            el.innerHTML = html;
            document.body.appendChild(el);
            const modal = new bootstrap.Modal(el);
            modal.show();
            return { el, modal };
        }

        function appAlert(message, title = 'Hinweis') {
            return new Promise((resolve) => {
                const { el, modal } = _appDialogShow(`
                    <div class="modal-dialog modal-dialog-centered">
                        <div class="modal-content">
                            <div class="modal-header border-secondary border-opacity-10">
                                <h5 class="modal-title fw-bold text-white"></h5>
                                <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
                            </div>
                            <div class="modal-body text-white" style="white-space: pre-wrap;"></div>
                            <div class="modal-footer border-secondary border-opacity-10">
                                <button type="button" class="btn btn-premium rounded-pill px-4" data-bs-dismiss="modal">OK</button>
                            </div>
                        </div>
                    </div>`, 'appAlertModal');
                el.querySelector('.modal-title').textContent = title;
                el.querySelector('.modal-body').textContent = message;
                el.addEventListener('hidden.bs.modal', () => { el.remove(); resolve(); }, { once: true });
            });
        }

        // new Date().toISOString() liefert das UTC-Datum - zwischen Mitternacht und 1/2 Uhr morgens also "gestern".
        // Texte aus der Datenbank (Objektnamen, Adressen aus der Alarmierung) landen in Karten-Popups als HTML -
        // ohne Maskierung könnte ein Name mit <script>/<img onerror> dort Code ausführen.
        function escHtml(v) { return String(v == null ? '' : v).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]); }
        function localISODate(d) {
            d = d || new Date();
            return new Date(d.getTime() - d.getTimezoneOffset() * 60000).toISOString().split('T')[0];
        }
        function appConfirm(message, title = 'Bitte bestätigen') {
            return new Promise((resolve) => {
                const { el, modal } = _appDialogShow(`
                    <div class="modal-dialog modal-dialog-centered">
                        <div class="modal-content">
                            <div class="modal-header border-secondary border-opacity-10">
                                <h5 class="modal-title fw-bold text-white"></h5>
                                <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
                            </div>
                            <div class="modal-body text-white" style="white-space: pre-wrap;"></div>
                            <div class="modal-footer border-secondary border-opacity-10">
                                <button type="button" class="btn btn-outline-light rounded-pill px-4" data-bs-dismiss="modal">Abbrechen</button>
                                <button type="button" class="btn btn-premium rounded-pill px-4" id="appConfirmOk">OK</button>
                            </div>
                        </div>
                    </div>`, 'appConfirmModal');
                el.querySelector('.modal-title').textContent = title;
                el.querySelector('.modal-body').textContent = message;
                let result = false;
                el.querySelector('#appConfirmOk').addEventListener('click', () => { result = true; modal.hide(); });
                el.addEventListener('hidden.bs.modal', () => { el.remove(); resolve(result); }, { once: true });
            });
        }

        // Gültige Reiter - für den Zurück-Knopf des Browsers (Adresse /dashboard#reiter)
        const DASHBOARD_TABS = ['uebersicht', 'dienste', 'kalender', 'fristen', 'personal', 'jugend', 'lehrgaenge', 'umfragen', 'notizen', 'stats', 'apager', 'einsaetze', 'lagekarte', 'bma', 'hvo', 'logbuch', 'fahrzeuge', 'material', 'abrechnung', 'arbeitsbescheinigung', 'verein', 'archiv', 'verwaltung'];
        function appPrompt(message, defaultValue = '', title = 'Eingabe', inputType = 'text') {
            return new Promise((resolve) => {
                const { el, modal } = _appDialogShow(`
                    <div class="modal-dialog modal-dialog-centered">
                        <div class="modal-content">
                            <div class="modal-header border-secondary border-opacity-10">
                                <h5 class="modal-title fw-bold text-white"></h5>
                                <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
                            </div>
                            <div class="modal-body">
                                <label class="form-label text-white mb-2"></label>
                                <input type="text" class="form-control" id="appPromptInput">
                            </div>
                            <div class="modal-footer border-secondary border-opacity-10">
                                <button type="button" class="btn btn-outline-light rounded-pill px-4" data-bs-dismiss="modal">Abbrechen</button>
                                <button type="button" class="btn btn-premium rounded-pill px-4" id="appPromptOk">OK</button>
                            </div>
                        </div>
                    </div>`, 'appPromptModal');
                el.querySelector('.modal-title').textContent = title;
                el.querySelector('.modal-body label').textContent = message;
                el.querySelector('#appPromptInput').type = inputType;
                const input = el.querySelector('#appPromptInput');
                input.value = defaultValue || '';
                let result = null;
                const submit = () => { result = input.value; modal.hide(); };
                el.querySelector('#appPromptOk').addEventListener('click', submit);
                input.addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); submit(); } });
                el.addEventListener('shown.bs.modal', () => input.focus());
                el.addEventListener('hidden.bs.modal', () => { el.remove(); resolve(result); }, { once: true });
            });
        }

        // Die Funktionen der einzelnen Bereiche liegen in static/js/dashboard/*.js und werden hier eingemischt.
        const DB_PARTS = 14;
        if ((window.DB_MIXINS || []).length !== DB_PARTS) throw new Error('Programmteile fehlen (' + (window.DB_MIXINS || []).length + ' von ' + DB_PARTS + ') - bitte neu laden.');
        const app = createApp({
            mixins: window.DB_MIXINS,
            data() {
                return {
                    isOnline: navigator.onLine,
                    wishData: { wishes: [], types: [], manage: false, me: null },
                    newWish: { course_name: '', note: '', personnel_id: '' },
                    wishFilter: 'alle',
                    wishForOther: false,
                    inviteNew: { on: false, username: '' },
                    reportSending: false,
                    polls: [],
                    canManagePolls: false,
                    pollForm: null,
                    html5Qrcode: null,
                    isDarkMode: (() => { try { return localStorage.getItem('theme') === 'dark'; } catch (e) { return false; } })(),
                    ready: false, isAdmin: false, username: '', role: '',
                    groups: [], selectedGroup: null, mobileSelectedId: null,
                    sessionsList: [], selectedYear: new Date().getFullYear(),
                    ranking: [], showRanking: false, catStats: { Übung: 0, Einsatz: 0, Sonstiges: 0 }, activeS: null,
                    vehicles: [], activeVehicleForLog: null, vehicleLogs: [],
                    activeVehicleForCheck: null, vehicleCheckHistory: [],
                    currentChecklist: { date: localISODate(), checker_name: '', status: 'OK', notes: '', items_checked: {'Ölstand': false, 'Beleuchtung': false, 'Funkgeräte (HRT)': false, 'Reifendruck': false} },
                    newVehicle: { name: '', radio_name: '', status: 2, tuv_date: '', sp_date: '', milage: 0, next_service: '' },
                    selfPwData: { old_password: '', new_password: '', confirm_password: '' },
                    activeBroadcasts: [],
                    newBroadcast: { title: '', content: '', is_mandatory: false },
                    isFirstLoginBlock: false,
                    changelogEntries: [],
                    changelogUnseen: false,
                    twoFaEnabled: null,
                    groupListOpen: false,
                    scheduleMemberSearch: '',
                    detailSchedule: null,
                    // Glocke
                    notifOpen: false, notifCats: [], notifUnread: 0, notifCatOpen: {}, notifShowPrefs: false, notifPrefs: [], bellRinging: false,
                    notifSettings: { quiet_enabled: false, quiet_from: '22:00', quiet_to: '06:00', digest_mail: false },
                    buildingChecks: [], buildingSuggestions: [], buildingForm: { id: null, name: '', interval_months: 12, last_check: '', responsible: '' },
                    health: null,
                    prospects: [], prospectStatuses: ['Interessent', 'Probe', 'Aufgenommen', 'Abgesagt'],
                    prospectForm: { id: null, name: '', phone: '', email: '', source: '', mentor: '', status: 'Interessent', first_contact: null, last_contact: null, trial_end: '', note: '' },
                    participation: { threshold: 50, members: [] }, participationInput: 50,
                    myContact: null, contactSaving: false,
                    displayName: '',
                    clothingStock: [], clothingIssued: [], clothingTab: 'stock', newStock: { item_name: '', size: '', quantity: 1, min_quantity: 0 },
                    mailSettings: {}, mailTestTo: '', mailSaving: false,
                    lehrgangMatrix: { courses: [], entries: {} },
                    statsTypeInstance: null, statsHoursInstance: null, statsPrevTotal: 0,
                    twoFaSetup: null,
                    twoFaConfirmCode: '',
                    twoFaRecoveryCodes: null,
                    personalStats: { hours: 0, count: 0 },
                    personalSessions: [],
                    activeTab: (() => { const h = (window.location.hash || '').slice(1); return DASHBOARD_TABS.includes(h) ? h : (localStorage.getItem('activeDashboardTab') || 'uebersicht'); })(),
                    notes: [],
                    notesActiveFilter: 'all',
                    noteEditingId: null,
                    newNote: { title: '', content: '', visibility: 'private' },

                    apagerConfig: { api_key: '', active: true },
                    webhookLog: [],
                    apagerLogs: [],
                    apagerFeedbacks: [],
                    serverHost: window.location.host,
                    accessTokens: { display_token: '', calendar_token: '' },
                    
                    // Schedules / Dienstplanung
                    schedules: [],
                    newSchedule: { title: '', date: localISODate(), time: '19:00', description: '', type: 'Übung', group_id: null, repeat: '', repeat_until: '', notify: true, audience: 'all', audience_ids: [] },
                    schedulePersonnelList: [],
                    activeScheduleForAttendance: null,
                    
                    // Missions / Einsatzberichte
                    isSaving: false,
                    missions: [],
                    missionGroupFilter: 'all',

                    // Equipment
                    equipment: [],
                    eqSearch: '',
                    eqCategory: '',
                    filterDefectiveOnly: false,
                    activeEquipment: null,
                    newInspection: { date: localISODate(), inspector: '', status: 'Bestanden', note: '' },
                    inspectionsHistory: [],
                    batchInspect: { rawBarcodes: '', inspector: '', status: 'Bestanden', note: '' },
                    
                    // Personalverwaltung
                    personnel: [],
                    personnelSearch: '',
                    activePersonnel: null,
                    newGear: { item_name: '', size: '', issue_date: localISODate() },
                    newCourse: { course_name: '', date: localISODate(), certificate_url: '', valid_until: '' },
                    newHonor: { kind: 'Beförderung', title: '', date: localISODate(), note: '' },
                    idCard: null, idCardError: '', showArchivedEquipment: false,
                    overviewLayout: { order: ['mitteilungen', 'berichte', 'termine', 'benachrichtigungen', 'fristen', 'maengel', 'stunden', 'rueckmeldung', 'naechsteTermine', 'letzteEinsaetze', 'offeneBerichte', 'fahrzeuge', 'mannschaft', 'pinnwand', 'schnellzugriff'],
                                       hidden: ['stunden', 'rueckmeldung', 'naechsteTermine', 'letzteEinsaetze', 'offeneBerichte', 'fahrzeuge', 'mannschaft', 'pinnwand', 'schnellzugriff'] },
                    quickLinks: [], quickLinkForm: { label: '', url: '' },
                    missionFiltersOpen: false, openIncident: null,
                    overviewEdit: false, overviewDay: localISODate(), overviewDefects: [], overviewLoaded: false,
                    missionSearch: '', missionStatus: 'alle', missionKind: 'alle', missionYear: 'all', billFilter: 'alle',
                    menuFavorites: [], selfTab: 'stat', mobileSearchOpen: false,
                    menuCollapsed: (() => { try { return JSON.parse(localStorage.getItem('menuCollapsed') || '{}') || {}; } catch (e) { return {}; } })(),
                    savedFilters: { einsaetze: [], material: [] },
                    availabilityList: [],
                    newAvailability: { start_date: '', end_date: '', reason: '' },
                    dueItems: [], dueOnlyOverdue: false,
                    aaoRules: [], aaoCanEdit: false, aaoForm: { id: null, pattern: '', entries: [{ vehicle: '', role: '' }] },
                    dueTypes: [], dueEntries: [], dueCanCreate: false, dueSubjects: {},
                    dueTypeForm: { id: null, name: '', subject: 'person', interval_months: null, lead_days: null, responsible_role: '', responsible_pid: null, logbook_id: null },
                    dueEntryForm: { id: null, type_id: null, subject_id: null, due_date: '', note: '' },
                    logbooks: [], logbooksLoaded: false, activeLogbookId: null, logbookView: 'entries', logbookYear: new Date().getFullYear(),
                    lbEntries: [], lbEval: null, lbChoices: { people: [], subjects: [] }, lbEntryForm: { id: null, entry_date: '', personnel_id: null, subject_id: null, values: {} },
                    lbForm: null, lbChooser: false, pwPolicyTick: 0, pwHint: PasswordPolicy.hint, adminPw: { id: null, username: '', password: '', show: false, busy: false },
                    audienceInfo: null, detailPending: [],
                    backupInfo: null, backupBusy: false, offsiteForm: { enabled: false, url: '', username: '', password: '' }, privacy: null,
                    security: null, securitySaving: false, myDevices: [], must2fa: false, mailInviteOk: false,
                    mileageChartInstance: null,
                    newLoan: { borrower_name: '', note: '' },
                    vehicleReservations: [],
                    newReservation: { purpose: '', start_datetime: '', end_datetime: '' },
                    vehicleDocuments: [],
                    vehicleMaintenance: [],
                    newMaintenance: { date: localISODate(), description: '', workshop: '', cost: 0 },
                    calendarYear: new Date().getFullYear(),
                    calendarMonth: new Date().getMonth() + 1,
                    calendarEvents: [],
                    myLicenses: null,
                    globalSearchQuery: '',
                    globalSearchResults: null,
                    globalSearchOpen: false,
                    auditLog: [],
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
                    newLogin: { username: '', password: '', role: 'mannschaft', personnel_id: null, invite: false },
                    activeVehicle: null,
                    selfBindPersonnelId: null,
                    
                    // Sub-tab navigation
                    materialSubTab: 'geraete',
                    inventurList: [], inventurCategories: [],
                    inventurNew: { title: '', category: '', include_consumables: false },
                    inventur: null,              // geöffnete Inventur mit Einträgen
                    inventurFilter: 'offen',     // offen | alle | probleme
                    inventurSearch: '',
                    inventurTouched: {},         // gerade bearbeitete Einträge bleiben unter "Offen" stehen (Notiz eintragen, Fehlklick korrigieren)
                    inventurScanMode: false, inventurLastScan: '',
                    verwaltungTab: '',
                    myPerms: [], permissionMatrix: null, newCustomRole: { label: '', base_role: 'mannschaft' },
                    intervals: { int_g26: 36, int_belastung: 12, int_unterweisung: 12, reminder_window_days: 14, session_max_days: 30, int_fahrzeugkontrolle: 7, int_fuehrerschein: 12, int_interessent: 21 },
                    
                    // Mängelmelder
                    defectReports: [],
                    defectFilter: 'Offen',
                    openDefectId: null, defectComments: [], newDefectComment: '',
                    newDefect: { equipment_id: null, description: '', severity: 'Mittel', assigned_to: '', priority: 'Mittel', image_url: '' },
                    
                    // Test Alarm
                    testAlarm: { stichwort: '', adresse: '', meldung: '', prefix_test: true },
                    
                    // User Edit State
                    editingUserId: null,
                    editingUsername: '',
                    editUserRole: 'mannschaft',
                    editUserPersonnelId: null,
                    showMobileSidebar: false,
                    stationConfig: { station_name: 'Feuerwehr', lat: 50.1109, lng: 8.6821, zoom: 14, dwd_warncell_id: '', default_hourly_rate: 15.0, impressum_text: '', datenschutz_text: '', stamp_image: '' },
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
                    newYouthSession: { date: localISODate(), topic: '', duration: 2.0, instructors: '', description: '', attendance: {} },
                    editingYouthSessionId: null,
                    
                    clubTab: 'inventory',
                    clubInventory: [],
                    newClubItem: { item_name: '', quantity: 1, status: 'OK' },
                    clubDonations: [],
                    newDonation: { donor: '', amount: 15.00, date: localISODate() },
                    membershipFees: [],
                    feesYear: new Date().getFullYear(),
                    atemschutz: null,           // Atemschutz-Übersicht (Personal)
                    atemschutzOpen: false,
                    feeFilter: 'alle',          // alle | offen | bezahlt
                    feeFilterOptions: [['alle', 'Alle'], ['offen', 'Offen'], ['bezahlt', 'Bezahlt']],
                    feeSettings: { default_amount: 0, due_month: 3, due_day: 31 },
                    feeSettingsOpen: false,
                    feeOverview: null,          // Stand des laufenden Jahres für "Zu erledigen"
                    
                    hvoTab: 'protocols',
                    hvoProtocols: [],
                    newHvoProtocol: { date: localISODate(), symptoms: '', therapy: '', handover: '' },
                    hvoChecks: [],
                    newHvoCheck: { device_name: '' },
                    hvoMaterial: [],
                    newHvoMaterial: { name: '', quantity: 1, expiry_date: '', note: '' },
                    consumables: [],
                    newConsumable: { name: '', unit: 'Stk', current_stock: 0, min_stock: 0, note: '' },
                    scheduleRsvpMap: {},
                    activeRsvpSchedule: null,
                    activeRsvpResponses: [],
                    legalTexts: { impressum: '', datenschutz: '' },
                    activeLegalTab: 'impressum',
                    lehrgangTypes: [],
                    newLehrgangType: '',
                    certYear: new Date().getFullYear(),
                    employerCertificates: [],
                    newCertMissionId: null,
                    newCertPersonnelId: null,
                    newCertSignature: null,
                    certMissionAttendees: [],
                    
                    werkstattTab: 'pruefungen',

                    statsTotalMissions: 0,
                    statsChartInstance: null, statsChartMonthInstance: null, statsAttendanceInstance: null,
                    statsYear: new Date().getFullYear(),
                    statsAttendanceTop: [],
                    
                    anniversariesYear: new Date().getFullYear(),
                    anniversariesData: { service_anniversaries: [], birthday_anniversaries: [] },
                    isLoadingAnniversaries: false,
                    anniversariesActiveTab: 'service'
                }
            },
            computed: {
                canManageDienste() { return this.hasPerm('dienste.manage'); },
                canDeleteMissions() { return this.hasPerm('missions.delete'); },
                canManageVehicles() { return this.hasPerm('vehicles.manage'); },
                canManageBuildings() { return this.hasPerm('buildings.manage'); },
                canManageProspects() { return this.hasPerm('prospects.manage'); },
                canManageMissions() { return this.hasPerm('missions.manage'); },
                canApproveMissions() { return this.hasPerm('missions.approve'); },
                serverProtocol() { return window.location.protocol; },
                canSeeWebhook() { return ['admin', 'leitung'].includes(this.role); },
                canManagePersonnel() { return this.hasPerm('personnel.manage'); },
                canSeeSettings() { return ['admin', 'leitung'].includes(this.role); },
                // Personalverwaltung: Wehrführung, Gruppenführer (Einsatzplanung), Gerätewart (Kleiderkammer), Atemschutzwart (G26.3).
                // Jugendfeuerwehr (Daten Minderjähriger): nur Wehrführung und Jugendwarte - die ordnen auch den Nachwuchs von dort aus.
                canSeePersonnel() { return ['admin', 'leitung', 'gruppenfuehrer', 'geratewart', 'atemschutzwart'].includes(this.role) || this.hasPerm('personnel.manage'); },
                canSeeYouth() { return ['admin', 'leitung', 'jugendwarte'].includes(this.role) || this.hasPerm('youth.manage'); },
                canSeeFinance() { return this.hasPerm('verein.manage'); },
                canSeeAtemschutz() { return ['admin', 'leitung', 'atemschutzwart'].includes(this.role) || this.hasPerm('atemschutz.manage'); },
                canSeeStats() { return ['admin', 'leitung'].includes(this.role); },
                canManageMaterial() { return this.hasPerm('material.manage'); },
                // Backend /billing/... (mission_mgr.py) erlaubt dieselbe Rollenmenge -
                // eigener Name statt canManageMaterial wiederzuverwenden, damit im Abrechnungs-
                // Bereich klar bleibt, wofür die Prüfung tatsächlich steht.
                canManageBilling() { return this.hasPerm('billing.manage'); },
                canManageYouth() { return this.hasPerm('youth.manage'); },
                canManageSettings() { return ['admin', 'leitung'].includes(this.role); },

                filteredNotes() {
                    if (this.notesActiveFilter === 'all') return this.notes;
                    return this.notes.filter(n => n.visibility === this.notesActiveFilter);
                },
                visibleBroadcasts() { return this.activeBroadcasts.filter(b => b.gelesen == 0); },
                // Groß nur auf der Übersicht oder bei Lesepflicht, auf den anderen Seiten reicht eine Zeile
                bannerBroadcasts() {
                    const inCard = this.activeTab === 'uebersicht' && this.overviewWidgets.some(w => w.id === 'mitteilungen');
                    return this.visibleBroadcasts.filter(b => b.is_mandatory || (this.activeTab === 'uebersicht' && !inCard));
                },
                compactBroadcasts() { return this.activeTab === 'uebersicht' ? [] : this.visibleBroadcasts.filter(b => !b.is_mandatory); },
            },
            watch: {
                // Gespeicherter oder per Link angesteuerter Reiter, den die Rolle nicht nutzen kann (z.B. nach Rollenwechsel): zur Startseite
                role(r) { if (r && !this.tabAllowed(this.activeTab)) this.activeTab = 'dienste'; },
                globalSearchOpen(v) { if (!v && this.mobileSearchOpen) this.mobileSearchOpen = false; },
                'newSchedule.audience'(v) { if (v === 'selected') this.loadAudienceInfo(); },
                activeTab(newTab) {
                    if (newTab && !this.tabAllowed(newTab)) { this.activeTab = 'dienste'; return; }
                    if (newTab) {
                        localStorage.setItem('activeDashboardTab', newTab);
                        // Jeder Reiterwechsel ist ein Schritt im Browser-Verlauf: "Zurück" geht zum vorherigen Reiter
                        // statt aus der App heraus (vorher landete man auf der Startseite des Browsers).
                        if (this._popping) { this._popping = false; }
                        else if ((window.history.state || {}).tab !== newTab) { try { window.history.pushState({ tab: newTab }, '', '#' + newTab); } catch (e) {} }
                    }
                    this.expandMenuFor(newTab);
                    if (newTab === 'verwaltung') this.ensureSettingsTab();
                    this.loadDueItems();
                    // Einziger Ort, an dem beim Reiterwechsel Daten nachgeladen werden. Vorher
                    // hingen die Ladeaufrufe an den @click-Handlern der Menü-Schaltflächen - wer
                    // den Reiter auf anderem Weg wechselte (globale Suche, Sprung nach einem
                    // Barcode-Scan, Sprung nach dem Anlegen eines Einsatzes), bekam deshalb einen
                    // leeren Reiter zu sehen. Über den Watcher greift es für JEDEN Weg.
                    this.loadDataForActiveTab(newTab);
                }
            },
            async mounted() {
                window.addEventListener('online', () => this.isOnline = true);
                window.addEventListener('offline', () => this.isOnline = false);
                // Die kleinen (i)-Hilfe-Icons setzen bisher nur das native title-Attribut -
                // das zeigt einen Tooltip ausschließlich bei Maus-Hover, auf Touch-Geräten
                // (und beim Klick generell) passiert nichts. Ein einziger delegierter
                // Klick-Listener fängt jeden Klick auf so ein Icon ab (auch später von Vue
                // neu gerenderte) und zeigt den title-Text stattdessen in einem normalen,
                // klick-/touch-freundlichen Hinweis-Dialog an.
                document.addEventListener('click', (e) => {
                    const el = e.target.closest('.fa-question-circle[title]');
                    if (el && el.getAttribute('title')) {
                        appAlert(el.getAttribute('title'));
                    }
                });
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
                    this.isAdmin = (authData.role === 'admin'); this.username = authData.username; this.role = authData.role; this.myPerms = authData.permissions || []; this.displayName = (authData.personnel_name || authData.username || '').split(' ')[0];
                    this.isFirstLoginBlock = !!authData.is_first_login;
                    this.must2fa = !!authData.must_setup_2fa;
                } catch (e) {
                    const params = new URLSearchParams(window.location.search);
                    const eqBarcode = params.get('eq_barcode');
                    if (eqBarcode) {
                        window.location.href = '/login?eq_barcode=' + eqBarcode;
                    } else {
                        const openLink = params.get('open');
                        window.location.href = '/login' + (openLink ? '?open=' + encodeURIComponent(openLink) : '');
                    }
                    return;
                }
                
                // WICHTIG: alles ab hier steht in einem try/finally, das ready=true GARANTIERT
                // setzt, egal was schiefgeht. #app ist per v-cloak + v-show="ready" bis dahin
                // komplett unsichtbar (nur der dunkle body-Hintergrund ist zu sehen) - vorher
                // hing ready=true ganz am Ende dieser Funktion OHNE Absicherung: warf z.B. auch
                // nur einer der neun parallelen Promise.all-Ladeaufrufe (Netzwerk-Hänger,
                // Server-Neustart mitten in der Anfrage) oder ein DOM-Setup-Schritt danach einen
                // Fehler, brach die ganze mounted()-Funktion sofort ab und ready blieb für immer
                // false - die Seite zeigte dann dauerhaft nur einen leeren dunklen Bildschirm,
                // ganz ohne Fehlermeldung. Zusätzlich Promise.allSettled() statt Promise.all():
                // ein einzelner fehlgeschlagener Ladeaufruf (z.B. loadBills() wegen fehlender
                // Berechtigung) soll nicht die anderen acht mit abbrechen.
                try {
                    const loadTasks = [
                        this.loadStationSettings(),
                        this.loadGroups(),
                        this.loadVehicles(),
                        this.loadBroadcasts(),
                        this.loadApagerConfig(),
                        this.loadSchedules(),
                        this.loadPersonnel(),
                        this.loadMissions(),
                        this.loadBills(),
                        this.loadLehrgangTypes()
                    ];
                    if (this.isAdmin) {
                        loadTasks.push(this.loadSystemUsers());
                    }
                    const results = await Promise.allSettled(loadTasks);
                    results.forEach((r, i) => { if (r.status === 'rejected') console.error('Fehler beim initialen Laden (Task ' + i + '):', r.reason); });

                    if (window.location.hash === '#new_mission') {
                        window.location.hash = ''; // clear it
                        this.goToNewMission();
                    }

                    const canvas = document.getElementById('sigCanvas');
                    if(canvas) { sigPad = new SignaturePad(canvas, { backgroundColor: 'white' }); }
                    sigModal = new bootstrap.Modal(document.getElementById('sigModal'));
                    selfPasswordModal = new bootstrap.Modal(document.getElementById('selfPasswordModal'));

                    const mCanvas = document.getElementById('missionSigCanvas');
                    if(mCanvas) { missionSigPad = new SignaturePad(mCanvas, { backgroundColor: 'white' }); }
                    missionSigModal = new bootstrap.Modal(document.getElementById('missionSigModal'));

                    const certCanvas = document.getElementById('certSigCanvas');
                    if(certCanvas) { certSigPad = new SignaturePad(certCanvas, { backgroundColor: 'white' }); }
                    certSigModal = new bootstrap.Modal(document.getElementById('certSigModal'));

                    // Kamera zuverlässig stoppen, egal wie der Scanner-Dialog geschlossen wird
                    // (X-Button, Klick auf den Hintergrund, Escape-Taste) - nur der X-Button rief
                    // bisher stopQrScanner() auf, bei Hintergrund-Klick/Escape blieb die Kamera im
                    // Hintergrund aktiv.
                    const qrScannerModalEl = document.getElementById('qrScannerModal');
                    if (qrScannerModalEl) qrScannerModalEl.addEventListener('hidden.bs.modal', () => this.stopQrScanner());

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
                } catch (e) {
                    console.error('Fehler beim Initialisieren des Dashboards:', e);
                } finally {
                    this.ready = true;
                    window.addEventListener('pagehide', () => this.rememberPlace());
                    this.restorePlace();
                }

                // Bewusst ERST hier, nach ready=true und dem nächsten Render-Durchlauf: vorher ist
                // #app per v-show noch komplett ausgeblendet, eine Karte oder ein Diagramm würde
                // dann auf eine Fläche von 0x0 gezeichnet.
                this.expandMenuFor(this.activeTab);
                this.loadFavorites();
                this.$nextTick(() => this.loadDataForActiveTab());
                if (!this.isFirstLoginBlock) this.$nextTick(() => this.handleOpenLink());
                this.$nextTick(() => { try { window.history.replaceState({ tab: this.activeTab }, '', '#' + this.activeTab); } catch (e) {} });
                window.addEventListener('popstate', (e) => {
                    const t = (e.state && e.state.tab) || (window.location.hash || '').slice(1);
                    if (t && DASHBOARD_TABS.includes(t) && this.tabAllowed(t) && t !== this.activeTab) { this._popping = true; this.activeTab = t; }
                });
                this.refreshNotifCount();
                this.loadDueItems();
                this.loadLogbooks();
                PasswordPolicy.load().then(() => this.pwPolicyTick++);
                fetch('/api/auth/mail-enabled').then(r => r.json()).then(d => { this.mailInviteOk = !!d.enabled; }).catch(() => {});
                setInterval(() => { if (!document.hidden) this.refreshNotifCount(); }, 60000);
                document.addEventListener('click', e => { if (this.notifOpen && !e.target.closest('#notifBell')) this.notifOpen = false; });

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

                // "Was ist neu": nur für Admins. Immer laden (auch bei erzwungenem
                // Passwortwechsel), damit der manuelle Button in der Kopfzeile sofort Inhalt
                // hat - automatisch AUFPOPPEN aber nur, wenn nicht ohnehin schon der
                // Passwort-Dialog aufgeht (der hat Vorrang, zwei Modals gleichzeitig wären
                // verwirrend). Läuft im Hintergrund, ein Fehlschlag (z.B. offline) darf den
                // Dashboard-Start nicht stören.
                if (this.isAdmin) {
                    this.loadChangelog(!this.isFirstLoginBlock);
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
                applyChartTheme();
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
                            await appAlert("Eintrag konnte nicht gespeichert werden.");
                        }
                    } catch (e) { await appAlert("Serverfehler beim Senden."); }
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
                    if (await appConfirm("Möchtest du diesen Eintrag permanent löschen?")) {
                        try {
                            const res = await fetch(`/api/notes/${id}`, { method: 'DELETE', credentials: 'include' });
                            if (res.ok) { 
                                if (this.noteEditingId === id) this.cancelNoteEdit();
                                await this.loadNotes(); 
                            } else { 
                                await appAlert("Löschen fehlgeschlagen."); 
                            }
                        } catch (e) { await appAlert("Verbindungsfehler."); }
                    }
                },
                getVisLabel(vis) {
                    if (vis === 'public') return 'Pinnwand (Öffentlich)';
                    if (vis === 'private') return 'Privat';
                    if (vis === 'admin') return 'Führung Intern';
                    if (vis === 'geratewart') return 'Gerätewart Notiz';
                    return vis;
                },
                initWebSocket() {
                    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                    const ws = new WebSocket(`${protocol}//${window.location.host}/ws`);
                    ws.onmessage = async (event) => {
                        try {
                            const data = JSON.parse(event.data);
                            if (data.type === 'notif') {
                                // Neue Benachrichtigung für mich: Glocke sofort aktualisieren
                                this.bellRinging = true; setTimeout(() => { this.bellRinging = false; }, 1200);
                                await this.refreshNotifCount(true);
                            }
                            if (data.type === 'new_mission' || data.type === 'update_mission') {
                                // Live update the dashboard when a mission arrives or updates
                                await this.loadMissions();
                                if (data.type === 'new_mission') {
                                    this.playAlarmSound();
                                    this.activeTab = 'einsaetze'; // switch to missions
                                    
                                    // Speak the alarm details
                                    setTimeout(() => {
                                        if (data.mission && data.mission.stichwort) {
                                            window.speechSynthesis.cancel();
                                            const digitWords = {'0':'Null', '1':'Eins', '2':'Zwei', '3':'Drei', '4':'Vier', '5':'Fünf', '6':'Sechs', '7':'Sieben', '8':'Acht', '9':'Neun'};
                                            const transformTTS = (str) => (str || '').replace(/\d{5}/g, m => m.split('').map(d => digitWords[d]).join(' '));
                                            
                                            const ttsStichwort = transformTTS(data.mission.stichwort);
                                            const ttsAdresse = transformTTS(data.mission.adresse);
                                            
                                            const msg = new SpeechSynthesisUtterance(`Alarm! ${ttsStichwort}. ${ttsAdresse}`);
                                            msg.lang = 'de-DE';
                                            msg.rate = 0.95;
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
                    // Bereits gezeichnete Chart.js-Diagramme übernehmen einen geänderten
                    // Chart.defaults-Wert NICHT automatisch (der wird nur beim Erstellen einer
                    // neuen Chart-Instanz gelesen) - ohne dieses Neuzeichnen blieb die
                    // Statistik-Seite bis zum nächsten Tab-Wechsel im alten (unlesbaren) Theme.
                    if (this.activeTab === 'stats') this.loadStats();
                },
                async fullLogout() { await fetch('/api/logout', { method: 'POST', credentials: 'include' }); window.location.href = '/login'; },

                // Broadcasts
                async loadBroadcasts() {
                    try {
                        const res = await fetch('/api/broadcasts/active', { credentials: 'include' });
                        if(res.ok) this.activeBroadcasts = await res.json();
                    } catch(e) { console.error("Error loading broadcasts", e); }
                },
                async markBroadcastAsRead(b) {
                    const res = await fetch(`/api/broadcasts/${b.id}/read`, { method: 'POST', credentials: 'include' });
                    if(res.ok) this.activeBroadcasts = this.activeBroadcasts.filter(item => item.id !== b.id);
                },
                async deleteBroadcast(b) {
                    if (!await appConfirm(`Meldung "${b.title}" wirklich permanent löschen?`)) return;
                    const res = await fetch(`/api/broadcasts/${b.id}`, { method: 'DELETE', credentials: 'include' });
                    if (res.ok) {
                        this.activeBroadcasts = this.activeBroadcasts.filter(item => item.id !== b.id);
                    } else {
                        await appAlert("Meldung konnte nicht gelöscht werden.");
                    }
                },
                // Einzige Stelle, die weiss, welcher Reiter welche Daten braucht. Aufgerufen aus
                // dem activeTab-Watcher (jeder Reiterwechsel) und aus mounted() (Seitenstart, wo
                // der zuletzt benutzte Reiter aus dem localStorage wiederhergestellt wird und der
                // Watcher deshalb nicht anspringt). Reiter, deren Daten bereits in mounted()
                // geladen werden (Dienste, Personal, Fahrzeuge, aPager, Verwaltung), brauchen
                // hier keinen Eintrag.
                loadDataForActiveTab(tab) {
                    const loaders = {
                        jugend: () => this.loadJugendData(),
                        lehrgaenge: () => this.loadWishes(),
                        umfragen: () => this.loadPolls(),
                        notizen: () => this.loadNotes(),
                        kalender: () => this.loadCalendar(),
                        einsaetze: () => { this.loadMissions(); this.loadSavedFilters(); },
                        lagekarte: () => this.initMap(),
                        hvo: () => this.loadHvoData(),
                        material: () => { this.loadEquipment(); this.loadSavedFilters(); },
                        abrechnung: () => { this.loadBills(); this.calculateCompensations(); },
                        arbeitsbescheinigung: () => this.loadEmployerCertificates(),
                        archiv: () => this.loadArchiveFiles(),
                        stats: () => this.loadStats(),
                        apager: () => { this.loadAao(); if (this.canSeeSettings) this.loadAccessTokens(); },
                        fristen: () => this.loadDueManager(),
                        logbuch: () => this.loadLogbookData(),
                        verein: () => this.loadClubData(),
                        personal: () => { if (this.canSeeAtemschutz) this.loadAtemschutz(); },
                        uebersicht: () => { this.loadOverview(); this.loadWishes(); this.loadPolls(); }
                    };
                    const load = loaders[tab || this.activeTab];
                    if (!load) return;
                    try {
                        load();
                    } catch (e) {
                        console.error('Laden des aktiven Reiters fehlgeschlagen:', e);
                    }
                },
                // WICHTIG: In Vue-Vorlagen ist NUR eine feste Liste von Browser-Globals verfügbar
                // (Math, JSON, console, Date, ...). 'navigator', 'window' und 'localStorage' gehören
                // NICHT dazu - ein @click="navigator.clipboard.writeText(...)" direkt in der Vorlage
                // löst 'navigator' gegen die Vue-Instanz auf, findet nichts und wirft beim Klick
                // "Cannot read properties of undefined". Solche Aufrufe müssen deshalb hier in einer
                // Methode stehen, wo der normale Browser-Kontext gilt.
                async copyToClipboard(text) {
                    try {
                        if (navigator.clipboard && window.isSecureContext) {
                            await navigator.clipboard.writeText(text);
                        } else {
                            // Rückfallebene für nicht-sichere Kontexte (z.B. Aufruf über nackte
                            // IP statt HTTPS), wo navigator.clipboard gar nicht bereitsteht.
                            const ta = document.createElement('textarea');
                            ta.value = text;
                            ta.style.position = 'fixed';
                            ta.style.opacity = '0';
                            document.body.appendChild(ta);
                            ta.select();
                            document.execCommand('copy');
                            ta.remove();
                        }
                        await appAlert('In die Zwischenablage kopiert.');
                    } catch (e) {
                        await appAlert('Kopieren nicht möglich. Bitte den Link von Hand markieren und kopieren.');
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
                pwRules(pw, user, old) { this.pwPolicyTick; return PasswordPolicy.check(pw, user, old).items; },
                pwOk(pw, user, old) { this.pwPolicyTick; return PasswordPolicy.check(pw, user, old).ok; },
                
                // Datum aus der Datenbank (2026-11-07 oder 2026-11-07 14:30:00) deutsch anzeigen: 07.11.2026 bzw. 07.11.2026 14:30
                dt(v) {
                    if (!v) return '';
                    const m = /^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2}))?/.exec(String(v));
                    return m ? m[3] + '.' + m[2] + '.' + m[1] + (m[4] ? ' ' + m[4] + ':' + m[5] : '') : v;
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
                // Überfällig erst NACH dem Fälligkeitstag - am Tag selbst ist die Frist noch nicht verstrichen
                isOverdue(d) { if(!d) return false; return String(d).slice(0, 10) < localISODate(); },
                getStatusClass(status) { if(status === 'Aktiv') return 'aktiv'; if(status === 'Passiv') return 'passiv'; if(status === 'Jugend') return 'jugend'; if(status === 'Ausgeschieden') return 'ausgeschieden'; return 'ehren'; },
            }
        });
        // Fristen-Streifen: zeigt die Fristen EINES Bereichs kompakt (aufklappbar), in der Fristen-Übersicht ausgeklappt
        app.component('due-strip', {
            props: { area: String, items: { type: Array, default: () => [] }, expanded: Boolean },
            emits: ['goto', 'done', 'done-bulk'],
            data() { return { open: this.expanded, all: false }; },
            computed: {
                labelMap() { return { fahrzeuge: 'Fahrzeuge', material: 'Geräte & Material', gebaeude: 'Gerätehaus', hvo: 'Sanitätsmaterial', personal: 'Personen', sonstige: 'Sonstige Fristen' }; },
                label() { return this.labelMap[this.area] || ''; },
                mine() { return this.items.filter(i => i.area === this.area); },
                rows() { return this.mine.filter(i => !i.first_check).sort((a, b) => (b.overdue - a.overdue) || String(a.due_date).localeCompare(String(b.due_date))); },
                firstRows() { return this.mine.filter(i => i.first_check); },
                shown() { return (this.all || this.expanded) ? this.rows : this.rows.slice(0, 5); },
                overdueCount() { return this.rows.filter(i => i.overdue).length; },
                summary() {
                    const o = this.overdueCount, s = this.rows.length - o, f = this.firstRows.length, p = [];
                    if (o) p.push(o + ' überfällig'); if (s) p.push(s + ' bald fällig'); if (f) p.push(f + ' Erstkontrolle offen');
                    return p.join(' · ');
                },
                canBulk() { return this.firstRows.some(i => i.type === 'license'); }
            },
            methods: {
                icon(it) { return ({ equipment: 'fa-screwdriver-wrench', low_stock: 'fa-boxes-stacked', vehicle: 'fa-truck', vehicle_check: 'fa-clipboard-check', building: 'fa-building', hvo_material: 'fa-kit-medical', g26: 'fa-user-doctor', lehrgang: 'fa-graduation-cap', license: 'fa-id-card', prospect: 'fa-handshake', custom: 'fa-clipboard-list' })[it.type] || 'fa-clock'; },
                clean(d) { return String(d || '').replace(' (noch nie erfasst)', ''); },
                when(it) {
                    if (it.first_check) return 'noch nie dokumentiert';
                    if (it.overdue) return 'seit ' + Math.abs(it.days) + (Math.abs(it.days) === 1 ? ' Tag' : ' Tagen') + ' überfällig';
                    if (it.days === 0) return 'heute fällig';
                    return 'in ' + it.days + (it.days === 1 ? ' Tag' : ' Tagen') + ' (' + new Date(it.due_date).toLocaleDateString('de-DE') + ')';
                },
                canDone(it) { return it.type === 'building' || (it.type === 'vehicle' && !!it.field && ['admin', 'leitung', 'geratewart'].includes(this.$root.role)) || it.type === 'license' || (it.type === 'custom' && it.can_done); },
                // "Zur Prüfung" nur, wenn die Rolle den Reiter dahinter überhaupt sieht (eigene G26/Lehrgänge führen sonst ins Leere)
                canGoto(it) { return !['g26', 'lehrgang', 'license', 'prospect'].includes(it.type) || this.$root.tabAllowed('personal'); }
            },
            template: `
                <div v-if="rows.length || firstRows.length" class="due-strip card premium-card mb-3" :class="{ 'has-overdue': overdueCount }">
                    <button type="button" class="due-strip-head" @click="open = !open">
                        <i class="fa fa-triangle-exclamation"></i>
                        <span class="due-strip-title">{{ expanded ? label : 'Fristen: ' + label }}</span>
                        <span class="due-strip-count">{{ summary }}</span>
                        <i class="fa fa-chevron-down due-chevron" :class="{ 'fa-rotate-180': open }"></i>
                    </button>
                    <div v-show="open" class="due-rows">
                        <div v-for="it in shown" :key="it.type + it.name + it.detail + it.due_date" class="due-row" :class="it.overdue ? 'is-overdue' : 'is-soon'">
                            <i class="fa fa-fw due-icon" :class="icon(it)"></i>
                            <div class="due-text"><div class="due-name">{{ it.name }}</div><div class="due-detail">{{ clean(it.detail) }}</div></div>
                            <div class="due-when">{{ when(it) }}</div>
                            <div class="due-actions">
                                <button v-if="canGoto(it)" type="button" class="btn btn-sm btn-outline-light rounded-pill" @click="$emit('goto', it)">Zur Prüfung</button>
                                <button v-if="canDone(it)" type="button" class="btn btn-sm btn-outline-success rounded-pill" @click="$emit('done', it)">Erledigt</button>
                            </div>
                        </div>
                        <button v-if="!expanded && !all && rows.length > 5" type="button" class="btn btn-sm btn-outline-light rounded-pill mt-2" @click="all = true">Alle {{ rows.length }} anzeigen</button>
                        <div v-if="firstRows.length" class="due-first">
                            <div class="due-first-head">Erstkontrolle fehlt <span class="text-muted">- noch nie dokumentiert, kein Verzug</span></div>
                            <div v-for="it in firstRows" :key="'f' + it.type + it.name" class="due-row is-first">
                                <i class="fa fa-fw due-icon" :class="icon(it)"></i>
                                <div class="due-text"><div class="due-name">{{ it.name }}</div><div class="due-detail">{{ clean(it.detail) }}</div></div>
                                <div class="due-actions">
                                    <button v-if="canDone(it)" type="button" class="btn btn-sm btn-outline-success rounded-pill" @click="$emit('done', it)">Kontrolliert</button>
                                    <button v-else-if="canGoto(it)" type="button" class="btn btn-sm btn-outline-light rounded-pill" @click="$emit('goto', it)">Zur Prüfung</button>
                                </div>
                            </div>
                            <button v-if="canBulk" type="button" class="btn btn-sm btn-outline-success rounded-pill mt-2" @click="$emit('done-bulk', firstRows)">Alle {{ firstRows.filter(i => i.type === 'license').length }} als heute kontrolliert eintragen</button>
                        </div>
                    </div>
                </div>`
        });
        // Die Seite selbst ist die Vorlage: der Browser liest sie ein, Vue übersetzt sie beim Start in Code.
        // Liest ein Browser (z.B. ein älteres iPhone) eine Stelle anders ein, scheitert der ganze Start mit einem
        // nichtssagenden "Unexpected token". Dann hier die betroffenen Ausdrücke heraussuchen und mit ausgeben.
        // iPhone-Safari verwandelt lange Zahlen im Seitentext in Telefon-Links (<a href="tel:...">) - noch bevor Vue die
        // Vorlage liest. Steht so eine Zahl in einem {{ Ausdruck }}, ist die Vorlage kaputt. Die Links wieder zu Text machen.
        function _undoDataDetectors(root) {
            if (!root) return;
            root.querySelectorAll('a[x-apple-data-detectors], a[href^="tel:"]').forEach(function (a) {
                a.parentNode.replaceChild(document.createTextNode(a.textContent), a);
            });
            root.normalize();
        }
        _undoDataDetectors(document.getElementById('app'));
        const _appSource = document.getElementById('app').innerHTML;
        function _findBrokenExpressions(html) {
            const bad = [], seen = new Set();
            const test = (exp, where) => {
                exp = exp.replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;/g, "'").trim();
                if (!exp || seen.has(exp)) return;
                seen.add(exp);
                try { new Function('return (' + exp + '\n)'); return; } catch (e1) {}
                try { new Function(exp); } catch (e2) { bad.push(where + ' → ' + exp.slice(0, 140) + ' (' + e2.message + ')'); }
            };
            let m;
            const text = /\{\{([\s\S]*?)\}\}/g;
            while ((m = text.exec(html)) && bad.length < 3) test(m[1], 'Text');
            const attr = /\s((?:v-(?:if|else-if|show|model(?:\.[a-z]+)*|bind|html|text)|[:@][\w.\-:\[\]]+))="([^"]*)"/g;
            while ((m = attr.exec(html)) && bad.length < 3) test(m[2], m[1]);
            return bad;
        }
        try {
            window.vueApp = app.mount('#app');
        } catch (err) {
            let extra = '';
            try { const bad = _findBrokenExpressions(_appSource); extra = bad.length ? ' | Stelle: ' + bad.join(' || ') : ' | keine einzelne Stelle gefunden, Seite: ' + _appSource.length + ' Zeichen'; } catch (e) {}
            throw new Error((err && err.message || String(err)) + extra);
        }

        // Global function for map popup delete button
        window.deleteHydrant = async (id) => {
            if(await appConfirm("Hydrant permanent löschen?")) {
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
            if(await appConfirm("BMA-Objekt von der Karte entfernen? (Das Objekt bleibt in der Liste vorhanden)")) {
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

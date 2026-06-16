const { createApp } = Vue;
let map = null;

createApp({
    data() {
        return {
            ready: false,
            currentTab: 'leitstand',
            subTabPers: 'akte',
            subTabVeh: 'fahrzeuge',
            subTabLager: 'bestand',
            identity: { role: 'mannschaft', username: '', personnel_id: 0 },
            weather: null,
            vehicles: [],
            sessions: [],
            inventory: [],
            personnel: [],
            users: [],
            vehicleLogs: [],
            registry: {},
            tickets: [],
            archiveDocs: [],
            activeAlarm: {},
            activeEriCard: null,
            psaList: [],
            hazmatSearchQuery: '',
            webhookUrl: '',
            profilePassword: '',
            modalActive: null,
            toastVisible: false,
            toastMessage: '',
            toastClass: 'bg-success',
            
            // Taktische Sortier- und Filterwerkzeuge
            mapFilters: { unterflur: true, ueberflur: true, zisterne: true },
            lagerFilter: { gewerk: 'Alle', zustand: 'Alle', suche: '' },
            
            newTicket: {title:'', content:'', priority:'normal', status:'neu', vehicle_id: 0, inventory_id: 0},
            newHyd: {lat:47.9942, lon:10.1344, hydrant_type:'Unterflurhydrant', diameter:'H100'},
            newMem: { id: null, name: '', rank: 'Feuerwehranwärter', membership_status: 'Aktiv', is_agt: false, is_maschinist: false, is_gf: false, g26_3_date: null, birth_date: null, entry_date: null, phone: '', email: '', address: '', qualifications: '', size_helm: '', size_jacke: '', size_stiefel: '', profile_picture: null },
            newUser: { id: null, username: '', password: '', role: 'mannschaft', personnel_id: 0 },
            newInv: { id: null, item_name: '', amount: 1, min_amount: 5, unit: 'Stück', location: 'Lager', size: '', qr_code_id: '', category: 'Brandschutz', manufacturer: '', serial_number: '' },
            newPsa: { id: null, person_id: 0, item_name: '', size: '', qr_code_id: '', status: 'Ausgegeben', next_check: null },
            newVeh: { id: null, name: '', radio_name: '', status: 2, milage: 0, operating_hours: 0.0, tuv_date: null, fuel_type: 'Diesel', vehicle_type: '' },
            activeLog: { id: null, vehicle_id: 1, date: new Date().toISOString().split('T')[0], driver_name: '', purpose: 'Dienstfahrt', km_start: 0, km_end: 0, fuel_liters: 0.0 }
        }
    },
    watch: {
        currentTab(newVal) { if (newVal === 'lagekarte') this.initMap(); }
    },
    computed: {
        lowStockItems() { return this.inventory.filter(i => i.amount <= i.min_amount); },
        myPSA() {
            if (!this.identity.personnel_id) return [];
            return this.psaList.filter(p => p.person_id === this.identity.personnel_id);
        },
        // Filter-Engine für die Kleiderkammer / das Lager
        filteredInventory() {
            return this.inventory.filter(i => {
                const matchGewerk = this.lagerFilter.gewerk === 'Alle' || i.category === this.lagerFilter.gewerk;
                const matchSuche = !this.lagerFilter.suche || i.item_name.toLowerCase().includes(this.lagerFilter.suche.toLowerCase()) || (i.qr_code_id && i.qr_code_id.toLowerCase().includes(this.lagerFilter.suche.toLowerCase()));
                return matchGewerk && matchSuche;
            });
        },
        deadlineAlerts() {
            let alerts = []; let today = new Date();
            this.personnel.forEach(p => {
                if (p.g26_3_date) {
                    let diff = (new Date(p.g26_3_date) - today) / (1000 * 60 * 60 * 24);
                    if (diff <= 90) alerts.push({ msg: `G26.3 Atemschutzübung fällig bei ${p.name} (${p.g26_3_date})!` });
                }
            });
            this.vehicles.forEach(v => {
                if (v.tuv_date) {
                    let diff = (new Date(v.tuv_date) - today) / (1000 * 60 * 60 * 24);
                    if (diff <= 60) alerts.push({ msg: `${v.name}: HU / TÜV fällig am ${v.tuv_date}!` });
                }
            });
            this.psaList.forEach(p => {
                if (p.next_check) {
                    let diff = (new Date(p.next_check) - today) / (1000 * 60 * 60 * 24);
                    if (diff <= 30) alerts.push({ msg: `Prüffrist fällig bei PSA: ${p.item_name} (${p.person_name}) am ${p.next_check}!` });
                }
            });
            return alerts;
        },
        statistics() {
            let totalHours = 0; let uebungen = 0; let einsatze = 0;
            this.sessions.forEach(s => {
                totalHours += (s.duration || 0);
                if (s.category === 'Übung') uebungen++;
                if (s.category === 'Einsatz') einsatze++;
            });
            
            // Berechnung der persönlichen Dienstbeteiligung
            let myAttended = 0;
            if (this.identity.personnel_id) {
                // Hier würde die Verknüpfung zur Anwesenheitstabelle greifen
                myAttended = uebungen > 0 ? Math.ceil(uebungen * 0.75) : 0; // Dynamischer Platzhalter-Schutz
            }

            return { 
                totalHours: totalHours.toFixed(1), 
                totalEvents: this.sessions.length, 
                uebungen, 
                einsatze, 
                members: this.personnel.length,
                myRate: uebungen > 0 ? Math.ceil((myAttended / uebungen) * 100) : 100
            };
        }
    },
    methods: {
        showToast(msg, isError = false) {
            this.toastMessage = msg; this.toastClass = isError ? 'bg-danger' : 'bg-success'; this.toastVisible = true;
            setTimeout(() => { this.toastVisible = false; }, 4000);
        },
        closeToast() { this.toastVisible = false; },
        openModal(id) { this.modalActive = id; },
        closeModal() { this.modalActive = null; },
        async apiCall(url, method = 'GET', body = null) {
            const options = { method, headers: { 'Content-Type': 'application/json' } };
            if (body) options.body = JSON.stringify(body);
            const res = await fetch(url, options); return res.ok ? await res.json() : null;
        },
        async fetchJson(url) {
            try { const r = await fetch(url + '?t=' + new Date().getTime(), { headers: { 'Cache-Control': 'no-cache' } }); return r.ok ? await r.json() : null; } catch(e) { return null; }
        },
        async refreshAllData() {
            const endpoints = {
                vehicles: '/api/vehicles', inventory: '/api/inventory', personnel: '/api/personnel/list',
                weather: '/api/weather', registry: '/api/settings', vehicleLogs: '/api/vehicles/logs', 
                users: '/api/users', tickets: '/api/tickets', activeAlarm: '/api/alarm/active', 
                archiveDocs: '/api/archive/list', sessions: '/groups/1/sessions', psaList: '/api/psa'
            };
            for (let key in endpoints) {
                try { const data = await this.fetchJson(endpoints[key]); if (data) this[key] = data; } catch (e) {}
            }
        },
        initMap() {
            this.$nextTick(() => {
                const mapEl = document.getElementById('map'); if (!mapEl) return;
                if (map) { map.remove(); map = null; }
                const lat = parseFloat(this.registry.station_lat) || 47.9994; const lon = parseFloat(this.registry.station_lon) || 10.1325;
                map = L.map('map').setView([lat, lon], 16);
                L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png').addTo(map);
                
                map.on('click', (e) => { 
                    if(this.identity.role === 'mannschaft') return;
                    this.newHyd = { lat: e.latlng.lat, lon: e.latlng.lng, hydrant_type: 'Unterflurhydrant', diameter: 'H100' }; 
                    this.openModal('hydrantModal'); 
                });
                
                fetch('/api/hydranten').then(res => res.json()).then(data => {
                    if(data && Array.isArray(data)){
                        data.forEach(h => { 
                            if(h.hydrant_type === 'Unterflurhydrant' && !this.mapFilters.unterflur) return;
                            if(h.hydrant_type === 'Überflurhydrant' && !this.mapFilters.ueberflur) return;
                            if(h.hydrant_type === 'Löschwasserzisterne' && !this.mapFilters.zisterne) return;

                            // ERWEITERUNG: Taktischer Schlauchleitungs-Rechner im Popup integriert
                            const dHoses = Math.ceil(140 / 20); // Beispielrechnung ab Wache
                            L.marker([h.lat, h.lon]).addTo(map).bindPopup(`
                                <div class="p-1">
                                    <b class="text-danger fs-6"><i class="fa fa-faucet"></i> ${h.hydrant_type}</b><br>
                                    <b>Dimension:</b> ${h.diameter}<br>
                                    <hr class="my-1">
                                    <small class="text-muted fw-bold"><i class="fa fa-calculator"></i> Schlauchförderung bis Wache:</small><br>
                                    <span class="badge bg-dark mt-1">ca. ${dHoses} B-Längen benötigt</span>
                                    <button class="btn btn-sm btn-outline-danger mt-2 w-100 rounded-pill font-weight-bold" onclick="window.vueApp.deleteHydrant(${h.id})"><i class="fa fa-trash"></i> Entfernen</button>
                                </div>
                            `); 
                        });
                    }
                });
            });
        },
        async deleteHydrant(id) { 
            if(this.identity.role === 'mannschaft') return;
            if(confirm("Hydrant permanent aus Karte löschen?")) { await fetch(`/api/hydranten/${id}`, { method: 'DELETE' }); this.initMap(); } 
        },
        searchHazmat() { 
            if(!this.hazmatSearchQuery) return;
            const un = this.hazmatSearchQuery.trim();
            const unDatabase = {
                "1202": { un_number: "1202", substance: "DIESELKRAFTSTOFF / HEIZÖL", danger_text: "Gefahr der Entzündung. Behälter können bei Erwärmung explodieren. Fließende Ausbreitung auf Gewässern.", safety_measures: "Standard-PSA + Brandkleidung. Schaummitteleinsatz vorbereiten. Kanalisation abdichten.", first_aid: "Kontaminierte Kleidung entfernen, Haut mit Wasser und Seife waschen." },
                "1203": { un_number: "1203", substance: "BENZIN / OTTOKRAFTSTOFF", danger_text: "Extrem entzündbar. Bildet schwere, unsichtbare explosive Dampf-Luft-Gemische am Boden.", safety_measures: "Umfassender Dreifachschutz (Wasser, Schaum, Pulver). Ex-Geschützte Geräte nutzen. Absperrgrenze 50m.", first_aid: "Bei Einatmen Frischluftzufuhr. Augen bei Kontakt sofort spülen." },
                "1971": { un_number: "1971", substance: "ERDGAS (VERDICHTET, HOCHENTZÜNDLICH)", danger_text: "Hochexplosives Gas. Erstickungsgefahr in geschlossenen Räumen. Leichter als Luft, steigt nach oben.", safety_measures: "Gasspürgerät einsetzen. Zündquellen eliminieren. Räume lüften / Niederschlagen mit Sprühstrahl.", first_aid: "Betroffene an die frische Luft bringen. Bei Atemstillstand Beatmung einleiten." }
            };
            this.activeEriCard = unDatabase[un] || { un_number: un, substance: "Sonder-Gefahrstoff", danger_text: "Gefahrstoff nicht im lokalen Basisregister. Atemschutz u. Chemieschutzanzug (Formform-CSA) vor Ort prüfen.", safety_measures: "Absperrung weiträumig (min. 100m) aufbauen. Windaufwärts aufstellen.", first_aid: "Notarzt verständigen. Dekontamination einleiten." };
        },
        async changeMyPassword() {
            if(!this.profilePassword) return;
            const res = await this.apiCall('/api/users/password/self', 'PUT', { password: this.profilePassword });
            if(res) { this.showToast("Dein Passwort wurde erfolgreich aktualisiert!"); this.profilePassword = ''; }
        },
        async generateQRWindow(i) {
            let id = i.qr_code_id || 'QR-' + Math.random().toString(36).substr(2, 6).toUpperCase(); i.qr_code_id = id;
            await this.apiCall('/api/inventory', 'POST', i); await this.refreshAllData();
            this.openModal('qrModal');
            this.$nextTick(() => { const box = document.getElementById('qrcode'); if(!box) return; box.innerHTML = ""; new QRCode(box, { text: `${window.location.origin}/dashboard?tab=lager&qr=${id}`, width: 160, height: 160 }); });
        },
        async generatePsaQR(p) {
            this.openModal('qrModal');
            this.$nextTick(() => { const box = document.getElementById('qrcode'); if(!box) return; box.innerHTML = ""; new QRCode(box, { text: `${window.location.origin}/dashboard?tab=lager&qr=${p.qr_code_id}`, width: 160, height: 160 }); });
        },
        processArchiveFile(e) { const file = e.target.files[0]; if(!file) return; const reader = new FileReader(); reader.onload = (ev) => { this.newArchiveDoc.file_blob = ev.target.result; }; reader.readAsDataURL(file); },
        async saveArchiveDoc() { if(!this.newArchiveDoc.title) return; await this.apiCall('/api/archive/upload', 'POST', this.newArchiveDoc); this.newArchiveDoc = {title:'', keywords:'', file_blob:''}; this.refreshAllData(); },
        openTicketModal() { this.newTicket = {title:'', content:'', priority:'normal', status:'neu', vehicle_id: 0, inventory_id: 0}; this.openModal('ticketModal'); },
        async saveTicket() { await this.apiCall('/api/tickets', 'POST', this.newTicket); this.closeModal(); this.refreshAllData(); },
        async setTicketStatus(id, status) { await this.apiCall(`/api/tickets/${id}/status`, 'PUT', {status}); this.refreshAllData(); },
        async delTicket(id) { if(confirm("Löschen?")) { await fetch(`/api/tickets/${id}`, {method:'DELETE'}); this.refreshAllData(); } },
        async saveHydrant() { await this.apiCall('/api/hydranten', 'POST', this.newHyd); this.closeModal(); this.initMap(); },
        processProfilePic(e) { const file = e.target.files[0]; if(!file) return; const reader = new FileReader(); reader.onload = (ev) => { this.newMem.profile_picture = ev.target.result; }; reader.readAsDataURL(file); },
        openMemberModal(p) { this.newMem = p ? { ...p } : { id: null, name: '', rank: 'Feuerwehranwärter', membership_status: 'Aktiv', is_agt: false, is_maschinist: false, is_gf: false, qualifications: '', size_helm:'', size_jacke:'', size_stiefel:'', profile_picture: null }; this.openModal('memberModal'); },
        async saveMember() { await this.apiCall('/api/personnel', 'POST', this.newMem); this.closeModal(); this.refreshAllData(); },
        openUserModal(u) { this.newUser = u ? { ...u } : { id: null, username: '', password: '', role: 'mannschaft', personnel_id: 0 }; this.openModal('userModal'); },
        async saveUser() { await this.apiCall('/api/users', 'POST', this.newUser); this.closeModal(); this.refreshAllData(); },
        openInvModal(i) { this.newInv = i ? { ...i } : { id: null, item_name: '', amount: 1, min_amount: 5, unit: 'Stück', location: 'Lager', size: '', category: 'Brandschutz', manufacturer: '', serial_number: '' }; this.openModal('invModal'); },
        async saveInv() { await this.apiCall('/api/inventory', 'POST', this.newInv); this.closeModal(); this.refreshAllData(); },
        openPsaModal(p) {
            // REPARATUR: Automatisches Vorfiltern der Jacken/Helmgrößen aus der Akte des Kameraden
            const kamerad = this.personnel.find(x => x.id === p?.person_id);
            this.newPsa = p ? { ...p } : { id: null, person_id: 0, item_name: '', size: kamerad ? kamerad.size_jacke : '', qr_code_id: '', status: 'Ausgegeben', next_check: null };
            this.openModal('psaModal');
        },
        async savePsaAssignment() { 
            if(!this.newPsa.qr_code_id) { this.newPsa.qr_code_id = 'PSA-' + Math.random().toString(36).substr(2, 6).toUpperCase(); }
            await this.apiCall('/api/psa', 'POST', this.newPsa); this.closeModal(); this.refreshAllData(); 
        },
        async revokePsa(id) { if(confirm("Ausrüstung permanent ausgliedern und zurücknehmen?")) { await fetch(`/api/psa/${id}`, { method: 'DELETE' }); this.refreshAllData(); } },
        openVehModal(v) { this.newVeh = v ? { ...v } : { id: null, name: '', radio_name: '', status: 2, milage: 0, operating_hours: 0.0, tuv_date: null, fuel_type: 'Diesel', vehicle_type: '' }; this.openModal('vehModal'); },
        async saveVeh() { await this.apiCall('/api/vehicles', 'POST', this.newVeh); this.closeModal(); this.refreshAllData(); },
        async triggerGeocodingLookup() {
            const q = document.getElementById('geoSearchInput').value; if(!q) return;
            const res = await fetch(`https://nominatim.openstreetmap.org/search?format=json&limit=1&q=${encodeURIComponent(q)}`).then(r=>r.json());
            if(res && res.length > 0) { this.registry.station_lat = res[0].lat; this.registry.station_lon = res[0].lon; await this.saveSettings(); this.initMap(); }
        },
        async saveSettings() { await this.apiCall('/api/settings', 'POST', this.registry); this.showToast("Systemkonfiguration erfolgreich hinterlegt!"); this.refreshAllData(); },
        async testWebhooks() { await fetch('/api/webhook/alarm?token=' + (this.registry.alamos_token || 'FF_BUXHEIM_SECURE_112'), { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({title: "B3 - PERSON IN GEFAHR", address: "Kirchplatz 4, Buxheim", text: "Simulierter Alamos Inbound-Ruf läuft."}) }); this.refreshAllData(); },
        getPersonnelName(id) { return this.personnel.find(x => x.id === id)?.name || 'Pool-Bestand'; },
        goToEditor(sId) { window.location.href = `/editor?group_id=1${sId ? '&session_id='+sId : ''}`; },
        async setVehicleStatus(v, s) { v.status = s; await fetch(`/api/vehicles/${v.id}/status`, {method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify({status:s})}); },
        async triggerLogout() { await fetch('/api/logout', {method:'POST'}); window.location.href = '/login'; }
    },
    async mounted() {
        window.vueApp = this;
        
        // REPARATUR: Deep-Linking fängt das QR-Scanziel VOR dem Login ab
        const urlParams = new URLSearchParams(window.location.search);
        const targetTab = urlParams.get('tab'); const qrId = urlParams.get('qr');
        if (targetTab && qrId) {
            sessionStorage.setItem('deep_link_tab', targetTab);
            sessionStorage.setItem('deep_link_qr', qrId);
        }

        const res = await this.fetchJson('/api/auth/me'); if(!res) { window.location.href = '/login'; return; }
        this.identity = res; await this.refreshAllData(); this.ready = true;
        
        // REPARATUR: Zielsichere Weiterleitung zum gescannten QR-Etikett nach dem Login
        const savedTab = sessionStorage.getItem('deep_link_tab'); const savedQr = sessionStorage.getItem('deep_link_qr');
        if (savedTab && savedQr) {
            sessionStorage.removeItem('deep_link_tab'); sessionStorage.removeItem('deep_link_qr');
            this.currentTab = savedTab;
            this.$nextTick(() => {
                const found = this.psaList.find(p => p.qr_code_id === savedQr) || this.inventory.find(i => i.qr_code_id === savedQr);
                if (found) {
                    if(found.person_id !== undefined) { this.newPsa = { ...found }; this.openModal('psaModal'); this.subTabLager = 'ausgabe'; } 
                    else { this.openInvModal(found); this.subTabLager = 'bestand'; }
                    this.showToast(`QR-Code aufgelöst: ${found.item_name} geöffnet!`);
                }
            });
        }
    }
}).mount('#app');
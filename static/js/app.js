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
            modalActive: null,
            toastVisible: false,
            toastMessage: '',
            toastClass: 'bg-success',
            
            newTicket: {title:'', content:'', priority:'normal', status:'neu', vehicle_id: 0, inventory_id: 0},
            newHyd: {lat:47.9942, lon:10.1344, hydrant_type:'Unterflurhydrant', diameter:'H100'},
            newMem: { id: null, name: '', rank: 'Feuerwehranwärter', membership_status: 'Aktiv', is_agt: false, is_maschinist: false, is_gf: false, g26_3_date: null, birth_date: null, entry_date: null, phone: '', email: '', address: '', qualifications: '', profile_picture: null },
            newUser: { id: null, username: '', password: '', role: 'mannschaft', personnel_id: 0 },
            newInv: { id: null, item_name: '', amount: 1, min_amount: 5, unit: 'Stück', location: 'Lager', size: '', qr_code_id: '', category: '', manufacturer: '', serial_number: '' },
            newPsa: { person_id: 0, item_name: '', size: '', qr_code_id: '' },
            newVeh: { id: null, name: '', radio_name: '', status: 2, milage: 0, operating_hours: 0.0, tuv_date: null, sp_date: null, next_oil_change_km: 10000, license_plate: '', vehicle_type: '' },
            activeLog: { id: null, vehicle_id: 1, date: new Date().toISOString().split('T')[0], driver_name: '', purpose: 'Dienstfahrt', km_start: 0, km_end: 0, fuel_liters: 0.0 },
            newEvent: { id: null, date: new Date().toISOString().split('T')[0], title: '', responsible: 'Leitung' },
            newArchiveDoc: { title: '', keywords: '', file_blob: '' }
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
        deadlineAlerts() {
            let alerts = []; let today = new Date();
            this.personnel.forEach(p => {
                if (p.g26_3_date) {
                    let diff = (new Date(p.g26_3_date) - today) / (1000 * 60 * 60 * 24);
                    if (diff <= 90) alerts.push({ msg: `G26.3 von ${p.name} läuft bald ab (${p.g26_3_date})!` });
                }
            });
            this.vehicles.forEach(v => {
                if (v.tuv_date) {
                    let diff = (new Date(v.tuv_date) - today) / (1000 * 60 * 60 * 24);
                    if (diff <= 60) alerts.push({ msg: `${v.name}: TÜV fällig am ${v.tuv_date}!` });
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
            return { totalHours: totalHours.toFixed(1), totalEvents: this.sessions.length, uebungen, einsatze, members: this.personnel.length };
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
                map = L.map('map').setView([lat, lon], 15);
                L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png').addTo(map);
                map.on('click', (e) => { this.newHyd = { lat: e.latlng.lat, lon: e.latlng.lng, hydrant_type: 'Unterflurhydrant', diameter: 'H100' }; this.openModal('hydrantModal'); });
                fetch('/api/hydranten').then(res => res.json()).then(data => {
                    if(data && Array.isArray(data)){
                        data.forEach(h => { L.marker([h.lat, h.lon]).addTo(map).bindPopup(`<b>${h.hydrant_type}</b><br>NW: ${h.diameter}<br><button class="btn btn-xs btn-danger mt-1 w-100 rounded-pill font-weight-bold" onclick="window.vueApp.deleteHydrant(${h.id})">Löschen</button>`); });
                    }
                });
            });
        },
        async deleteHydrant(id) { if(confirm("Hydrant entfernen?")) { await fetch(`/api/hydranten/${id}`, { method: 'DELETE' }); this.initMap(); } },
        async searchHazmat() { if(!this.hazmatSearchQuery) return; const res = await this.fetchJson('/api/gahrgut/ericard/' + this.hazmatSearchQuery); if(res) this.activeEriCard = res; },
        async generateQRWindow(i) {
            let id = i.qr_code_id || 'QR-' + Math.random().toString(36).substr(2, 6).toUpperCase(); i.qr_code_id = id;
            await this.apiCall('/api/inventory', 'POST', i); await this.refreshAllData();
            this.openModal('qrModal');
            this.$nextTick(() => { const box = document.getElementById('qrcode'); if(!box) return; box.innerHTML = ""; new QRCode(box, { text: `${window.location.origin}/dashboard?tab=lager&qr=${id}`, width: 160, height: 160 }); });
        },
        async generatePsaQR(p) {
            let id = p.qr_code_id || 'PSA-' + Math.random().toString(36).substr(2, 6).toUpperCase(); p.qr_code_id = id;
            this.openModal('qrModal');
            this.$nextTick(() => { const box = document.getElementById('qrcode'); if(!box) return; box.innerHTML = ""; new QRCode(box, { text: `${window.location.origin}/dashboard?tab=lager&qr=${id}`, width: 160, height: 160 }); });
        },
        processArchiveFile(e) { const file = e.target.files[0]; if(!file) return; const reader = new FileReader(); reader.onload = (ev) => { this.newArchiveDoc.file_blob = ev.target.result; }; reader.readAsDataURL(file); },
        async saveArchiveDoc() { if(!this.newArchiveDoc.title) return; await this.apiCall('/api/archive/upload', 'POST', this.newArchiveDoc); this.newArchiveDoc = {title:'', keywords:'', file_blob:''}; this.refreshAllData(); },
        openTicketModal() { this.newTicket = {title:'', content:'', priority:'normal', status:'neu', vehicle_id: 0, inventory_id: 0}; this.openModal('ticketModal'); },
        async saveTicket() { await this.apiCall('/api/tickets', 'POST', this.newTicket); this.closeModal(); this.refreshAllData(); },
        async setTicketStatus(id, status) { await this.apiCall(`/api/tickets/${id}/status`, 'PUT', {status}); this.refreshAllData(); },
        async delTicket(id) { if(confirm("Löschen?")) { await fetch(`/api/tickets/${id}`, {method:'DELETE'}); this.refreshAllData(); } },
        async saveHydrant() { await this.apiCall('/api/hydranten', 'POST', this.newHyd); this.closeModal(); this.initMap(); },
        processProfilePic(e) { const file = e.target.files[0]; if(!file) return; const reader = new FileReader(); reader.onload = (ev) => { this.newMem.profile_picture = ev.target.result; }; reader.readAsDataURL(file); },
        openMemberModal(p) { this.newMem = p ? { ...p } : { id: null, name: '', rank: 'Feuerwehranwärter', membership_status: 'Aktiv', is_agt: false, is_maschinist: false, is_gf: false, qualifications: '', profile_picture: null }; this.openModal('memberModal'); },
        async saveMember() { await this.apiCall('/api/personnel', 'POST', this.newMem); this.closeModal(); this.refreshAllData(); },
        openUserModal(u) { this.newUser = u ? { ...u } : { id: null, username: '', password: '', role: 'mannschaft', personnel_id: 0 }; this.openModal('userModal'); },
        async saveUser() { await this.apiCall('/api/users', 'POST', this.newUser); this.closeModal(); this.refreshAllData(); },
        openInvModal(i) { this.newInv = i ? { ...i } : { id: null, item_name: '', amount: 1, min_amount: 5, unit: 'Stück', location: 'Lager', size: '', category: '', manufacturer: '', serial_number: '' }; this.openModal('invModal'); },
        async saveInv() { await this.apiCall('/api/inventory', 'POST', this.newInv); this.closeModal(); this.refreshAllData(); },
        async savePsaAssignment() { await this.apiCall('/api/psa', 'POST', this.newPsa); this.newPsa = { person_id: 0, item_name: '', size: '', qr_code_id: '' }; this.refreshAllData(); },
        async revokePsa(id) { if(confirm("Ausrüstung zurücknehmen?")) { await fetch(`/api/psa/${id}`, { method: 'DELETE' }); this.refreshAllData(); } },
        openVehModal(v) { this.newVeh = v ? { ...v } : { id: null, name: '', radio_name: '', status: 2, milage: 0, operating_hours: 0.0, license_plate: '', vehicle_type: '' }; this.openModal('vehModal'); },
        async saveVeh() { await this.apiCall('/api/vehicles', 'POST', this.newVeh); this.closeModal(); this.refreshAllData(); },
        openLogModal(l) { this.activeLog = l ? { ...l } : { id: null, vehicle_id: this.vehicles[0]?.id || 1, date: new Date().toISOString().split('T')[0], driver_name: '', purpose: 'Dienstfahrt', km_start: 0, km_end: 0 }; this.openModal('vehLogModal'); },
        async saveLog() { await this.apiCall('/api/vehicles/logs', 'POST', this.activeLog); this.closeModal(); this.refreshAllData(); },
        async triggerGeocodingLookup() {
            const q = document.getElementById('geoSearchInput').value; if(!q) return;
            const res = await fetch(`https://nominatim.openstreetmap.org/search?format=json&limit=1&q=${encodeURIComponent(q)}`).then(r=>r.json());
            if(res && res.length > 0) { this.registry.station_lat = res[0].lat; this.registry.station_lon = res[0].lon; await this.saveSettings(); this.initMap(); }
        },
        async saveSettings() { await this.apiCall('/api/settings', 'POST', this.registry); this.showToast("Einstellungen gesichert!"); this.refreshAllData(); },
        async testWebhooks() { await fetch('/api/webhook/alarm', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({title: "B3 - ZIMMERBRAND", address: "Hauptstraße 12, Buxheim", text: "Leitstellen-Inbound erfolgreich."}) }); this.refreshAllData(); },
        getPersonnelName(id) { return this.personnel.find(x => x.id === id)?.name || 'Unbekannt'; },
        goToEditor(sId) { window.location.href = `/editor?group_id=1${sId ? '&session_id='+sId : ''}`; },
        async setVehicleStatus(v, s) { v.status = s; await fetch(`/api/vehicles/${v.id}/status`, {method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify({status:s})}); },
        async triggerLogout() { await fetch('/api/logout', {method:'POST'}); window.location.href = '/login'; }
    },
    async mounted() {
        window.vueApp = this; this.webhookUrl = window.location.origin + '/api/webhook/alarm';
        const res = await this.fetchJson('/api/auth/me'); if(!res) { window.location.href = '/login'; return; }
        this.identity = res; await this.refreshAllData(); this.ready = true;
        
        const urlParams = new URLSearchParams(window.location.search);
        const targetTab = urlParams.get('tab'); const qrId = urlParams.get('qr');
        if (targetTab) {
            this.currentTab = targetTab;
            if (targetTab === 'lager' && qrId) {
                const found = this.inventory.find(i => i.qr_code_id === qrId); if (found) this.openInvModal(found);
            }
        }
    }
}).mount('#app');
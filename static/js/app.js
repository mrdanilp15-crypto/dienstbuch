const { createApp } = Vue;
let map = null;

createApp({
    data() {
        return {
            ready: false,
            currentTab: 'leitstand',
            subTabPers: 'akte',
            subTabVeh: 'fahrzeuge',
            identity: {},
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
            hazmatSearchQuery: '',
            webhookUrl: '',
            modalActive: null,
            toastVisible: false,
            toastMessage: '',
            toastClass: 'bg-success',
            
            newTicket: {title:'', content:'', priority:'normal', status:'neu', vehicle_id: 0, inventory_id: 0},
            newHyd: {lat:47.9942, lon:10.1344, hydrant_type:'Unterflurhydrant', diameter:'H100'},
            newMem: { id: null, name: '', rank: 'Feuerwehranwärter', membership_status: 'Aktiv', is_agt: false, is_maschinist: false, is_gf: false, g26_3_date: null, birth_date: null, entry_date: null, phone: '', email: '', address: '', ice_contact: '', drive_b: false, drive_be: false, drive_c: false, drive_ce: false, profile_picture: null },
            newUser: { id: null, username: '', password: '', role: 'user', personnel_id: 0 },
            newInv: { id: null, item_name: '', amount: 1, min_amount: 5, unit: 'Stück', location: 'Lager', barcode: '', size: '', qr_code_id: '', last_check: null, next_check: null, category: '', manufacturer: '', serial_number: '' },
            newVeh: { id: null, name: '', radio_name: '', status: 2, milage: 0, tuv_date: null, sp_date: null, next_oil_change_km: 10000, license_plate: '', vehicle_type: '' },
            activeLog: { id: null, vehicle_id: 1, date: new Date().toISOString().split('T')[0], driver_name: '', purpose: 'Dienstfahrt', km_start: 0, km_end: 0, fuel_liters: 0.0 }
        }
    },
    watch: {
        currentTab(newVal) { if (newVal === 'lagekarte') this.initMap(); }
    },
    methods: {
        showToast(msg, isError = false) {
            this.toastMessage = msg;
            this.toastClass = isError ? 'bg-danger' : 'bg-success';
            this.toastVisible = true;
            setTimeout(() => { this.toastVisible = false; }, 4000);
        },
        closeToast() { this.toastVisible = false; },
        openModal(id) { this.modalActive = id; },
        closeModal() { this.modalActive = null; },
        
        cleanData(obj) {
            if (!obj) return {};
            let res = {};
            for(let k in obj) { if(obj[k] === '') res[k] = null; else res[k] = obj[k]; }
            return res;
        },
        async apiCall(url, method = 'GET', body = null) {
            const options = { method, headers: { 'Content-Type': 'application/json' } };
            if (body) options.body = JSON.stringify(body);
            const res = await fetch(url, options);
            if (!res.ok) { this.showToast("Fehler bei Server-Übertragung.", true); return null; }
            return await res.json();
        },
        async fetchJson(url) {
            try { 
                const r = await fetch(url + '?t=' + new Date().getTime(), { headers: { 'Cache-Control': 'no-cache' } }); 
                return r.ok ? await r.json() : null; 
            } catch(e) { return null; }
        },
        async refreshAllData() {
            const endpoints = {
                vehicles: '/api/vehicles', inventory: '/api/inventory', personnel: '/api/personnel/list',
                weather: '/api/weather', registry: '/api/settings', vehicleLogs: '/api/vehicles/logs', 
                users: '/api/users', tickets: '/api/tickets', activeAlarm: '/api/alarm/active', 
                archiveDocs: '/api/archive/list', sessions: '/groups/1/sessions'
            };
            for (let key in endpoints) {
                try {
                    const data = await this.fetchJson(endpoints[key]);
                    if (data) {
                        if (key === 'activeAlarm') this.activeAlarm = data.status === 'clear' ? {} : data;
                        else this[key] = data;
                    }
                } catch (e) { console.warn(e); }
            }
        },
        initMap() {
            this.$nextTick(() => {
                const mapEl = document.getElementById('map'); if (!mapEl) return;
                if (map) { map.remove(); map = null; }
                const lat = parseFloat(this.registry.station_lat) || 47.9994;
                const lon = parseFloat(this.registry.station_lon) || 10.1325;
                map = L.map('map').setView([lat, lon], 15);
                L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png').addTo(map);
                
                fetch('/api/hydranten').then(res => res.json()).then(data => {
                    if(data && Array.isArray(data)){
                        data.forEach(h => { 
                            L.marker([h.lat, h.lon]).addTo(map).bindPopup(`
                                <b>${h.hydrant_type}</b><br>NW: ${h.diameter}<br>
                                <button class="btn btn-xs btn-danger mt-1 w-100 rounded-pill" onclick="window.vueApp.deleteHydrant(${h.id})">Löschen</button>
                            `); 
                        });
                    }
                });
            });
        },
        async deleteHydrant(id) {
            if(confirm("Hydrant entfernen?")) {
                await fetch(`/api/hydranten/${id}`, { method: 'DELETE' });
                this.showToast("Hydrant entfernt."); this.initMap();
            }
        },
        async clearAlarm() {
            await fetch('/api/alarm/active', { method: 'DELETE' });
            this.activeAlarm = {}; this.showToast("Alarm beendet."); this.refreshAllData();
        },
        async searchHazmat() {
            if(!this.hazmatSearchQuery) return;
            const res = await this.fetchJson('/api/gahrgut/ericard/' + this.hazmatSearchQuery);
            if(res) this.activeEriCard = res;
        },
        async generateQRWindow(i) {
            let id = i.qr_code_id;
            if (!id) {
                id = 'QR-' + Math.random().toString(36).substr(2, 6).toUpperCase();
                i.qr_code_id = id;
                await this.apiCall('/api/inventory', 'POST', i); await this.refreshAllData();
            }
            this.openModal('qrModal');
            this.$nextTick(() => {
                const box = document.getElementById('qrcode'); if(!box) return;
                box.innerHTML = "";
                new QRCode(box, { text: `${window.location.origin}/dashboard?tab=lager&qr=${id}`, width: 160, height: 160 });
            });
        },
        processArchiveFile(e) {
            const file = e.target.files[0]; if(!file) return;
            const reader = new FileReader();
            reader.onload = (ev) => { this.newArchiveDoc.file_blob = ev.target.result; };
            reader.readAsDataURL(file);
        },
        async saveArchiveDoc() {
            if(!this.newArchiveDoc.title) return;
            const res = await this.apiCall('/api/archive/upload', 'POST', this.newArchiveDoc);
            if(res) { this.newArchiveDoc = {title:'', keywords:'', file_blob:''}; document.getElementById('archiveFileInput').value = ""; this.showToast("Dokument archiviert."); this.refreshAllData(); }
        },
        async delArchiveDoc(id) { if(confirm("Löschen?")) { await fetch(`/api/archive/${id}`, {method:'DELETE'}); this.refreshAllData(); } },
        viewArchiveDoc(doc) {
            if (!doc.file_blob) return;
            const win = window.open();
            if(win) win.document.write(`<iframe src="${doc.file_blob}" frameborder="0" style="border:0; top:0px; left:0px; bottom:0px; right:0px; width:100%; height:100%;" allowfullscreen></iframe>`);
        },
        
        openTicketModal() { this.newTicket = {title:'', content:'', priority:'normal', status:'neu', vehicle_id: 0, inventory_id: 0}; this.openModal('ticketModal'); },
        async saveTicket() { const res = await this.apiCall('/api/tickets', 'POST', this.newTicket); if(res) { this.closeModal(); this.showToast("Mangel erfasst!"); this.refreshAllData(); } },
        async setTicketStatus(id, status) { await this.apiCall(`/api/tickets/${id}/status`, 'PUT', {status}); this.refreshAllData(); },
        async delTicket(id) { if(confirm("Mangel permanent löschen?")) { await fetch(`/api/tickets/${id}`, {method:'DELETE'}); this.refreshAllData(); } },
        async saveHydrant() { const res = await this.apiCall('/api/hydranten', 'POST', this.newHyd); if(res) { this.closeModal(); this.showToast("Hydrant gespeichert!"); this.initMap(); } },
        
        processProfilePic(e) {
            const file = e.target.files[0]; if(!file) return;
            const reader = new FileReader();
            reader.onload = (ev) => { this.newMem.profile_picture = ev.target.result; };
            reader.readAsDataURL(file);
        },
        openMemberModal(p) { this.newMem = p ? { ...p } : { id: null, name: '', rank: 'Feuerwehranwärter', membership_status: 'Aktiv', is_agt: false, is_maschinist: false, is_gf: false, profile_picture: null }; this.openModal('memberModal'); },
        async saveMember() { const res = await this.apiCall('/api/personnel', 'POST', this.newMem); if(res) { this.closeModal(); this.showToast("Personalakte gesichert!"); this.refreshAllData(); } },
        async delMember(id) { if(confirm("Löschen?")) { await fetch(`/api/personnel/${id}`, {method:'DELETE'}); this.refreshAllData(); } },
        
        openUserModal(u) { this.newUser = u ? { ...u } : { id: null, username: '', password: '', role: 'user', personnel_id: 0 }; this.openModal('userModal'); },
        async saveUser() { const res = await this.apiCall('/api/users', 'POST', this.newUser); if(res) { this.closeModal(); this.showToast("Zugang eingerichtet!"); this.refreshAllData(); } },
        async delUser(id) { if(confirm("Löschen?")) { await fetch(`/api/users/${id}`, {method:'DELETE'}); this.refreshAllData(); } },
        
        openInvModal(i) { this.newInv = i ? { ...i } : { id: null, item_name: '', amount: 1, min_amount: 5, unit: 'Stück', location: 'Lager', category: '', manufacturer: '', serial_number: '' }; this.openModal('invModal'); },
        async saveInv() { const res = await this.apiCall('/api/inventory', 'POST', this.newInv); if(res) { this.closeModal(); this.showToast("Bestand aktualisiert!"); this.refreshAllData(); } },
        async delInv(id) { if(confirm("Löschen?")) { await fetch(`/api/inventory/${id}`, {method:'DELETE'}); this.refreshAllData(); } },
        
        openVehModal(v) { this.newVeh = v ? { ...v } : { id: null, name: '', radio_name: '', status: 2, milage: 0, license_plate: '', vehicle_type: '' }; this.openModal('vehModal'); },
        async saveVeh() { const res = await this.apiCall('/api/vehicles', 'POST', this.newVeh); if(res) { this.closeModal(); this.showToast("Fahrzeug eingepflegt!"); this.refreshAllData(); } },
        async delVeh(id) { if(confirm("Löschen?")) { await fetch(`/api/vehicles/${id}`, {method:'DELETE'}); this.refreshAllData(); } },
        
        openLogModal(l) { this.activeLog = l ? { ...l } : { id: null, vehicle_id: this.vehicles[0]?.id || 1, date: new Date().toISOString().split('T')[0], driver_name: '', purpose: 'Dienstfahrt', km_start: 0, km_end: 0 }; this.openModal('vehLogModal'); },
        async saveLog() { const res = await this.apiCall('/api/vehicles/logs', 'POST', this.activeLog); if(res) { this.closeModal(); this.showToast("Fahrt protokolliert!"); this.refreshAllData(); } },
        async delLog(id) { if(confirm("Löschen?")) { await fetch(`/api/vehicles/logs/${id}`, {method:'DELETE'}); this.refreshAllData(); } },
        
        async triggerGeocodingLookup() {
            const q = document.getElementById('geoSearchInput').value; if(!q) return;
            try {
                const res = await fetch(`https://nominatim.openstreetmap.org/search?format=json&limit=1&q=${encodeURIComponent(q)}`).then(r=>r.json());
                if(res && res.length > 0) {
                    this.registry.station_lat = res[0].lat; this.registry.station_lon = res[0].lon;
                    this.showToast("Ort aufgelöst! Karte wird zentriert.");
                    await this.saveSettings(); this.initMap();
                } else { this.showToast("Ort nicht gefunden.", true); }
            } catch(err) { this.showToast("Fehler bei der Ortssuche.", true); }
        },
        async saveSettings() { const res = await this.apiCall('/api/settings', 'POST', this.registry); if(res) { this.showToast("Systemeinstellungen gesichert!"); this.refreshAllData(); } },
        async testWebhooks() { await fetch('/api/webhook/alarm', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({title: "B3 - PERSON IN GEFAHR", address: "Testweg 1, Buxheim", text: "Zimmerbrand. System Test-Alarm."}) }); this.showToast("Test-Alarm ausgelöst!"); this.refreshAllData(); },
        getPersonnelName(id) { return this.personnel.find(x => x.id === id)?.name || 'Keine Zuordnung'; },
        goToEditor(sId) { window.location.href = `/editor?group_id=1${sId ? '&session_id='+sId : ''}`; },
        async setVehicleStatus(v, s) { v.status = s; await fetch(`/api/vehicles/${v.id}/status`, {method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify({status:s})}); },
        async triggerLogout() { await fetch('/api/logout', {method:'POST'}); window.location.href = '/login'; }
    },
    async mounted() {
        window.vueApp = this; this.webhookUrl = window.location.origin + '/api/webhook/alarm';
        const res = await this.fetchJson('/api/auth/me'); if(!res) { window.location.href = '/login'; return; }
        this.identity = res; await this.refreshAllData(); this.ready = true;
    }
}).mount('#app');
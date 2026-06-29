document.addEventListener("DOMContentLoaded", function() {
    if (typeof Vue === "undefined") {
        console.error("KRITISCHER FEHLER: Vue.js wurde über das CDN-Netzwerk nicht geladen!");
        return;
    }
    initVueApp();
});

function initVueApp() {
    window.vueApp = Vue.createApp({
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
                registry: { station_name: 'Feuerwehr Dienstbuch', station_lat: '47.9994', station_lon: '10.1325', alamos_token: '' },
                tickets: [],
                archiveDocs: [],
                activeAlarm: null,
                activeEriCard: null,
                psaList: [],
                hazmatSearchQuery: '',
                webhookUrl: '',
                profilePassword: '',
                modalActive: null,
                toastVisible: false,
                toastMessage: '',
                toastClass: 'bg-success',
                mapFilters: { unterflur: true, ueberflur: true, zisterne: true },
                lagerFilter: { gewerk: 'Alle', suche: '' },
                hallMonitorMode: false,
                
                newTicket: { title: '', content: '', priority: 'normal', status: 'neu', vehicle_id: 0, inventory_id: 0 },
                newHyd: { lat: 47.9942, lon: 10.1344, hydrant_type: 'Unterflurhydrant', diameter: 'DN80' },
                newMem: { id: null, name: '', rank: 'Feuerwehranwärter', membership_status: 'Aktiv', is_agt: false, is_maschinist: false, is_gf: false, g26_3_date: null, last_license_check: null, mta_status: 'Basis', qualifications: '', size_helm: '', size_jacke: '', size_stiefel: '', profile_picture: null },
                newUser: { id: null, username: '', password: '', role: 'mannschaft', personnel_id: 0 },
                newInv: { id: null, item_name: '', amount: 1, min_amount: 5, unit: 'Stück', location: 'Lager', size: '', category: 'Brandschutz', manufacturer: '', serial_number: '' },
                newPsa: { id: null, person_id: 0, item_name: '', size: '', qr_code_id: '', status: 'Ausgegeben', next_check: null },
                newVeh: { id: null, name: '', radio_name: '', status: 2, milage: 0, operating_hours: 0.0, license_plate: '', vehicle_type: 'LF 16/12', fuel_type: 'Diesel', tuv_date: null },
                hydrantInspection: { hydrant_id: 0, tester_name: '', kappe_gefettet: true, schild_lesbar: true, maengel_text: '' },
                newArchiveDoc: { title: '', keywords: 'Dienstvorschrift', file_blob: '' }
            }
        },
        watch: {
            currentTab(newVal) {
                if (newVal === 'lagekarte') { this.initMap(); }
            }
        },
        computed: {
            // FIX: Berechnet die offenen Mängel browserkompatibel ohne Pfeilfunktionen im HTML
            openTicketsCount() {
                if (!Array.isArray(this.tickets)) return 0;
                return this.tickets.filter(function(t) { return t.status === 'neu'; }).length;
            },
            lowStockItems() {
                if (this.identity.role !== 'admin' && this.identity.role !== 'geratewart') return [];
                if (!Array.isArray(this.inventory)) return [];
                return this.inventory.filter(function(i) { return i.amount <= i.min_amount; });
            },
            myPSA() {
                var self = this;
                if (!this.identity.personnel_id || !Array.isArray(this.psaList)) return [];
                return this.psaList.filter(function(p) { return p.person_id === self.identity.personnel_id; });
            },
            filteredInventory() {
                var self = this;
                if (!Array.isArray(this.inventory)) return [];
                return this.inventory.filter(function(i) {
                    var matchGewerk = self.lagerFilter.gewerk === 'Alle' || i.category === self.lagerFilter.gewerk;
                    var matchSuche = !self.lagerFilter.suche || 
                        i.item_name.toLowerCase().includes(self.lagerFilter.suche.toLowerCase()) || 
                        (i.qr_code_id && i.qr_code_id.toLowerCase().includes(self.lagerFilter.suche.toLowerCase()));
                    return matchGewerk && matchSuche;
                });
            },
            deadlineAlerts() {
                if (this.identity.role !== 'admin' && this.identity.role !== 'geratewart') return [];
                if (!Array.isArray(this.personnel)) return [];
                
                var alerts = []; var today = new Date();
                this.personnel.forEach(function(p) {
                    if (p.g26_3_date) {
                        var diff = (new Date(p.g26_3_date) - today) / (1000 * 60 * 60 * 24);
                        if (diff <= 90) alerts.push({ msg: "G26.3 Untersuchung läuft ab bei: " + p.name + " (" + p.g26_3_date + ")!" });
                    }
                    if (p.last_license_check) {
                        var diffDays = (today - new Date(p.last_license_check)) / (1000 * 60 * 60 * 24);
                        if (diffDays >= 365) alerts.push({ msg: "Führerschein-Prüfung fällig bei Maschinist: " + p.name + "!" });
                    }
                });
                if (Array.isArray(this.vehicles)) {
                    this.vehicles.forEach(function(v) {
                        if (v.tuv_date) {
                            var diff = (new Date(v.tuv_date) - today) / (1000 * 60 * 60 * 24);
                            if (diff <= 60) alerts.push({ msg: "HU/TÜV fällig bei Fahrzeug: " + v.name + " (" + v.tuv_date + ")!" });
                        }
                    });
                }
                return alerts;
            },
            statistics() {
                var totalHours = 0; var uebungen = 0; var einsatze = 0; var sonstiges = 0;
                var agtCount = 0; var maschinistCount = 0; var gfCount = 0;
                var mtaBasis = 0; var mtaErgaenzung = 0; var mtaFertig = 0;

                var safeSessions = Array.isArray(this.sessions) ? this.sessions : (this.sessions && Array.isArray(this.sessions.sessions) ? this.sessions.sessions : []);
                safeSessions.forEach(function(s) {
                    totalHours += (parseFloat(s.duration) || 0);
                    if (s.category === 'Übung') uebungen++;
                    else if (s.category === 'Einsatz') einsatze++;
                    else sonstiges++;
                });

                var safePersonnel = Array.isArray(this.personnel) ? this.personnel : [];
                safePersonnel.forEach(function(p) {
                    // FIX: Tolerantes Matching fängt MySQL TINYINT (1/0) sauber als Boolean ab
                    if (p.is_agt == 1 || p.is_agt === true || p.is_agt === 'true') agtCount++;
                    if (p.is_maschinist == 1 || p.is_maschinist === true || p.is_maschinist === 'true') maschinistCount++;
                    if (p.is_gf == 1 || p.is_gf === true || p.is_gf === 'true') gfCount++;
                    
                    if (p.mta_status === 'Basis' || p.mta_status === 'MTA-Basismodul') mtaBasis++;
                    else if (p.mta_status === 'Ergänzung' || p.mta_status === 'MTA-Ergänzungsmodul') mtaErgaenzung++;
                    else if (p.mta_status === 'Truppführer' || p.mta_status === 'MTA-Truppführer absolviert') mtaFertig++;
                });

                return {
                    totalHours: totalHours.toFixed(1), totalEvents: safeSessions.length, uebungen, einsatze, sonstiges,
                    members: safePersonnel.length, agtCount, maschinistCount, gfCount, mtaBasis, mtaErgaenzung, mtaFertig
                };
            }
        },
        methods: {
            showToast(msg, isError) {
                this.toastMessage = msg; this.toastClass = isError ? 'bg-danger' : 'bg-success'; this.toastVisible = true;
                setTimeout(() => { this.toastVisible = false; }, 4000);
            },
            closeToast() { this.toastVisible = false; },
            openModal(id) { this.modalActive = id; },
            closeModal() { this.modalActive = null; },
            
            async apiCall(url, method, body) {
                var options = { method: method || 'GET', headers: { 'Content-Type': 'application/json' } };
                if (body) options.body = JSON.stringify(body);
                try {
                    var res = await fetch(url, options);
                    return res.ok ? await res.json() : null;
                } catch (e) { this.showToast("Schnittstellenfehler zum Zentralrechner.", true); return null; }
            },
            async fetchJson(url) {
                try {
                    var r = await fetch(url + '?t=' + new Date().getTime(), { headers: { 'Cache-Control': 'no-cache' } });
                    return r.ok ? await r.json() : null;
                } catch (e) { return null; }
            },
            async refreshAllData() {
                var endpoints = {
                    vehicles: '/api/vehicles', inventory: '/api/inventory', personnel: '/api/personnel/list',
                    weather: '/api/weather', registry: '/api/settings', vehicleLogs: '/api/vehicles/logs',
                    users: '/api/users', tickets: '/api/tickets', activeAlarm: '/api/alarm/active',
                    archiveDocs: '/api/archive/list', sessions: '/groups/1/sessions', psaList: '/api/psa'
                };
                for (var key in endpoints) {
                    try {
                        var data = await this.fetchJson(endpoints[key]);
                        if (key === 'activeAlarm') {
                            // GLOBALER ALARM-ABGLEICH: Wenn der Server "no_alarm" meldet, schließt sich das Banner überall
                            if (data && data.status !== 'no_alarm') { this.activeAlarm = data; } 
                            else { this.activeAlarm = null; }
                        } else if (data) {
                            this[key] = data;
                        }
                    } catch (e) {}
                }
            },
            
            // SERVERBASIERTE ALARM-QUITTIERUNG (Gilt live für alle Monitore)
            async clearActiveAlarm() {
                await fetch('/api/alarm/clear', { method: 'POST' });
                this.activeAlarm = null;
                this.showToast("Einsatzmeldung global auf allen Stationen beendet.");
                this.refreshAllData();
            },

            async deleteSessionReport(id) {
                if (confirm("Möchtest du diesen Dienstbericht permanent aus dem Logbuch löschen?")) {
                    var res = await fetch('/groups/1/sessions/' + id, { method: 'DELETE' });
                    if (res.ok) { this.showToast("Bericht erfolgreich gelöscht."); this.refreshAllData(); }
                }
            },
            goToEditor(sId) { window.location.href = '/editor?group_id=1' + (sId ? '&session_id=' + sId : ''); },
            async setVehicleStatus(v, s) {
                v.status = s;
                await fetch('/api/vehicles/' + v.id + '/status', {
                    method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ status: s })
                });
                this.showToast(v.name + " setzt Status " + s);
            },
            
            initMap() {
                var self = this;
                this.$nextTick(function() {
                    var mapEl = document.getElementById('map'); if (!mapEl) return;
                    if (!window.feuerwehrMapInstance) {
                        var lat = parseFloat(self.registry.station_lat) || 47.9994; var lon = parseFloat(self.registry.station_lon) || 10.1325;
                        window.feuerwehrMapInstance = L.map('map').setView([lat, lon], 16);
                        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png').addTo(window.feuerwehrMapInstance);
                        window.feuerwehrMapInstance.on('click', function(e) {
                            if (self.identity.role === 'mannschaft') return;
                            self.newHyd = { lat: e.latlng.lat, lon: e.latlng.lng, hydrant_type: 'Unterflurhydrant', diameter: 'DN80' };
                            self.openModal('hydrantModal');
                        });
                    } else { setTimeout(function() { window.feuerwehrMapInstance.invalidateSize(); }, 50); }
                    if (!window.hydrantLayerGroup) { window.hydrantLayerGroup = L.layerGroup().addTo(window.feuerwehrMapInstance); } 
                    else { window.hydrantLayerGroup.clearLayers(); }
                    
                    fetch('/api/hydranten').then(function(res) { return res.json(); }).then(function(data) {
                        if (data && Array.isArray(data)) {
                            data.forEach(function(h) {
                                if (h.hydrant_type === 'Unterflurhydrant' && !self.mapFilters.unterflur) return;
                                if (h.hydrant_type === 'Überflurhydrant' && !self.mapFilters.ueberflur) return;
                                if (h.hydrant_type === 'Löschwasserzisterne' && !self.mapFilters.zisterne) return;
                                L.marker([h.lat, h.lon]).addTo(window.hydrantLayerGroup).bindPopup(`
                                    <div class="p-1">
                                        <b class="text-danger fs-6"><i class="fa fa-faucet"></i> ${h.hydrant_type}</b><br>
                                        <b>Dimension:</b> ${h.diameter}<br><hr class="my-1">
                                        <button class="btn btn-xs btn-dark mt-2 w-100 rounded-pill font-weight-bold" onclick="window.vueApp.startHydrantCheck(${h.id})">Prüfung</button>
                                        <button class="btn btn-xs btn-outline-danger mt-1 w-100 rounded-pill font-weight-bold" onclick="window.vueApp.deleteHydrant(${h.id})">Löschen</button>
                                    </div>
                                `);
                            });
                        }
                    });
                });
            },
            startHydrantCheck(id) {
                this.hydrantInspection = { hydrant_id: id, tester_name: this.identity.username, kappe_gefettet: true, schild_lesbar: true, maengel_text: '' };
                this.openModal('hydrantCheckModal');
            },
            async saveHydrantCheck() {
                if (!this.hydrantInspection.kappe_gefettet || !this.hydrantInspection.schild_lesbar || this.hydrantInspection.maengel_text) {
                    var text = "Prüfmangel bei Hydrant #" + this.hydrantInspection.hydrant_id + ": " + (this.hydrantInspection.maengel_text || "Mangel bei Routineprüfung.");
                    await this.apiCall('/api/tickets', 'POST', { title: 'Hydrantenmangel', content: text, priority: 'normal', status: 'neu', vehicle_id: 0 });
                }
                this.showToast("Hydrantenprüfbericht hinterlegt!"); this.closeModal(); this.refreshAllData();
            },
            async deleteHydrant(id) {
                if (this.identity.role === 'mannschaft') return;
                if (confirm("Soll diese Wasserentnahmestelle permanent gelöscht werden?")) {
                    await fetch('/api/hydranten/' + id, { method: 'DELETE' }); this.initMap();
                }
            },
            async saveHydrant() { await this.apiCall('/api/hydranten', 'POST', this.newHyd); this.closeModal(); this.initMap(); },

            searchHazmat() {
                if (!this.hazmatSearchQuery) return;
                var un = this.hazmatSearchQuery.trim();
                var unDatabase = {
                    "1202": { un_number: "1202", substance: "DIESELKRAFTSTOFF / HEIZÖL", danger_text: "Entzündbar. Gewässerschaden droht.", safety_measures: "Standard-Brandschutz. Schaumbereitschaft. Gewässer schützen.", radius: "50 Meter", gear: "Standard-PSA + Atemschutz", water_risk: "Stark Wassergefährdend!" },
                    "1203": { un_number: "1203", substance: "BENZIN / OTTOKRAFTSTOFF", danger_text: "Extrem entzündbar. Bildet explosive Dampfwolken.", safety_measures: "Dreifachschutz aufbauen. Funkenflug verhindern.", radius: "100 Meter", gear: "Brandschutzkleidung + Atemschutz", water_risk: "Wassergefährdend." }
                };
                this.activeEriCard = unDatabase[un] || { un_number: un, substance: "Sonder-Gefahrstoff", danger_text: "Stoff nicht im Schnellregister.", safety_measures: "Sicherheitsabsperrung weiträumig aufbauen. Fachberater anfordern.", radius: "100 Meter", gear: "CSA empfohlen", water_risk: "Gefahr annehmen." };
            },

            async generateQRWindow(i) {
                var id = i.qr_code_id || 'QR-' + Math.random().toString(36).substr(2, 6).toUpperCase(); i.qr_code_id = id;
                await this.apiCall('/api/inventory', 'POST', i); this.refreshAllData(); this.openModal('qrModal');
                this.$nextTick(function() {
                    var box = document.getElementById('qrcode'); if (!box) return; box.innerHTML = "";
                    new QRCode(box, { text: window.location.origin + "/dashboard?tab=lager&qr=" + id, width: 160, height: 160 });
                });
            },
            async generatePsaQR(p) {
                this.openModal('qrModal');
                this.$nextTick(function() {
                    var box = document.getElementById('qrcode'); if (!box) return; box.innerHTML = "";
                    new QRCode(box, { text: window.location.origin + "/dashboard?tab=lager&qr=" + p.qr_code_id, width: 160, height: 160 });
                });
            },

            processArchiveFile(e) {
                var self = this; var file = e.target.files[0]; if (!file) return;
                var reader = new FileReader(); reader.onload = function(ev) { self.newArchiveDoc.file_blob = ev.target.result; }; reader.readAsDataURL(file);
            },
            async saveArchiveDoc() {
                if (!this.newArchiveDoc.title) return;
                await this.apiCall('/api/archive/upload', 'POST', this.newArchiveDoc);
                this.newArchiveDoc = { title: '', keywords: 'Dienstvorschrift', file_blob: '' };
                this.showToast("Dokument im Wachenarchiv gesichert."); this.refreshAllData();
            },
            async delArchiveDoc(id) {
                if (confirm("Dokument permanent löschen?")) { await fetch('/api/archive/' + id, { method: 'DELETE' }); this.refreshAllData(); }
            },
            viewArchiveDoc(doc) {
                if (!doc.file_blob) return; var win = window.open();
                if (win) win.document.write(`<iframe src="${doc.file_blob}" frameborder="0" style="border:0; top:0px; left:0px; width:100%; height:100%;" allowfullscreen></iframe>`);
            },

            // AUTOMATISCHE UNTERSCHEIDUNG FÜR EDITIEREN (PUT) VS ERSTELLEN (POST)
            async saveTicket() { 
                var url = this.newTicket.id ? '/api/tickets/' + this.newTicket.id : '/api/tickets';
                var method = this.newTicket.id ? 'PUT' : 'POST';
                await this.apiCall(url, method, this.newTicket); 
                this.closeModal(); this.showToast("Mängelbericht synchronisiert."); this.refreshAllData(); 
            },
            async setTicketStatus(id, status) { await this.apiCall('/api/tickets/' + id + '/status', 'PUT', { status: status }); this.refreshAllData(); },
            async delTicket(id) { if (confirm("Mangel permanent löschen?")) { await fetch('/api/tickets/' + id, { method: 'DELETE' }); this.refreshAllData(); } },

            async changeMyPassword() {
                if (!this.profilePassword || this.profilePassword.trim().length < 4) { this.showToast("Das Passwort muss mindestens 4 Zeichen lang sein!", true); return; }
                var res = await this.apiCall('/api/users/password/self', 'PUT', { password: this.profilePassword.trim() });
                if (res && res.status === 'success') { this.showToast("Passwort aktualisiert!"); this.profilePassword = ''; }
            },

            async saveSettings() {
                var res = await this.apiCall('/api/settings', 'POST', this.registry);
                if (res) { this.showToast("Wachenkonfiguration gesichert!"); this.refreshAllData(); }
            },

            processProfilePic(e) {
                var self = this; var file = e.target.files[0]; if (!file) return;
                var reader = new FileReader(); reader.onload = function(ev) { self.newMem.profile_picture = ev.target.result; }; reader.readAsDataURL(file);
            },
            openMemberModal(p) {
                this.newMem = p ? Object.assign({}, p) : { id: null, name: '', rank: 'Feuerwehranwärter', membership_status: 'Aktiv', is_agt: false, is_maschinist: false, is_gf: false, qualifications: '', size_helm: '', size_jacke: '', size_stiefel: '', last_license_check: null, mta_status: 'Basis', profile_picture: null };
                this.openModal('memberModal');
            },
            async saveMember() { 
                var url = this.newMem.id ? '/api/personnel/' + this.newMem.id : '/api/personnel';
                var method = this.newMem.id ? 'PUT' : 'POST';
                await this.apiCall(url, method, this.newMem); 
                this.closeModal(); this.showToast("Kameradenakte synchronisiert."); this.refreshAllData(); 
            },
            openUserModal(u) {
                this.newUser = u ? Object.assign({}, u) : { id: null, username: '', password: '', role: 'mannschaft', personnel_id: 0 };
                this.openModal('userModal');
            },
            async saveUser() { 
                var url = this.newUser.id ? '/api/users/' + this.newUser.id : '/api/users';
                var method = this.newUser.id ? 'PUT' : 'POST';
                await this.apiCall(url, method, this.newUser); 
                this.closeModal(); this.showToast("Systemlogin konfiguriert."); this.refreshAllData(); 
            },
            openInvModal(i) {
                this.newInv = i ? Object.assign({}, i) : { id: null, item_name: '', amount: 1, min_amount: 5, unit: 'Stück', location: 'Lager', size: '', category: 'Brandschutz', manufacturer: '', serial_number: '' };
                this.openModal('invModal');
            },
            async saveInv() { 
                var url = this.newInv.id ? '/api/inventory/' + this.newInv.id : '/api/inventory';
                var method = this.newInv.id ? 'PUT' : 'POST';
                await this.apiCall(url, method, this.newInv); 
                this.closeModal(); this.showToast("Lagerpool aktualisiert."); this.refreshAllData(); 
            },
            openPsaModal(p) {
                var self = this;
                var kamerad = this.personnel.find(function(x) { return x.id === (p ? p.person_id : null); });
                this.newPsa = p ? Object.assign({}, p) : { id: null, person_id: 0, item_name: '', size: kamerad ? kamerad.size_jacke : '', qr_code_id: '', status: 'Ausgegeben', next_check: null };
                this.openModal('psaModal');
            },
            async savePsaAssignment() {
                if (!this.newPsa.qr_code_id) { this.newPsa.qr_code_id = 'PSA-' + Math.random().toString(36).substr(2, 6).toUpperCase(); }
                var url = this.newPsa.id ? '/api/psa/' + this.newPsa.id : '/api/psa';
                var method = this.newPsa.id ? 'PUT' : 'POST';
                await this.apiCall(url, method, this.newPsa); 
                this.closeModal(); this.showToast("Ausrüstung zugewiesen."); this.refreshAllData();
            },
            async revokePsa(id) {
                if (confirm("Soll diese Ausrüstung vom Kameraden zurückgenommen werden?")) {
                    await fetch('/api/psa/' + id, { method: 'DELETE' }); this.showToast("Ausrüstung zurückgenommen."); this.refreshAllData();
                }
            },
            openVehModal(v) {
                this.newVeh = v ? Object.assign({}, v) : { id: null, name: '', radio_name: '', status: 2, milage: 0, operating_hours: 0.0, license_plate: '', vehicle_type: 'LF 16/12', fuel_type: 'Diesel', tuv_date: null };
                this.openModal('vehModal');
            },
            async saveVeh() { 
                var url = this.newVeh.id ? '/api/vehicles/' + this.newVeh.id : '/api/vehicles';
                var method = this.newVeh.id ? 'PUT' : 'POST';
                await this.apiCall(url, method, this.newVeh); 
                this.closeModal(); this.showToast("Fahrzeugstamm aktualisiert."); this.refreshAllData(); 
            },
            getPersonnelName(id) {
                if (!id || id === 0) return 'Systemzugang (Reiner App-User)'; if (!Array.isArray(this.personnel)) return 'ID: ' + id;
                var found = this.personnel.find(function(x) { return x.id === id; }); return found ? found.name : 'ID: ' + id;
            },
            async triggerLogout() { await fetch('/api/logout', { method: 'POST' }); window.location.href = '/login'; }
        },
        async mounted() {
            var self = this;
            this.webhookUrl = window.location.origin + '/api/webhook/alarm';
            var urlParams = new URLSearchParams(window.location.search);
            var targetTab = urlParams.get('tab'); var qrId = urlParams.get('qr');
            if (targetTab && qrId) { sessionStorage.setItem('deep_link_tab', targetTab); sessionStorage.setItem('deep_link_qr', qrId); }
            
            var res = await this.fetchJson('/api/auth/me');
            if (!res) { window.location.href = '/login'; return; }
            this.identity = res;
            
            await this.refreshAllData();
            this.ready = true;
            
            var savedTab = sessionStorage.getItem('deep_link_tab'); var savedQr = sessionStorage.getItem('deep_link_qr');
            if (savedTab && savedQr) {
                sessionStorage.removeItem('deep_link_tab'); sessionStorage.removeItem('deep_link_qr'); this.currentTab = savedTab;
                this.$nextTick(function() {
                    var found = self.psaList.find(function(p) { return p.qr_code_id === savedQr; }) || self.inventory.find(function(i) { return i.qr_code_id === savedQr; });
                    if (found) {
                        if (found.person_id !== undefined) { self.newPsa = Object.assign({}, found); self.openModal('psaModal'); self.subTabLager = 'ausgabe'; } 
                        else { self.openInvModal(found); self.subTabLager = 'bestand'; }
                    }
                });
            }
        }
    }).mount('#app');
}
// Fahrzeuge, Reservierungen und Fahrtenbuch.
// Teil der Dashboard-App (Vue-Mixin): Daten stehen in ../dashboard.js, "this" ist die ganze App.
window.DB_MIXINS = window.DB_MIXINS || [];
window.DB_MIXINS.push({
            methods: {

                // Vehicles & Logs
                async loadVehicles() {
                    try {
                        const res = await fetch('/api/vehicles', { credentials: 'include' });
                        if (res.ok) this.vehicles = await res.json();
                    } catch(e) { console.error("Error loading vehicles", e); }
                },
                async quickStatusChange(v) { await fetch(`/api/vehicles/${v.id}/status`, { method: 'PUT', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify({ status: parseInt(v.status) }) }); },
                async loadVehicleLogs() {
                    if(!this.activeVehicleForLog) return;
                    const res = await fetch(`/api/material/vehicles/${this.activeVehicleForLog.id}/log`, { credentials: 'include' });
                    if(res.ok) this.vehicleLogs = await res.json();
                    this.$nextTick(() => this.renderMileageChart());
                    await this.loadVehicleReservations();
                },
                async loadVehicleReservations() {
                    if (!this.activeVehicleForLog) { this.vehicleReservations = []; return; }
                    try {
                        const res = await fetch(`/api/vehicles/${this.activeVehicleForLog.id}/reservations`, { credentials: 'include' });
                        this.vehicleReservations = res.ok ? await res.json() : [];
                    } catch(e) { this.vehicleReservations = []; }
                },
                async openReservationModal() {
                    this.newReservation = { purpose: '', start_datetime: '', end_datetime: '' };
                    if (!this.myLicenses) {
                        try {
                            const res = await fetch('/api/personnel/me/licenses', { credentials: 'include' });
                            this.myLicenses = res.ok ? await res.json() : {};
                        } catch(e) { this.myLicenses = {}; }
                    }
                    new bootstrap.Modal(document.getElementById('vehicleReservationModal')).show();
                },
                async submitVehicleReservation() {
                    const res = await fetch(`/api/vehicles/${this.activeVehicleForLog.id}/reservations`, { method: 'POST', headers: {'Content-Type':'application/json'}, credentials: 'include', body: JSON.stringify(this.newReservation) });
                    if (res.ok) {
                        bootstrap.Modal.getInstance(document.getElementById('vehicleReservationModal')).hide();
                        await this.loadVehicleReservations();
                    } else {
                        const err = await res.json().catch(() => ({}));
                        await appAlert(err.detail || "Reservierung konnte nicht gespeichert werden.");
                    }
                },
                async deleteVehicleReservation(id) {
                    if (!await appConfirm("Reservierung stornieren?")) return;
                    const res = await fetch(`/api/vehicles/reservations/${id}`, { method: 'DELETE', credentials: 'include' });
                    if (res.ok) { await this.loadVehicleReservations(); }
                    else {
                        const err = await res.json().catch(() => ({}));
                        await appAlert(err.detail || "Stornieren fehlgeschlagen.");
                    }
                },
                renderMileageChart() {
                    const ctx = document.getElementById('mileageChart');
                    if (!ctx) return;
                    if (this.mileageChartInstance) this.mileageChartInstance.destroy();
                    // Backend liefert absteigend (neueste zuerst) - für den Verlauf über die Zeit
                    // wird hier aufsteigend sortiert gebraucht.
                    const ordered = [...this.vehicleLogs].sort((a, b) => new Date(a.date) - new Date(b.date));
                    this.mileageChartInstance = new Chart(ctx, {
                        type: 'line',
                        data: {
                            labels: ordered.map(l => l.date),
                            datasets: [{
                                label: 'Kilometerstand',
                                data: ordered.map(l => l.mileage_end),
                                borderColor: 'rgba(183, 28, 28, 1)',
                                backgroundColor: 'rgba(183, 28, 28, 0.15)',
                                fill: true,
                                tension: 0.2
                            }]
                        },
                        options: { responsive: true, maintainAspectRatio: false, animation: false, scales: { y: { beginAtZero: false } } }
                    });
                },
                openLogRideModal() {
                    this.newLogRide = { date: localISODate(), mileage_start: this.activeVehicleForLog.milage || 0, mileage_end: (this.activeVehicleForLog.milage || 0) + 10, driver_name: '', purpose: '' };
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

                // Vehicles Form Actions
                async openChecklist(veh) {
                    this.activeVehicleForCheck = veh;
                    this.currentChecklist = { date: localISODate(), checker_name: this.username, status: 'OK', notes: '', items_checked: {'Ölstand (Motor)': false, 'Reifendruck': false, 'Beleuchtung / Blaulicht': false, 'Funkgeräte (HRT/MRT)': false, 'Erste-Hilfe-Rucksack': false, 'Atemschutzgeräte': false} };
                    await this.loadCheckHistory(veh.id);
                    new bootstrap.Modal(document.getElementById('vehicleCheckModal')).show();
                },
                async loadCheckHistory(vehId) {
                    try {
                        const res = await fetch(`/api/material/vehicles/${vehId}/checks`, { credentials: 'include' });
                        if(res.ok) this.vehicleCheckHistory = await res.json();
                    } catch(e) { console.error(e); }
                },
                async deleteVehicleCheck(checkId) {
                    if (!await appConfirm("Diesen Fahrzeug-Check unwiderruflich löschen?")) return;
                    const res = await fetch(`/api/material/vehicles/checks/${checkId}`, { method: 'DELETE', credentials: 'include' });
                    if (res.ok) {
                        await this.loadCheckHistory(this.activeVehicleForCheck.id);
                    } else {
                        await appAlert("Löschen fehlgeschlagen.");
                    }
                },
                async submitChecklist() {
                    // Check if any items are unchecked
                    const allChecked = Object.values(this.currentChecklist.items_checked).every(v => v);
                    this.currentChecklist.status = allChecked ? 'OK' : 'Mängel';
                    if (!allChecked && !this.currentChecklist.notes) {
                        await appAlert("Da nicht alle Punkte abgehakt wurden, trage bitte die gefundenen Mängel in die Bemerkungen ein!");
                        return;
                    }
                    try {
                        const res = await fetch(`/api/material/vehicles/${this.activeVehicleForCheck.id}/checks`, {
                            method: 'POST', headers: {'Content-Type':'application/json'}, credentials: 'include', body: JSON.stringify(this.currentChecklist)
                        });
                        if (res.ok) {
                            bootstrap.Modal.getInstance(document.getElementById('vehicleCheckModal')).hide();
                            await appAlert("Fahrzeug-Check erfolgreich gespeichert!");
                            await this.loadVehicles(); // update status if needed
                        } else {
                            await appAlert("Fahrzeug-Check konnte nicht gespeichert werden.");
                        }
                    } catch(e) { await appAlert("Verbindung fehlgeschlagen."); }
                },
                openVehicleFormModal(veh) {
                    if (veh) {
                        this.activeVehicle = { ...veh };
                        this.loadVehicleDocuments(veh.id);
                        this.loadVehicleMaintenance(veh.id);
                    } else {
                        this.activeVehicle = { id: null, name: '', radio_name: '', status: 2, tuv_date: '', sp_date: '', milage: 0, next_service: '', required_license: null, purchase_value: null, insurance_policy: '' };
                        this.vehicleDocuments = [];
                        this.vehicleMaintenance = [];
                    }
                    new bootstrap.Modal(document.getElementById('vehicleFormModal')).show();
                },
                async loadVehicleMaintenance(vehicleId) {
                    try {
                        const res = await fetch(`/api/vehicles/${vehicleId}/maintenance`, { credentials: 'include' });
                        this.vehicleMaintenance = res.ok ? await res.json() : [];
                    } catch(e) { this.vehicleMaintenance = []; }
                },
                async submitVehicleMaintenance() {
                    if (!this.newMaintenance.description.trim()) return;
                    const res = await fetch(`/api/vehicles/${this.activeVehicle.id}/maintenance`, {
                        method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'include',
                        body: JSON.stringify(this.newMaintenance)
                    });
                    if (res.ok) {
                        this.newMaintenance = { date: localISODate(), description: '', workshop: '', cost: 0 };
                        await this.loadVehicleMaintenance(this.activeVehicle.id);
                    }
                },
                async deleteVehicleMaintenance(id) {
                    if (!await appConfirm("Eintrag löschen?")) return;
                    await fetch(`/api/vehicles/maintenance/${id}`, { method: 'DELETE', credentials: 'include' });
                    await this.loadVehicleMaintenance(this.activeVehicle.id);
                },
                async loadVehicleDocuments(vehicleId) {
                    try {
                        const res = await fetch(`/api/vehicles/${vehicleId}/documents`, { credentials: 'include' });
                        this.vehicleDocuments = res.ok ? await res.json() : [];
                    } catch(e) { this.vehicleDocuments = []; }
                },
                async uploadVehicleDocument(event) {
                    const file = event.target.files[0];
                    if (!file) return;
                    const formData = new FormData();
                    formData.append('file', file);
                    formData.append('is_public', 'true');
                    formData.append('vehicle_id', this.activeVehicle.id);
                    const res = await fetch('/api/archive/upload', { method: 'POST', credentials: 'include', body: formData });
                    if (res.ok) {
                        await this.loadVehicleDocuments(this.activeVehicle.id);
                    } else {
                        const err = await res.json().catch(() => ({}));
                        await appAlert(err.detail || "Upload fehlgeschlagen.");
                    }
                    if (this.$refs.vehicleDocFile) this.$refs.vehicleDocFile.value = '';
                },
                async deleteVehicleDocument(docId) {
                    if (!await appConfirm("Dokument löschen?")) return;
                    const res = await fetch(`/api/archive/files/${docId}`, { method: 'DELETE', credentials: 'include' });
                    if (res.ok) await this.loadVehicleDocuments(this.activeVehicle.id);
                    else await appAlert("Löschen fehlgeschlagen.");
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
                    if (await appConfirm("Fahrzeug aus Fuhrpark löschen?")) {
                        const res = await fetch(`/api/vehicles/${id}`, { method: 'DELETE', credentials: 'include' });
                        if (res.ok) await this.loadVehicles();
                    }
                },
            }
});

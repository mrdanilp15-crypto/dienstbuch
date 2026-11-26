// Objekte, Brandmeldeanlagen, Lagekarte.
// Teil der Dashboard-App (Vue-Mixin): Daten stehen in ../dashboard.js, "this" ist die ganze App.
window.DB_MIXINS = window.DB_MIXINS || [];
window.DB_MIXINS.push({
            computed: {
                filteredMapHydrants() {
                    const q = this.mapSearchQuery.toLowerCase();
                    if (this.mapFilterCategory !== 'all' && this.mapFilterCategory !== 'hydrant') return [];
                    return (this.hydrantsList || []).filter(h => h.label.toLowerCase().includes(q) || h.type.toLowerCase().includes(q));
                },
                filteredMapBmas() {
                    const q = this.mapSearchQuery.toLowerCase();
                    if (this.mapFilterCategory !== 'all' && this.mapFilterCategory !== 'bma') return [];
                    return (this.bmas || []).filter(b => b.object_name.toLowerCase().includes(q) || b.address.toLowerCase().includes(q) || b.bma_number.toLowerCase().includes(q));
                },
            },
            methods: {

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
                    if(await appConfirm("Objekt löschen?")) {
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
                                map.setView([this.stationConfig.lat, this.stationConfig.lng], this.stationConfig.zoom);
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
                            .bindPopup(`<b>${escHtml(h.label)}</b><br>Typ: ${escHtml(h.type)}<br><button class="btn btn-xs btn-danger mt-1 text-white border-0" onclick="window.deleteHydrant(${h.id})">Löschen</button>`)
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
                                .bindPopup(`<b>BMA: ${escHtml(b.object_name)}</b><br>BMA-Nr: ${escHtml(b.bma_number)}<br>Schlüssel-Depot: ${b.key_depot ? 'JA' : 'NEIN'}<br><button class="btn btn-xs btn-danger mt-1 text-white border-0" onclick="window.deleteBmaOnMap(${b.id})">Entfernen</button>`)
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
                        await appAlert("Fehler beim Speichern des Objekts.");
                    }
                },
                panToCoords(lat, lng, zoom) {
                    if (map) {
                        map.setView([lat, lng], zoom);
                    }
                },
                async deleteHydrantItem(id) {
                    if (await appConfirm("Objekt wirklich permanent von der Karte löschen?")) {
                        const res = await fetch(`/api/material/hydrants/${id}`, { method: 'DELETE', credentials: 'include' });
                        if (res.ok) this.loadHydrants();
                    }
                },
                async deleteBmaItem(id) {
                    if (await appConfirm("BMA-Objekt wirklich permanent aus dem Verzeichnis und von der Karte löschen?")) {
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
                    if (await appConfirm("Bild wirklich permanent löschen?")) {
                        const res = await fetch(`/api/material/drone-images/${id}`, { method: 'DELETE', credentials: 'include' });
                        if (res.ok) await this.loadDroneImages();
                    }
                },
            }
});

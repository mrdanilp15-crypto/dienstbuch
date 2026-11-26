// Einstellungen: Rechte, Konten, Sicherung, Wache, Archiv.
// Teil der Dashboard-App (Vue-Mixin): Daten stehen in ../dashboard.js, "this" ist die ganze App.
window.DB_MIXINS = window.DB_MIXINS || [];
window.DB_MIXINS.push({
            computed: {
                calendarFeedUrl() { return `${window.location.protocol}//${this.serverHost}/api/calendar/feed.ics?token=${this.accessTokens.calendar_token}`; },
                displayUrl() { return `${window.location.protocol}//${this.serverHost}/static/alarmdisplay.html?token=${this.accessTokens.display_token}`; },
                settingsGroups() {
                    const admin = this.role === 'admin';
                    const groups = [
                        { label: 'Konten & Sicherheit', tabs: [
                            { key: 'logins', label: 'System-Logins', icon: 'fa-key', show: admin },
                            { key: 'rechte', label: 'Rechte', icon: 'fa-user-lock', show: admin },
                            { key: 'audit', label: 'Aktivitäts-Log', icon: 'fa-clock-rotate-left', show: admin },
                            { key: 'sicherheit', label: 'Sicherheit & Datenschutz', icon: 'fa-shield-halved', show: admin } ] },
                        { label: 'Betrieb', tabs: [
                            { key: 'betrieb', label: 'Systemzustand & Sicherung', icon: 'fa-heart-pulse', show: admin } ] },
                        { label: 'Organisation', tabs: [
                            { key: 'wache', label: 'Wache & Fristen', icon: 'fa-map-pin', show: this.canSeeSettings } ] },
                        { label: 'Kommunikation', tabs: [
                            { key: 'mail', label: 'E-Mail-Versand', icon: 'fa-envelope', show: admin } ] }
                    ];
                    return groups.map(g => ({ label: g.label, tabs: g.tabs.filter(t => t.show) })).filter(g => g.tabs.length);
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
            },
            methods: {
                async loadHealth() {
                    this.health = null;
                    const res = await fetch('/api/admin/health', { credentials: 'include' });
                    if (res.ok) this.health = await res.json();
                },
                hasPerm(key) { return this.role === 'admin' || (this.myPerms || []).includes(key); },
                async loadPermissionMatrix() {
                    try {
                        const res = await fetch('/api/admin/permissions', { credentials: 'include' });
                        this.permissionMatrix = res.ok ? await res.json() : null;
                    } catch (e) { this.permissionMatrix = null; }
                },
                async setPermission(p, role, allowed) {
                    const res = await fetch('/api/admin/permissions', { method: 'PUT', headers: {'Content-Type': 'application/json'}, credentials: 'include',
                        body: JSON.stringify({ role, permission: p.key, allowed }) });
                    if (!res.ok) await appAlert((await res.json().catch(() => ({}))).detail || 'Recht konnte nicht geändert werden.');
                    await this.loadPermissionMatrix();
                },
                async resetPermissions() {
                    if (!await appConfirm('Alle Rechte auf die Standardwerte zurücksetzen?')) return;
                    const res = await fetch('/api/admin/permissions/reset', { method: 'POST', credentials: 'include' });
                    if (res.ok) await this.loadPermissionMatrix();
                },
                customRoleList() { return this.permissionMatrix ? this.permissionMatrix.roles.filter(r => r.custom) : []; },
                async createCustomRole() {
                    const res = await fetch('/api/admin/custom-roles', { method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify(this.newCustomRole) });
                    if (res.ok) { this.newCustomRole.label = ''; await this.loadPermissionMatrix(); }
                    else await appAlert((await res.json().catch(() => ({}))).detail || 'Rolle konnte nicht angelegt werden.');
                },
                async deleteCustomRole(r) {
                    if (!await appConfirm(`Rolle „${r.label}“ löschen? Konten mit dieser Rolle fallen auf ihre Basisrolle (${r.base_label}) zurück.`)) return;
                    const res = await fetch('/api/admin/custom-roles/' + encodeURIComponent(r.key), { method: 'DELETE', credentials: 'include' });
                    if (res.ok) await this.loadPermissionMatrix();
                    else await appAlert((await res.json().catch(() => ({}))).detail || 'Rolle konnte nicht gelöscht werden.');
                },
                permissionGroups() {
                    if (!this.permissionMatrix) return [];
                    const g = {};
                    this.permissionMatrix.permissions.forEach(p => { (g[p.group] = g[p.group] || []).push(p); });
                    return Object.keys(g).map(k => ({ name: k, items: g[k] }));
                },
                firstSettingsTab() { const g = this.settingsGroups; return g.length ? g[0].tabs[0].key : ''; },
                ensureSettingsTab() {
                    const keys = this.settingsGroups.flatMap(g => g.tabs.map(t => t.key));
                    if (!keys.includes(this.verwaltungTab)) { this.verwaltungTab = this.firstSettingsTab(); }
                    this.openSettingsTab(this.verwaltungTab);
                },
                openSettingsTab(key) {
                    this.verwaltungTab = key;
                    const loaders = {
                        logins: () => { this.loadSystemUsers(); this.loadPermissionMatrix(); }, audit: () => this.loadAuditLog(), rechte: () => this.loadPermissionMatrix(),
                        sicherheit: () => { this.loadSecurity(); this.loadPrivacy(); },
                        betrieb: () => { this.loadHealth(); this.loadBackupInfo(); },
                        mail: () => this.loadMailSettings(), wache: () => { this.loadIntervals(); this.loadDueManager(); this.loadLogbooks(); }
                    };
                    if (loaders[key]) loaders[key]();
                },
                async loadIntervals() {
                    try {
                        const res = await fetch('/api/personnel/settings', { credentials: 'include' });
                        if (res.ok) { const d = await res.json(); this.intervals = { ...this.intervals, ...d }; }
                    } catch (e) {}
                },
                async saveIntervals() {
                    const res = await fetch('/api/personnel/settings', { method: 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(this.intervals) });
                    await appAlert(res.ok ? 'Gespeichert.' : 'Speichern fehlgeschlagen (keine Berechtigung?).');
                },
                async openCalendarFeed() {
                    await this.loadAccessTokens();
                    bootstrap.Modal.getOrCreateInstance(document.getElementById('calendarFeedModal')).show();
                },
                async loadBackupInfo() {
                    if (!this.isAdmin) return;
                    const res = await fetch('/api/admin/backup-status', { credentials: 'include' });
                    if (res.ok) { this.backupInfo = await res.json(); const o = this.backupInfo.offsite; this.offsiteForm = { enabled: o.enabled, url: o.url, username: o.username, password: '' }; }
                },
                async saveOffsite() {
                    const res = await fetch('/api/admin/backup-settings', { method: 'PUT', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(this.offsiteForm) });
                    const err = await res.json().catch(() => ({}));
                    await appAlert(res.ok ? 'Gespeichert.' : (typeof err.detail === 'string' ? err.detail : 'Speichern fehlgeschlagen.'));
                    if (res.ok) await this.loadBackupInfo();
                },
                async testOffsite() {
                    const res = await fetch('/api/admin/backup-settings/test', { method: 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(this.offsiteForm) });
                    const d = await res.json().catch(() => ({}));
                    await appAlert(d.ok ? d.detail : ('Verbindung fehlgeschlagen: ' + (d.detail || 'unbekannter Fehler')));
                },
                async runBackupNow() {
                    this.backupBusy = true;
                    try {
                        const res = await fetch('/api/admin/backup/run-now', { method: 'POST', credentials: 'include' });
                        await appAlert(res.ok ? 'Sicherung abgeschlossen.' : 'Die Sicherung ist fehlgeschlagen - bitte Protokoll prüfen.');
                        await this.loadBackupInfo(); await this.loadHealth();
                    } finally { this.backupBusy = false; }
                },
                async loadPrivacy() {
                    if (!this.isAdmin) return;
                    const res = await fetch('/api/admin/privacy', { method: 'GET', credentials: 'include' });
                    if (res.ok) this.privacy = await res.json();
                },
                async savePrivacy() {
                    const res = await fetch('/api/admin/privacy-settings', { method: 'PUT', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(this.privacy.settings) });
                    const err = await res.json().catch(() => ({}));
                    await appAlert(res.ok ? 'Gespeichert.' : (typeof err.detail === 'string' ? err.detail : 'Speichern fehlgeschlagen.'));
                    if (res.ok) await this.loadPrivacy();
                },
                async runRetentionNow() {
                    const res = await fetch('/api/admin/privacy/run-now', { method: 'POST', credentials: 'include' });
                    const d = await res.json().catch(() => ({}));
                    await appAlert(res.ok ? ('Bereinigt: ' + Object.entries(d.deleted || {}).map(([k, v]) => `${k} ${v}`).join(', ')) : 'Das hat nicht geklappt.');
                    await this.loadPrivacy();
                },
                async anonymizePerson(c) {
                    if (!await appConfirm(`Personenbezug von „${c.name}“ unwiderruflich entfernen?\n\nName, Kontaktdaten, Geburtsdatum, Foto, Notizen, Prüfdaten und ein eventuelles Login werden gelöscht. Dienstzeiten und Einsatzprotokolle bleiben ohne Namen („Ehemaliges Mitglied“) erhalten.`)) return;
                    const res = await fetch(`/api/admin/privacy/anonymize/${c.id}`, { method: 'POST', credentials: 'include' });
                    const err = await res.json().catch(() => ({}));
                    await appAlert(res.ok ? 'Personenbezug entfernt.' : (typeof err.detail === 'string' ? err.detail : 'Das hat nicht geklappt.'));
                    await this.loadPrivacy();
                },
                async loadSecurity() {
                    if (!this.isAdmin) return;
                    const res = await fetch('/api/admin/security', { credentials: 'include' });
                    if (res.ok) this.security = await res.json();
                },
                async saveSecurity() {
                    this.securitySaving = true;
                    try {
                        const res = await fetch('/api/admin/security-settings', { method: 'PUT', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(this.security.settings) });
                        await appAlert(res.ok ? 'Sicherheits-Einstellungen gespeichert.' : 'Speichern fehlgeschlagen.');
                        if (res.ok) await this.loadSecurity();
                    } finally { this.securitySaving = false; }
                },
                async resendInvite(usr) {
                    if (!await appConfirm(`Einladung für „${usr.username}“ erneut per E-Mail schicken? Ältere Links werden damit ungültig.`)) return;
                    const res = await fetch(`/api/users/${usr.id}/invite`, { method: 'POST', credentials: 'include' });
                    const err = await res.json().catch(() => ({}));
                    await appAlert(res.ok ? (err.message || 'Einladung verschickt.') : (typeof err.detail === 'string' ? err.detail : 'Einladung konnte nicht verschickt werden.'));
                },
                async resetUser2fa(usr) {
                    if (!await appConfirm(`Zwei-Faktor für „${usr.username}“ ausschalten? Bestehende Anmeldungen des Kontos werden beendet, die Person muss 2FA danach neu einrichten.`)) return;
                    const res = await fetch(`/api/users/${usr.id}/2fa-reset`, { method: 'POST', credentials: 'include' });
                    const err = await res.json().catch(() => ({}));
                    await appAlert(res.ok ? 'Zwei-Faktor zurückgesetzt.' : (typeof err.detail === 'string' ? err.detail : 'Zurücksetzen fehlgeschlagen.'));
                    if (res.ok) await this.loadSystemUsers();
                },
                async loadMyDevices() {
                    try {
                        const res = await fetch('/api/auth/devices', { credentials: 'include' });
                        this.myDevices = res.ok ? (await res.json()).map(d => ({ ...d, icon: /Android|iPhone/.test(d.label || '') ? 'fa-mobile-screen' : 'fa-desktop' })) : [];
                    } catch (e) { this.myDevices = []; }
                },
                async logoutAllDevices() {
                    if (!await appConfirm('Alle anderen Geräte abmelden? Dieses Gerät bleibt angemeldet.')) return;
                    const res = await fetch('/api/auth/logout-all', { method: 'POST', credentials: 'include' });
                    await appAlert(res.ok ? 'Alle anderen Geräte wurden abgemeldet.' : 'Das hat nicht geklappt.');
                    await this.loadMyDevices();
                },
                async bindSelfAccount() {
                    if (!this.selfBindPersonnelId) return await appAlert("Wähle einen Kameraden aus!");
                    const res = await fetch('/api/users/me/bind-personnel', { method: 'PUT', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify({ personnel_id: this.selfBindPersonnelId }) });
                    if (res.ok) {
                        await appAlert("Konto erfolgreich verknüpft!");
                        await this.fetchPersonalStats();
                    }
                },
                
                
                async submitNewLogin() {
                    if (!this.newLogin.username) return await appAlert("Benutzername ist erforderlich!");
                    if (this.newLogin.invite) {
                        if (!this.newLogin.personnel_id) return await appAlert("Für eine Einladung bitte unten einen Kameraden verknüpfen - die Mail geht an dessen Adresse.");
                    } else {
                        if (!this.newLogin.password) return await appAlert("Bitte ein Passwort vergeben oder „Per E-Mail einladen“ wählen.");
                        if (!this.pwOk(this.newLogin.password, this.newLogin.username)) return await appAlert("Das Passwort erfüllt die Regeln noch nicht (siehe Liste unter dem Feld).");
                    }
                    const res = await fetch('/api/users/add', { method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify(this.newLogin) });
                    if (res.ok) {
                        await appAlert(this.newLogin.invite ? "System-Login angelegt - die Einladung per E-Mail ist unterwegs. Die Person legt ihr Passwort über den Link selbst fest." : "System-Login angelegt!");
                        this.newLogin = { username: '', password: '', role: 'mannschaft', personnel_id: null, invite: false };
                        await this.loadSystemUsers();
                    } else {
                        const err = await res.json();
                        await appAlert("Fehler: " + (err.detail || "Unbekannter Fehler"));
                    }
                },
                async deleteUserAccount(id) {
                    if (await appConfirm("System-Login permanent löschen?")) {
                        const res = await fetch(`/api/users/${id}`, { method: 'DELETE', credentials: 'include' });
                        if (res.ok) {
                            await appAlert("Konto gelöscht.");
                            await this.loadSystemUsers();
                        }
                    }
                },
                async unlockUserAccount(usr) {
                    if (!await appConfirm(`Login-Sperre für '${usr.username}' sofort aufheben?`)) return;
                    const res = await fetch(`/api/users/${usr.id}/unlock`, { method: 'PUT', credentials: 'include' });
                    if (res.ok) {
                        await this.loadSystemUsers();
                    } else {
                        const err = await res.json().catch(() => ({}));
                        await appAlert("Fehler: " + (err.detail || "Unbekannter Fehler"));
                    }
                },
                resetUserPassword(usr) {
                    // Eigenes Fenster statt appPrompt: so ist die Checkliste der Passwort-Regeln sichtbar
                    this.adminPw = { id: usr.id, username: usr.username, password: '', show: false, busy: false };
                    const el = document.getElementById('adminPwModal');
                    bootstrap.Modal.getOrCreateInstance(el).show();
                    el.addEventListener('shown.bs.modal', () => document.getElementById('adminPwInput')?.focus(), { once: true });
                },
                async submitAdminPw() {
                    if (!this.pwOk(this.adminPw.password, this.adminPw.username)) return;
                    this.adminPw.busy = true;
                    try {
                        const res = await fetch(`/api/users/${this.adminPw.id}/password`, { method: 'PUT', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify({ password: this.adminPw.password }) });
                        if (res.ok) {
                            bootstrap.Modal.getInstance(document.getElementById('adminPwModal'))?.hide();
                            this.adminPw.password = '';
                            await appAlert("Passwort zurückgesetzt. Beim nächsten Login muss die Person ein eigenes Passwort festlegen.");
                        } else {
                            const err = await res.json().catch(() => ({}));
                            await appAlert("Das Passwort wurde NICHT geändert: " + (typeof err.detail === 'string' ? err.detail : "Unbekannter Fehler"));
                        }
                    } finally { this.adminPw.busy = false; }
                },
                isDormantAccount(usr) {
                    if (!usr.last_login) return true;
                    const days = (new Date() - new Date(usr.last_login)) / (1000 * 60 * 60 * 24);
                    return days > 90;
                },
                async loadAuditLog() {
                    try {
                        const res = await fetch('/api/admin/audit-log', { credentials: 'include' });
                        this.auditLog = res.ok ? await res.json() : [];
                    } catch(e) { this.auditLog = []; }
                },
                async loadMailSettings() {
                    const res = await fetch('/api/admin/mail-settings', { credentials: 'include' });
                    if (!res.ok) return;
                    const data = await res.json();
                    if (!data.app_url) data.app_url = window.location.origin;
                    data.smtp_password = '';
                    this.mailSettings = data;
                },
                async saveMailSettings() {
                    this.mailSaving = true;
                    try {
                        const res = await fetch('/api/admin/mail-settings', {
                            method: 'PUT', credentials: 'include',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify(this.mailSettings)
                        });
                        const data = await res.json().catch(() => ({}));
                        if (!res.ok) { await appAlert(data.detail || 'Speichern fehlgeschlagen.'); return; }
                        await this.loadMailSettings();
                        await appAlert('E-Mail-Einstellungen gespeichert.');
                    } finally { this.mailSaving = false; }
                },
                async sendTestMail() {
                    if (!this.mailTestTo) { await appAlert('Bitte eine Empfängeradresse für die Test-Mail eingeben.'); return; }
                    this.mailSaving = true;
                    try {
                        const res = await fetch('/api/admin/mail-settings/test', {
                            method: 'POST', credentials: 'include',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ to: this.mailTestTo })
                        });
                        const data = await res.json().catch(() => ({}));
                        await appAlert(res.ok ? 'Test-Mail verschickt. Bitte Posteingang (und Spam-Ordner) prüfen.' : (data.detail || 'Versand fehlgeschlagen.'));
                    } finally { this.mailSaving = false; }
                },
                
                async uploadBackupFile() {
                    const fileInput = this.$refs.backupFileInput;
                    if (!fileInput || !fileInput.files || fileInput.files.length === 0) {
                        await appAlert("Bitte wähle zuerst eine Backup-JSON-Datei aus!");
                        return;
                    }
                    if (!await appConfirm("WARNUNG: Möchtest du diese Datenbank-Sicherung wirklich einspielen? Vorhandene Datensätze werden aktualisiert.")) {
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
                            await appAlert(`✅ Backup erfolgreich importiert (${data.imported_rows} Datensätze aktualisiert)!`);
                            fileInput.value = '';
                            location.reload();
                        } else {
                            await appAlert("Fehler beim Importieren: " + (data.detail || "Unbekannter Fehler"));
                        }
                    } catch (err) {
                        await appAlert("Verbindung zum Server fehlgeschlagen!");
                    }
                },
                
                // === USER EDIT METHODS ===
                async loadSystemUsers() {
                    try {
                        const res = await fetch('/api/users/list', { credentials: 'include' });
                        if (res.ok) {
                            const users = await res.json();
                            // Enrich with personnel name lookup
                            this.systemUsers = users.map(u => ({
                                ...u,
                                personnel_name: u.personnel_id ? (this.personnel.find(p => p.id === u.personnel_id)?.name || `ID ${u.personnel_id}`) : null
                            }));
                        }
                    } catch(e) { console.error("Error loading system users", e); }
                },
                startUserEdit(usr) {
                    this.editingUserId = usr.id;
                    this.editingUsername = usr.username;
                    this.editUserRole = usr.custom_role ? 'custom:' + usr.custom_role : usr.role;
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
                        await appAlert('✅ Login erfolgreich aktualisiert!');
                        this.cancelUserEdit();
                        await this.loadSystemUsers();
                    } else { await appAlert('Fehler beim Speichern. Bitte erneut versuchen.'); }
                },
                async loadStationSettings() {
                    try {
                        const res = await fetch('/api/settings/station', { credentials: 'include' });
                        if (res.ok) {
                            this.stationConfig = await res.json();
                            if (this.stationConfig.default_hourly_rate) this.sepaRate = this.stationConfig.default_hourly_rate;
                        }
                    } catch(e) { console.error("Error loading station settings", e); }
                },
                async loadAccessTokens() {
                    try {
                        const res = await fetch('/api/settings/access-tokens', { credentials: 'include' });
                        if (res.ok) this.accessTokens = await res.json();
                    } catch(e) { console.error("Error loading access tokens", e); }
                },
                async regenerateAccessToken(type) {
                    const label = type === 'display' ? 'Hallen-Display' : 'Kalender-Feed';
                    if (!await appConfirm(`${label}-Token wirklich neu erzeugen? Bisherige Links (${type === 'display' ? 'auf Kiosk-Bildschirmen' : 'in Kalender-Apps'}) funktionieren danach nicht mehr.`)) return;
                    const res = await fetch('/api/settings/access-tokens/regenerate', {
                        method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'include',
                        body: JSON.stringify({ type })
                    });
                    if (res.ok) {
                        await this.loadAccessTokens();
                        await appAlert(`${label}-Token wurde erneuert.`);
                    } else {
                        await appAlert("Token konnte nicht erneuert werden.");
                    }
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
                            await appAlert("✅ Standort-Einstellungen erfolgreich gespeichert!");
                            await this.loadStationSettings();
                            if (map) map.setView([this.stationConfig.lat, this.stationConfig.lng], this.stationConfig.zoom);
                        } else {
                            const d = await res.json();
                            await appAlert(d.detail || "Fehler beim Speichern der Wachen-Einstellungen.");
                        }
                    } catch(e) {
                        console.error(e);
                        await appAlert("Verbindung fehlgeschlagen.");
                    }
                },
                async uploadStampImage(event) {
                    const file = event.target.files[0];
                    if (!file) return;
                    const reader = new FileReader();
                    reader.onload = async (e) => {
                        const res = await fetch('/api/settings/stamp', {
                            method: 'PUT', headers: {'Content-Type': 'application/json'}, credentials: 'include',
                            body: JSON.stringify({ stamp_image: e.target.result })
                        });
                        if (res.ok) {
                            this.stationConfig.stamp_image = e.target.result;
                        } else {
                            await appAlert("Stempel konnte nicht gespeichert werden.");
                        }
                        if (this.$refs.stampFileInput) this.$refs.stampFileInput.value = '';
                    };
                    reader.readAsDataURL(file);
                },
                async removeStampImage() {
                    if (!await appConfirm("Stempel-Bild entfernen?")) return;
                    const res = await fetch('/api/settings/stamp', {
                        method: 'PUT', headers: {'Content-Type': 'application/json'}, credentials: 'include',
                        body: JSON.stringify({ stamp_image: null })
                    });
                    if (res.ok) this.stationConfig.stamp_image = '';
                },
                async lookupStationCoordinates() {
                    const query = this.stationConfig.station_name.trim();
                    if (!query) {
                        await appAlert("Bitte gib zuerst einen Wachennamen an!");
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
                                await appAlert(`✅ Koordinaten gefunden:\nBreitengrad: ${lat}\nLängengrad: ${lon}`);
                            } else {
                                await appAlert("Es konnten keine Koordinaten für diesen Namen gefunden werden. Bitte genauer angeben (z. B. 'Feuerwehrhaus Buxheim' oder inklusive Postleitzahl/Ort).");
                            }
                        } else {
                            await appAlert("Fehler bei der Verbindung zum Geokodierungs-Dienst.");
                        }
                    } catch(e) {
                        console.error(e);
                        await appAlert("Fehler beim Suchen der Koordinaten.");
                    }
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
                            await appAlert("✅ Datei erfolgreich ins Archiv hochgeladen!");
                            this.$refs.archiveFileInput.value = '';
                            await this.loadArchiveFiles();
                        } else {
                            await appAlert("Fehler beim Hochladen ins Archiv.");
                        }
                    } catch(e) {
                        console.error(e);
                        await appAlert("Verbindung fehlgeschlagen.");
                    }
                },
                async deleteArchiveFile(id) {
                    if (!await appConfirm("Datei wirklich permanent aus dem Archiv löschen?")) return;
                    try {
                        const res = await fetch(`/api/archive/files/${id}`, {
                            method: 'DELETE',
                            credentials: 'include'
                        });
                        if (res.ok) {
                            await this.loadArchiveFiles();
                        } else {
                            const d = await res.json();
                            await appAlert(d.detail || "Fehler beim Löschen.");
                        }
                    } catch(e) {
                        console.error(e);
                        await appAlert("Verbindung fehlgeschlagen.");
                    }
                },
            }
});

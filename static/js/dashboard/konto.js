// Eigenes Konto, Benachrichtigungen und Push.
// Teil der Dashboard-App (Vue-Mixin): Daten stehen in ../dashboard.js, "this" ist die ganze App.
window.DB_MIXINS = window.DB_MIXINS || [];
window.DB_MIXINS.push({
            methods: {
                async requestPushPermission() {
                    const permission = await Notification.requestPermission();
                    if (permission === 'granted') {
                        try {
                            const reg = await navigator.serviceWorker.ready;
                            let sub = await reg.pushManager.getSubscription();
                            if (sub) await sub.unsubscribe();
                        } catch(e) {}
                        await this.setupPushNotifications(true);
                        if (navigator.userAgent.includes('SamsungBrowser')) {
                            await appAlert(
                                "Push-Benachrichtigungen aktiviert!\n\n⚠️ Achtung: Du nutzt Samsung Internet. Dieser Browser hat eine bekannte Einschränkung und spielt bei Alarmen oft keinen Ton ab und weckt den Bildschirm nicht zuverlässig auf - auch wenn alle Benachrichtigungs-Einstellungen korrekt sind.\n\nFür zuverlässigen Alarmempfang bitte stattdessen Google Chrome auf diesem Handy verwenden (Seite dort öffnen, Benachrichtigungen erlauben).",
                                "Wichtiger Hinweis"
                            );
                        } else {
                            await appAlert("Push-Benachrichtigungen aktiviert!");
                        }
                    } else {
                        await appAlert("Berechtigung verweigert.");
                    }
                },
                async setupPushNotifications(forceReset = false) {
                    if (!('serviceWorker' in navigator) || !('PushManager' in window)) return;
                    try {
                        const reg = await navigator.serviceWorker.ready;
                        let sub = await reg.pushManager.getSubscription();
                        const res = await fetch('/api/push/public-key');
                        const { public_key } = await res.json();
                        const lastVapid = localStorage.getItem('last_vapid_key');
                        
                        if (!sub || forceReset || lastVapid !== public_key) {
                            if (sub) await sub.unsubscribe();
                            const convertedVapidKey = this.urlBase64ToUint8Array(public_key);
                            sub = await reg.pushManager.subscribe({
                                userVisibleOnly: true,
                                applicationServerKey: convertedVapidKey
                            });
                            localStorage.setItem('last_vapid_key', public_key);
                        }
                        await fetch('/api/push/subscribe', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            credentials: 'include',
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
                async loadChangelog(autoShowIfUnseen) {
                    try {
                        const res = await fetch('/api/users/me/changelog', { credentials: 'include' });
                        if (!res.ok) return;
                        const data = await res.json();
                        this.changelogEntries = data.entries || [];
                        this.changelogUnseen = !!data.unseen;
                        if (autoShowIfUnseen && this.changelogUnseen) {
                            // Nach einem Update übernimmt der neue Service Worker kurz nach dem Start und lädt die Seite
                            // einmal neu. Das Fenster erst danach zeigen - sonst blitzt es auf und ist sofort wieder weg.
                            this.$nextTick(() => {
                                setTimeout(() => { if (!window.__swReloading) this.openChangelogModal(); }, 2200);
                            });
                        }
                    } catch (e) { /* offline o.ä. - kein Grund, den Nutzer zu stören */ }
                },
                openChangelogModal() {
                    const el = document.getElementById('changelogModal');
                    if (!el) return;
                    let m = bootstrap.Modal.getInstance(el);
                    if (!m) m = new bootstrap.Modal(el);
                    m.show();
                    // Als gesehen gilt es erst, wenn das Fenster geschlossen wird - nicht schon beim Anzeigen.
                    // Sonst ging der Hinweis verloren, wenn die Seite gleich danach neu lud (Update) oder
                    // ein Admin offline war, ohne ihn je gelesen zu haben.
                    if (this.changelogUnseen && !this._changelogSeenHook) {
                        this._changelogSeenHook = true;
                        el.addEventListener('hidden.bs.modal', () => {
                            this._changelogSeenHook = false;
                            if (!this.changelogUnseen) return;
                            this.changelogUnseen = false;
                            fetch('/api/users/me/changelog/seen', { method: 'POST', credentials: 'include' }).catch(() => {});
                        }, { once: true });
                    }
                },
                async refreshNotifCount(force) {
                    try {
                        const res = await fetch('/api/notifications/count', { credentials: 'include' });
                        if (!res.ok) return;
                        const n = (await res.json()).unread;
                        if (n !== this.notifUnread || (force && this.notifOpen)) { this.notifUnread = n; if (this.notifOpen) this.loadNotifs(); }
                    } catch (e) {}
                },
                async loadNotifs() {
                    try {
                        const res = await fetch('/api/notifications', { credentials: 'include' });
                        if (!res.ok) return;
                        const d = await res.json();
                        this.notifCats = d.categories; this.notifUnread = d.unread;
                        // Fenster mit ungelesenen Meldungen starten offen, der Rest zugeklappt - eigene Wahl bleibt
                        d.categories.forEach(c => { if (!(c.key in this.notifCatOpen)) this.notifCatOpen[c.key] = c.unread > 0; });
                    } catch (e) { console.error(e); }
                },
                toggleNotifPanel() {
                    this.notifOpen = !this.notifOpen;
                    this.notifShowPrefs = false;
                    if (this.notifOpen) { this.notifCatOpen = {}; this.loadNotifs(); }
                },
                toggleNotifCat(key) { this.notifCatOpen[key] = !this.notifCatOpen[key]; },
                notifAgo(n) {
                    const m = n.age_min;
                    if (m < 1) return 'gerade eben';
                    if (m < 60) return `vor ${m} Min.`;
                    if (m < 1440) return `vor ${Math.floor(m / 60)} Std.`;
                    if (m < 10080) return `vor ${Math.floor(m / 1440)} Tg.`;
                    return n.created_at;
                },
                async openNotif(n) {
                    if (!n.read) {
                        n.read = true; this.notifUnread = Math.max(0, this.notifUnread - 1);
                        const c = this.notifCats.find(x => x.items.includes(n)); if (c) c.unread = Math.max(0, c.unread - 1);
                        fetch(`/api/notifications/${n.id}/read`, { method: 'POST', credentials: 'include' }).catch(() => {});
                    }
                    this.notifOpen = false;
                    if (!n.link) return;
                    const u = new URL(n.link, window.location.origin);
                    if (u.pathname === '/dashboard' || u.pathname === '/') {
                        if (u.search) { window.history.replaceState(null, '', '/dashboard' + u.search); await this.handleOpenLink(); }
                    } else {
                        window.location.href = u.pathname + u.search;
                    }
                },
                async moreNotifs(c) {
                    const last = c.items[c.items.length - 1];
                    const res = await fetch(`/api/notifications/category/${c.key}?before_id=${last.id}`, { credentials: 'include' });
                    if (res.ok) c.items.push(...await res.json());
                },
                async readAllNotifs(key) {
                    await fetch('/api/notifications/read-all?category=' + (key || ''), { method: 'POST', credentials: 'include' });
                    await this.loadNotifs();
                },
                async clearNotifs(c) {
                    await fetch(`/api/notifications/category/${c.key}`, { method: 'DELETE', credentials: 'include' });
                    await this.loadNotifs();
                },
                async toggleNotifPrefs() {
                    this.notifShowPrefs = !this.notifShowPrefs;
                    if (this.notifShowPrefs) {
                        const res = await fetch('/api/notifications/prefs', { credentials: 'include' });
                        if (res.ok) this.notifPrefs = await res.json();
                        const rs = await fetch('/api/notifications/settings', { credentials: 'include' });
                        if (rs.ok) this.notifSettings = await rs.json();
                    }
                },
                async saveNotifSettings() {
                    const res = await fetch('/api/notifications/settings', { method: 'PUT', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(this.notifSettings) });
                    if (!res.ok) await appAlert('Einstellung konnte nicht gespeichert werden.');
                },
                async setNotifPref(p, on) {
                    p.push = on;
                    const res = await fetch(`/api/notifications/prefs/${p.key}`, { method: 'PUT', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ push: on }) });
                    if (!res.ok) { p.push = !on; await appAlert('Einstellung konnte nicht gespeichert werden.'); }
                },
                async handleOpenLink() {
                    // Links aus Push/E-Mail: /dashboard?open=termin:123 | einsatz:45 | maengel:7 | fristen | alarm | kalender
                    const raw = new URLSearchParams(window.location.search).get('open');
                    if (!raw) return;
                    window.history.replaceState(null, '', window.location.pathname);
                    const [type, id] = raw.split(':');
                    try {
                        if (type === 'termin') {
                            let s = (this.schedules || []).find(x => String(x.id) === id);
                            if (!s) { await this.loadSchedules(); s = (this.schedules || []).find(x => String(x.id) === id); }
                            if (!s) { await appAlert('Dieser Termin existiert nicht mehr oder ist nicht für dich bestimmt.'); return; }
                            this.activeTab = 'dienste';
                            await this.openScheduleDetail(s);
                        } else if (type === 'einsatz' && id) {
                            window.location.href = `/editor?mission_id=${encodeURIComponent(id)}`;
                        } else if (type === 'maengel') {
                            this.activeTab = 'material'; this.materialSubTab = 'maengel'; this.defectFilter = 'alle';
                            await this.loadDefectReports();
                            const dr = (this.defectReports || []).find(x => String(x.id) === id);
                            if (dr) await this.toggleDefectComments(dr);
                        } else if (type === 'fristen') {
                            this.activeTab = 'fristen';
                        } else if (type === 'alarm') {
                            this.activeTab = 'apager';
                        } else if (type === 'lehrgaenge' || type === 'umfragen') {
                            this.activeTab = type;
                        } else if (type === 'kalender') {
                            this.activeTab = 'kalender';
                        }
                    } catch (e) { console.error('Link konnte nicht geöffnet werden:', e); }
                },
                async loadMyContact() {
                    this.myContact = null;
                    try {
                        const res = await fetch('/api/personnel/me/contact', { credentials: 'include' });
                        if (res.ok) this.myContact = await res.json();
                    } catch (e) { console.error(e); }
                },
                async saveMyContact() {
                    this.contactSaving = true;
                    try {
                        const res = await fetch('/api/personnel/me/contact', {
                            method: 'PUT', credentials: 'include', headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify(this.myContact)
                        });
                        const data = await res.json().catch(() => ({}));
                        await appAlert(res.ok ? 'Kontaktdaten gespeichert.' : (typeof data.detail === 'string' ? data.detail : 'Speichern fehlgeschlagen.'));
                    } finally { this.contactSaving = false; }
                },
                async openIdCard() {
                    this.idCard = null; this.idCardError = '';
                    const el = document.getElementById('idCardModal');
                    if (el) { let m = bootstrap.Modal.getInstance(el); if (!m) m = new bootstrap.Modal(el); m.show(); }
                    try {
                        const res = await fetch('/api/users/me/id-card', { credentials: 'include' });
                        if (res.ok) this.idCard = await res.json();
                        else this.idCardError = (await res.json().catch(() => ({}))).detail || 'Dienstausweis konnte nicht geladen werden.';
                    } catch (e) { this.idCardError = 'Keine Verbindung zum Server.'; }
                },
                openSelfPasswordModal() {
                    this.selfTab = 'stat';
                    this.selfPwData = { old_password: '', new_password: '', confirm_password: '' };
                    this.twoFaSetup = null; this.twoFaConfirmCode = ''; this.twoFaRecoveryCodes = null;
                    this.fetchPersonalStats();
                    this.loadMyDevices();
                    this.loadTwoFaStatus();
                    this.loadMyContact();
                    const el = document.getElementById('selfPasswordModal');
                    if (el) {
                        let m = bootstrap.Modal.getInstance(el);
                        if (!m) m = new bootstrap.Modal(el);
                        m.show();
                    }
                },
                async loadTwoFaStatus() {
                    try {
                        const res = await fetch('/api/auth/2fa/status', { credentials: 'include' });
                        this.twoFaEnabled = res.ok ? (await res.json()).enabled : false;
                    } catch(e) { this.twoFaEnabled = false; }
                },
                async startSetup2fa() {
                    const res = await fetch('/api/auth/2fa/setup', { method: 'POST', credentials: 'include' });
                    if (!res.ok) { await appAlert("Einrichtung konnte nicht gestartet werden."); return; }
                    this.twoFaSetup = await res.json();
                    this.twoFaConfirmCode = '';
                    // Erst NACH dem Rendern des Canvas (v-if oben) existiert das Element im DOM.
                    this.$nextTick(() => {
                        const canvas = document.getElementById('twoFaQrCanvas');
                        if (canvas && window.QRious) {
                            new QRious({ element: canvas, value: this.twoFaSetup.otpauth_url, size: 180 });
                        }
                    });
                },
                async confirmSetup2fa() {
                    if (!this.twoFaConfirmCode || this.twoFaConfirmCode.length < 6) { await appAlert("Bitte den 6-stelligen Code eingeben."); return; }
                    const res = await fetch('/api/auth/2fa/confirm', {
                        method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'include',
                        body: JSON.stringify({ code: this.twoFaConfirmCode })
                    });
                    if (!res.ok) {
                        const err = await res.json().catch(() => ({}));
                        await appAlert(err.detail || "Code falsch.");
                        return;
                    }
                    const data = await res.json();
                    this.twoFaEnabled = true; this.must2fa = false;
                    this.twoFaRecoveryCodes = data.recovery_codes;
                },
                async startDisable2fa() {
                    const password = await appPrompt("Zur Bestätigung dein aktuelles Passwort eingeben:", '', 'Passwort bestätigen', 'password');
                    if (!password) return;
                    const res = await fetch('/api/auth/2fa/disable', {
                        method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'include',
                        body: JSON.stringify({ password })
                    });
                    if (!res.ok) {
                        const err = await res.json().catch(() => ({}));
                        await appAlert(err.detail || "Deaktivieren fehlgeschlagen.");
                        return;
                    }
                    this.twoFaEnabled = false;
                    await appAlert("Zwei-Faktor-Authentifizierung wurde deaktiviert.");
                },
                async submitSelfPassword() {
                    if (this.selfPwData.new_password !== this.selfPwData.confirm_password) { await appAlert("Die neuen Passwörter stimmen nicht überein!"); return; }
                    if (!this.pwOk(this.selfPwData.new_password, this.username, this.selfPwData.old_password)) { await appAlert("Das neue Passwort erfüllt die Regeln noch nicht (siehe Liste unter dem Feld)."); return; }
                    const res = await fetch('/api/auth/change-password', { method: 'PUT', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify({ old_password: this.selfPwData.old_password, new_password: this.selfPwData.new_password }) });
                    if (res.ok) {
                        await appAlert("Dein Passwort wurde erfolgreich aktualisiert!");
                        this.isFirstLoginBlock = false;
                        const el = document.getElementById('selfPasswordModal');
                        if (el) {
                            const m = bootstrap.Modal.getInstance(el);
                            if (m) m.hide();
                        }
                    } else {
                        const err = await res.json().catch(() => ({}));
                        await appAlert(typeof err.detail === 'string' ? err.detail : 'Das Passwort konnte nicht geändert werden.');
                    }
                },
                async openLegalModal(which) {
                    this.activeLegalTab = which;
                    if (!this.legalTexts.impressum && !this.legalTexts.datenschutz) {
                        try {
                            const res = await fetch('/api/legal');
                            if (res.ok) this.legalTexts = await res.json();
                        } catch(e) {}
                    }
                    new bootstrap.Modal(document.getElementById('legalModal')).show();
                },
            }
});

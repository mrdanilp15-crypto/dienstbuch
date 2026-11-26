// Alarmierung, Webhook und Ausrückeordnung.
// Teil der Dashboard-App (Vue-Mixin): Daten stehen in ../dashboard.js, "this" ist die ganze App.
window.DB_MIXINS = window.DB_MIXINS || [];
window.DB_MIXINS.push({
            computed: {
                activeFeedbacksCount() {
                    return this.apagerFeedbacks.filter(f => f.status === 'Komme').length;
                },
                latestAlarmLog() {
                    return this.apagerLogs && this.apagerLogs.length > 0 ? this.apagerLogs[0] : null;
                },
            },
            methods: {

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
                    if (this.canSeeWebhook) await this.loadWebhookLog();
                },
                async loadWebhookLog() {
                    try {
                        const res = await fetch('/api/apager/webhook-log?_=' + Date.now(), { credentials: 'include' });
                        if (res.ok) this.webhookLog = await res.json();
                    } catch(e) {}
                },
                async generateApiKey() {
                    try {
                        const res = await fetch('/api/apager/config', { method: 'POST', credentials: 'include' });
                        if(res.ok) this.apagerConfig = await res.json();
                    } catch(e) {}
                },
                async submitFeedback(status) {
                    const alarmId = (this.apagerLogs && this.apagerLogs.length > 0) ? this.apagerLogs[0].id : null;
                    let url = `/api/apager/feedbacks?status=${encodeURIComponent(status)}`;
                    if (alarmId !== null) url += `&alarm_id=${alarmId}`;
                    try {
                        const res = await fetch(url, { method: 'POST', credentials: 'include' });
                        if(res.ok) {
                            await this.loadApagerConfig();
                        } else {
                            const d = await res.json().catch(() => ({}));
                            await appAlert(d.detail || "Rückmeldung konnte nicht gesendet werden.");
                        }
                    } catch(e) {
                        await appAlert("Rückmeldung konnte nicht gesendet werden: Verbindung fehlgeschlagen.");
                    }
                },
                async deleteApagerLog(id) {
                    if (await appConfirm("Alarmierungseintrag wirklich löschen?")) {
                        const res = await fetch(`/api/apager/logs/${id}`, { method: 'DELETE', credentials: 'include' });
                        if(res.ok) await this.loadApagerConfig();
                    }
                },
                async clearApagerLogs() {
                    if (await appConfirm("Ganzes Alarmierungsprotokoll wirklich unwiderruflich leeren?")) {
                        const res = await fetch('/api/apager/logs', { method: 'DELETE', credentials: 'include' });
                        if(res.ok) await this.loadApagerConfig();
                    }
                },
                async editApagerLog(log) {
                    const stichwort = await appPrompt("Stichwort anpassen:", log.stichwort);
                    if (stichwort === null) return;
                    const adresse = await appPrompt("Einsatzort anpassen:", log.adresse);
                    if (adresse === null) return;
                    const meldung = await appPrompt("Meldung anpassen:", log.meldung);
                    if (meldung === null) return;
                    const res = await fetch(`/api/apager/logs/${log.id}`, {
                        method: 'PUT',
                        headers: {'Content-Type': 'application/json'},
                        credentials: 'include',
                        body: JSON.stringify({ stichwort, adresse, meldung })
                    });
                    if(res.ok) await this.loadApagerConfig();
                },
                // ---- AAO fürs Hallen-Display
                async loadAao() {
                    try {
                        const res = await fetch('/api/aao', { credentials: 'include' });
                        if (res.ok) { const d = await res.json(); this.aaoRules = d.rules; this.aaoCanEdit = d.can_edit; }
                    } catch (err) {}
                },
                resetAaoForm() { this.aaoForm = { id: null, pattern: '', entries: [{ vehicle: '', role: '' }] }; },
                editAaoRule(r) { this.aaoForm = { id: r.id, pattern: r.pattern, entries: r.entries.map(e => ({ ...e })) }; },
                async saveAaoRule() {
                    const f = this.aaoForm;
                    const entries = f.entries.filter(e => (e.vehicle || '').trim());
                    const res = await fetch(f.id ? `/api/aao/${f.id}` : '/api/aao', { method: f.id ? 'PUT' : 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ pattern: f.pattern, entries }) });
                    if (!res.ok) { const d = await res.json().catch(() => ({})); await appAlert(typeof d.detail === 'string' ? d.detail : 'Bitte Muster und mindestens ein Fahrzeug angeben.'); return; }
                    this.resetAaoForm();
                    await this.loadAao();
                },
                async deleteAaoRule(r) {
                    if (!await appConfirm(`AAO-Regel „${r.pattern}“ löschen?`)) return;
                    const res = await fetch(`/api/aao/${r.id}`, { method: 'DELETE', credentials: 'include' });
                    if (res.ok) await this.loadAao();
                },
                
                // === TEST ALARM METHODS ===
                isTestAlarmLog(log) {
                    return !!(log && log.stichwort && log.stichwort.startsWith('[TEST]'));
                },
                async sendTestAlarm() {
                    if (!await appConfirm('Test-Alarm wirklich auslösen? Dies erscheint im Protokoll.')) return;
                    const res = await fetch('/api/apager/test-alarm', {
                        method: 'POST', headers: {'Content-Type': 'application/json'},
                        credentials: 'include', body: JSON.stringify(this.testAlarm)
                    });
                    if (res.ok) {
                        await appAlert('✅ Test-Alarm wurde im Protokoll eingetragen!');
                        this.testAlarm = { stichwort: '', adresse: '', meldung: '', prefix_test: this.testAlarm.prefix_test };
                        await this.loadApagerConfig();
                    } else { const d = await res.json(); await appAlert(d.detail || 'Fehler beim Test-Alarm.'); }
                },
                async copyWebhookUrl() {
                    const url = `${window.location.protocol}//${this.serverHost}/api/apager/webhook?api_key=${this.apagerConfig.api_key || ''}`;
                    await navigator.clipboard.writeText(url);
                    await appAlert('✅ Webhook-URL in Zwischenablage kopiert!');
                },
            }
});

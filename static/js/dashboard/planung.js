// Lehrgangswünsche, Umfragen und Helfersuche.
// Teil der Dashboard-App (Vue-Mixin): Daten stehen in ../dashboard.js, "this" ist die ganze App.
window.DB_MIXINS = window.DB_MIXINS || [];
window.DB_MIXINS.push({
            computed: {
                // Wie oft wird welcher Lehrgang gewünscht (nur offene Wünsche und zugeteilte Plätze)
                wishDemand() {
                    const by = {};
                    this.wishData.wishes.filter(w => w.status === 'Wunsch' || w.status === 'Zugeteilt').forEach(w => {
                        const k = w.course_name.trim().toLowerCase();
                        by[k] = by[k] || { name: w.course_name.trim(), open: 0, assigned: 0 };
                        if (w.status === 'Wunsch') by[k].open++; else by[k].assigned++;
                    });
                    return Object.values(by).sort((a, b) => (b.open - a.open) || a.name.localeCompare(b.name));
                },
                wishList() { return this.wishData.wishes.filter(w => this.wishFilter === 'alle' || w.status === this.wishFilter); },
            },
            methods: {
                // --- Lehrgangsplanung ---
                async loadWishes() {
                    try {
                        const res = await fetch('/api/course-wishes', { credentials: 'include' });
                        if (res.ok) this.wishData = await res.json();
                    } catch (e) { console.error(e); }
                },
                async planningError(res) {
                    const err = await res.json().catch(() => ({}));
                    await appAlert(typeof err.detail === 'string' ? err.detail : 'Das hat nicht geklappt.');
                },
                async addWish() {
                    const body = { course_name: this.newWish.course_name, note: this.newWish.note, personnel_id: this.newWish.personnel_id || null };
                    const res = await fetch('/api/course-wishes', { method: 'POST', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify(body) });
                    if (!res.ok) return this.planningError(res);
                    this.newWish = { course_name: '', note: '', personnel_id: '' };
                    this.wishForOther = false;
                    await this.loadWishes();
                },
                async decideWish(w, status) {
                    let course_date = w.course_date, reply = w.reply || '';
                    if (status === 'Zugeteilt') {
                        course_date = await appPrompt(`Wann findet "${w.course_name}" statt? (leer lassen, wenn noch offen)`, w.course_date || '', 'Platz zuteilen', 'date');
                        if (course_date === null) return;
                    } else if (status === 'Teilgenommen') {
                        course_date = await appPrompt(`Wann hat ${w.name} teilgenommen? Der Lehrgang wird in die Personalakte eingetragen.`, w.course_date || localISODate(), 'Teilnahme eintragen', 'date');
                        if (!course_date) return;
                    } else if (status === 'Abgelehnt') {
                        reply = await appPrompt(`Kurze Begründung für ${w.name} (optional):`, '', 'Wunsch ablehnen');
                        if (reply === null) return;
                    }
                    const res = await fetch(`/api/course-wishes/${w.id}`, { method: 'PUT', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify({ status, course_date: course_date || null, reply }) });
                    if (!res.ok) return this.planningError(res);
                    await this.loadWishes();
                },
                async deleteWish(w) {
                    if (!await appConfirm(`Wunsch "${w.course_name}" entfernen?`)) return;
                    const res = await fetch(`/api/course-wishes/${w.id}`, { method: 'DELETE', credentials: 'include' });
                    if (!res.ok) return this.planningError(res);
                    await this.loadWishes();
                },
                wishTone(status) { return { Wunsch: 'bg-warning text-dark', Zugeteilt: 'bg-primary', Teilgenommen: 'bg-success', Abgelehnt: 'bg-secondary' }[status]; },
                germanDate(d) { return d ? new Date(d + 'T12:00:00').toLocaleDateString('de-DE', { day: '2-digit', month: '2-digit', year: 'numeric' }) : ''; },
                // --- Umfragen und Helfersuche ---
                async loadPolls() {
                    try {
                        const res = await fetch('/api/polls', { credentials: 'include' });
                        if (!res.ok) return;
                        const data = await res.json();
                        this.polls = data.polls;
                        this.canManagePolls = data.manage;
                    } catch (e) { console.error(e); }
                },
                startPoll(kind) {
                    this.pollForm = { id: null, kind, title: '', description: '', multi: false, deadline: '', event_date: '', event_time: '', options: [{ label: '', capacity: '' }, { label: '', capacity: '' }] };
                },
                editPoll(p) {
                    this.pollForm = { id: p.id, kind: p.kind, title: p.title, description: p.description, multi: p.multi, deadline: p.deadline || '', event_date: p.event_date || '', event_time: p.event_time || '',
                        options: p.options.map(o => ({ id: o.id, label: o.label, capacity: o.capacity || '', count: o.count })), before: p.options.map(o => ({ id: o.id, label: o.label, count: o.count })) };
                    this.$nextTick(() => { const el = document.getElementById('pollTitle'); if (el) { el.scrollIntoView({ block: 'center' }); el.focus(); } });
                },
                addPollOption() { this.pollForm.options.push({ label: '', capacity: '' }); },
                removePollOption(i) { this.pollForm.options.splice(i, 1); },
                async savePoll() {
                    const f = this.pollForm;
                    const options = f.options.filter(o => o.label.trim()).map(o => ({ id: o.id || null, label: o.label, capacity: f.kind === 'helfer' && o.capacity ? Number(o.capacity) : null }));
                    const lost = (f.before || []).filter(b => b.count && !options.some(o => o.id === b.id));
                    if (lost.length && !await appConfirm(`Beim Speichern gehen Eintragungen verloren: ${lost.map(b => `${b.label} (${b.count})`).join(', ')}. Trotzdem speichern?`)) return;
                    const body = JSON.stringify({ kind: f.kind, title: f.title, description: f.description, multi: f.multi, deadline: f.deadline || null, event_date: f.event_date || null, event_time: f.event_time || '', options });
                    const opts = { headers: {'Content-Type': 'application/json'}, credentials: 'include', body };
                    const res = f.id ? await fetch(`/api/polls/${f.id}`, { method: 'PUT', ...opts }) : await fetch('/api/polls', { method: 'POST', ...opts });
                    if (!res.ok) return this.planningError(res);
                    this.pollForm = null;
                    await this.loadPolls();
                },
                async votePoll(poll, option) {
                    const chosen = poll.options.filter(o => o.mine).map(o => o.id);
                    let ids;
                    if (option.mine) ids = chosen.filter(id => id !== option.id);
                    else ids = poll.multi ? chosen.concat(option.id) : [option.id];
                    const res = await fetch(`/api/polls/${poll.id}/vote`, { method: 'PUT', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify({ option_ids: ids }) });
                    if (!res.ok) await this.planningError(res);
                    await this.loadPolls();
                },
                async closePoll(poll, closed) {
                    const res = await fetch(`/api/polls/${poll.id}/closed`, { method: 'PUT', headers: {'Content-Type': 'application/json'}, credentials: 'include', body: JSON.stringify({ closed }) });
                    if (!res.ok) return this.planningError(res);
                    await this.loadPolls();
                },
                async deletePoll(poll) {
                    if (!await appConfirm(`"${poll.title}" mit allen Eintragungen löschen? Das kann nicht rückgängig gemacht werden.`)) return;
                    const res = await fetch(`/api/polls/${poll.id}`, { method: 'DELETE', credentials: 'include' });
                    if (!res.ok) return this.planningError(res);
                    await this.loadPolls();
                },
                pollShare(poll, option) {
                    if (option.capacity) return Math.min(100, Math.round(option.count / option.capacity * 100));
                    return poll.participants ? Math.round(option.count / poll.participants * 100) : 0;
                },
            }
});

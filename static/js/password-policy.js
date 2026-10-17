// Live-Anzeige der Passwort-Richtlinie. Spiegelt core/password_policy.py (maßgeblich ist immer der Server).
window.PasswordPolicy = (function () {
    let policy = { min: 8, max: 128, common: [], context: [], sequences: [] };

    function core(pw) { return pw.toLowerCase().replace(/[^\p{L}]+$/u, ''); }
    function isRun(low) {
        if (new Set(low).size === 1) return true;
        return policy.sequences.some(s => s.includes(low) || [...s].reverse().join('').includes(low));
    }
    function isRepeated(low) {
        for (let n = 1; n <= low.length / 2; n++) {
            if (low.length % n === 0 && low.slice(0, n).repeat(low.length / n) === low) return true;
        }
        return false;
    }

    // Liste der Regeln mit Erfüllt-Status; items[].ok === null heißt "noch nichts eingegeben"
    function check(pw, username, old) {
        pw = pw || '';
        const typed = pw.length > 0;
        const low = pw.toLowerCase(), c = core(pw), u = (username || '').trim().toLowerCase();
        const common = new Set(policy.common);
        const isCommon = typed && pw.length >= policy.min && (common.has(low) || (c.length >= 4 && common.has(c)) || policy.context.includes(c));
        const isUser = typed && !!u && (low === u || (u.length >= 4 && c === u));
        const simple = typed && pw.length >= policy.min && (isRun(low) || isRepeated(low));
        const items = [
            { text: `Mindestens ${policy.min} Zeichen`, ok: typed ? pw.length >= policy.min : null },
            { text: 'Kein bekanntes oder leicht erratbares Passwort (z. B. 12345678, Passwort1, Feuerwehr)', ok: typed ? !(isCommon || simple) : null },
            { text: 'Nicht der Benutzername', ok: typed ? !isUser : null },
        ];
        if (old !== undefined && old !== null) items.push({ text: 'Anders als das bisherige Passwort', ok: typed ? pw !== old : null });
        if (pw.length > policy.max) items.push({ text: `Höchstens ${policy.max} Zeichen`, ok: false });
        return { ok: typed && items.every(i => i.ok === true), items };
    }

    async function load() {
        try {
            const res = await fetch('/api/auth/password-policy');
            if (res.ok) policy = await res.json();
        } catch (e) { /* Standardwerte (nur Länge) gelten, der Server prüft ohnehin */ }
    }

    // Für Seiten ohne Vue: füllt ein Element mit der Checkliste
    function render(el, result) {
        el.innerHTML = result.items.map(i => {
            const cls = i.ok === null ? 'text-muted' : (i.ok ? 'text-success' : 'text-danger');
            const icon = i.ok === null ? '○' : (i.ok ? '✓' : '✗');
            return `<div class="${cls}"><span style="display:inline-block;width:1.2em">${icon}</span>${i.text}</div>`;
        }).join('');
    }

    return { load, check, render, hint: 'Tipp: Ein Satz aus mehreren Wörtern ist sicherer als „Passwort1!“. Leerzeichen und Sonderzeichen sind erlaubt.' };
})();

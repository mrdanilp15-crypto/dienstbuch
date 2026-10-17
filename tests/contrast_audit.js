// Prüft jeden sichtbaren Text im Hauptbereich: Kontrast (WCAG 2.x) gegen den tatsächlichen Untergrund und Schriftgröße.
// Rückgabe: { checked, bad: [{key, ratio, text, size}] } - key fasst gleiche Fälle (Tag.Klassen + Farben) zusammen.
window.__audit = function (rootSel) {
    const parse = c => { const m = c.match(/rgba?\(([^)]+)\)/); if (!m) return null; const p = m[1].split(',').map(parseFloat); return { r: p[0], g: p[1], b: p[2], a: p.length > 3 ? p[3] : 1 }; };
    const blend = (f, b) => ({ r: f.r * f.a + b.r * (1 - f.a), g: f.g * f.a + b.g * (1 - f.a), b: f.b * f.a + b.b * (1 - f.a), a: 1 });
    const lum = c => { const f = v => { v /= 255; return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4); }; return 0.2126 * f(c.r) + 0.7152 * f(c.g) + 0.0722 * f(c.b); };
    const ratio = (a, b) => { const l1 = lum(a), l2 = lum(b); return (Math.max(l1, l2) + 0.05) / (Math.min(l1, l2) + 0.05); };
    const bgOf = el => {
        const chain = [];
        for (let e = el; e; e = e.parentElement) {
            const cs = getComputedStyle(e), bg = parse(cs.backgroundColor), hasImg = cs.backgroundImage !== 'none';
            if (bg && bg.a > 0) chain.push(bg);
            if (bg && bg.a >= 0.99 && !hasImg) break;
            if (hasImg && !(bg && bg.a > 0)) return null; // Verlauf/Bild: nicht berechenbar
        }
        let acc = { r: 255, g: 255, b: 255, a: 1 };
        const page = parse(getComputedStyle(document.body).backgroundColor);
        if (page && page.a > 0) acc = blend(page, acc);
        for (let i = chain.length - 1; i >= 0; i--) acc = blend(chain[i], acc);
        return acc;
    };
    const opacityOf = el => { let o = 1; for (let e = el; e; e = e.parentElement) o *= parseFloat(getComputedStyle(e).opacity); return o; };
    const bad = {};
    let checked = 0;
    const root = (rootSel && document.querySelector(rootSel)) || document.querySelector('.main-content') || document.body;
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
    const seen = new Set();
    while (walker.nextNode()) {
        const n = walker.currentNode, t = n.nodeValue.trim();
        if (t.length < 2) continue;
        const el = n.parentElement;
        if (!el || seen.has(el) || ['SCRIPT', 'STYLE', 'OPTION', 'TEXTAREA'].includes(el.tagName)) continue;
        const r = el.getBoundingClientRect();
        if (r.width < 2 || r.height < 2) continue;
        const cs = getComputedStyle(el);
        if (cs.visibility === 'hidden' || cs.display === 'none' || (!el.offsetParent && cs.position !== 'fixed')) continue;
        seen.add(el);
        const bg = bgOf(el);
        let fg = parse(cs.color);
        if (!bg || !fg) continue;
        fg = { ...fg, a: fg.a * opacityOf(el) };
        const f = blend(fg, bg), rt = ratio(f, bg);
        checked++;
        const size = parseFloat(cs.fontSize), bold = parseInt(cs.fontWeight) >= 700;
        const need = (size >= 24 || (size >= 18.66 && bold)) ? 3 : 4.5;
        const tooSmall = size < 11.5;
        if (rt < need || tooSmall) {
            const key = el.tagName.toLowerCase() + '.' + [...el.classList].slice(0, 5).join('.') + ' | ' + Math.round(f.r) + ',' + Math.round(f.g) + ',' + Math.round(f.b) + ' auf ' + Math.round(bg.r) + ',' + Math.round(bg.g) + ',' + Math.round(bg.b) + (tooSmall ? ' | ' + size.toFixed(1) + 'px' : '');
            (bad[key] = bad[key] || { n: 0, ratio: +rt.toFixed(2), text: t.slice(0, 40), size }).n++;
        }
    }
    return { checked, bad: Object.entries(bad).map(([key, v]) => ({ key, ...v })).sort((a, b) => a.ratio - b.ratio) };
};

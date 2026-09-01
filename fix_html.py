import re
with open('c:/Users/Daniel Hegemann/Documents/antigravity/static/dashboard.html', 'r', encoding='utf-8', errors='ignore') as f:
    text = f.read()

bad_str = '<canvas id="statsChart"></canvas></div></div></div><div class="col-12 col-lg-8"><div class="card premium-card p-4"><h5 class="fw-bold text-white mb-3">Eins?tze pro Monat</h5><div style="height: 400px; position: relative;"><canvas id="statsChartMonth"></canvas>'
good_str = '<canvas id="statsChart"></canvas>\n                                </div>\n                            </div>\n                        </div>\n                        <div class="col-12 col-lg-4">\n                            <div class="card premium-card p-4">\n                                <h5 class="fw-bold text-white mb-3">Einsätze pro Monat</h5>\n                                <div style="height: 400px; position: relative;">\n                                    <canvas id="statsChartMonth"></canvas>'

# Let's just do a regex replace to be safe
text = re.sub(r'<canvas id="statsChart"></canvas>.*?<canvas id="statsChartMonth"></canvas>', good_str, text, flags=re.DOTALL)

with open('c:/Users/Daniel Hegemann/Documents/antigravity/static/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(text)
print('Done')

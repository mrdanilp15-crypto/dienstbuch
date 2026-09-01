import re
with open('c:/Users/Daniel Hegemann/Documents/antigravity/static/dashboard.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Add button
btn = '''                        <li class="nav-item" v-if="role === 'admin' || role === 'leitung'">
                            <button class="nav-link" :class="verwaltungTab === 'standort' ? 'active bg-danger' : 'text-white'" @click="verwaltungTab = 'standort'"><i class="fa fa-map-pin me-1"></i>Feuerwache-Standort</button>
                        </li>
                        <li class="nav-item" v-if="role === 'admin' || role === 'leitung'">
                            <button class="nav-link" :class="verwaltungTab === 'bankdaten' ? 'active bg-danger' : 'text-white'" @click="verwaltungTab = 'bankdaten'"><i class="fa fa-university me-1"></i>Bankdaten</button>
                        </li>'''

text = re.sub(r'<li class="nav-item" v-if="role === \'admin\' \|\| role === \'leitung\'">\s*<button class="nav-link" :class="verwaltungTab === \'standort\' \? \'active bg-danger\' : \'text-white\'" @click="verwaltungTab = \'standort\'"><i class="fa fa-map-pin me-1\"></i>Feuerwache-Standort</button>\s*</li>', btn, text)

# Move content
card_regex = r'<div class="card premium-card p-4 mt-4">\s*<h5 class="fw-bold text-white mb-3"><i class="fa fa-university text-danger me-2"></i>Bankverbindung / Abrechnungsdaten</h5>.*?</div>\s*(?=</div>\s*</div>\s*</div>)'

# extract it
match = re.search(card_regex, text, flags=re.DOTALL)
if match:
    card_html = match.group(0)
    # remove it from current location
    text = text.replace(card_html, '')
    
    # insert as new tab
    new_tab = f'''
                        <!-- SUB-TAB: Bankdaten -->
                        <div v-show="verwaltungTab === 'bankdaten'">
                            <div class="row g-4">
                                <div class="col-12 col-lg-6">
                                    {card_html.replace('mt-4', '')}
                                </div>
                            </div>
                        </div>
'''
    text = text.replace('<!-- TAB: STATISTIKEN -->', new_tab + '\n                <!-- TAB: STATISTIKEN -->')

with open('c:/Users/Daniel Hegemann/Documents/antigravity/static/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(text)
print('Done')

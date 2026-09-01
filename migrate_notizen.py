import re

with open('c:/Users/Daniel Hegemann/Documents/antigravity/static/notizen.html', 'r', encoding='utf-8') as f:
    notizen_html = f.read()

start_idx = notizen_html.find('<div class="container mt-4">')
end_idx = notizen_html.find('<script>', start_idx)
html_part = notizen_html[start_idx:end_idx].strip()
# Notizen HTML ends with: </div> </div> </div> </body> </html> (last 2 divs belong to app/body)
html_part = html_part.rsplit('</div>', 2)[0]

# Rename variables
html_part = html_part.replace('activeFilter', 'notesActiveFilter')
html_part = html_part.replace('editingId', 'noteEditingId')
html_part = html_part.replace('startEdit', 'startNoteEdit')
html_part = html_part.replace('cancelEdit', 'cancelNoteEdit')

new_tab = f'''                <!-- TAB: NOTIZEN -->
                <div v-show="activeTab === 'notizen'">
                    <div class="d-flex justify-content-between align-items-center mb-4 flex-wrap gap-2">
                        <div>
                            <h2 class="fw-bold m-0 text-white"><i class="fa fa-sticky-note text-danger me-2"></i>Digitales Notizbuch</h2>
                            <small class="text-muted">Mängelmeldungen, Funkraum-Notizen und Whiteboard</small>
                        </div>
                    </div>
                    {html_part}
                </div>'''

with open('c:/Users/Daniel Hegemann/Documents/antigravity/static/dashboard.html', 'r', encoding='utf-8') as f:
    db_html = f.read()

empty_tab = '''                <!-- TAB: NOTIZEN -->
                <div v-show="activeTab === 'notizen'">
                    
                </div>'''

db_html = db_html.replace(empty_tab, new_tab)

with open('c:/Users/Daniel Hegemann/Documents/antigravity/static/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(db_html)

print('Done!')

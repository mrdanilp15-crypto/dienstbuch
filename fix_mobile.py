import re
with open('c:/Users/Daniel Hegemann/Documents/antigravity/static/dashboard.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Hide on mobile for top navbar buttons
text = text.replace('<div class="d-flex align-items-center gap-3">\n                    <button class="btn btn-outline-secondary', 
    '<div class="d-none d-lg-flex align-items-center gap-3">\n                    <button class="btn btn-outline-secondary')

# Add to mobile sidebar
mobile_buttons = '''
                    <div class="sidebar-section-header d-lg-none">Aktionen</div>
                    <button class="sidebar-btn d-lg-none" @click="toggleTheme"><i class="fa" :class="isDarkMode ? 'fa-sun' : 'fa-moon'"></i> Theme wechseln</button>
                    <a href="/static/alarmdisplay.html" target="_blank" class="sidebar-btn text-decoration-none d-lg-none" style="display: flex; align-items: center;"><i class="fa fa-desktop me-2"></i> Hallen-Display</a>
                    <button class="sidebar-btn d-lg-none text-info" @click="requestPushPermission"><i class="fa fa-bell"></i> Push-Alarm Test</button>
                    <button class="sidebar-btn d-lg-none text-danger" @click="fullLogout"><i class="fa fa-sign-out-alt"></i> Logout</button>

                    <div class="sidebar-section-header">System</div>'''

text = text.replace('<div class="sidebar-section-header">System</div>', mobile_buttons)

with open('c:/Users/Daniel Hegemann/Documents/antigravity/static/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(text)
print('Done')

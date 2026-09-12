const CACHE_NAME = 'fw-app-cache-v48';
// Alle Bibliotheken werden jetzt selbst gehostet (static/vendor/) statt live von CDNs geladen -
// dadurch sind das hier alles gleiche-Origin-Anfragen. Wichtig, denn cache.addAll() ist atomar:
// schlägt (bei früheren Cross-Origin-URLs) auch nur EINE einzelne Anfrage fehl (CDN-Hänger,
// CORS-Eigenheit bei Opaque Responses), bricht die KOMPLETTE Installation dieses Service Workers
// ab. Gleiche-Origin-Anfragen gegen den eigenen Server sind dagegen zuverlässig.
const urlsToCache = [
  '/static/manifest.json',
  '/static/vendor/bootstrap-5.3.0/css/bootstrap.min.css',
  '/static/vendor/bootstrap-5.3.0/js/bootstrap.bundle.min.js',
  '/static/vendor/fontawesome-6.4.0/css/all.min.css',
  '/static/vendor/fontawesome-6.4.0/webfonts/fa-solid-900.woff2',
  '/static/vendor/fontawesome-6.4.0/webfonts/fa-regular-400.woff2',
  '/static/vendor/fontawesome-6.4.0/webfonts/fa-brands-400.woff2',
  '/static/vendor/vue-3.5.42/vue.global.prod.js',
  '/static/vendor/leaflet-1.9.4/leaflet.css',
  '/static/vendor/leaflet-1.9.4/leaflet.js',
  '/static/vendor/fonts-outfit/outfit.css',
  '/static/vendor/fonts-outfit/outfit-latin.woff2',
  '/static/vendor/fonts-outfit/outfit-latin-ext.woff2',
  '/static/js/dashboard.js',
  '/static/css/dashboard.css'
];

self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => {
        // Einzeln statt addAll(): addAll() ist atomar und würde die KOMPLETTE Installation
        // abbrechen, wenn auch nur eine einzige Datei aus der Liste fehlschlägt (z.B. während
        // eines Deploys kurzzeitig nicht erreichbar) - dann bliebe die App für immer auf der
        // alten Service-Worker-Version hängen. Mit allSettled() schlägt höchstens die einzelne
        // Datei fehl, der Rest wird trotzdem zwischengespeichert und der neue Worker aktiviert.
        return Promise.allSettled(urlsToCache.map(url => cache.add(url).catch(err => {
          console.error('SW precache fehlgeschlagen für', url, err);
        })));
      })
  );
  self.skipWaiting();
});

self.addEventListener('fetch', event => {
  if (event.request.method !== 'GET') return;
  
  // Do not cache API or HTML navigation requests to prevent login/logout loops
  const isDynamic = event.request.url.includes('/api/') || 
                    event.request.url.includes('/groups/') || 
                    event.request.url.includes('/sessions/') || 
                    event.request.url.includes('/users/');
                    
  if (isDynamic || event.request.mode === 'navigate') {
      event.respondWith(fetch(event.request));
      return;
  }
  
  event.respondWith(
    caches.match(event.request)
      .then(response => {
        if (response) return response;
        return fetch(event.request).then(
          function(response) {
            if(!response || response.status !== 200 || response.type !== 'basic' || !event.request.url.startsWith('http')) return response;
            var responseToCache = response.clone();
            caches.open(CACHE_NAME).then(function(cache) {
                cache.put(event.request, responseToCache);
            });
            return response;
          }
        ).catch(err => {
          // Ohne dieses catch() landete ein reiner Netzwerkfehler (WLAN-Aussetzer im
          // Gerätehaus, DNS-Hänger) als unbehandelte Promise-Ablehnung in respondWith() -
          // der betroffene <script>/<link> lud dann lautlos gar nicht, ohne jede Fehlermeldung
          // ("nach dem Login passiert nichts"). Seit alle Bibliotheken gleiche-Origin liegen,
          // versuchen wir hier zumindest noch einmal den (ggf. leeren) Cache, bevor wir den
          // Fehler sauber durchreichen statt ihn unbehandelt zu lassen.
          console.error('SW fetch fehlgeschlagen für', event.request.url, err);
          return caches.match(event.request).then(cached => cached || Promise.reject(err));
        });
      })
  );
});

self.addEventListener('activate', event => {
  const cacheWhitelist = [CACHE_NAME];
  event.waitUntil(
    caches.keys().then(cacheNames => {
      return Promise.all(
        cacheNames.map(cacheName => {
          if (cacheWhitelist.indexOf(cacheName) === -1) {
            return caches.delete(cacheName);
          }
        })
      );
    }).then(() => self.clients.claim())
  );
});

// Basic Push Notification Event Listener
self.addEventListener('push', event => {
  let data = {};
  try {
    if (event.data) {
      data = event.data.json();
    }
  } catch(e) {
    console.error('Push payload parse error', e);
  }
  
  const title = data.title || 'Neuer Einsatz!';
  
  // Resolve absolute URL for icons to prevent Android dropping it
  const iconUrl = new URL(data.icon || '/static/favicon.png', self.location.origin).href;
  
  const options = {
    body: data.body || 'Bitte Dashboard öffnen.',
    icon: iconUrl,
    vibrate: [500, 250, 500, 250, 500, 250, 500, 250, 500, 250, 500],
    requireInteraction: true,
    silent: false, // explizit NICHT stumm - ob wirklich ein Ton kommt, entscheidet aber am Ende das Handy (siehe unten)
    data: { url: data.url || '/' },
    tag: 'alarm-' + Date.now(),
    renotify: true
  };
  // WICHTIG: Ob der Bildschirm angeht, ein Ton kommt oder die Meldung als große
  // "Heads-up"-Ansicht statt nur im Benachrichtigungsfenster erscheint, entscheidet
  // NICHT diese Seite, sondern der vom Handy-Betriebssystem für diese Website/PWA
  // angelegte Benachrichtigungskanal. Ohne "Wichtig/Dringend"-Einstufung dieses Kanals
  // ist Ton/Bildschirm-Aufwecken technisch von hier aus nicht erzwingbar (siehe README/Chat).

  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener('notificationclick', event => {
  event.notification.close();
  event.waitUntil(
    clients.matchAll({ type: 'window' }).then(windowClients => {
      for (var i = 0; i < windowClients.length; i++) {
        var client = windowClients[i];
        if (client.url.includes('/') && 'focus' in client) {
          return client.focus();
        }
      }
      if (clients.openWindow) {
        return clients.openWindow(event.notification.data.url || '/');
      }
    })
  );
});

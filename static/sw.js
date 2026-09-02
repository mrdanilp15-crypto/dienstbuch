const CACHE_NAME = 'fw-app-cache-v9';
const urlsToCache = [
  '/static/manifest.json',
  'https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css',
  'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css',
  'https://unpkg.com/vue@3/dist/vue.global.prod.js',
  'https://unpkg.com/leaflet@1.9.4/dist/leaflet.css',
  'https://unpkg.com/leaflet@1.9.4/dist/leaflet.js'
];

self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => {
        return cache.addAll(urlsToCache);
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
        );
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
    data: { url: data.url || '/' },
    tag: 'alarm-' + Date.now(),
    renotify: true
  };

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

const CACHE = 'autotrader-v1';
const STATIC = ['/', '/index.html', '/manifest.json'];

self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(STATIC)));
  self.skipWaiting();
});

self.addEventListener('activate', e => {
  e.waitUntil(caches.keys().then(keys =>
    Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
  ));
  self.clients.claim();
});

self.addEventListener('fetch', e => {
  // Always network-first for API calls
  if (e.request.url.includes('api.twelvedata.com') ||
      e.request.url.includes('api.anthropic.com') ||
      e.request.url.includes('api.etrade.com')) {
    return; // pass through
  }
  e.respondWith(
    fetch(e.request).catch(() => caches.match(e.request))
  );
});

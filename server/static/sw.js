/* ── sw.js — service worker mínimo do NestVault ─────────────────────────────
   Objetivo: instalabilidade PWA + cache-first dos assets estáticos.
   APIs e páginas HTML seguem sempre pela rede (dados de backup nunca
   devem vir de cache). Servido em /sw.js (escopo raiz) via main.py. */

const CACHE = 'nestvault-static-v1';
const STATIC_ASSETS = [
  '/static/theme.css',
  '/static/app.css',
  '/static/nav.css',
  '/static/app.js',
  '/static/nav.js',
  '/static/icon-192.png',
  '/static/icon-512.png',
  '/static/fonts/ibm-plex-mono-300.woff2',
  '/static/fonts/ibm-plex-mono-400.woff2',
  '/static/fonts/ibm-plex-mono-500.woff2',
  '/static/fonts/ibm-plex-mono-600.woff2',
];

self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(STATIC_ASSETS)));
  self.skipWaiting();
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);
  // Só assets estáticos: stale-while-revalidate. Todo o resto vai à rede.
  if (e.request.method !== 'GET' || url.origin !== location.origin
      || !url.pathname.startsWith('/static/')) return;
  e.respondWith(
    caches.match(e.request).then(cached => {
      const fresh = fetch(e.request).then(resp => {
        if (resp.ok) caches.open(CACHE).then(c => c.put(e.request, resp.clone()));
        return resp;
      }).catch(() => cached);
      return cached || fresh;
    })
  );
});

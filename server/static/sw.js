/* ── sw.js — service worker mínimo do NestVault ─────────────────────────────
   Objetivo: instalabilidade PWA + cache-first dos assets estáticos.
   APIs e páginas HTML seguem sempre pela rede (dados de backup nunca
   devem vir de cache). Servido em /sw.js (escopo raiz) via main.py. */

/* A versão do cache é a do servidor (main.py substitui __NV_VERSION__ ao servir
   /sw.js): todo release renova o cache sozinho. Antes era um número manual, e
   esquecer o bump misturava HTML novo com CSS/JS antigo. */
const CACHE = 'nestvault-static-__NV_VERSION__';
const CACHE_PREFIX = 'nestvault-static-';
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
  // allSettled: uma fonte ausente não impede a instalação do resto (addAll
  // falhava o install inteiro).
  e.waitUntil(caches.open(CACHE).then(c => Promise.allSettled(STATIC_ASSETS.map(a => c.add(a)))));
  self.skipWaiting();
});

self.addEventListener('activate', e => {
  // Só os caches deste app: apagar todos os caches da origem derrubava os de
  // outros apps servidos no mesmo host.
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k.startsWith(CACHE_PREFIX) && k !== CACHE).map(k => caches.delete(k)))
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

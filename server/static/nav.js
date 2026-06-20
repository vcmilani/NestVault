(function () {
  var HAM = '<svg viewBox="0 0 16 16"><line x1="2" y1="4" x2="14" y2="4"/><line x1="2" y1="8" x2="14" y2="8"/><line x1="2" y1="12" x2="14" y2="12"/></svg>';
  var CLO = '<svg viewBox="0 0 16 16"><line x1="2" y1="2" x2="14" y2="14"/><line x1="14" y1="2" x2="2" y2="14"/></svg>';

  var btn, menu, backdrop;

  function open() {
    menu.classList.add('open');
    backdrop.classList.add('open');
    btn.innerHTML = CLO;
    btn.setAttribute('aria-expanded', 'true');
  }

  function close() {
    menu.classList.remove('open');
    backdrop.classList.remove('open');
    btn.innerHTML = HAM;
    btn.setAttribute('aria-expanded', 'false');
  }

  window.closeNav = close;

  function init() {
    btn = document.querySelector('.nav-hamburger');
    menu = document.querySelector('.mobile-nav');
    backdrop = document.querySelector('.mobile-nav-backdrop');
    if (!btn || !menu || !backdrop) return;

    btn.innerHTML = HAM;
    btn.setAttribute('aria-label', 'Menu');
    btn.setAttribute('aria-expanded', 'false');

    btn.addEventListener('click', function () {
      menu.classList.contains('open') ? close() : open();
    });

    backdrop.addEventListener('click', close);

    menu.querySelectorAll('a').forEach(function (a) {
      a.addEventListener('click', close);
    });

    window.addEventListener('resize', function () {
      if (window.innerWidth > 768) close();
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();

// ── Shared utilities (available to all pages) ──────────────────────────────

// Relative time formatter — returns HTML span with ISO date as tooltip
window.relTime = function(isoStr) {
  if (!isoStr) return '—';
  var raw  = isoStr.includes('T') ? isoStr : isoStr.replace(' ', 'T');
  var d    = new Date(raw);
  var full = isoStr.replace('T', ' ').slice(0, 19);
  if (isNaN(d.getTime())) return full;
  var secs = Math.round((Date.now() - d.getTime()) / 1000);
  var rel;
  if (Math.abs(secs) < 10)    rel = 'agora';
  else if (secs < 60)         rel = secs + 's atrás';
  else if (secs < 3600)       rel = Math.floor(secs / 60) + 'min atrás';
  else if (secs < 86400)      rel = Math.floor(secs / 3600) + 'h atrás';
  else if (secs < 86400 * 30) rel = Math.floor(secs / 86400) + 'd atrás';
  else                        rel = full.slice(0, 10);
  return '<span title="' + full + '" style="cursor:default;border-bottom:1px dotted currentColor;color:inherit">' + rel + '</span>';
};

// Auto-detect and mark active nav link based on current URL
(function() {
  var path = location.pathname.replace(/\/$/, '') || '/';
  document.querySelectorAll('a.hbtn, .mobile-nav a').forEach(function(a) {
    try {
      var u = new URL(a.href, location.origin);
      var p = u.pathname.replace(/\/$/, '') || '/';
      if (p === path) a.classList.add('active');
    } catch(_) {}
  });
})();

// Keyboard shortcut: R = refresh current page (when not typing)
document.addEventListener('keydown', function(e) {
  var tag = e.target.tagName;
  if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || e.target.isContentEditable) return;
  if (e.metaKey || e.ctrlKey || e.altKey) return;
  if (e.key === 'r' || e.key === 'R') {
    e.preventDefault();
    if (typeof loadAll === 'function')  loadAll();
    else if (typeof load  === 'function') load();
    else if (typeof poll  === 'function') poll();
  }
});

// Horizontal scroll overflow indicator for .table-wrap elements
function _updateOverflowHints() {
  document.querySelectorAll('.table-wrap').forEach(function(el) {
    el.classList.toggle('has-h-overflow', el.scrollWidth > el.clientWidth + 4);
  });
}
(function() {
  function setup() {
    _updateOverflowHints();
    window.addEventListener('resize', _updateOverflowHints);
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', setup);
  else setup();
})();

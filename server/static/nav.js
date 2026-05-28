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

/* ── app.js — utilidades compartilhadas por todas as páginas ────────────────
   Auth (API key + login overlay padrão), tema, formatação, tick "atualizado
   há Xs" com pausa em aba oculta e registro do service worker.
   Deve ser carregado ANTES do <script> inline de cada página. */

var API_KEY = localStorage.getItem('backupApiKey') || '';
const H = () => ({'X-API-Key': API_KEY});

const fmtSize = b => {
  if (!b) return '0 B';
  const u = ['B','KB','MB','GB','TB'];
  for (const unit of u) { if (Math.abs(b) < 1024) return b.toFixed(1)+' '+unit; b /= 1024; }
  return b.toFixed(1)+' PB';
};
// Escapa também aspas simples. ATENÇÃO: isso NÃO torna seguro interpolar em
// handlers inline (onclick="fn('${esc(x)}')"): o navegador decodifica as
// entidades HTML antes de executar o JS, e a aspa volta. Para passar dados a um
// handler use data-* + this.dataset (ver users.html).
const esc = s => String(s ?? '')
  .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
  .replace(/"/g,'&quot;').replace(/'/g,'&#39;');
const fmtDate = s => s ? s.replace('T',' ').slice(0,19) : '—';

// Tempo relativo em HTML (span com a data completa no title). Mora aqui, e não
// em nav.js, porque nav.js é carregado DEPOIS do script de cada página — todas
// precisavam do guard `window.relTime ? ... : fmtDate(...)`. A data vem do
// servidor e passa por esc(): quando não parseia, voltava crua para innerHTML.
function relTime(isoStr) {
  if (!isoStr) return '—';
  const raw  = String(isoStr).includes('T') ? String(isoStr) : String(isoStr).replace(' ', 'T');
  const d    = new Date(raw);
  const full = esc(String(isoStr).replace('T', ' ').slice(0, 19));
  if (isNaN(d.getTime())) return full;
  const secs = Math.round((Date.now() - d.getTime()) / 1000);
  let rel;
  if (Math.abs(secs) < 10)    rel = 'agora';
  else if (secs < 60)         rel = secs + 's atrás';
  else if (secs < 3600)       rel = Math.floor(secs / 60) + 'min atrás';
  else if (secs < 86400)      rel = Math.floor(secs / 3600) + 'h atrás';
  else if (secs < 86400 * 30) rel = Math.floor(secs / 86400) + 'd atrás';
  else                        rel = full.slice(0, 10);
  return '<span title="' + full + '" style="cursor:default;border-bottom:1px dotted currentColor;color:inherit">' + rel + '</span>';
}
window.relTime = relTime;

// Download de um arquivo pelo cookie de sessão do /cloud (o navegador faz o
// streaming; nada é montado em memória). O explorer baixava via fetch + blob:
// um arquivo de vários GB derrubava a aba, e revogar a URL logo depois do
// click() cancelava o download no Safari/Firefox.
let _nvSessionReady = false;
async function nvEnsureSession() {
  if (_nvSessionReady) return true;
  const r = await fetch('/cloud/session', {method: 'POST', headers: H(), credentials: 'same-origin'});
  if (r.status === 401) { showLogin(API_KEY ? 'API Key inválida' : ''); return false; }
  if (!r.ok) { showError('Falha ao abrir sessão (' + r.status + ')'); return false; }
  _nvSessionReady = true;
  return true;
}
async function nvDownload(fileId) {
  if (!(await nvEnsureSession())) return;
  location.href = '/cloud/content/' + encodeURIComponent(fileId) + '?download=true';
}

// ── Barra de erro / login overlay padrão ────────────────────────────────────
function showError(msg) {
  const b = document.getElementById('errorBar');
  if (!b) return;
  b.textContent = '⚠ ' + msg; b.classList.add('visible');
}
function clearError() {
  const b = document.getElementById('errorBar');
  if (b) b.classList.remove('visible');
}
function showLogin(msg = '') {
  const o = document.getElementById('loginOverlay');
  if (!o) return;
  o.style.display = 'flex';
  const e = document.getElementById('loginError');
  if (e) e.textContent = msg;
}
function hideLogin() {
  const o = document.getElementById('loginOverlay');
  if (o) o.style.display = 'none';
}

// Recarrega os dados da página atual (cada página define uma dessas funções)
function _nvRefresh() {
  if (typeof loadAll === 'function') loadAll();
  else if (typeof load === 'function') load();
  else if (typeof poll === 'function') poll();
  else if (typeof loadFiles === 'function') loadFiles();
}

// ── Tema ────────────────────────────────────────────────────────────────────
function toggleTheme() {
  const isLight = document.documentElement.getAttribute('data-theme') === 'light';
  if (isLight) { document.documentElement.removeAttribute('data-theme'); localStorage.setItem('nv-theme','dark'); }
  else { document.documentElement.setAttribute('data-theme','light'); localStorage.setItem('nv-theme','light'); }
  updateThemeBtn();
}
function updateThemeBtn() {
  const btn = document.getElementById('themeBtn');
  if (!btn) return;
  const light = document.documentElement.getAttribute('data-theme') === 'light';
  btn.innerHTML = light ? '☾<span class="hbtn-label"> Escuro</span>' : '☀<span class="hbtn-label"> Claro</span>';
  btn.title = light ? 'Tema escuro' : 'Tema claro';
}

// ── "atualizado há Xs" — pausa quando a aba fica oculta ────────────────────
let _nvLastUpdate = null;
let _nvTickTimer  = null;

function _nvTickRender() {
  if (!_nvLastUpdate) return;
  const el = document.getElementById('lastUpdated');
  if (el) el.textContent = 'atualizado há ' + Math.round((Date.now() - _nvLastUpdate) / 1000) + 's';
}
function _nvStartTick() {
  clearInterval(_nvTickTimer);
  if (document.hidden) return;
  _nvTickTimer = setInterval(_nvTickRender, 1000);
}
// Marca o momento da última carga de dados e (re)inicia o tick de 1s.
function nvMarkUpdated() {
  _nvLastUpdate = Date.now();
  _nvTickRender();
  _nvStartTick();
}
document.addEventListener('visibilitychange', () => {
  if (document.hidden) { clearInterval(_nvTickTimer); _nvTickTimer = null; }
  else if (_nvLastUpdate) { _nvTickRender(); _nvStartTick(); }
});

// ── Modal de confirmação compartilhado ──────────────────────────────────────
// nvConfirm(title, message[, {word}]) → Promise<boolean>. Com {word}, o botão
// de confirmar só habilita após digitar a palavra. (maintenance.html mantém a
// própria versão, que sobrepõe esta.)
let _nvConfirmResolve = null;
let _nvConfirmWord    = null;

function _nvEnsureConfirmModal() {
  if (document.getElementById('nvConfirmModal')) return;
  document.body.insertAdjacentHTML('beforeend', `
<div id="nvConfirmModal" class="nv-confirm-overlay">
  <div class="nv-confirm-box">
    <div class="nv-confirm-title" id="nvConfirmTitle">Confirmar operação</div>
    <div class="nv-confirm-msg" id="nvConfirmMsg"></div>
    <div id="nvConfirmWordWrap" style="display:none">
      <div class="nv-confirm-word-label" id="nvConfirmWordLabel"></div>
      <input class="nv-confirm-word-input" id="nvConfirmWordInput" autocomplete="off">
    </div>
    <div class="nv-confirm-actions">
      <button class="nv-confirm-cancel" id="nvConfirmCancelBtn">Cancelar</button>
      <button class="nv-confirm-ok" id="nvConfirmOkBtn">Confirmar</button>
    </div>
  </div>
</div>`);
  const modal = document.getElementById('nvConfirmModal');
  modal.addEventListener('click', e => { if (e.target === modal) _nvConfirmClose(false); });
  document.getElementById('nvConfirmCancelBtn').addEventListener('click', () => _nvConfirmClose(false));
  document.getElementById('nvConfirmOkBtn').addEventListener('click', () => _nvConfirmClose(true));
  document.getElementById('nvConfirmWordInput').addEventListener('input', e => {
    document.getElementById('nvConfirmOkBtn').disabled = e.target.value !== _nvConfirmWord;
  });
}

function _nvConfirmClose(ok) {
  document.getElementById('nvConfirmModal').classList.remove('visible');
  if (_nvConfirmResolve) { _nvConfirmResolve(ok); _nvConfirmResolve = null; }
}

function nvConfirm(title, message, opts) {
  _nvEnsureConfirmModal();
  return new Promise(resolve => {
    _nvConfirmResolve = resolve;
    _nvConfirmWord    = opts && opts.word ? opts.word : null;
    document.getElementById('nvConfirmTitle').textContent = title;
    document.getElementById('nvConfirmMsg').textContent   = message;
    document.getElementById('nvConfirmWordInput').value   = '';
    document.getElementById('nvConfirmOkBtn').disabled    = !!_nvConfirmWord;
    const wrap = document.getElementById('nvConfirmWordWrap');
    if (_nvConfirmWord) {
      document.getElementById('nvConfirmWordLabel').textContent = 'Digite "' + _nvConfirmWord + '" para confirmar:';
      document.getElementById('nvConfirmWordInput').placeholder = _nvConfirmWord;
      wrap.style.display = 'block';
    } else {
      wrap.style.display = 'none';
    }
    document.getElementById('nvConfirmModal').classList.add('visible');
  });
}

// ── Área do administrador ───────────────────────────────────────────────────
// A página inicial (Fotos/Cloud) usa a chave PESSOAL (backupApiKey). As páginas
// administrativas (<body data-admin-page>) usam a chave de ADMIN (nvAdminKey) —
// ou a pessoal, quando ela já é de admin. Uma chave de usuário comum nunca vira
// admin: a área pede uma credencial de admin, guardada à parte, e sair da área
// não desloga das fotos.
const NV_ADMIN_PAGE = !!(document.body && document.body.hasAttribute('data-admin-page'));
if (NV_ADMIN_PAGE) {
  // Síncrono, antes do <script> da página: o loadAll() dela já sai com a chave certa.
  API_KEY = localStorage.getItem('nvAdminKey') || API_KEY;
}

function nvRoleCacheClear() { try { sessionStorage.removeItem('nv-role'); } catch (_) {} }

// Papel da chave: 'admin' | 'user' | 'invalid' (401) | null (erro de rede).
// Cacheado na aba por 5 min, amarrado ao fim da chave — um admin rebaixado via
// pela aba inteira a UI de admin (o servidor sempre barrou; era só visual).
const NV_ROLE_TTL_MS = 5 * 60 * 1000;
async function nvRole(key) {
  if (!key) return 'invalid';
  const tag = key.slice(-8);
  try {
    const c = JSON.parse(sessionStorage.getItem('nv-role') || 'null');
    if (c && c.k === tag && Date.now() - (c.ts || 0) < NV_ROLE_TTL_MS) return c.role;
  } catch (_) {}
  let r;
  try { r = await fetch('/cloud/me', {headers: {'X-API-Key': key}}); } catch (_) { return null; }
  if (r.status === 401) return 'invalid';
  if (!r.ok) return null;
  const role = (await r.json()).role;
  try { sessionStorage.setItem('nv-role', JSON.stringify({k: tag, role, ts: Date.now()})); } catch (_) {}
  return role;
}

// Sair das fotos limpa TAMBÉM a chave de admin: num computador compartilhado,
// o logout da página inicial deixava a área do administrador aberta.
function nvLogout() {
  nvRoleCacheClear();
  localStorage.removeItem('backupApiKey');
  localStorage.removeItem('nvAdminKey');
  fetch('/cloud/session', {method: 'DELETE', credentials: 'same-origin'}).catch(() => {});
  API_KEY = '';
  showLogin();
}

function nvAdminLoginTexts() {
  const t = document.querySelector('#loginOverlay .login-title');
  const sub = document.querySelector('#loginOverlay .login-sub');
  if (t) t.textContent = '◈ Área do administrador';
  if (sub) sub.textContent = 'Informe uma chave de administrador para acessar os painéis.';
}

function nvAdminLogin(msg) {
  nvAdminLoginTexts();
  const inp = document.getElementById('apiKeyInput');
  if (inp) inp.value = '';
  showLogin(msg || '');
}

// Chamado pelas páginas admin quando a API responde 403 (chave sem papel de admin).
function nvNotAdmin() {
  localStorage.removeItem('nvAdminKey');
  nvRoleCacheClear();
  nvAdminLogin('Esta chave não é de administrador.');
}

// true = pode seguir; false = está pedindo a credencial de admin.
async function nvGuardAdmin() {
  if (!NV_ADMIN_PAGE) return true;
  if (!API_KEY) { nvAdminLogin(); return false; }
  const role = await nvRole(API_KEY);
  if (role === 'admin' || role === null) return true;  // null: rede — a página mostra o erro
  if (localStorage.getItem('nvAdminKey') === API_KEY) localStorage.removeItem('nvAdminKey');
  nvAdminLogin(role === 'invalid' ? 'Chave de administrador inválida ou revogada.' : '');
  return false;
}

// ── Wiring (login padrão, tema, service worker) ─────────────────────────────
(function () {
  function init() {
    updateThemeBtn();
    if (NV_ADMIN_PAGE) nvAdminLoginTexts();  // o 401 das próprias páginas usa o mesmo overlay
    const form = document.getElementById('loginForm');
    if (form) {
      form.addEventListener('submit', async e => {
        e.preventDefault();
        const key = document.getElementById('apiKeyInput').value.trim();
        if (!key) return;
        if (NV_ADMIN_PAGE) {
          // Área admin: só aceita chave de admin, e guarda à parte — a chave
          // pessoal (das fotos) nunca é sobrescrita aqui.
          const role = await nvRole(key);
          if (role !== 'admin') {
            showLogin(role === 'user' ? 'Esta chave não é de administrador.'
                    : role === 'invalid' ? 'API Key inválida.' : 'Não foi possível validar a chave.');
            return;
          }
          localStorage.setItem('nvAdminKey', key);
        } else {
          localStorage.setItem('backupApiKey', key);
        }
        API_KEY = key;
        hideLogin(); _nvRefresh();
      });
      const logout = document.getElementById('logoutBtn');
      if (logout) {
        if (NV_ADMIN_PAGE) {
          logout.innerHTML = '⎋<span class="hbtn-label"> Sair do admin</span>';
          logout.title = 'Sair da área do administrador (continua logado nas fotos)';
        }
        logout.addEventListener('click', () => {
          nvRoleCacheClear();
          if (NV_ADMIN_PAGE) {
            localStorage.removeItem('nvAdminKey');
            location.href = '/';
            return;
          }
          nvLogout();
        });
      }
    }
    nvGuardAdmin();
    // Botões só com ícone (✕, ↓, ≡): o title vira aria-label para leitores de tela.
    document.querySelectorAll('button[title]:not([aria-label])').forEach(b => {
      if (!/[A-Za-zÀ-ÿ]{2,}/.test(b.textContent)) b.setAttribute('aria-label', b.title);
    });
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();

  // Linhas e cards clicáveis (<tr onclick>, .col-row) não eram alcançáveis pelo
  // teclado: com role="button" + tabindex="0", Enter/Espaço disparam o click.
  document.addEventListener('keydown', e => {
    if (e.key !== 'Enter' && e.key !== ' ') return;
    const el = e.target;
    if (!el || el.tagName === 'BUTTON' || el.tagName === 'A' || el.tagName === 'INPUT'
        || el.tagName === 'TEXTAREA' || el.tagName === 'SELECT') return;
    if (el.getAttribute('role') !== 'button') return;
    e.preventDefault();
    el.click();
  });

  // Atalho: R recarrega os dados da página (quando não se está digitando).
  // Era duplicado em nav.js, com a própria lista de funções.
  document.addEventListener('keydown', e => {
    const tag = e.target.tagName;
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || e.target.isContentEditable) return;
    if (e.metaKey || e.ctrlKey || e.altKey) return;
    if (e.key === 'r' || e.key === 'R') { e.preventDefault(); _nvRefresh(); }
  });

  if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('/sw.js').catch(() => {});
  }
})();

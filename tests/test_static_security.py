"""Frontend — guardas estáticas da revisão v10.4.0: cabeçalhos de segurança,
service worker versionado pelo servidor, dados do servidor sempre escapados nos
templates conhecidos, helpers sem duplicata e version_key validado."""
import re
from pathlib import Path

from conftest import make_backup

STATIC = Path(__file__).resolve().parents[1] / "server" / "static"


def test_security_headers_and_sw_version(client):
    r = client.get("/cloud")
    assert "frame-ancestors 'none'" in r.headers["content-security-policy"]
    assert r.headers["x-content-type-options"] == "nosniff"
    sw = client.get("/sw.js")
    assert sw.status_code == 200
    assert "__NV_VERSION__" not in sw.text
    assert "nestvault-static-10." in sw.text
    assert sw.headers["cache-control"] == "no-cache"


def test_version_key_is_validated(client):
    make_backup(client, "b1")
    for bad in ("a b", "x/y", "<img>", "k" * 65, ""):
        assert client.post("/backups/b1/versions", json={"version_key": bad}).status_code == 422, bad
    assert client.post("/backups/b1/versions", json={"version_key": "2026-01-01T00:00:00"}).status_code == 200


def test_no_server_data_interpolated_into_inline_handlers():
    """onclick="fn('${esc(x)}')" não é seguro (o navegador decodifica as entidades
    antes do JS). Dados vão em data-* e o handler lê this.dataset."""
    for page in STATIC.glob("*.html"):
        text = page.read_text(encoding="utf-8")
        assert not re.search(r'on[a-z]+="[^"]*\$\{esc\(', text), page.name


def test_badges_escape_status_values():
    for page, needle in (("index.html", "badge-${esc(s)}"), ("rclone.html", "badge-${esc(j.last_run_status)}")):
        assert needle in (STATIC / page).read_text(encoding="utf-8"), page


def test_shared_helpers_live_in_app_js_only():
    app_js = (STATIC / "app.js").read_text(encoding="utf-8")
    nav_js = (STATIC / "nav.js").read_text(encoding="utf-8")
    assert "function relTime(" in app_js and "window.relTime" not in nav_js
    assert "e.key === 'r'" in app_js and "e.key === 'r'" not in nav_js
    # relTime escapa a data antes de devolver HTML
    assert "const full = esc(" in app_js


def test_explorer_downloads_via_session_not_blob():
    text = (STATIC / "explorer.html").read_text(encoding="utf-8")
    assert "createObjectURL" not in text and "nvDownload(" in text

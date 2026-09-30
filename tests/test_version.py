"""Garante que a versão declarada é a mesma em todo o projeto.

A versão vive em cinco lugares que não podem se importar entre si — o cliente
é empacotado sozinho pelo PyInstaller, sem acesso a server/ nem à raiz — então
em vez de uma fonte única este teste falha assim que um deles divergir.
A referência é a entrada mais recente do CHANGELOG.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SEMVER = r"(\d+\.\d+\.\d+)"


def _find(path: str, pattern: str) -> str:
    text = (ROOT / path).read_text(encoding="utf-8")
    m = re.search(pattern, text, re.MULTILINE)
    assert m, f"versão não encontrada em {path} (padrão {pattern!r})"
    return m.group(1)


def test_all_declared_versions_match_changelog():
    expected = _find("CHANGELOG.md", rf"^\*\*v{SEMVER}\*\*")
    declared = {
        "README.md (título)": _find("README.md", rf"^# .*`v{SEMVER}`"),
        "client/nestvault.py (docstring)": _find("client/nestvault.py", rf"^NestVault\s+v{SEMVER}"),
        "client/nestvault.py (VERSION)": _find("client/nestvault.py", rf'^VERSION = "v{SEMVER}"'),
        "server/main.py (docstring)": _find("server/main.py", rf"^NestVault\s+v{SEMVER}"),
        "server/main.py (FastAPI)": _find("server/main.py", rf'FastAPI\(.*version="{SEMVER}"'),
    }
    wrong = {where: v for where, v in declared.items() if v != expected}
    assert not wrong, f"CHANGELOG diz {expected}, mas: {wrong}"


def test_static_pages_do_not_hardcode_version():
    # O dashboard lê a versão do /health; um "v9.1" escrito à mão no HTML
    # ficou esquecido por várias releases.
    hits = [
        f"{p.relative_to(ROOT)}: {m.group(0)}"
        for p in sorted((ROOT / "server" / "static").glob("*.[hj][ts]*"))
        for m in re.finditer(r"\bv\d+\.\d+(\.\d+)?\b", p.read_text(encoding="utf-8"))
    ]
    assert not hits, f"versão fixa nas páginas: {hits}"

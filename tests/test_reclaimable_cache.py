"""Invalidação do cache de espaço liberável.

_get_reclaimable_bytes tinha só um TTL de 60s. Como /storage/info e /api/activity
são justamente as telas que o usuário olha logo depois de apagar alguma coisa,
o número mostrado podia ser o de antes da limpeza por até um minuto. Agora o
cache também carrega a geração de invalidate_activity() e é descartado quando
uma escrita acontece.
"""
import main as m
from conftest import make_backup, make_version, upload_file, finish_version


def _reclaimable(client):
    r = client.get("/storage/info")
    assert r.status_code == 200
    return r.json()["reclaimable_bytes"]


def _setup_stale_version(client):
    """v1 com conteúdo próprio + v2 (keeper): o conteúdo de v1 é liberável."""
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    upload_file(client, "b1", "v1", path="/antigo.txt", content=b"conteudo antigo")
    finish_version(client, "b1", "v1", status="done")

    make_version(client, "b1", "v2")
    upload_file(client, "b1", "v2", path="/novo.txt", content=b"novo")
    finish_version(client, "b1", "v2", status="done")


def test_reclaimable_is_cached_between_reads(client):
    """Sem escrita nenhuma no meio, a segunda leitura vem do cache."""
    _setup_stale_version(client)
    first = _reclaimable(client)
    assert first == len(b"conteudo antigo")

    # Mexe direto no cache: se a segunda leitura recalculasse, o valor plantado
    # seria descartado e o teste falharia — é assim que se observa o hit.
    m._reclaimable_cache["value"] = 123456
    assert _reclaimable(client) == 123456


def test_delete_version_invalidates_reclaimable_cache(client):
    """Depois de apagar a versão, o valor tem que refletir a limpeza na hora —
    sem esperar os 60s de TTL."""
    _setup_stale_version(client)
    assert _reclaimable(client) == len(b"conteudo antigo")

    r = client.delete("/backups/b1/versions/v1")
    assert r.status_code == 200

    # O cleanup de órfãos roda como BackgroundTask e remove o FileContent de
    # /antigo.txt; nada mais sobra para liberar.
    assert _reclaimable(client) == 0

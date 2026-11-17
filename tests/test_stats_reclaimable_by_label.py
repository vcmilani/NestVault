"""/api/stats: espaço liberável por label (Q11) e o TTL adaptativo do cache."""
import threading
import time

import main as m

from conftest import make_backup, make_version, finish_version, upload_file


def _version(client, label, key, files):
    make_version(client, label, key)
    for path, content in files:
        upload_file(client, label, key, path=path, content=content)
    finish_version(client, label, key)


def test_reclaimable_by_label_conta_so_conteudo_exclusivo_das_antigas(client):
    make_backup(client, "a")
    make_backup(client, "b")
    # "x" e "w" só existem em versões antigas de "a" → liberáveis.
    # "y" está na mais recente de "a" e "z" na mais recente de "b" → não.
    # "x" aparece em duas versões antigas e conta uma vez só.
    _version(client, "a", "2026-01-01T00:00:00",
             [("/x", b"xxx"), ("/y", b"yy-shared"), ("/z", b"z-em-b")])
    _version(client, "a", "2026-01-02T00:00:00", [("/x", b"xxx"), ("/w", b"wwww")])
    _version(client, "a", "2026-01-03T00:00:00", [("/y", b"yy-shared")])
    _version(client, "b", "2026-01-01T00:00:00", [("/z", b"z-em-b")])

    data = client.get("/api/stats", params={"refresh": True}).json()
    by_label = {e["label"]: e for e in data["reclaimable_by_label"]}
    assert set(by_label) == {"a"}
    assert by_label["a"]["old_version_count"] == 2
    assert by_label["a"]["old_versions_size_bytes"] == len(b"xxx") + len(b"wwww")


def test_ttl_cresce_com_a_duracao_do_ultimo_calculo(client):
    client.get("/api/stats")
    m._stats_cache["build_s"] = 400.0  # cálculo lento: TTL efetivo = 1200s
    m._stats_cache["ts"] = time.monotonic() - 600  # vencido pelo TTL fixo, não pelo adaptativo
    called = []
    orig = m._refresh_stats_async
    m._refresh_stats_async = lambda: called.append(1)
    try:
        client.get("/api/stats")
        assert called == []
        m._stats_cache["ts"] = time.monotonic() - 1300
        client.get("/api/stats")
        assert called == [1]
    finally:
        m._refresh_stats_async = orig


def test_refresh_com_calculo_em_andamento_reaproveita_o_resultado(client):
    client.get("/api/stats")
    builds = []
    orig_build = m._build_stats_data

    def counting_build(db):
        builds.append(1)
        return orig_build(db)

    m._build_stats_data = counting_build
    try:
        # Simula um recálculo em background segurando o lock e terminando depois.
        assert m._stats_refresh_lock.acquire(blocking=False)

        def finish_background():
            time.sleep(0.3)
            db = m.SessionLocal()
            try:
                m._compute_stats_into_cache(db)
            finally:
                db.close()
                m._stats_refresh_lock.release()

        t = threading.Thread(target=finish_background)
        t.start()
        r = client.get("/api/stats", params={"refresh": True})
        t.join()
        assert r.status_code == 200
        assert len(builds) == 1  # o clique não disparou um segundo cálculo
    finally:
        m._build_stats_data = orig_build

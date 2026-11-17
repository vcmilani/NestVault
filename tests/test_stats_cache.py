"""/api/stats: idade do cache (age_seconds) e recálculo forçado (?refresh=true)."""
import time

import main as m

from conftest import make_backup


def test_age_seconds_reflete_idade_do_cache(client):
    client.get("/api/stats")  # garante cache preenchido
    m._stats_cache["ts"] = time.monotonic() - 120
    age = client.get("/api/stats").json()["age_seconds"]
    assert 119 <= age < 130


def test_sem_refresh_devolve_cache_mesmo_com_dados_novos(client):
    before = client.get("/api/stats").json()["total_backups"]
    make_backup(client, "novo")
    assert client.get("/api/stats").json()["total_backups"] == before


def test_refresh_recalcula_inline_e_zera_idade(client):
    before = client.get("/api/stats").json()["total_backups"]
    make_backup(client, "novo")
    m._stats_cache["ts"] = time.monotonic() - 120
    data = client.get("/api/stats", params={"refresh": True}).json()
    assert data["total_backups"] == before + 1
    assert data["age_seconds"] < 5
    # O valor recalculado fica no cache para os próximos requests.
    assert client.get("/api/stats").json()["total_backups"] == before + 1


def test_refresh_exige_admin(two_users):
    _admin, alice, _bob = two_users
    assert alice.get("/api/stats", params={"refresh": True}).status_code == 403

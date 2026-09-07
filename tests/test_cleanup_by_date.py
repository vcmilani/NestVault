"""Testa /maintenance/cleanup-by-date(/preview) — em particular a validação do
parâmetro `before` (N5): um valor malformado deve virar 400, não 500."""
from conftest import make_backup, make_version, finish_version


def test_cleanup_by_date_preview_invalid_before_returns_400(client):
    r = client.get("/maintenance/cleanup-by-date/preview", params={"before": "not-a-date"})
    assert r.status_code == 400


def test_cleanup_by_date_invalid_before_returns_400(client):
    r = client.post("/maintenance/cleanup-by-date", params={"before": "not-a-date"})
    assert r.status_code == 400


def test_cleanup_by_date_preview_valid_before(client):
    make_backup(client, "lbl")
    make_version(client, "lbl", "2020-01-01T00:00:00")
    finish_version(client, "lbl", "2020-01-01T00:00:00")

    r = client.get("/maintenance/cleanup-by-date/preview", params={"before": "2026-01-01T00:00:00"})
    assert r.status_code == 200

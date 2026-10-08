"""Front cloud — fotos: indexação (backfill, EXIF, miniaturas, falhas, órfãos),
timeline por usuário e miniaturas com isolamento."""
import io
import os
from datetime import datetime

import pytest
from PIL import Image

import database as db_mod
import main as m
import storage as storage_mod
from cloud_ui import media
from conftest import make_backup, make_version, finish_version, upload_file

V1 = "2026-01-01T00:00:00"


def jpeg(color=(200, 10, 10), size=(400, 200), taken=None, orientation=None):
    im = Image.new("RGB", size, color)
    ex = Image.Exif()
    if orientation:
        ex[274] = orientation
    if taken:
        ex.get_ifd(0x8769)[36867] = taken
    buf = io.BytesIO()
    im.save(buf, "JPEG", exif=ex)
    return buf.getvalue()


def png_rgba():
    buf = io.BytesIO()
    Image.new("RGBA", (50, 80), (0, 0, 255, 0)).save(buf, "PNG")
    return buf.getvalue()


def backup(c, label, files, version=V1):
    make_backup(c, label=label)
    make_version(c, label=label, version_key=version)
    for path, (data, mtime) in files.items():
        upload_file(c, label, version, path=path, content=data, mtime=mtime)
    finish_version(c, label, version)


def index():
    return media.indexer.run_once(lambda: m.SessionLocal(), should_continue=lambda: True)


def photos(c, **params):
    r = c.get("/cloud/photos", params=params)
    assert r.status_code == 200, r.text
    return r.json()


def session():
    return m.SessionLocal()


TS_2023 = datetime(2023, 7, 14, 10, 30).timestamp()


def test_backfill_existing_backup_and_timeline(client):
    backup(client, "fotos", {
        "/Users/a/Pictures/velha.jpg": (jpeg(taken="2023:07:14 10:30:00", orientation=6), 1_700_000_000),
        "/Users/a/Pictures/sem-exif.jpg": (jpeg((0, 200, 0)), 1_750_000_000),
        "/Users/a/Pictures/logo.png": (png_rgba(), 1_600_000_000),
        "/Users/a/Pictures/notas.txt": (b"nao e foto", 1_760_000_000),
    })
    # Antes de indexar: tudo pendente, com mtime como data provisória
    st = client.get("/cloud/photos/indexing").json()
    assert st["total"] == 3 and st["pending"] == 3 and st["indexed"] == 0
    assert all(i["state"] == "pending" and not i["thumb"] for i in photos(client)["items"])

    assert index() == 3

    items = photos(client)["items"]
    assert [i["name"] for i in items] == ["sem-exif.jpg", "velha.jpg", "logo.png"]
    velha = items[1]
    assert velha["ts"] == pytest.approx(TS_2023) and velha["dated"] is True
    assert (velha["w"], velha["h"]) == (200, 400)  # orientação 6 gira 90°
    assert items[0]["dated"] is False and items[0]["ts"] == 1_750_000_000
    assert all(i["state"] == "done" and i["thumb"] for i in items)
    assert client.get("/cloud/photos/indexing").json()["indexed"] == 3

    # Miniatura é JPEG de verdade, já girada, e respeita a caixa
    r = client.get(f"/cloud/thumb/{velha['sha256']}")
    assert r.status_code == 200 and r.headers["content-type"] == "image/jpeg"
    t = Image.open(io.BytesIO(r.content))
    assert t.size[1] <= media.SM_BOX[1] and t.size[0] < t.size[1]
    lg = Image.open(io.BytesIO(client.get(f"/cloud/thumb/{velha['sha256']}", params={"size": "lg"}).content))
    assert lg.size == (200, 400)

    # Já indexado: nova rodada não reprocessa
    assert index() == 0


def test_same_photo_in_two_labels_appears_once(client):
    data = jpeg(taken="2022:01:01 00:00:00")
    backup(client, "celular", {"/DCIM/a.jpg": (data, 1)})
    backup(client, "notebook", {"/home/x/copia.jpg": (data, 1)})
    assert index() == 1  # um conteúdo, processado uma vez
    assert len(photos(client)["items"]) == 1


def test_pagination_and_months(client):
    files = {f"/p/{i}.jpg": (jpeg((i * 20 % 255, 0, 0)), 1_700_000_000 + i * 86400 * 40) for i in range(6)}
    backup(client, "fotos", files)
    index()
    first = photos(client, limit=4)
    assert len(first["items"]) == 4 and first["total"] == 6
    rest = photos(client, limit=4, **first["next"])
    assert rest["next"] is None
    names = [i["name"] for i in first["items"] + rest["items"]]
    assert names == [f"{i}.jpg" for i in range(5, -1, -1)]
    ms = client.get("/cloud/photos/months").json()
    assert sum(x["count"] for x in ms) == 6
    # O cursor do mês aponta para o primeiro item dele
    jump = photos(client, before_ts=ms[1]["ts"], before_id=ms[1]["id"] + 1, limit=1)["items"][0]
    assert jump["id"] == ms[1]["id"]


def test_isolation_timeline_and_thumbs(two_users):
    _admin, alice, bob = two_users
    backup(alice, "alice-fotos", {"/a/1.jpg": (jpeg(), 1)})
    backup(bob, "bob-fotos", {"/b/1.jpg": (jpeg((0, 0, 255)), 1)})
    index()
    a_items = photos(alice)["items"]
    b_items = photos(bob)["items"]
    assert [i["label"] for i in a_items] == ["alice-fotos"]
    assert [i["label"] for i in b_items] == ["bob-fotos"]
    assert bob.get(f"/cloud/thumb/{a_items[0]['sha256']}").status_code == 404
    assert alice.get(f"/cloud/thumb/{a_items[0]['sha256']}").status_code == 200


def test_trashed_label_leaves_timeline_and_thumb(two_users):
    _admin, alice, _bob = two_users
    backup(alice, "alice-fotos", {"/a/1.jpg": (jpeg(), 1)})
    index()
    sha = photos(alice)["items"][0]["sha256"]
    assert alice.delete("/backups/alice-fotos").status_code == 200
    assert photos(alice)["items"] == []
    assert alice.get(f"/cloud/thumb/{sha}").status_code == 404


def test_encrypted_source_gets_encrypted_thumbs(client, monkeypatch):
    monkeypatch.setattr(storage_mod, "ENCRYPTION_ENABLED", True, raising=False)
    monkeypatch.setattr(storage_mod, "encryption_key", os.urandom(32), raising=False)
    backup(client, "enc", {"/e/foto.jpg": (jpeg(taken="2021:05:05 05:05:05"), 1)})
    assert index() == 1
    db = session()
    mi = db.query(db_mod.MediaInfo).one()
    db.close()
    assert mi.thumb_encrypted is True
    with open(mi.thumb_sm, "rb") as f:
        assert f.read(2) != b"\xff\xd8"  # no disco não é JPEG legível
    r = client.get(f"/cloud/thumb/{mi.sha256}")
    assert r.status_code == 200 and r.content[:2] == b"\xff\xd8"
    assert Image.open(io.BytesIO(r.content)).size[1] <= media.SM_BOX[1]
    assert photos(client)["items"][0]["ts"] == pytest.approx(datetime(2021, 5, 5, 5, 5, 5).timestamp())


def test_corrupt_image_fails_with_retry_limit(client, monkeypatch):
    backup(client, "fotos", {"/p/quebrada.jpg": (b"isto nao e um jpeg", 1)})
    assert index() == 1
    db = session()
    mi = db.query(db_mod.MediaInfo).one()
    assert mi.status == "failed" and mi.attempts == 1 and mi.error
    db.close()
    # Falha recente não é retentada no mesmo ciclo/na hora
    media.indexer._scanned.clear()
    assert index() == 0
    assert photos(client)["items"][0]["state"] == "pending"

    # Depois do intervalo, retenta até MAX_ATTEMPTS e desiste
    monkeypatch.setattr(media, "RETRY_AFTER", media.timedelta(seconds=-1))
    for _ in range(5):
        media.indexer._scanned.clear()
        index()
    db = session()
    assert db.query(db_mod.MediaInfo).one().attempts == media.MAX_ATTEMPTS
    db.close()
    assert photos(client)["items"][0]["state"] == "failed"
    assert client.get("/cloud/photos/indexing").json()["failed"] == 1


def test_orphan_media_info_is_swept(client, tmp_vol):
    thumb = tmp_vol / "_thumbs" / "ab" / "orfa_sm.jpg"
    thumb.parent.mkdir(parents=True)
    thumb.write_bytes(b"x")
    db = session()
    db.add(db_mod.MediaInfo(sha256="ab" * 32, kind="image", status="done", attempts=1,
                            thumb_sm=str(thumb)))
    db.commit()
    db.close()
    index()
    db = session()
    assert db.query(db_mod.MediaInfo).count() == 0
    db.close()
    assert not thumb.exists()


def test_missing_thumb_file_resets_for_reindex(client):
    backup(client, "fotos", {"/p/a.jpg": (jpeg(), 1)})
    index()
    item = photos(client)["items"][0]
    db = session()
    os.remove(db.query(db_mod.MediaInfo).one().thumb_sm)
    db.close()
    assert client.get(f"/cloud/thumb/{item['sha256']}").status_code == 404
    media.indexer._scanned.clear()
    assert index() == 1
    assert client.get(f"/cloud/thumb/{item['sha256']}").status_code == 200


def test_video_without_ffmpeg_is_listed_without_thumb(client, monkeypatch):
    monkeypatch.setattr(media.shutil, "which", lambda _: None)
    backup(client, "v", {"/v/clip.mp4": (b"\x00\x00\x00\x18ftypmp42", 1_700_000_000)})
    assert index() == 1
    it = photos(client)["items"][0]
    assert it["kind"] == "video" and it["state"] == "done" and it["thumb"] is False


def test_video_meta_from_probe():
    meta = media.video_meta_from_probe({
        "format": {"duration": "12.5", "tags": {"creation_time": "2024-07-01T12:00:00.000000Z"}},
        "streams": [{"codec_type": "audio"},
                    {"codec_type": "video", "width": 1920, "height": 1080,
                     "side_data_list": [{"rotation": -90}]}],
    })
    assert (meta["width"], meta["height"]) == (1080, 1920)
    assert meta["duration"] == 12.5
    assert meta["taken_ts"] == datetime.fromisoformat("2024-07-01T12:00:00+00:00").timestamp()


@pytest.mark.parametrize("start,end,hour,expected", [
    (0, 0, 13, True), (1, 7, 3, True), (1, 7, 7, False), (22, 6, 23, True), (22, 6, 5, True), (22, 6, 12, False),
])
def test_window(monkeypatch, start, end, hour, expected):
    vals = {"photos.window_start_hour": start, "photos.window_end_hour": end}
    monkeypatch.setattr(media.config, "get", lambda k: vals[k])
    assert media.in_window(datetime(2026, 1, 1, hour)) is expected


def test_finishing_a_version_wakes_indexer(client, monkeypatch):
    calls = []
    monkeypatch.setattr(media.indexer, "wake", lambda: calls.append(1))
    backup(client, "fotos", {"/p/a.jpg": (jpeg(), 1)})
    assert calls == [1]


def test_photos_page_served(client):
    r = client.get("/photos")
    assert r.status_code == 200 and "NestVault" in r.text


# -- Escolha de quais backups entram -----------------------------------------

def _set(c, label, enabled):
    return c.put(f"/cloud/photos/labels/{label}", json={"enabled": enabled})


def test_labels_default_enabled_with_counts(client):
    backup(client, "fotos", {"/p/a.jpg": (jpeg(), 1), "/p/b.mp4": (b"v", 1), "/p/n.txt": (b"t", 1)})
    assert client.get("/cloud/photos/labels").json() == [
        {"label": "fotos", "client_name": None, "enabled": True, "media_count": 2}]


def test_disabled_label_leaves_timeline_and_is_not_indexed(client):
    backup(client, "fotos", {"/p/a.jpg": (jpeg(), 1)})
    backup(client, "docs", {"/d/scan.jpg": (jpeg((0, 0, 255)), 2)})
    assert _set(client, "docs", False).status_code == 200

    assert [i["label"] for i in photos(client)["items"]] == ["fotos"]
    assert client.get("/cloud/photos/indexing").json()["total"] == 1
    assert index() == 1  # só a foto de "fotos"; o scan de "docs" não é processado

    # Religar traz de volta e indexa o que faltava
    assert _set(client, "docs", True).status_code == 200
    assert {i["label"] for i in photos(client)["items"]} == {"fotos", "docs"}
    assert index() == 1


def test_only_owner_toggles_and_cookie_cannot_write(two_users):
    from fastapi.testclient import TestClient
    admin, alice, bob = two_users
    backup(alice, "alice-fotos", {"/a/1.jpg": (jpeg(), 1)})
    assert _set(bob, "alice-fotos", False).status_code == 403
    assert _set(admin, "alice-fotos", False).status_code == 403  # galeria é pessoal
    assert _set(alice, "nao-existe", False).status_code == 404

    browser = TestClient(m.app)
    browser.post("/cloud/session", headers={"X-API-Key": "alice-key"})
    assert browser.get("/cloud/photos/labels").status_code == 200  # leitura por cookie ok
    assert browser.put("/cloud/photos/labels/alice-fotos", json={"enabled": False}).status_code == 401
    assert [l["enabled"] for l in alice.get("/cloud/photos/labels").json()] == [True]


def test_photos_enabled_column_migrates_existing_db(tmp_path, monkeypatch):
    """Banco anterior à v10.0 (sem a coluna) ganha photos_enabled = true no init_db."""
    import sqlalchemy as sa
    eng = sa.create_engine(f"sqlite:///{tmp_path / 'old.db'}")
    with eng.begin() as c:
        c.execute(sa.text("CREATE TABLE backup_ids (id INTEGER PRIMARY KEY, label VARCHAR NOT NULL UNIQUE, "
                          "client_name VARCHAR, prefix VARCHAR, created_at DATETIME, status VARCHAR)"))
        c.execute(sa.text("INSERT INTO backup_ids (label, status) VALUES ('antigo', 'active')"))
    monkeypatch.setattr(db_mod, "engine", eng)
    db_mod.init_db()
    with eng.connect() as c:
        assert c.execute(sa.text("SELECT photos_enabled FROM backup_ids")).scalar() in (1, True)


# -- Live Photos -------------------------------------------------------------

def test_live_photo_pairs_still_and_motion(client, monkeypatch):
    monkeypatch.setattr(media.shutil, "which", lambda _: None)
    backup(client, "iphone", {
        "/DCIM/IMG_0001.HEIC": (jpeg(taken="2024:05:01 10:00:00"), 1),   # bytes JPEG: só importa o nome
        "/DCIM/IMG_0001.MOV": (b"\x00\x00\x00\x14ftypqt  live", 1),
        "/DCIM/IMG_0002.JPG": (jpeg((0, 0, 200)), 2),                     # foto comum
        "/DCIM/clip.mov": (b"\x00\x00\x00\x14ftypqt  clip", 3),            # vídeo comum
        "/Outra/IMG_0001.MOV": (b"\x00\x00\x00\x14ftypqt  outra", 4),      # mesmo nome, outra pasta
    })
    index()
    items = photos(client)["items"]
    by_name = {(i["path"]): i for i in items}
    assert set(by_name) == {"DCIM/IMG_0001.HEIC", "DCIM/IMG_0002.JPG", "DCIM/clip.mov", "Outra/IMG_0001.MOV"}
    live = by_name["DCIM/IMG_0001.HEIC"]["live"]
    assert live["name"] == "IMG_0001.MOV"
    assert client.get(f"/cloud/content/{live['id']}").status_code == 200
    assert "live" not in by_name["DCIM/IMG_0002.JPG"]
    assert client.get("/cloud/photos/indexing").json()["total"] == 4


def test_long_video_with_same_name_is_not_a_live_photo():
    items = [
        {"id": 1, "sha256": "a", "path": "d/IMG_1.JPG", "label": "x", "name": "IMG_1.JPG", "duration": None, "thumb": True},
        {"id": 2, "sha256": "b", "path": "d/img_1.mov", "label": "x", "name": "img_1.mov", "duration": 42.0, "thumb": True},
        {"id": 3, "sha256": "c", "path": "d/IMG_2.HEIC", "label": "x", "name": "IMG_2.HEIC", "duration": None, "thumb": False},
        {"id": 4, "sha256": "d", "path": "d/IMG_2.MOV", "label": "x", "name": "IMG_2.MOV", "duration": 2.9, "thumb": True},
        {"id": 5, "sha256": "e", "path": "d/IMG_3.HEIC", "label": "outro", "name": "IMG_3.HEIC", "duration": None, "thumb": True},
        {"id": 6, "sha256": "f", "path": "d/IMG_3.MOV", "label": "x", "name": "IMG_3.MOV", "duration": 2.0, "thumb": True},
    ]
    out = {i["id"]: i for i in media.pair_live_photos(items)}
    assert set(out) == {1, 2, 3, 5, 6}           # só o par 3+4 vira Live Photo
    assert out[3]["live"]["id"] == 4 and out[3]["live"]["thumb"] is True
    assert "live" not in out[1] and "live" not in out[5]


# -- Logs --------------------------------------------------------------------

def test_every_indexed_item_and_cycle_is_logged(client, caplog):
    import logging
    backup(client, "fotos", {"/p/ok.jpg": (jpeg(taken="2023:01:02 03:04:05"), 1),
                             "/p/ruim.jpg": (b"corrompido", 1)})
    with caplog.at_level(logging.INFO, logger="backup-server"):
        index()
    msgs = [r.getMessage() for r in caplog.records]
    assert any("ciclo de indexacao iniciado" in m for m in msgs)
    assert any("processando fotos/" in m for m in msgs)
    assert any("ok.jpg" in m and "miniaturas ok" in m and "2023-01-02 03:04" in m for m in msgs)
    assert any("ruim.jpg falhou (tentativa 1/3" in m for m in msgs)
    assert any("ciclo de indexacao concluido: 2 processado(s), 1 falha(s)" in m for m in msgs)

    # E aparece na Atividade como job
    db = session()
    job = db.query(db_mod.MaintenanceJob).filter_by(job_type="photos-index").one()
    db.close()
    assert job.status == "done" and "2 arquivo(s) processado(s), 1 falha(s)" in job.summary


def test_idle_cycle_creates_no_job_and_pause_is_reported(client, monkeypatch):
    assert index() == 0
    db = session()
    assert db.query(db_mod.MaintenanceJob).filter_by(job_type="photos-index").count() == 0
    db.close()
    monkeypatch.setattr(media.config, "get", lambda k: {"photos.indexing_enabled": False}.get(k, 0))
    assert "desligada" in media.pause_reason()


def test_user_actions_are_logged(two_users, caplog):
    import logging
    from fastapi.testclient import TestClient
    _admin, alice, _bob = two_users
    backup(alice, "alice-fotos", {"/a/1.jpg": (jpeg(), 1)})
    fid = photos(alice)["items"][0]["id"]
    with caplog.at_level(logging.INFO, logger="backup-server"):
        TestClient(m.app).post("/cloud/session", headers={"X-API-Key": "alice-key"})
        _set(alice, "alice-fotos", False)
        alice.get(f"/cloud/content/{fid}")
    msgs = " | ".join(r.getMessage() for r in caplog.records)
    assert "sessao aberta para alice" in msgs
    assert "backup 'alice-fotos' removido da galeria" in msgs
    assert f"alice file_id={fid} abre '1.jpg'" in msgs


# -- Selecionar/desmarcar todos e pastas ocultas ----------------------------

def test_set_all_labels_at_once_only_own(two_users):
    admin, alice, bob = two_users
    backup(alice, "a1", {"/a/1.jpg": (jpeg(), 1)})
    backup(alice, "a2", {"/a/2.jpg": (jpeg((0, 0, 9)), 2)})
    backup(bob, "b1", {"/b/1.jpg": (jpeg((9, 0, 0)), 3)})

    assert alice.put("/cloud/photos/labels", json={"enabled": False}).json() == {"enabled": False, "count": 2}
    assert [l["enabled"] for l in alice.get("/cloud/photos/labels").json()] == [False, False]
    assert photos(alice)["items"] == []
    assert [l["enabled"] for l in bob.get("/cloud/photos/labels").json()] == [True]  # não mexe no alheio

    assert alice.put("/cloud/photos/labels", json={"enabled": True}).status_code == 200
    assert {i["label"] for i in photos(alice)["items"]} == {"a1", "a2"}


def test_hidden_folders_are_left_out_unless_asked(client):
    backup(client, "icloud", {
        "/PrimarySync/All Photos/a.jpg": (jpeg(), 1),
        "/PrimarySync/Hidden/segredo.jpg": (jpeg((1, 2, 3)), 2),
        "/PrimarySync/Favoritos/segredo.jpg": (jpeg((1, 2, 3)), 2),  # mesma foto em outro álbum
        "/PrimarySync/.cache/x.jpg": (jpeg((4, 5, 6)), 3),
        "/PrimarySync/Hiddenness/b.jpg": (jpeg((7, 8, 9)), 4),       # só o nome exato conta
    })
    assert sorted(i["name"] for i in photos(client)["items"]) == ["a.jpg", "b.jpg"]
    assert photos(client)["total"] == 2
    assert sum(m["count"] for m in client.get("/cloud/photos/months").json()) == 2
    st = client.get("/cloud/photos/indexing").json()
    assert st["total"] == 2 and st["hidden"] == 2

    shown = photos(client, show_hidden=True)["items"]
    assert sorted(i["name"] for i in shown) == ["a.jpg", "b.jpg", "segredo.jpg", "x.jpg"]
    assert client.get("/cloud/photos/indexing", params={"show_hidden": True}).json()["total"] == 4

import errno

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import database as db_mod
import storage as storage_mod


def _make_session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    db_mod.Base.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine)()


def test_process_ssd_pending_moves_survives_all_volumes_below_threshold(tmp_path, monkeypatch):
    """Quando pick_volume() levanta StorageThresholdExceeded (todos os volumes abaixo
    do limiar) durante o redirect de ENOSPC, o worker deve tratar como 'sem volume
    disponível' e seguir para o retry normal, em vez de propagar a exceção e abortar
    o lote inteiro (Bug A — regressão do commit d8c109b7)."""
    db = _make_session()

    ssd_path = tmp_path / "ssd_content.bin"
    ssd_path.write_bytes(b"conteudo")
    dest_vol = tmp_path / "hdd1"
    dest_vol.mkdir()

    fc = db_mod.FileContent(sha256="a" * 64, stored_at=str(dest_vol / "a"), size=8)
    db.add(fc)
    move = db_mod.SsdCachePendingMove(
        sha256="a" * 64,
        ssd_path=str(ssd_path),
        dest_volume=str(dest_vol),
        dest_path=str(dest_vol / "_content" / "aa" / ("a" * 64)),
    )
    db.add(move)
    db.commit()

    def fake_copy_raises_enospc(_src, _dst):
        raise OSError(errno.ENOSPC, "No space left on device")

    def fake_pick_volume_all_below_threshold():
        raise storage_mod.StorageThresholdExceeded("todos os volumes abaixo do limiar")

    monkeypatch.setattr(storage_mod, "_copy_with_sha256", fake_copy_raises_enospc)
    monkeypatch.setattr(storage_mod, "pick_volume", fake_pick_volume_all_below_threshold)

    # Não deve propagar StorageThresholdExceeded.
    completed, moved = storage_mod.process_ssd_pending_moves(db)

    assert completed == 0
    assert moved == []
    refreshed = db.query(db_mod.SsdCachePendingMove).filter_by(sha256="a" * 64).first()
    assert refreshed is not None
    assert refreshed.retry_count == 1

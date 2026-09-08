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


def test_integrity_error_com_pendencia_viva_conclui_o_move(tmp_path, monkeypatch):
    """Meio-estado de uma tentativa anterior: a cópia do destino já está
    registrada, mas a linha de SsdCachePendingMove sobreviveu (o commit do
    `finally` do worker persiste exatamente isso quando ensure_replicas levanta
    exceção depois do flush).

    Antes, toda tentativa seguinte batia na constraint uq_sha256_volume e saía
    pelo ramo de IntegrityError sem contar retry nem remover a pendência — o
    arquivo ficava preso no SSD para sempre e o monitor abria um job de move a
    cada 30s. Agora o move é concluído a partir da cópia que já existe."""
    db = _make_session()

    sha = "b" * 64
    ssd_dir = tmp_path / "ssd"
    (ssd_dir / "_content" / "bb").mkdir(parents=True)
    ssd_file = ssd_dir / "_content" / "bb" / sha
    ssd_file.write_bytes(b"conteudo")

    dest_vol = tmp_path / "hdd1"
    dest_path = dest_vol / "_content" / "bb" / sha
    dest_path.parent.mkdir(parents=True)

    db.add(db_mod.FileContent(sha256=sha, stored_at=str(ssd_file), size=8))
    db.add(db_mod.FileContentCopy(sha256=sha, stored_at=str(ssd_file), volume_path=str(ssd_dir)))
    # Cópia do destino já registrada pela tentativa anterior — é ela que provoca
    # o IntegrityError quando o worker tenta inserir a mesma linha de novo.
    db.add(db_mod.FileContentCopy(sha256=sha, stored_at=str(dest_path), volume_path=str(dest_vol)))
    db.add(db_mod.SsdCachePendingMove(
        sha256=sha,
        ssd_path=str(ssd_file),
        dest_volume=str(dest_vol),
        dest_path=str(dest_path),
    ))
    db.commit()

    monkeypatch.setattr(storage_mod, "target_replicas", lambda: 1)

    completed, moved = storage_mod.process_ssd_pending_moves(db)

    assert completed == 1
    assert moved == [sha]
    assert db.query(db_mod.SsdCachePendingMove).count() == 0
    assert not ssd_file.exists()                      # SSD liberado
    assert dest_path.exists()                         # cópia do destino preservada
    assert db.get(db_mod.FileContent, sha).stored_at == str(dest_path)
    copies = [c.stored_at for c in db.query(db_mod.FileContentCopy).all()]
    assert copies == [str(dest_path)]                 # linha do SSD removida


def test_reconcile_recria_pendencia_de_arquivo_preso_so_no_ssd(tmp_path, monkeypatch):
    """Arquivo no SSD, sem cópia no HDD e sem move pendente: antes a
    reconciliação só emitia um warning e o arquivo ficava parado até o próximo
    reinício (recover_stuck_ssd_files, que conserta o mesmo estado, só roda no
    startup). Agora a pendência é recriada na hora."""
    db = _make_session()

    sha = "c" * 64
    ssd_dir = tmp_path / "ssd"
    (ssd_dir / "_content" / "cc").mkdir(parents=True)
    ssd_file = ssd_dir / "_content" / "cc" / sha
    ssd_file.write_bytes(b"conteudo")

    dest_vol = tmp_path / "hdd1"
    dest_vol.mkdir()

    db.add(db_mod.FileContent(sha256=sha, stored_at=str(ssd_file), size=8))
    db.add(db_mod.FileContentCopy(sha256=sha, stored_at=str(ssd_file), volume_path=str(ssd_dir)))
    db.commit()

    monkeypatch.setattr(storage_mod, "SSD_CACHE_ENABLED", True)
    monkeypatch.setattr(storage_mod, "SSD_CACHE_DIR", ssd_dir)
    monkeypatch.setattr(storage_mod, "pick_volume", lambda: dest_vol)

    fixed = storage_mod.reconcile_orphaned_ssd_copies(db)

    assert fixed == 1
    move = db.query(db_mod.SsdCachePendingMove).filter_by(sha256=sha).first()
    assert move is not None
    assert move.ssd_path == str(ssd_file)
    assert move.dest_volume == str(dest_vol)

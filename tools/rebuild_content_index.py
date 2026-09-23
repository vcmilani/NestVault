#!/usr/bin/env python3
"""Reconcilia o índice do banco com o que está de fato em _content/.

Toda reconciliação do NestVault parte do banco (rereplicate_*, backfill,
cleanup de órfãos). Nada varre o disco. Se uma linha de file_contents ou
file_content_copies some, os bytes continuam lá e o servidor esquece que
existem — e remontar o disco não traz nada de volta.

Esta ferramenta faz o caminho inverso: lê <volume>/_content/<2-hex>/<sha256>
(a convenção de storage.content_path) e compara com o banco.

    python3 tools/rebuild_content_index.py                  # relatório, não grava
    python3 tools/rebuild_content_index.py --apply          # readota as cópias
    python3 tools/rebuild_content_index.py --apply --verify # confere o sha256 antes

Sem --apply nada é escrito.
"""

import argparse
import hashlib
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "server"))

_READ_CHUNK = 1024 * 1024


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(_READ_CHUNK):
            h.update(chunk)
    return h.hexdigest()


def scan_volume(vol: Path):
    """Gera (sha256, caminho) para cada arquivo de conteúdo do volume.

    Ignora os temporários (_enc_*, _tmp_*) que vivem no mesmo diretório e
    qualquer nome que não seja um sha256 — um arquivo cujo nome não é o hash
    não é conteúdo endereçável e não pode ser readotado com segurança.
    """
    content = vol / "_content"
    if not content.is_dir():
        return
    for shard in sorted(content.iterdir()):
        if not shard.is_dir() or len(shard.name) != 2:
            continue
        for f in sorted(shard.iterdir()):
            name = f.name
            if len(name) != 64 or not all(c in "0123456789abcdef" for c in name):
                continue
            if not f.is_file():
                continue
            yield name, f


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true",
                    help="grava as readoções (sem isto, só relata)")
    ap.add_argument("--verify", action="store_true",
                    help="recomputa o sha256 antes de readotar (lento, mas seguro)")
    args = ap.parse_args()

    import storage
    from database import SessionLocal, FileContent, FileContentCopy, VersionFile
    from sqlalchemy import exists as sa_exists

    db = SessionLocal()
    try:
        readoptable: list[tuple[str, Path, Path]] = []   # (sha, arquivo, volume)
        resurrectable: list[tuple[str, Path, Path]] = []
        mismatched: list[tuple[str, Path]] = []
        on_disk: set[str] = set()

        for vol in storage.STORAGE_VOLUMES:
            if not (vol / "_content").is_dir():
                print(f"!! {vol}/_content não existe ou não é legível — volume ignorado")
                continue
            n_vol = 0
            for sha, path in scan_volume(vol):
                n_vol += 1
                on_disk.add(sha)
                fc = db.query(FileContent).filter(FileContent.sha256 == sha).first()
                if fc is None:
                    resurrectable.append((sha, path, vol))
                    continue
                has_copy = db.query(
                    sa_exists().where(
                        (FileContentCopy.sha256 == sha) &
                        (FileContentCopy.volume_path == str(vol))
                    )
                ).scalar()
                if not has_copy:
                    readoptable.append((sha, path, vol))
            print(f"== {vol}: {n_vol} arquivo(s) de conteúdo no disco")

        # Linhas que o banco tem e o disco não — só relatadas.
        ghosts = []
        for c in db.query(FileContentCopy).all():
            try:
                os.stat(c.stored_at)
            except FileNotFoundError:
                ghosts.append((c.sha256, c.stored_at))
            except OSError as e:
                print(f"?? {c.stored_at} ilegível ({e}) — não classificado")

        print()
        print(f"== cópias readotáveis (arquivo no disco, falta file_content_copies): {len(readoptable)}")
        for sha, path, vol in readoptable[:20]:
            print(f"   {sha[:12]}…  {path}")
        if len(readoptable) > 20:
            print(f"   … +{len(readoptable) - 20}")

        print(f"\n== conteúdos sem file_contents (ressuscitáveis): {len(resurrectable)}")
        for sha, path, vol in resurrectable[:20]:
            print(f"   {sha[:12]}…  {path}")
        if len(resurrectable) > 20:
            print(f"   … +{len(resurrectable) - 20}")
        if resurrectable:
            print("   NÃO são recriados por esta ferramenta, nem com --apply.")
            print("   Sem um VersionFile apontando para eles, a linha recriada é órfã e")
            print("   a limpeza da noite seguinte apaga o arquivo. A associação")
            print("   versão↔caminho só existe no banco: restaure um dump de _db_backups/.")

        print(f"\n== linhas fantasma (banco tem, disco não): {len(ghosts)}")
        for sha, path in ghosts[:20]:
            print(f"   {sha[:12]}…  {path}")
        if len(ghosts) > 20:
            print(f"   … +{len(ghosts) - 20}")

        if not args.apply:
            print("\n(dry-run — nada foi gravado; use --apply para readotar as cópias)")
            return 0

        adopted = 0
        for sha, path, vol in readoptable:
            if args.verify:
                try:
                    actual = sha256_of(path)
                except OSError as e:
                    print(f"!! {path}: ilegível ({e}) — não readotado")
                    continue
                # Conteúdo cifrado em repouso tem sha256 do PLAINTEXT no nome, então
                # o hash do arquivo difere legitimamente. Só dá para verificar o que
                # está em claro; o resto exigiria a chave e uma decifragem completa.
                fc = db.query(FileContent).filter(FileContent.sha256 == sha).first()
                if fc is not None and not fc.encrypted and actual != sha:
                    mismatched.append((sha, path))
                    print(f"!! {path}: sha256 não confere ({actual[:12]}…) — NÃO readotado")
                    continue
                if fc is not None and fc.encrypted:
                    print(f"   {sha[:12]}… cifrado — verificação de hash pulada")

            db.add(FileContentCopy(sha256=sha, stored_at=str(path), volume_path=str(vol)))
            try:
                db.commit()
                adopted += 1
            except Exception as e:
                db.rollback()
                print(f"!! {path}: falha ao gravar ({e})")

        print(f"\n== {adopted} cópia(s) readotada(s)"
              + (f"; {len(mismatched)} rejeitada(s) por sha256" if mismatched else ""))
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())

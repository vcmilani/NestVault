#!/usr/bin/env python3
"""Diagnóstico read-only da limpeza noturna de versões.

Reproduz, fora do servidor, a decisão que run_nightly_cleanup() toma para cada
versão failed/incomplete, e checa as condições que fazem um label inteiro nunca
ser visitado pelo loop de limpeza.

    python3 tools/diag_retention.py /caminho/backup.db
"""

import sqlite3
import sys
from datetime import datetime


def parse_dt(s):
    if not s:
        return None
    s = str(s).replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


def main(path):
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    now = datetime.now()

    print(f"== banco: {path}   agora: {now:%Y-%m-%d %H:%M:%S}\n")

    # 1. Últimas execuções da limpeza noturna -------------------------------
    print("== últimas execuções de nightly-cleanup")
    rows = db.execute(
        "SELECT status, started_at, finished_at, summary FROM maintenance_jobs "
        "WHERE job_type='nightly-cleanup' ORDER BY id DESC LIMIT 10"
    ).fetchall()
    if not rows:
        print("  NENHUMA execução registrada — a limpeza nunca rodou.")
    for r in rows:
        print(f"  [{r['status']:<7}] {r['started_at']} → {r['finished_at'] or '(não terminou)'}")
        print(f"            {r['summary']}")
    print()

    # 2. Labels que o loop da limpeza nunca visita --------------------------
    # run_nightly_cleanup itera sobre backup_ids.label; versão cujo label não
    # existe ali é invisível para a limpeza.
    print("== labels com versões mas SEM linha em backup_ids (invisíveis à limpeza)")
    orphan_labels = db.execute(
        "SELECT backup_label, COUNT(*) n FROM backup_versions v "
        "WHERE NOT EXISTS (SELECT 1 FROM backup_ids b WHERE b.label = v.backup_label) "
        "GROUP BY backup_label"
    ).fetchall()
    if not orphan_labels:
        print("  (nenhum)")
    for r in orphan_labels:
        print(f"  !! {r['backup_label']!r}: {r['n']} versão(ões) — NUNCA limpas")
    print()

    # 3. created_at nulo/ilegível quebra a comparação e aborta a rotina -----
    print("== versões com created_at nulo (abortam a limpeza inteira com TypeError)")
    bad = db.execute(
        "SELECT id, backup_label, version_key, status FROM backup_versions WHERE created_at IS NULL"
    ).fetchall()
    if not bad:
        print("  (nenhuma)")
    for r in bad:
        print(f"  !! id={r['id']} {r['backup_label']}/{r['version_key']} [{r['status']}]")
    print()

    # 4. Ordem de visita dos labels + peso (quem pode travar/abortar antes) --
    print("== labels na ordem em que a limpeza os visita (id de backup_ids)")
    for r in db.execute(
        "SELECT b.id, b.label, b.client_name, "
        "  (SELECT COUNT(*) FROM backup_versions v WHERE v.backup_label=b.label) vers, "
        "  (SELECT COUNT(*) FROM version_files f JOIN backup_versions v ON v.id=f.version_id "
        "     WHERE v.backup_label=b.label) files "
        "FROM backup_ids b ORDER BY b.id"
    ):
        print(f"  #{r['id']:<3} {r['label']:<28} origem={r['client_name'] or '?':<8} "
              f"versões={r['vers']:<5} arquivos={r['files']}")
    print()

    # 5. Veredito por versão failed/incomplete ------------------------------
    print("== veredito da regra stale (failed/incomplete)")
    labels = [r["label"] for r in db.execute("SELECT label FROM backup_ids ORDER BY id")]
    for label in labels:
        vs = db.execute(
            "SELECT id, version_key, status, created_at, finished_at "
            "FROM backup_versions WHERE backup_label=? ORDER BY created_at", (label,)
        ).fetchall()
        stale = [v for v in vs if v["status"] in ("failed", "incomplete")]
        if not stale:
            continue
        done = [v for v in vs if v["status"] == "done"]
        done_dts = [parse_dt(v["created_at"]) for v in done]
        done_dts = [d for d in done_dts if d]
        newest_done = max(done_dts) if done_dts else None
        print(f"\n  {label}  ({len(vs)} versões, {len(done)} done, {len(stale)} stale)")
        print(f"    done mais recente por created_at: "
              f"{newest_done:%Y-%m-%d %H:%M} " if newest_done else "    NENHUMA versão done")
        for v in stale:
            c = parse_dt(v["created_at"])
            has_newer_done = c is not None and any(d > c for d in done_dts)
            if c is None:
                verdict = "created_at ilegível — esta linha ABORTA a limpeza do label"
            elif has_newer_done:
                verdict = ("SERÁ REMOVIDA na próxima limpeza — se ela sobreviveu à última, "
                           "a rotina não chegou neste label (ver seção de execuções acima)")
            else:
                verdict = "mantida: é a falha mais recente do label (nenhuma done depois dela)"
            print(f"    [{v['status']:<10}] key={v['version_key'][:19]} "
                  f"created={v['created_at']} finished={v['finished_at'] or '—'}")
            print(f"                 → {verdict}")
    print()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "backup.db")

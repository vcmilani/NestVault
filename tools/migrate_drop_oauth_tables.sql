-- =============================================================================
-- NestVault v7.3.0 — Remoção das tabelas OAuth de cloud backup
-- =============================================================================
-- Contexto:
--   A v7.3.0 removeu o sistema de cloud backup baseado em OAuth direto
--   (Google Drive e OneDrive). O rclone passou a ser o único backend de
--   cloud backup, gerenciando autenticação via ~/.config/rclone/rclone.conf.
--
--   As tabelas abaixo ficaram obsoletas:
--     • cloud_backup_jobs  — jobs agendados por credencial OAuth
--     • cloud_credentials  — tokens OAuth armazenados criptografados
--
--   A aplicação não acessa mais essas tabelas a partir da v7.3.0; elas podem
--   ser dropadas a qualquer momento sem afetar o funcionamento do sistema.
--
-- Procedimento recomendado:
--   1. Confirme que nenhuma instância antiga (< v7.3.0) ainda está rodando.
--   2. Faça backup do banco antes de executar (cp backup.db backup.db.bak).
--   3. Execute o bloco correspondente ao seu backend abaixo.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- SQLite  (padrão — arquivo backup.db)
-- -----------------------------------------------------------------------------
-- Execute via:
--   sqlite3 backup.db < tools/migrate_drop_oauth_tables.sql
-- Ou interativamente:
--   sqlite3 backup.db
--   sqlite> .read tools/migrate_drop_oauth_tables.sql
--
-- Ordem importa: cloud_backup_jobs referencia cloud_credentials via FK.

PRAGMA foreign_keys = OFF;

DROP TABLE IF EXISTS cloud_backup_jobs;
DROP TABLE IF EXISTS cloud_credentials;

PRAGMA foreign_keys = ON;

-- Opcional: recupera espaço liberado pelas linhas removidas.
-- VACUUM;


-- -----------------------------------------------------------------------------
-- PostgreSQL  (opcional, via DATABASE_URL)
-- -----------------------------------------------------------------------------
-- Execute via:
--   psql "$DATABASE_URL" -f tools/migrate_drop_oauth_tables.sql
-- Ou conectado ao banco:
--   \i tools/migrate_drop_oauth_tables.sql
--
-- Descomente o bloco abaixo se usar PostgreSQL:

-- DROP TABLE IF EXISTS cloud_backup_jobs;
-- DROP TABLE IF EXISTS cloud_credentials;

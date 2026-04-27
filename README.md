# 🗄️ Backup Files — Raspberry Pi  `v2.1`

Sistema de backup com **versionamento**, **deduplicação de conteúdo** e **isolamento por label**.

Cada execução de backup cria uma nova versão dentro do label. O servidor armazena o conteúdo físico apenas uma vez por sha256 — versões diferentes que compartilham arquivos idênticos não duplicam o storage. Arquivos deletados são marcados na versão, nunca apagados do storage diretamente.

> **v2.1** — upload por stream binário puro (sem multipart), queries agregadas, WAL no SQLite, índices compostos, session HTTP reutilizada no cliente. Dependência `requests-toolbelt` removida.

---

## Estrutura

```
backup_system/
├── server/
│   ├── main.py              ← API FastAPI
│   ├── database.py          ← Modelos SQLite/SQLAlchemy
│   ├── requirements.txt
│   └── static/
│       └── index.html       ← Dashboard web
├── client/
│   ├── backup_client.py     ← Cliente de backup/restore
│   └── requirements.txt
├── .gitignore
└── README.md
```

---

## ⚠️ Atualizando da v2.0 para v2.1

A v2.1 adiciona índices novos e usa WAL mode no SQLite — o schema é compatível, mas precisa criar os índices.

### Opção 1 — recomendada: migração in-place

Faça backup do banco antes:

```bash
# Pare o serviço primeiro
sudo systemctl stop backup-server

# Backup do banco
cp /mnt/hd-externo/backup.db /mnt/hd-externo/backup.db.bak.v2.0

# Aplica os novos índices e WAL mode
sqlite3 /mnt/hd-externo/backup.db <<SQL
PRAGMA journal_mode=WAL;
CREATE INDEX IF NOT EXISTS idx_label_status_key ON backup_versions(backup_label, status, version_key);
CREATE INDEX IF NOT EXISTS idx_version_status ON version_files(version_id, status);
CREATE INDEX IF NOT EXISTS idx_sha256 ON version_files(sha256);
CREATE INDEX IF NOT EXISTS ix_backup_ids_client_name ON backup_ids(client_name);
CREATE INDEX IF NOT EXISTS ix_backup_versions_status ON backup_versions(status);
ANALYZE;
SQL

# Atualiza o código e reinicia
cd /home/pi/backup_system
git pull   # ou copie os arquivos manualmente
source server/.venv/bin/activate
pip install -r server/requirements.txt
sudo systemctl start backup-server
```

### Opção 2 — recriar do zero

Se você não se importa em perder os dados (uso ainda em testes):

```bash
sudo systemctl stop backup-server
rm /mnt/hd-externo/backup.db
rm -rf /mnt/hd-externo/backups/_content
sudo systemctl start backup-server
```

O `init_db()` na inicialização cria as tabelas com todos os novos índices.

### Verificar se a migração funcionou

```bash
sqlite3 /mnt/hd-externo/backup.db "PRAGMA journal_mode;"
# Deve retornar: wal

sqlite3 /mnt/hd-externo/backup.db ".indexes"
# Deve listar idx_label_status_key, idx_version_status, idx_sha256, etc.
```

---

## ⚙️ Servidor (Raspberry Pi)

### 1. Criar venv e instalar dependências

```bash
cd server
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configurar variáveis de ambiente

```bash
export BACKUP_API_KEY="uma-chave-secreta-forte-aqui"   # omitir = sem autenticação
export STORAGE_DIR="/mnt/hd-externo/backups"
export DB_PATH="/mnt/hd-externo/backup.db"
```

### 3. Iniciar o servidor

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

### 4. Rodar como serviço (systemd)

Crie `/etc/systemd/system/backup-server.service`:

```ini
[Unit]
Description=Backup Files — Raspberry Pi
After=network.target

[Service]
User=pi
WorkingDirectory=/home/pi/backup_system/server
Environment="BACKUP_API_KEY=sua-chave-aqui"
Environment="STORAGE_DIR=/mnt/hd-externo/backups"
Environment="DB_PATH=/mnt/hd-externo/backup.db"
ExecStart=/home/pi/backup_system/server/.venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000
Restart=always

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable backup-server
sudo systemctl start backup-server
```

---

## 💻 Cliente

### 1. Criar venv e instalar dependências

```bash
cd client
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

> Dependências do cliente: `requests` e `tqdm` apenas. `requests-toolbelt` não é mais necessário.

### 2. Configurar API key

```bash
export BACKUP_API_KEY="uma-chave-secreta-forte-aqui"
```

> Se o servidor estiver sem autenticação, basta omitir a variável.

---

## 🚀 Comandos

O cliente possui cinco subcomandos: `backup`, `backups`, `versions`, `restore` e `cleanup`.

---

### backup

Envia arquivos para o servidor criando uma **nova versão** a cada execução. A versão é identificada automaticamente pela data e hora de início (`2026-04-25T10:42:31`).

Arquivos cujo conteúdo já existe no storage (mesmo sha256) são apenas **registrados** na nova versão — zero bytes trafegam na rede. Arquivos sem alteração desde a última versão são **ignorados**.

```bash
# Backup simples — cria nova versão automaticamente
python backup_client.py backup ~/documentos \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000

# Com prefixo de path no servidor
python backup_client.py backup ~/documentos \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000 \
  --prefix /home/joao/documentos

# Ignorar subpastas
python backup_client.py backup ~/projeto \
  --label "projeto-alpha" \
  --server http://192.168.1.100:8000 \
  --exclude node_modules .git __pycache__ .venv dist build

# Aumentar paralelismo (padrão: 4 workers)
python backup_client.py backup ~/documentos \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000 \
  --workers 8

# Verificar sem enviar
python backup_client.py backup ~/documentos \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000 \
  --dry-run
```

**Opções:**

| Opção | Obrigatório | Descrição |
|-------|:-----------:|-----------|
| `--label` | ✅ | Identificador único do backup |
| `--server` | | URL do servidor (padrão: `http://localhost:8000`) |
| `--prefix` | | Prefixo do path no servidor |
| `--client` | | Nome do cliente — padrão é o hostname da máquina |
| `--exclude` | | Subpastas a ignorar — aceita múltiplos valores |
| `--workers` | | Uploads paralelos (padrão: `4`) |
| `--dry-run` | | Apenas verifica, não envia |

**Resumo ao final do backup:**

```
=======================================================
  Backup      : [notebook-joao]
  Versao      : 2026-04-25T10:42:31
  Verificados : 142
  Enviados    : 3    ← conteúdo novo, upload completo
  Registrados : 12   ← conteúdo já no storage, só registrou
  Ignorados   : 127  ← idênticos à versão anterior
  Deletados   : 1    ← marcados como deleted nesta versão
  Erros       : 0
=======================================================
```

**Recomendação de workers:**

| Cenário | Workers |
|---------|:-------:|
| Pi com cartão SD | 2 |
| Pi com HD externo USB | 4–6 |
| Pi com SSD | 6–8 |
| Muitos arquivos pequenos | 8+ |
| Arquivos grandes (>100 MB) | 2–3 |

---

### backups

Lista todos os backups registrados no servidor.

```bash
python backup_client.py backups --server http://192.168.1.100:8000

# Filtrar por cliente
python backup_client.py backups --server http://192.168.1.100:8000 --client "notebook-joao"
```

| Opção | Descrição |
|-------|-----------|
| `--server` | URL do servidor |
| `--client` | Filtrar por nome do cliente |

Exemplo de saída:

```
LABEL                           CLIENTE               VERSOES  ARQUIVOS     TAMANHO  ULTIMA VERSAO
----------------------------------------------------------------------------------------------------------
notebook-joao                   notebook-joao               8       142      1.4 GB  2026-04-25T10:42:31
servidor-web                    servidor-web                5        38     320.5 MB  2026-04-21T03:00:00
projeto-alpha                   notebook-joao              12       891      4.7 GB  2026-04-20T14:30:00
```

---

### versions

Lista todas as versões de um backup, com contagem de arquivos ativos, deletados e tamanho.

```bash
python backup_client.py versions \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000
```

| Opção | Obrigatório | Descrição |
|-------|:-----------:|-----------|
| `--label` | ✅ | Label do backup |
| `--server` | | URL do servidor |

Exemplo de saída:

```
Versoes de [notebook-joao]:
  VERSAO                  STATUS    ARQUIVOS  DELETADOS     TAMANHO
  -----------------------------------------------------------------
  2026-04-25T10:42:31     done           142          1      1.4 GB
  2026-04-24T02:00:00     done           141          0      1.4 GB
  2026-04-23T02:00:00     done           139          3      1.3 GB
```

---

### restore

Baixa os arquivos de uma **versão específica** e reconstrói a estrutura de pastas no destino. Apenas arquivos com status `active` são restaurados — deletados são ignorados.

```bash
# Restaurar uma versão específica
python backup_client.py restore /tmp/restore \
  --label "notebook-joao" \
  --version "2026-04-25T10:42:31" \
  --server http://192.168.1.100:8000

# Restaurar apenas um subdiretório
python backup_client.py restore /tmp/restore \
  --label "notebook-joao" \
  --version "2026-04-25T10:42:31" \
  --server http://192.168.1.100:8000 \
  --prefix /home/joao/documentos

# Ver o que seria restaurado sem baixar
python backup_client.py restore /tmp/restore \
  --label "notebook-joao" \
  --version "2026-04-25T10:42:31" \
  --dry-run

# Sobrescrever arquivos existentes
python backup_client.py restore /tmp/restore \
  --label "notebook-joao" \
  --version "2026-04-25T10:42:31" \
  --overwrite
```

| Opção | Obrigatório | Descrição |
|-------|:-----------:|-----------|
| `--label` | ✅ | Label do backup |
| `--version` | ✅ | Chave da versão (obtida via `versions`) |
| `--server` | | URL do servidor |
| `--prefix` | | Restaurar apenas arquivos com esse prefixo |
| `--overwrite` | | Sobrescreve arquivos existentes |
| `--dry-run` | | Apenas lista, não baixa |

A integridade de cada arquivo é validada após o download pelo SHA-256.

---

### cleanup

Remove versões antigas de um ou todos os backups, mantendo apenas as `N` mais recentes. Arquivos físicos órfãos (não referenciados por nenhuma versão remanescente) são apagados do storage automaticamente.

```bash
# Limpar um label específico
python backup_client.py cleanup \
  --label "notebook-joao" \
  --keep 5 \
  --server http://192.168.1.100:8000

# Limpar TODOS os labels de uma vez
python backup_client.py cleanup \
  --all \
  --keep 5 \
  --server http://192.168.1.100:8000
```

| Opção | Descrição |
|-------|-----------|
| `--label` | Label específico a limpar (mutuamente exclusivo com `--all`) |
| `--all` | Limpa todos os labels do servidor |
| `--keep` | Quantas versões manter por label (padrão: `5`) |
| `--server` | URL do servidor |

Exemplo de saída com `--all`:

```
Cleanup em todos os labels (3 encontrados), keep=5

  [notebook-joao]  mantidas=5  removidas=2  storage=4 arquivo(s) apagado(s)
    - 2026-04-10T02:00:00
    - 2026-04-03T02:00:00
  [servidor-web]   mantidas=5  removidas=0  storage=0 arquivo(s) apagado(s)
  [projeto-alpha]  mantidas=5  removidas=7  storage=12 arquivo(s) apagado(s)

==================================================
  Labels processados : 3
  Versoes removidas  : 9
  Arquivos do storage: 16
==================================================
```

---

### Agendar com cron

```cron
# Backup todo dia às 02:00
0 2 * * * BACKUP_API_KEY=sua-chave \
  /home/usuario/client/.venv/bin/python \
  /home/usuario/client/backup_client.py backup ~/docs \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000 \
  --exclude node_modules .git \
  --workers 4 \
  >> /var/log/backup.log 2>&1

# Cleanup semanal — manter 10 versões em todos os labels
0 3 * * 0 BACKUP_API_KEY=sua-chave \
  /home/usuario/client/.venv/bin/python \
  /home/usuario/client/backup_client.py cleanup \
  --all --keep 10 \
  --server http://192.168.1.100:8000 \
  >> /var/log/backup-cleanup.log 2>&1
```

---

## 🖥️ Dashboard Web

Acessível pelo browser, servido diretamente pelo FastAPI:

```
http://<ip-da-pi>:8000/
```

Na primeira visita com autenticação ativada, o browser pedirá a API Key — salva no `localStorage`. Para trocar, clique em **⌀ API Key** no header.

**O que o dashboard exibe:**

- **Stats globais** — total de backups, versões, arquivos, storage total
- **Tabela de backups** — clique em um label para expandir as versões
- **Versões** — clique em uma versão para ver os arquivos, incluindo os marcados como `deleted` (em cinza)
- **Auto-refresh** a cada 30 segundos

---

## ⚡ Otimizações da v2.1

| Componente | Otimização | Ganho típico |
|---|---|---|
| **Upload (protocolo)** | Stream binário puro — sem multipart/MIME | Elimina encoding no cliente e parsing no servidor |
| **Upload (memória)** | Stream para disco via `request.stream()` | Arquivos grandes não travam a Pi |
| **Upload (hash)** | SHA-256 calculado em paralelo com a escrita | Single-pass — sem segunda leitura do arquivo |
| **Cliente** | `_ProgressReader` leve + `Session` HTTP reutilizada | Sem overhead de toolbelt, TCP keep-alive |
| **Stats** | Queries agregadas (`func.count`, `func.sum`) | 10x+ mais rápido em backups grandes |
| **`/files`** | JOIN explícito ao invés de lazy load | Elimina N+1 queries |
| **Cleanup** | Subquery `WHERE NOT IN` em vez de loop | 100x+ mais rápido |
| **Delete** | Cascade automático via SQLAlchemy | Bulk delete |
| **SQLite** | WAL mode + cache 64MB + mmap 256MB | Leituras paralelas com escritas |
| **Índices** | Compostos em `(version_id, status)` etc. | Queries do dashboard rápidas |

---

## 🗃️ Arquitetura de dados

```
BackupID (label)
  └── BackupVersion (version_key = datetime ISO)
        └── VersionFile (original_path, sha256, status = active | deleted)
                └── FileContent (sha256, stored_at) ← arquivo físico único por conteúdo
```

**Storage físico:**
```
storage/
└── _content/
    ├── ab/
    │   └── abcd1234ef567890...   ← conteúdo único por sha256
    └── f7/
        └── f7a923bc11d24e5f...
```

O conteúdo de cada arquivo é armazenado **uma única vez**, independente de quantas versões ou labels o referenciem.

---

## 🔌 Endpoints da API

### Dashboard e Health

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `GET` | `/` | Dashboard web |
| `GET` | `/health` | Status do servidor e versão |

### Backups

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/backups` | Cria backup — idempotente |
| `GET` | `/backups` | Lista todos os backups |
| `GET` | `/backups/{label}` | Detalhes de um backup |
| `DELETE` | `/backups/{label}` | Remove backup e todas as versões |

### Versões

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/backups/{label}/versions` | Cria nova versão |
| `GET` | `/backups/{label}/versions` | Lista versões |
| `GET` | `/backups/{label}/versions/{key}` | Detalhes de uma versão |
| `PATCH` | `/backups/{label}/versions/{key}` | Finaliza versão (done/failed) |
| `DELETE` | `/backups/{label}/versions/{key}` | Remove versão |
| `POST` | `/backups/{label}/cleanup` | Mantém apenas `keep` versões mais recentes |

### Arquivos

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/check` | Verifica se arquivo precisa upload |
| `POST` | `/upload` | Registra arquivo na versão |
| `POST` | `/sync` | Marca como deleted arquivos ausentes no cliente |
| `GET` | `/files` | Lista arquivos de uma versão |
| `GET` | `/files/{id}/download` | Faz download |

> Paths com caracteres especiais são transmitidos em **base64** no header `X-Original-Path`.

---

## 📐 Contrato da API (Schemas)

Todos os endpoints possuem **schemas Pydantic explícitos** para entrada e saída. O Swagger UI (`/docs`) mostra todos os formatos detalhadamente, e o `openapi.json` pode ser usado para gerar clientes em outras linguagens.

### Convenções gerais

- Datas e horários: strings ISO 8601 (`2026-04-25T10:42:31`)
- Tamanhos: sempre em bytes
- SHA-256: string hexadecimal de 64 caracteres
- Campos enumerados usam `Literal` (validação estrita do valor)

---

### Schemas de Request

#### `BackupCreate`
```json
{
  "label": "notebook-joao",        // obrigatório, único
  "client_name": "notebook-joao",  // opcional
  "prefix": "/home/joao/docs"      // opcional
}
```

#### `VersionCreate`
```json
{
  "version_key": "2026-04-25T10:42:31"  // ISO datetime
}
```

#### `VersionFinish`
```json
{
  "status": "done"  // "done" | "failed"
}
```

#### `CheckRequest`
```json
{
  "backup_label": "notebook-joao",
  "version_key":  "2026-04-25T10:42:31",
  "original_path": "/home/joao/docs/relatorio.pdf",
  "sha256": "abc123...",      // exatamente 64 chars
  "size":   204800,           // bytes, >= 0
  "mtime":  1713700000.0      // epoch float
}
```

#### `SyncRequest`
```json
{
  "backup_label":   "notebook-joao",
  "version_key":    "2026-04-25T10:42:31",
  "existing_paths": ["/home/joao/docs/a.pdf", "/home/joao/docs/b.pdf"]
}
```

#### `CleanupRequest`
```json
{
  "backup_label": "notebook-joao",
  "keep": 5      // >= 0
}
```

---

### Schemas de Response

#### `HealthResponse`
```json
{
  "status":  "ok",
  "version": "2.1.0",
  "time":    "2026-04-25T10:42:31.123456"
}
```

#### `BackupInfo`
Stats agregados refletem a **última versão** do backup (qualquer status).
```json
{
  "id": 1,
  "label": "notebook-joao",
  "client_name": "notebook-joao",
  "prefix": "/home/joao",
  "status": "active",
  "created_at": "2026-04-01 00:00:00",
  "last_version": "2026-04-25T10:42:31",
  "version_count": 8,
  "file_count": 142,
  "total_size_bytes": 1503238553
}
```

#### `BackupCreatedResponse`
```json
{
  "created": true,    // false se já existia (idempotente)
  "backup":  { /* BackupInfo */ }
}
```

#### `BackupDeletedResponse`
```json
{
  "status": "deleted",
  "label":  "notebook-joao"
}
```

#### `VersionInfo`
```json
{
  "id": 42,
  "version_key": "2026-04-25T10:42:31",
  "backup_label": "notebook-joao",
  "status": "done",                         // "running" | "done" | "failed"
  "created_at": "2026-04-25 10:42:31",
  "finished_at": "2026-04-25 10:45:12",
  "file_count": 142,                        // arquivos active
  "deleted_count": 1,                       // arquivos marcados como deleted
  "total_size_bytes": 1503238553
}
```

#### `VersionCreatedResponse`
```json
{
  "created": true,
  "version": { /* VersionInfo */ }
}
```

#### `VersionDeletedResponse`
```json
{
  "status": "deleted",
  "version_key": "2026-04-10T02:00:00",
  "files_removed_from_storage": 4   // contents órfãos removidos
}
```

#### `CheckResponse`
```json
{
  "needs_upload": true,
  "content_exists": false,           // se true, cliente pode pular o body do upload
  "reason": "Upload necessario",
  "file_id": null                    // não null se já estava registrado
}
```

#### `UploadResponse`
```json
{
  "status": "registered",
  "file_id": 1234,
  "sha256": "abc123...",
  "uploaded": true   // false = só registrou (conteúdo já estava no storage)
}
```

#### `SyncResponse`
```json
{
  "marked_deleted": ["/home/joao/docs/antigo.pdf"],
  "deleted_count": 1
}
```

#### `FileInfo`
```json
{
  "id": 1234,
  "original_path": "/home/joao/docs/relatorio.pdf",
  "sha256": "abc123...",
  "size": 204800,
  "mtime": 1713700000.0,
  "status": "active",                       // "active" | "deleted"
  "created_at": "2026-04-25 10:42:35"
}
```

#### `CleanupResponse`
```json
{
  "kept": 5,
  "versions_removed": ["2026-04-10T02:00:00", "2026-04-03T02:00:00"],
  "storage_files_removed": 4
}
```

---

### Mapeamento Endpoint → Schemas

| Endpoint | Request | Response |
|---|---|---|
| `GET /health` | — | `HealthResponse` |
| `POST /backups` | `BackupCreate` | `BackupCreatedResponse` |
| `GET /backups` | — | `list[BackupInfo]` |
| `GET /backups/{label}` | — | `BackupInfo` |
| `DELETE /backups/{label}` | — | `BackupDeletedResponse` |
| `POST /backups/{label}/versions` | `VersionCreate` | `VersionCreatedResponse` |
| `GET /backups/{label}/versions` | — | `list[VersionInfo]` |
| `GET /backups/{label}/versions/{key}` | — | `VersionInfo` |
| `PATCH /backups/{label}/versions/{key}` | `VersionFinish` | `VersionInfo` |
| `DELETE /backups/{label}/versions/{key}` | — | `VersionDeletedResponse` |
| `POST /backups/{label}/cleanup` | `CleanupRequest` | `CleanupResponse` |
| `POST /check` | `CheckRequest` | `CheckResponse` |
| `POST /upload` | binary stream + headers `X-*` | `UploadResponse` |
| `POST /sync` | `SyncRequest` | `SyncResponse` |
| `GET /files` | query: `backup_label`, `version_key`, `include_deleted` | `list[FileInfo]` |
| `GET /files/{id}/download` | — | binary stream |

---

### Headers especiais

Endpoints que usam headers customizados:

| Header | Endpoint | Descrição |
|---|---|---|
| `X-API-Key` | todos (se autenticação ativa) | Chave de autenticação |
| `X-Backup-Label` | `POST /upload` | Label do backup |
| `X-Version-Key` | `POST /upload` | Chave da versão |
| `X-Original-Path` | `POST /upload` | Path original (base64-encoded) |
| `X-Mtime` | `POST /upload` | Modification time (epoch float) |
| `X-Content-Sha256` | `POST /upload` | SHA-256 do conteúdo (modo "só registrar", sem body) |

---

## 📊 Documentação automática

Com o servidor rodando:
- **Dashboard**: `http://<ip-da-pi>:8000/`
- **Swagger UI**: `http://<ip-da-pi>:8000/docs`
- **ReDoc**: `http://<ip-da-pi>:8000/redoc`
# 🗄️ Backup Files — Raspberry Pi  `v2.6`

Sistema de backup com **versionamento**, **deduplicação de conteúdo** e **isolamento por label**.

Cada execução de backup cria uma nova versão dentro do label. O servidor armazena o conteúdo físico apenas uma vez por sha256 — versões diferentes que compartilham arquivos idênticos não duplicam o storage.

> **v2.6** — verificação de arquivos em lote: novo endpoint `POST /check/batch` reduz drasticamente o número de round-trips em backups com muitos arquivos pequenos (ex: 8 mil arquivos → 80 requests em vez de 8 mil). O cliente detecta automaticamente o suporte ao batch pelo `/health` e usa fallback individual em servidores antigos. Novo argumento `--batch-size` para ajustar o tamanho do lote.
>
> **v2.5** — limpeza de arquivos ao deletar label ou versão agora é assíncrona (retorna imediatamente ao cliente); verificação de espaço em disco ao finalizar backup também é assíncrona; novo endpoint `POST /maintenance/cleanup-orphans` para limpeza forçada; novos comandos `delete-label` e `cleanup-orphans` no cliente.
>
> **v2.4** — comparação entre versões no dashboard (adicionados, removidos, modificados); cache mtime+size no client elimina leitura de disco para arquivos inalterados; auto-refresh removido do dashboard.
>
> **v2.3** — limpeza automática por espaço em disco: ao finalizar cada backup, o servidor verifica o espaço livre no filesystem onde o storage está montado. Se menor que 5%, versões antigas são apagadas automaticamente, mantendo sempre ao menos 1 versão por label.
>
> **v2.2** — remoção do conceito de soft-delete: arquivos ausentes em uma versão simplesmente não aparecem nela. Cada versão é um snapshot completo e independente.
>
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

## ⚠️ Atualizando da v2.1 para v2.2

A v2.2 remove a coluna `status` e o índice `idx_version_status` da tabela `version_files`.

### Opção 1 — recomendada: migração in-place

```bash
# Pare o serviço primeiro
sudo systemctl stop backup-server

# Backup do banco
cp /mnt/hd-externo/backup.db /mnt/hd-externo/backup.db.bak.v2.1

# Remove registros com status="deleted" (já sem utilidade) e a coluna
sqlite3 /mnt/hd-externo/backup.db <<SQL
DELETE FROM version_files WHERE status = 'deleted';
DROP INDEX IF EXISTS idx_version_status;
ALTER TABLE version_files DROP COLUMN status;
VACUUM;
SQL

# Atualiza o código e reinicia
cd /home/pi/backup_system
git pull   # ou copie os arquivos manualmente
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

O `init_db()` na inicialização cria as tabelas com o schema atualizado.

### Verificar se a migração funcionou

```bash
sqlite3 /mnt/hd-externo/backup.db ".schema version_files"
# Não deve conter a coluna "status"

sqlite3 /mnt/hd-externo/backup.db ".indexes"
# Não deve listar idx_version_status
```

---

## ⚠️ Atualizando da v2.0 para v2.1

A v2.1 adiciona índices novos e usa WAL mode no SQLite — o schema é compatível, mas precisa criar os índices.

```bash
sudo systemctl stop backup-server
cp /mnt/hd-externo/backup.db /mnt/hd-externo/backup.db.bak.v2.0

sqlite3 /mnt/hd-externo/backup.db <<SQL
PRAGMA journal_mode=WAL;
CREATE INDEX IF NOT EXISTS idx_label_status_key ON backup_versions(backup_label, status, version_key);
CREATE INDEX IF NOT EXISTS idx_sha256 ON version_files(sha256);
CREATE INDEX IF NOT EXISTS ix_backup_ids_client_name ON backup_ids(client_name);
CREATE INDEX IF NOT EXISTS ix_backup_versions_status ON backup_versions(status);
ANALYZE;
SQL

cd /home/pi/backup_system
git pull
source server/.venv/bin/activate
pip install -r server/requirements.txt
sudo systemctl start backup-server
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

O cliente possui sete subcomandos: `backup`, `backups`, `versions`, `restore`, `cleanup`, `delete-label` e `cleanup-orphans`.

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

# Ajustar tamanho do lote de verificação (padrão: 100 arquivos/request)
python backup_client.py backup ~/documentos \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000 \
  --batch-size 200

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
| `--batch-size` | | Arquivos por request no `/check/batch` (padrão: `100`) |
| `--dry-run` | | Apenas verifica, não envia |
| `--verbose` | | Logs detalhados (arquivos cacheados e ignorados) |

**Resumo ao final do backup:**

```
=======================================================
  Backup      : [notebook-joao]
  Versao      : 2026-04-25T10:42:31
  Verificados : 142
  Enviados    : 3    ← conteúdo novo, upload completo
  Registrados : 12   ← conteúdo já no storage, só registrou
  Cacheados   : 126  ← mtime+size inalterados, sem leitura de disco
  Ignorados   : 1    ← retomada de backup interrompido
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

Lista todas as versões de um backup, com contagem de arquivos e tamanho.

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
  VERSAO                  STATUS    ARQUIVOS     TAMANHO    DURACAO
  ----------------------------------------------------------------------
  2026-04-25T10:42:31     done           142      1.4 GB        42s
  2026-04-24T02:00:00     done           141      1.4 GB        38s
  2026-04-23T02:00:00     done           139      1.3 GB        41s
```

---

### restore

Baixa os arquivos de uma **versão específica** e reconstrói a estrutura de pastas no destino.

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

### delete-label

Exclui permanentemente um label e **todas as suas versões**. Os arquivos físicos órfãos são apagados do storage em background pelo servidor — o cliente recebe a confirmação imediatamente.

```bash
# Com confirmação interativa (padrão)
python backup_client.py delete-label \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000

# Sem confirmação — para uso em scripts
python backup_client.py delete-label \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000 \
  --force
```

| Opção | Obrigatório | Descrição |
|-------|:-----------:|-----------|
| `--label` | ✅ | Label a excluir |
| `--server` | | URL do servidor |
| `--force` | | Pula a confirmação interativa |

> ⚠️ Operação irreversível. Use `--force` apenas em scripts onde a confirmação não é possível.

---

### cleanup-orphans

Força a limpeza imediata de arquivos físicos que não estão mais referenciados por nenhuma versão ativa. Útil após deleções em massa ou para liberar espaço rapidamente.

```bash
python backup_client.py cleanup-orphans \
  --server http://192.168.1.100:8000
```

| Opção | Descrição |
|-------|-----------|
| `--server` | URL do servidor |

Exemplo de saída:

```
Iniciando limpeza forcada de arquivos orfaos...
Limpeza concluida: 14 arquivo(s) removido(s), 312.4 MB liberados
```

---

### Limpeza automática por espaço em disco

O servidor verifica automaticamente o espaço livre **ao finalizar cada backup** (status → `done`). Se o espaço livre no disco estiver abaixo de **5%**, versões antigas são apagadas até que o espaço seja normalizado. Essa verificação ocorre **em background** — o cliente recebe a confirmação do backup imediatamente, sem esperar o scan de disco.

Da mesma forma, ao excluir um label (`DELETE /backups/{label}`) ou uma versão (`DELETE /backups/{label}/versions/{key}`), a remoção dos registros no banco é imediata, mas a limpeza dos arquivos físicos órfãos ocorre em background.

**Comportamento:**

- Verifica o espaço do filesystem onde `STORAGE_DIR` está montado — funciona corretamente com discos secundários (ex: `/mnt/hd-externo`)
- Apaga as versões mais antigas primeiro, distribuindo entre todos os labels
- **Nunca apaga a versão mais recente** de cada label — cada label sempre terá ao menos 1 versão
- Após cada deleção, reavalia o espaço e para assim que atingir 5%
- Registra no terminal do servidor cada versão apagada e o espaço livre atualizado

**Logs de exemplo:**

```
[auto-cleanup] Espaço livre: 3.2% — abaixo de 5%, iniciando limpeza...
[auto-cleanup] Removida notebook-joao/2026-03-01T02:00:00 — 4 arquivo(s) do storage — livre: 3.8%
[auto-cleanup] Removida servidor-web/2026-03-05T03:00:00 — 2 arquivo(s) do storage — livre: 4.3%
[auto-cleanup] Removida notebook-joao/2026-03-08T02:00:00 — 7 arquivo(s) do storage — livre: 5.1%
[auto-cleanup] Espaço normalizado (5.1%), encerrando.
```

> Essa limpeza é um mecanismo de segurança para evitar disco cheio. Para controle previsível de retenção, use o comando [`cleanup`](#cleanup) agendado via cron.

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
- **Versões** — clique em uma versão para ver os arquivos
- **Comparação de versões** — selecione duas versões com as checkboxes e clique em ⇄ Comparar: veja arquivos adicionados, removidos, modificados e o delta de tamanho de cada um

---

## ⚡ Otimizações

### v2.6

| Componente | Mudança |
|---|---|
| **`POST /check/batch` (server)** | Novo endpoint que verifica N arquivos em uma única request. Valida a versão uma vez e itera os itens reutilizando a lógica do `/check` — erros por item não abortam o lote. Retorna resultados na mesma ordem da entrada. |
| **Fase 1 — hashing + batch (client)** | `backup_directory` separada em duas fases: (1) cache hits → hashing sha256 em paralelo → lotes para `/check/batch`; (2) uploads/registers em paralelo via `ThreadPoolExecutor` |
| **Detecção automática de suporte (client)** | `_server_supports_batch()` consulta `/health` e compara a versão. Servidores < 2.6 usam o `/check` individual automaticamente |
| **`--batch-size` (client)** | Novo argumento para ajustar o tamanho do lote (padrão: `100`). Valores maiores reduzem round-trips; valores menores reduzem o impacto de falhas parciais |

**Ganho esperado:**

| Cenário | Antes (v2.5) | Depois (v2.6) |
|---|---|---|
| 1.000 arquivos, rede local 5ms | ~5s só em checks | ~0,5s |
| 8.000 arquivos, rede local 5ms | ~40s só em checks | ~4s |
| 8.000 arquivos, Wi-Fi 20ms | ~160s só em checks | ~16s |

### v2.5

| Componente | Mudança |
|---|---|
| **Delete label (server)** | Limpeza de arquivos órfãos movida para `BackgroundTasks` — resposta imediata ao cliente |
| **Delete versão (server)** | Idem — `files_removed_from_storage` retorna `0` (limpeza ocorre em background) |
| **Finalizar backup (server)** | Verificação de espaço em disco (`_auto_cleanup_if_needed`) movida para background |
| **`POST /maintenance/cleanup-orphans`** | Novo endpoint para limpeza forçada e síncrona de arquivos sem referência |
| **Client — `delete-label`** | Novo comando para excluir label com confirmação interativa ou `--force` |
| **Client — `cleanup-orphans`** | Novo comando que chama o endpoint de limpeza e exibe arquivos removidos e bytes liberados |

### v2.4

| Componente | Mudança |
|---|---|
| **Comparação de versões** | Endpoint `GET /backups/{label}/compare` retorna diff completo (adicionados, removidos, modificados) entre duas versões via 2 queries SQL + set operations em Python |
| **Dashboard** | Checkboxes nas versões + painel de diff; auto-refresh removido (apenas refresh manual) |
| **Client — cache mtime+size** | Antes de calcular SHA-256, verifica mtime e size contra a versão anterior. Se idênticos, registra o arquivo direto com o hash cacheado — sem leitura de disco |
| **Client — `--verbose`** | Flag que ativa logs DEBUG mostrando cada arquivo cacheado ou ignorado |

### v2.3

| Componente | Mudança |
|---|---|
| **Auto-cleanup de disco** | Ao finalizar backup, verifica espaço livre no filesystem do storage e apaga versões antigas se `< 5%` livre, mantendo sempre 1 por label |

### v2.2

| Componente | Mudança |
|---|---|
| **Modelo de dados** | Removido soft-delete — cada versão é um snapshot completo |
| **`version_files`** | Coluna `status` e índice `idx_version_status` removidos |
| **`/sync`** | Simplificado — apenas confirma sincronização, sem UPDATE em massa |
| **`/files`** | Removido parâmetro `include_deleted` |
| **`FileInfo`** | Removido campo `status` |
| **`VersionInfo`** | Removido campo `deleted_count` |

### v2.1

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

---

## 🗃️ Arquitetura de dados

```
BackupID (label)
  └── BackupVersion (version_key = datetime ISO)
        └── VersionFile (original_path, sha256, mtime)
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
| `GET` | `/backups/{label}/compare` | Diff de arquivos entre duas versões (`?v1=...&v2=...`) |

### Arquivos

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/check` | Verifica se um arquivo precisa upload |
| `POST` | `/check/batch` | Verifica N arquivos em uma única request |
| `POST` | `/upload` | Registra arquivo na versão |
| `POST` | `/sync` | Confirma sincronização da versão com o cliente |
| `GET` | `/files` | Lista arquivos de uma versão |
| `GET` | `/files/{id}/download` | Faz download |

> Paths com caracteres especiais são transmitidos em **base64** no header `X-Original-Path`.

### Manutenção

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/maintenance/cleanup-orphans` | Remove todos os arquivos físicos não referenciados por nenhuma versão |

> Retorna `{ "files_removed": N, "bytes_freed": N }`. Útil após deleções em massa ou para liberar espaço imediatamente. Operação **síncrona** — aguarda a conclusão antes de responder.

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

#### `CheckBatchRequest`
```json
{
  "backup_label": "notebook-joao",
  "version_key":  "2026-04-25T10:42:31",
  "files": [
    {
      "original_path": "/home/joao/docs/relatorio.pdf",
      "sha256": "abc123...",
      "size":   204800,
      "mtime":  1713700000.0
    },
    {
      "original_path": "/home/joao/docs/planilha.xlsx",
      "sha256": "def456...",
      "size":   81920,
      "mtime":  1713600000.0
    }
  ]
}
```

Limite: entre 1 e 500 itens por request. O tamanho do lote é definido pelo cliente via `--batch-size`.

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
  "version": "2.6.0",
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
  "file_count": 142,
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

#### `CheckBatchResultItem` (um por arquivo no batch)
```json
{
  "needs_upload": true,
  "content_exists": false,
  "reason": "Upload necessario",
  "file_id": null
}
```

A resposta de `/check/batch` é `list[CheckBatchResultItem]` na mesma ordem dos arquivos enviados. Um erro em um item não descarta o restante do lote — o servidor retorna `needs_upload: true` com um `reason` descritivo para itens problemáticos.

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
  "synced": true
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

#### `OrphanCleanupResponse`
```json
{
  "files_removed": 14,
  "bytes_freed": 327680000
}
```

#### `CompareResponse`
```json
{
  "label": "notebook-joao",
  "v1": "2026-04-24T02:00:00",
  "v2": "2026-04-25T10:42:31",
  "added": [
    { "original_path": "/home/joao/docs/novo.pdf", "sha256": "abc...", "size": 40960, "mtime": 1713800000.0 }
  ],
  "deleted": [
    { "original_path": "/home/joao/docs/velho.txt", "sha256": "def...", "size": 1024, "mtime": 1713700000.0 }
  ],
  "modified": [
    {
      "original_path": "/home/joao/docs/relatorio.pdf",
      "v1_sha256": "aaa...", "v2_sha256": "bbb...",
      "v1_size": 204800, "v2_size": 215040,
      "size_delta": 10240
    }
  ],
  "summary_unchanged": 139
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
| `GET /backups/{label}/compare` | query: `v1`, `v2` | `CompareResponse` |
| `POST /check` | `CheckRequest` | `CheckResponse` |
| `POST /check/batch` | `CheckBatchRequest` | `list[CheckBatchResultItem]` |
| `POST /upload` | binary stream + headers `X-*` | `UploadResponse` |
| `POST /sync` | `SyncRequest` | `SyncResponse` |
| `GET /files` | query: `backup_label`, `version_key` | `list[FileInfo]` |
| `GET /files/{id}/download` | — | binary stream |
| `POST /maintenance/cleanup-orphans` | — | `OrphanCleanupResponse` |

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
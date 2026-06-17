# 🗄️ NestVault  `v7.0.0`

Sistema de backup com **versionamento**, **deduplicação de conteúdo** e **isolamento por label**.

Cada execução de backup cria uma nova versão dentro do label. O servidor armazena o conteúdo físico apenas uma vez por sha256 — versões diferentes que compartilham arquivos idênticos não duplicam o storage.

Projetado para consumir poucos recursos: roda bem em **Raspberry Pi** e em **computadores antigos**, inclusive com discos externos USB.

> **v7.0.0** — integração rclone como opção paralela de cloud backup: elimina a necessidade de registrar app no Google Cloud Console ou Azure Portal — o usuário configura os remotes via `rclone config` e o NestVault os referencia pelo nome. Suporta todos os provedores compatíveis com rclone (Google Drive, OneDrive, S3, Backblaze B2, Dropbox e 70+ outros). O pipeline producer-consumer, deduplicação, criptografia e replicação funcionam de forma idêntica ao cloud backup OAuth nativo. Skip por `mtime` garante que syncs recorrentes só baixam arquivos novos ou alterados — sem re-download de tudo. Novos endpoints em `/rclone/*` + nova tabela `rclone_backup_jobs`.
>
> **v6.1.0** — otimizações de performance: trabalho bloqueante (criptografia, hashing, cópias) removido do event loop via `asyncio.to_thread` — o servidor permanece responsivo durante uploads grandes e jobs cloud simultâneos. Verificação de dedup passa a ser leve (tamanho esperado calculado por fórmula AES-GCM) em vez de decifrar o arquivo inteiro a cada hit; integridade profunda fica com o job `validate-integrity`. SSD cache move reduz de 3 para 2 leituras por arquivo (`_copy_with_sha256`). Cloud runner reutiliza um único `httpx.AsyncClient` por job — elimina handshake TCP/TLS por arquivo. Cliente Python ganha pool de conexões dimensionado para alta concorrência (`pool_maxsize=32`) e retry com backoff exponencial em erros transientes.
>
> **v6.0.0** — SSD cache tier para backup local: quando `SSD_CACHE_ENABLED=true`, uploads são gravados primeiro no SSD (via `SSD_CACHE_DIR`), o servidor responde ao cliente imediatamente, e a movimentação para o disco lento ocorre de forma assíncrona em background. Se o SSD atingir o limite configurado em `SSD_CACHE_MAX_GB`, o upload recai silenciosamente para o HDD sem interrupção. Moves pendentes sobrevivem a reinicializações do servidor (persistidos em `ssd_cache_pending_moves` no banco). Ganho especialmente relevante em redes 2.5 GbE, onde o HDD se torna o gargalo claro (100–150 MB/s vs. 312 MB/s de rede). Desabilitado por padrão — zero impacto para configurações sem SSD cache.
>
> **v5.3.0** — refinamento da política de retenção noturna: a faixa "guardar tudo" foi reduzida de 30 dias para 24 horas; entre 1 dia e 30 dias passa a ser guardada 1 versão `done` por dia (a mais recente de cada dia calendário). As demais faixas permanecem iguais: 30–180 dias → 1 por semana; acima de 180 dias → 1 por mês.
>
> **v5.2.0** — limpeza noturna automática com política de retenção progressiva: versões `failed`/`incomplete` com mais de 1 semana são removidas se houver versão `done` mais recente; dentro de 1 mês todas as versões eram preservadas; entre 1 e 6 meses é guardada 1 versão `done` por semana; acima de 6 meses, 1 versão `done` por mês. A rotina roda automaticamente à meia-noite e pode ser acionada manualmente via `POST /maintenance/nightly-cleanup`. O resultado de cada execução fica registrado no histórico de manutenção da tela de atividade.
>
> **v5.1.0** — opção de reconectar conta cloud: quando um refresh token é revogado ou expira, os jobs de backup ficam marcados com `reauth_required`. A nova coluna "Status" na aba de contas do dashboard exibe `⚠ TOKEN REVOGADO` e apresenta o botão `↺ Reconectar`, que reabre o fluxo OAuth reutilizando a credencial existente — todos os jobs associados são preservados. Ao concluir, os tokens são atualizados e o status `reauth_required` dos jobs é limpo automaticamente. Corrigido: OneDrive agora lança `TokenRevokedError` em caso de `invalid_grant`, fazendo o runner marcar o job como `reauth_required` em vez de `error`.
>
> **v5.0** — limpeza de versões por data: nova opção na tela de manutenção para remover permanentemente versões criadas antes de uma data escolhida. Suporta escopo global (todos os labels) ou por label específico via dropdown. Exibe preview detalhado por label antes de executar, mostrando quantas versões cada backup perderá. A versão `done` mais recente de cada label é **sempre preservada** — mesmo que seja anterior à data de corte. Versões em status `running` nunca são removidas. Dois novos endpoints: `GET /maintenance/cleanup-by-date/preview` (preview sem efeito colateral) e `POST /maintenance/cleanup-by-date` (execução). Prompt interativo de API Key no cliente Python: ao receber erro 401 (chave ausente ou inválida), o cliente solicita a chave via terminal (`getpass`) e retenta automaticamente a operação sem necessidade de reiniciar o comando.
>
> **v4.8.0** — `restore --exclude`: o cliente passou a aceitar `--exclude` no comando `restore`, com o mesmo comportamento do `backup` — filtra arquivos cujo caminho relativo contenha o componente de diretório especificado. Client e server agora compartilham o mesmo número de versão.
>
> **v4.7.0** — performance e observabilidade: N+1 queries eliminadas em `cleanup_orphans` e `encrypt_existing` (substituídas por `.in_()` batch); replicação paralela entre volumes via `ThreadPoolExecutor` em `storage.py`; novos índices compostos em `backup_versions` e `cloud_backup_jobs`; endpoint `GET /backups/disk-summary` permite ao dashboard buscar espaço de todos os discos em uma chamada em vez de N paralelas; debounce de 250 ms no filtro do Explorer evita queries redundantes. Cobertura de logs: todos os uploads agora logam `[upload] label/version ← path — modo sha256… (MB)` com contexto de label e versão; criação e finalização de versões logam `[versao] label/key criada` e `[versao] label/key → status` (inclusive erros/incomplete antes silenciosos); jobs cloud com ≥ 10 arquivos logam progresso a cada ~25% em `[cloud-runner] [i/total] path`.
>
> **v4.6.0** — página de atividade em tempo real (`/activity`): nova interface com polling adaptativo (3 s quando algo está em execução, 10 s em idle) que consolida em uma só tela o estado atual do servidor. Exibe cards animados de backups locais e jobs cloud em execução, barra de armazenamento segmentada (usado / liberável / livre) com mini-cards por volume, e tabelas de atividade das últimas 24 h (versões de backup finalizadas e jobs cloud recentes). Endpoint `GET /api/activity` no backend retorna tudo em uma chamada agregada — versões rodando com contagem de arquivos e bytes acumulados, jobs cloud ativos, storage total e por disco, e histórico recente. Link "◎ Atividade" adicionado à navegação de todas as páginas existentes.
>
> **v4.5.1** — pipeline producer-consumer no cloud backup: download e processamento de arquivos agora ocorrem em paralelo via `asyncio.Queue`. O producer baixa arquivos do cloud enquanto o consumer simultaneamente realiza deduplicação, armazenamento, criptografia e replicação. `crypto.encrypt_stream` (CPU-bound) movida para `run_in_executor`, liberando o event loop durante a criptografia. Fila limitada a 4 itens para controle de backpressure — evita acúmulo excessivo de arquivos temporários em disco.
>
> **v4.5** — digest diário via Telegram: resumo automático das atividades do dia (backups realizados, novos arquivos armazenados e jobs cloud) enviado via Telegram Bot API. A geração do texto usa Claude Haiku se `ANTHROPIC_API_KEY` estiver configurada, com fallback para Ollama local e, por último, uma mensagem estruturada sem IA. O agendamento é integrado ao APScheduler já existente — sem dependência do cron do sistema. Configurável via `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `ANTHROPIC_API_KEY` (opcional), `OLLAMA_URL` (opcional) e `DIGEST_HOUR` (padrão `18` — hora local da máquina).
>
> **v4.2** — prioridade de escrita por ordem de declaração dos discos: `STORAGE_DIRS` agora define também a ordem de prioridade de escrita. O servidor usa o primeiro disco da lista que ainda tenha espaço livre acima do limiar configurável `STORAGE_FALLBACK_THRESHOLD_GB` (padrão 10 GB). Quando um disco esgota, o próximo da lista assume automaticamente — sem intervenção manual. Apenas quando todos os discos estão esgotados o servidor recorre ao de maior espaço livre. Útil para cenários com um disco de fallback grande compartilhado com o sistema (ex.: disco de 2 TB declarado por último). Corrigida duplicação silenciosa da função `_pick_volume()` em `main.py` que tornava o wrapper correto código morto.
>
> **v4.1** — verificação de integridade pós-escrita: após cada upload, o servidor relê o arquivo do disco e confronta o SHA-256, detectando corrupção silenciosa de I/O antes de registrar o conteúdo no banco. Em modo com criptografia, a verificação usa `decrypt_chunks()` que autentica os GCM tags por chunk. Na deduplicação (arquivo já existente), o conteúdo em disco é verificado antes de aceitar a referência — arquivos corrompidos desde o upload original são detectados e o backup falha com erro 500 em vez de referenciar dados inválidos.
>
> **v4.0** — cloud backup: o servidor conecta-se a contas **Google Drive** e **OneDrive** e baixa arquivos de pastas configuradas, armazenando-os localmente como versões NestVault (deduplicação, criptografia e replicação funcionam transparentemente). Suporte a múltiplas contas e múltiplas pastas por conta. Agendamento cron nativo no servidor via APScheduler — sem depender do cron do sistema. Autenticação OAuth2 implementada diretamente via `httpx`, sem SDKs de terceiros. Tokens armazenados criptografados no banco (Fernet + SHA-256 da API key). Novo módulo `cloud/` + `scheduler.py` + `storage.py` (helpers extraídos de `main.py`). Duas novas tabelas no banco: `cloud_credentials` e `cloud_backup_jobs`. Novos endpoints `/cloud/*` e seção "Cloud Backup" no dashboard.
>
> **v3.1** — criptografia em repouso com AES-256-GCM: ativada via `ENCRYPTION_ENABLED=true` + `ENCRYPTION_KEY` (Base64, 32 bytes). Opt-in — desabilitada por padrão para quem já usa LUKS/ZFS/FileVault. Arquivos existentes continuam legíveis; a migração é feita sob demanda via `encrypt-existing` (cliente) ou `POST /maintenance/encrypt-existing`. Download descriptografa em streaming, sem buffer completo em memória. Novo módulo `crypto.py` com chunked AES-256-GCM (1 MB/chunk, nonce único por chunk). Replicação já copia arquivos cifrados — nenhum dado trafega em claro entre volumes.
>
> **v3.0** — redundância de dados por replicação entre volumes: cada arquivo pode ser mantido em N cópias físicas em volumes distintos via `REPLICATION_FACTOR` (padrão `1` = comportamento anterior, sem replicação). Downloads fazem fallback automático para cópias sobreviventes. Quando um volume degraded se recupera, arquivos sub-replicados são restaurados em background. Compatível com RAID/ZFS físico — sem replicação por padrão.
>
> **v2.9** — hashing paralelo com `ProcessPoolExecutor`: a fase de cálculo de SHA-256 passou de `ThreadPoolExecutor` para `ProcessPoolExecutor`, contornando o GIL do Python e utilizando todos os núcleos da CPU. O número de processos de hash é independente dos workers de upload e padreia para `os.cpu_count()`. Novo argumento `--hash-workers` para controle manual. Ganho típico de 4–8× em máquinas com 8+ núcleos comparado à v2.8.
>
> **v2.8** — suporte a múltiplos discos no servidor: a variável `STORAGE_DIRS` aceita uma lista de pontos de montagem separados por vírgula (`/mnt/disk1,/mnt/disk2`). O servidor distribui automaticamente os uploads para o disco com mais espaço livre; leitura, download e restore continuam funcionando sem nenhuma mudança — `FileContent.stored_at` guarda o path absoluto. O endpoint `GET /storage/info` agrega total/livre/usado de todos os volumes. O auto-cleanup dispara se qualquer disco estiver abaixo de 5%. O cliente não precisa de nenhuma alteração.
>
> **v2.7** — informações de disco no dashboard: novo endpoint `GET /storage/info` expõe espaço livre/total do disco montado e espaço liberável ao apagar versões antigas. Dashboard exibe dois novos stat boxes com barra visual de uso e o total recuperável com um cleanup.
>
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
NestVault/
├── server/
│   ├── main.py              ← API FastAPI
│   ├── database.py          ← Modelos SQLite/SQLAlchemy
│   ├── storage.py           ← Helpers de storage compartilhados (v4.0)
│   ├── crypto.py            ← Criptografia AES-256-GCM (v3.1)
│   ├── auth.py              ← Autenticação via API key
│   ├── scheduler.py         ← APScheduler para jobs de cloud backup (v4.0)
│   ├── cloud/               ← Módulo de cloud backup (v4.0)
│   │   ├── base.py          ← Abstração CloudProvider
│   │   ├── gdrive.py        ← GoogleDriveProvider
│   │   ├── onedrive.py      ← OneDriveProvider
│   │   ├── runner.py        ← Lógica de execução de job (OAuth)
│   │   ├── router.py        ← Endpoints /cloud/*
│   │   ├── rclone_runner.py ← Lógica de execução via rclone (v7.0)
│   │   └── rclone_router.py ← Endpoints /rclone/* (v7.0)
│   ├── requirements.txt
│   └── static/
│       └── index.html       ← Dashboard web
├── client/
│   ├── nestvault.py         ← Cliente de backup/restore
│   └── requirements.txt
├── .gitignore
└── README.md
```

---

## 📱 Clientes disponíveis

| Cliente | Plataforma | Repositório |
|---------|------------|-------------|
| **nestvault.py** | Linux / macOS / Windows (CLI Python) | este repositório — `client/nestvault.py` |
| **NestVault para macOS** | macOS (app nativo SwiftUI) | [github.com/vcmilani/NestVault_Xcode](https://github.com/vcmilani/NestVault_Xcode) |

O servidor expõe uma API REST padrão — qualquer cliente que implemente o [contrato da API](#-endpoints-da-api) funciona sem modificações no servidor.

---

## ⚠️ Atualizando da v3.x para v4.0

A v4.0 adiciona duas novas tabelas ao banco: `cloud_credentials` e `cloud_backup_jobs`. O `init_db()` cria as tabelas automaticamente no startup — **sem downtime, sem intervenção manual**.

Nenhuma migração de dados existente é necessária. Para verificar:

```bash
sqlite3 /mnt/hd-externo/backup.db ".tables"
# Deve listar cloud_credentials e cloud_backup_jobs
```

Para usar o cloud backup, adicione as credenciais OAuth ao serviço (veja [Configuração Cloud](#configuração-cloud)):

```ini
# Google Drive (Google Cloud Console)
Environment="GDRIVE_CLIENT_ID=<client-id>"
Environment="GDRIVE_CLIENT_SECRET=<client-secret>"

# OneDrive (Azure Portal → App registrations — public client, sem secret)
Environment="ONEDRIVE_CLIENT_ID=<client-id>"

# URL pública do servidor (para redirect OAuth — padrão localhost:8000)
Environment="BASE_URL=http://192.168.1.100:8000"
```

Sem essas variáveis, o servidor continua funcionando normalmente — o módulo de cloud backup simplesmente não conseguirá autenticar.

---

## ⚠️ Atualizando da v3.0 para v3.1

A v3.1 adiciona a coluna `encrypted` à tabela `file_contents`. O `init_db()` executa o `ALTER TABLE` automaticamente no startup via `try/except` — sem downtime, sem intervenção manual.

**Verificar migração:**

```bash
sqlite3 /mnt/hd-externo/backup.db ".schema file_contents"
# Deve conter a coluna: encrypted INTEGER NOT NULL DEFAULT 0
```

**Para ativar a criptografia** (opcional — padrão é desabilitada):

```bash
# Gerar uma chave aleatória de 32 bytes
python3 -c "import os, base64; print(base64.b64encode(os.urandom(32)).decode())"
# Exemplo: dGhpcyBpcyBhIDMyLWJ5dGUga2V5IGZvciBleGFtcGxl

# Adicionar ao serviço systemd:
Environment="ENCRYPTION_ENABLED=true"
Environment="ENCRYPTION_KEY=<chave-gerada-acima>"
sudo systemctl daemon-reload && sudo systemctl restart backup-server
```

> **Guarde a chave em local seguro.** Se perdida, arquivos cifrados se tornam irrecuperáveis. Rotação de chave não está disponível na v3.1.

**Migrar arquivos existentes** (após ativar `ENCRYPTION_ENABLED=true`):

```bash
nestvault encrypt-existing --server http://192.168.1.100:8000
```

Arquivos existentes sem criptografia continuam legíveis enquanto a migração não roda — a flag `encrypted` no banco distingue os dois estados.

---

## ⚠️ Atualizando da v2.x para v3.0

A v3.0 adiciona a tabela `file_content_copies` ao banco. O `init_db()` cria a tabela automaticamente no startup. Um backfill automático migra os `FileContent` existentes para a nova tabela em background — sem downtime.

Nenhuma ação manual é necessária. Para verificar:

```bash
sqlite3 /mnt/hd-externo/backup.db ".tables"
# Deve listar file_content_copies

sqlite3 /mnt/hd-externo/backup.db "SELECT COUNT(*) FROM file_content_copies;"
# Deve retornar o mesmo número de linhas de file_contents
```

Para ativar a replicação após migrar:

```bash
# Editar o serviço systemd e adicionar:
Environment="REPLICATION_FACTOR=2"
sudo systemctl daemon-reload && sudo systemctl restart backup-server
```

Novos uploads serão replicados. Conteúdos existentes **não** são re-replicados automaticamente retroativamente — apenas quando sofrem novo upload ou quando um volume degraded se recupera.

### Adicionando um disco novo ao cluster

Se você adicionar um novo ponto de montagem ao `STORAGE_DIRS`, o servidor o reconhece como volume saudável imediatamente — mas **não re-replica os arquivos existentes para ele**. Apenas novos uploads passarão a usar o disco novo.

Para forçar a re-replicação dos conteúdos existentes, será necessário um endpoint de manutenção (planejado para versão futura). Por enquanto, a alternativa é aguardar que os arquivos sejam naturalmente re-enviados pelo cliente.

### Trocando um disco defeituoso

O servidor identifica volumes **exclusivamente pelo caminho de montagem** — não há rastreamento de UUID ou número de série. Isso tem uma consequência importante:

**✅ Mesmo ponto de montagem — re-replicação automática:**
```
disco /mnt/disk2 falha → servidor marca /mnt/disk2 como degraded
usuário troca o disco físico, formata e remonta em /mnt/disk2
→ _volume_health_monitor detecta que /mnt/disk2 voltou a responder
→ re-replicação automática em background: arquivos sub-replicados são copiados para o disco novo
```

**⚠️ Caminho diferente — sem re-replicação automática:**
```
disco /mnt/disk2 falha → degraded
usuário monta o disco novo em /mnt/disk3 e adiciona ao STORAGE_DIRS
→ servidor vê /mnt/disk3 como volume novo e saudável
→ nenhuma re-replicação: arquivos existentes continuam com cópia única em /mnt/disk1
→ novos uploads passam a usar /mnt/disk3 normalmente
```

**Recomendação:** ao trocar um disco defeituoso, sempre monte o substituto no **mesmo caminho** do disco antigo. Isso garante que a re-replicação ocorra automaticamente sem intervenção manual.

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

## ⚙️ Servidor

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
export DB_PATH="/mnt/hd-externo/backup.db"

# Um disco (compatibilidade legada)
export STORAGE_DIR="/mnt/hd-externo/backups"

# Dois ou mais discos — use STORAGE_DIRS (tem precedência sobre STORAGE_DIR)
export STORAGE_DIRS="/mnt/disk1/backups,/mnt/disk2/backups"

# Replicação entre volumes (opcional — padrão 1 = sem replicação)
# 1 = sem replicação (compatível com RAID físico ou disco único)
# 2 = espelhar para 2 volumes
# 0 = espelhar para todos os volumes saudáveis
export REPLICATION_FACTOR=2

# Criptografia em repouso AES-256-GCM (opcional — padrão desabilitada)
# Omitir se o disco já tem criptografia (LUKS, ZFS encryption, macOS FileVault)
export ENCRYPTION_ENABLED=true
export ENCRYPTION_KEY="$(python3 -c 'import os,base64; print(base64.b64encode(os.urandom(32)).decode())')"

# Cloud backup — Google Drive (Google Cloud Console → APIs & Services → Credentials)
export GDRIVE_CLIENT_ID="..."
export GDRIVE_CLIENT_SECRET="..."

# Cloud backup — OneDrive (portal.azure.com → App registrations — public client, sem secret)
export ONEDRIVE_CLIENT_ID="..."

# URL base do servidor para OAuth callback (padrão: http://localhost:8000)
# Deve ser acessível pelo browser do usuário ao autenticar
export BASE_URL="http://192.168.1.100:8000"

# Threshold mínimo de espaço livre (GB) antes de usar o próximo disco da lista (padrão: 10)
export STORAGE_FALLBACK_THRESHOLD_GB=10

# SSD cache tier (opcional — padrão desabilitado)
# Uploads são gravados no SSD primeiro; movidos para HDD em background
export SSD_CACHE_ENABLED=true
export SSD_CACHE_DIR="/tmp/nestvault_ssd_cache"   # diretório no SSD
export SSD_CACHE_MAX_GB=20                         # limite de staging no SSD (padrão: 20 GB)

# Daily digest via Telegram (opcional — omitir desabilita o envio)
export TELEGRAM_BOT_TOKEN="123456789:ABCdef..."   # token gerado pelo @BotFather
export TELEGRAM_CHAT_ID="987654321"               # seu chat_id (veja abaixo como obter)

# Geração do resumo por IA (opcional — sem nenhuma das duas usa texto estruturado)
export ANTHROPIC_API_KEY="sk-ant-..."             # Claude Haiku (console.anthropic.com)
export OLLAMA_URL="http://localhost:11434"         # fallback local se não houver API key
export OLLAMA_MODEL="llama3"                       # modelo Ollama a usar

# Horário de envio do digest em horário local (padrão: 18h)
export DIGEST_HOUR=18

# rclone backup (opcional — omitir usa ~/.config/rclone/rclone.conf)
export RCLONE_CONFIG="/etc/rclone/rclone.conf"
```

#### Configuração Cloud (OAuth nativo)

| Variável | Obrigatório | Descrição |
|---|:-:|---|
| `GDRIVE_CLIENT_ID` | | Client ID do app OAuth2 no Google Cloud Console |
| `GDRIVE_CLIENT_SECRET` | | Client Secret correspondente |
| `ONEDRIVE_CLIENT_ID` | | Application (client) ID no Azure Portal |
| `BASE_URL` | | URL pública do servidor para callback OAuth (padrão: `http://localhost:8000`) |

Sem essas variáveis o servidor funciona normalmente — apenas o cloud backup OAuth ficará indisponível.

#### Configuração rclone

| Variável | Obrigatório | Padrão | Descrição |
|---|:-:|---|---|
| `RCLONE_CONFIG` | | `~/.config/rclone/rclone.conf` | Path do arquivo de configuração do rclone. Útil quando o servidor roda como systemd service com usuário diferente do que configurou o rclone |

Sem `RCLONE_CONFIG` o NestVault usa o config padrão do usuário que executa o processo. O rclone precisa estar instalado e acessível no `PATH`.

#### Configuração Daily Digest

| Variável | Obrigatório | Padrão | Descrição |
|---|:-:|---|---|
| `TELEGRAM_BOT_TOKEN` | ✓ | — | Token do bot gerado pelo @BotFather no Telegram |
| `TELEGRAM_CHAT_ID` | ✓ | — | ID do chat que receberá o digest (veja como obter abaixo) |
| `ANTHROPIC_API_KEY` | | — | Usa Claude Haiku para gerar o resumo ([console.anthropic.com](https://console.anthropic.com)) |
| `OLLAMA_URL` | | `http://localhost:11434` | Fallback local quando não há `ANTHROPIC_API_KEY` |
| `OLLAMA_MODEL` | | `llama3` | Modelo Ollama a usar |
| `DIGEST_HOUR` | | `18` | Hora de envio (horário local da máquina) |

**Como obter o `TELEGRAM_CHAT_ID`:** crie o bot com @BotFather, mande qualquer mensagem para ele e acesse `https://api.telegram.org/bot<TOKEN>/getUpdates` no browser — o campo `chat.id` no JSON é o valor a usar.

Sem `TELEGRAM_BOT_TOKEN` e `TELEGRAM_CHAT_ID` o digest é gerado internamente mas não enviado. Sem variável de IA o servidor envia um resumo estruturado com os dados brutos do banco.

#### Configuração SSD Cache

| Variável | Obrigatório | Padrão | Descrição |
|---|:-:|---|---|
| `SSD_CACHE_ENABLED` | | `false` | Habilita o cache tier no SSD |
| `SSD_CACHE_DIR` | ✓ se enabled | — | Caminho de um diretório **no SSD** para staging de uploads |
| `SSD_CACHE_MAX_GB` | | `20.0` | Limite máximo de uso do SSD pela fila pendente (GB) |

Quando habilitado, uploads são escritos no SSD e o servidor responde ao cliente imediatamente; a movimentação para o HDD ocorre em background. Se o SSD atingir o limite ou tiver menos de 2 GB livres, o upload recai silenciosamente para o HDD. Moves pendentes sobrevivem a reinicializações (persistidos em `ssd_cache_pending_moves` no banco).

> **Não use MicroSD como `SSD_CACHE_DIR`.** Write sequencial de cartões rápidos (~130 MB/s) é marginalmente melhor que HDD, mas sofrem throttling térmico sob carga e têm endurance muito inferior a um SSD real. O benefício é nulo e o desgaste é alto.

`STORAGE_DIRS` e `STORAGE_DIR` são mutuamente compatíveis: se apenas `STORAGE_DIR` estiver definido, o servidor opera normalmente com um único volume. Se `STORAGE_DIRS` estiver definido, ele tem precedência e pode listar quantos pontos de montagem forem necessários.

### 3. Iniciar o servidor

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

### 4. Rodar como serviço (systemd)

Crie `/etc/systemd/system/backup-server.service`:

```ini
[Unit]
Description=NestVault
After=network.target

[Service]
User=pi
WorkingDirectory=/home/pi/backup_system/server
Environment="BACKUP_API_KEY=sua-chave-aqui"
Environment="STORAGE_DIRS=/mnt/disk1/backups,/mnt/disk2/backups"
Environment="DB_PATH=/mnt/disk1/backup.db"
Environment="REPLICATION_FACTOR=2"
# Criptografia em repouso — omitir se o disco já tem criptografia própria
# Environment="ENCRYPTION_ENABLED=true"
# Environment="ENCRYPTION_KEY=<chave-base64-32-bytes>"
# Cloud backup — omitir se não for usar Google Drive / OneDrive
# Environment="GDRIVE_CLIENT_ID=<id>"
# Environment="GDRIVE_CLIENT_SECRET=<secret>"
# Environment="ONEDRIVE_CLIENT_ID=<id>"
# Environment="BASE_URL=http://192.168.1.100:8000"
# SSD cache — omitir se não houver SSD interno ou ganho não for necessário
# Environment="SSD_CACHE_ENABLED=true"
# Environment="SSD_CACHE_DIR=/tmp/nestvault_ssd_cache"
# Environment="SSD_CACHE_MAX_GB=20"
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
>
> **v5.0 — prompt interativo:** se `BACKUP_API_KEY` não estiver definida ou a chave estiver errada, o cliente detecta o erro 401 e solicita a chave via terminal antes de retentar automaticamente. A operação original é executada sem precisar reiniciar o comando.

---

## 🚀 Comandos

O cliente possui dez subcomandos: `backup`, `backups`, `versions`, `restore`, `cleanup`, `delete-label`, `cleanup-orphans`, `rereplicate`, `reconcile-replication` e `encrypt-existing`.

---

### backup

Envia arquivos para o servidor criando uma **nova versão** a cada execução. A versão é identificada automaticamente pela data e hora de início (`2026-04-25T10:42:31`).

Arquivos cujo conteúdo já existe no storage (mesmo sha256) são apenas **registrados** na nova versão — zero bytes trafegam na rede. Arquivos sem alteração desde a última versão são **ignorados**.

```bash
# Backup simples — cria nova versão automaticamente
nestvault backup ~/documentos \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000

# Com prefixo de path no servidor
nestvault backup ~/documentos \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000 \
  --prefix /home/joao/documentos

# Ignorar subpastas
nestvault backup ~/projeto \
  --label "projeto-alpha" \
  --server http://192.168.1.100:8000 \
  --exclude node_modules .git __pycache__ .venv dist build

# Aumentar paralelismo de upload (padrão: 4 workers)
nestvault backup ~/documentos \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000 \
  --workers 8

# Controlar processos de hashing (padrão: os.cpu_count())
nestvault backup ~/documentos \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000 \
  --hash-workers 16

# Ajustar tamanho do lote de verificação (padrão: 100 arquivos/request)
nestvault backup ~/documentos \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000 \
  --batch-size 200

# Verificar sem enviar
nestvault backup ~/documentos \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000 \
  --dry-run

# Modo acumulativo — acumula todos os arquivos já vistos entre execuções (ideal para galerias de fotos)
nestvault backup /Volumes/HD/Fotos \
  --label "fotos" \
  --server http://192.168.1.100:8000 \
  --accumulate
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
| `--hash-workers` | | Processos paralelos para cálculo de SHA-256 (padrão: `os.cpu_count()`) |
| `--batch-size` | | Arquivos por request no `/check/batch` (padrão: `100`) |
| `--accumulate` | | Modo acumulativo: herda arquivos ausentes da versão anterior — veja [Modo Acumulativo](#modo-acumulativo) |
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

Com `--accumulate`, aparece também a linha `Herdados`:

```
=======================================================
  Backup      : [fotos]
  Versao      : 2026-05-10T14:00:00
  Verificados : 120
  Enviados    : 120
  Registrados : 0
  Cacheados   : 0
  Ignorados   : 0
  Erros       : 0
  Herdados    : 100  ← arquivos ausentes herdados da versão anterior
=======================================================
```

**Recomendação de workers:**

`--workers` controla uploads paralelos (bound pela rede); `--hash-workers` controla processos de SHA-256 (bound pela CPU/disco). Os dois são independentes.

| Cenário | `--workers` | `--hash-workers` |
|---------|:-----------:|:----------------:|
| Pi com cartão SD | 2 | 2 |
| Pi com HD externo USB | 4–6 | `cpu_count()` (padrão) |
| Pi com SSD | 6–8 | `cpu_count()` (padrão) |
| Muitos arquivos pequenos (200k+) | 4 | `cpu_count() * 2` |
| Arquivos grandes (>100 MB) | 2–3 | `cpu_count()` (padrão) |
| NFS / rede lenta | 2 | 4 |

---

### Modo Acumulativo

O modo padrão do NestVault é **snapshot**: cada versão representa exatamente o que estava no diretório naquele momento. Se um arquivo for deletado do cliente e um backup posterior rodar, ele desaparece do servidor ao se executar um cleanup.

O modo `--accumulate` resolve o caso de acervos que **nunca estão completos no cliente ao mesmo tempo** — o exemplo típico é uma galeria de fotos espalhada em HDs externos: no mês 1 você conecta o HD com fotos de janeiro, no mês 2 conecta outro HD com fotos de fevereiro, e nunca os dois estão disponíveis simultaneamente.

**Como funciona:**

Ao finalizar um backup com `--accumulate`, o cliente chama o endpoint `/absorb` do servidor, que copia para a versão atual todos os `VersionFile`s da versão anterior que **não existem** na versão atual (pelo `original_path`). O resultado é que a versão mais recente sempre acumula todos os arquivos já vistos em backups anteriores.

```
Backup 1 — HD com fotos de janeiro (100 fotos):
  versão 2026-03-01  →  100 fotos

Backup 2 — HD com fotos de fevereiro (120 fotos):
  upload: 120 fotos novas
  absorb: herda 100 fotos de janeiro da versão anterior
  versão 2026-05-10  →  220 fotos no total

Backup 3 — HD com fotos de março (80 fotos):
  upload: 80 fotos novas
  absorb: herda 220 fotos das versões anteriores
  versão 2026-07-15  →  300 fotos no total
```

**Regras do absorb:**

| Situação | Comportamento |
|---|---|
| Arquivo presente no cliente | Upload normal; não é afetado pelo absorb |
| Arquivo ausente do cliente (deletado) | Herdado da versão anterior — preservado no servidor |
| Arquivo modificado (mesmo path, novo conteúdo) | Versão nova tem o novo conteúdo; absorb ignora (path já existe) |

**Deduplicação:** o absorb é uma operação puramente de banco de dados — copia apenas referências (`VersionFile`), sem mover ou duplicar arquivos físicos. O storage crescerá apenas com conteúdos genuinamente novos.

**Cleanup:** versões antigas podem ser removidas normalmente com `cleanup --keep 1`. Como a versão mais recente já absorbeu todos os arquivos únicos das versões anteriores, nenhum conteúdo será perdido ao deletá-las.

> **Atenção:** com `--accumulate`, arquivos deletados do cliente são **intencionalmente preservados** no servidor. Se precisar remover um arquivo do acervo acumulado, a forma correta é deletar a versão manualmente pelo dashboard ou pela API.

---

### backups

Lista todos os backups registrados no servidor.

```bash
nestvault backups --server http://192.168.1.100:8000

# Filtrar por cliente
nestvault backups --server http://192.168.1.100:8000 --client "notebook-joao"
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
nestvault versions \
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
nestvault restore /tmp/restore \
  --label "notebook-joao" \
  --version "2026-04-25T10:42:31" \
  --server http://192.168.1.100:8000

# Restaurar apenas um subdiretório
nestvault restore /tmp/restore \
  --label "notebook-joao" \
  --version "2026-04-25T10:42:31" \
  --server http://192.168.1.100:8000 \
  --prefix /home/joao/documentos

# Ver o que seria restaurado sem baixar
nestvault restore /tmp/restore \
  --label "notebook-joao" \
  --version "2026-04-25T10:42:31" \
  --dry-run

# Sobrescrever arquivos existentes
nestvault restore /tmp/restore \
  --label "notebook-joao" \
  --version "2026-04-25T10:42:31" \
  --overwrite

# Restaurar ignorando diretórios específicos
nestvault restore /tmp/restore \
  --label "notebook-joao" \
  --version "2026-04-25T10:42:31" \
  --exclude cache node_modules .venv
```

| Opção | Obrigatório | Descrição |
|-------|:-----------:|-----------|
| `--label` | ✅ | Label do backup |
| `--version` | ✅ | Chave da versão (obtida via `versions`) |
| `--server` | | URL do servidor |
| `--prefix` | | Restaurar apenas arquivos com esse prefixo |
| `--exclude` | | Nomes de diretório a ignorar — aceita múltiplos valores |
| `--overwrite` | | Sobrescreve arquivos existentes |
| `--dry-run` | | Apenas lista, não baixa |

A integridade de cada arquivo é validada após o download pelo SHA-256.

---

### cleanup

Remove versões antigas de um ou todos os backups, mantendo apenas as `N` mais recentes. Arquivos físicos órfãos (não referenciados por nenhuma versão remanescente) são apagados do storage automaticamente.

```bash
# Limpar um label específico
nestvault cleanup \
  --label "notebook-joao" \
  --keep 5 \
  --server http://192.168.1.100:8000

# Limpar TODOS os labels de uma vez
nestvault cleanup \
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
nestvault delete-label \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000

# Sem confirmação — para uso em scripts
nestvault delete-label \
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
nestvault cleanup-orphans \
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

### rereplicate

Força a re-replicação de todos os arquivos que possuem menos cópias físicas do que o `REPLICATION_FACTOR` configurado no servidor. Use após:

- Adicionar um disco novo ao cluster (arquivos existentes não são replicados automaticamente)
- Recuperar um disco que ficou `degraded` por um longo período
- Aumentar o valor de `REPLICATION_FACTOR`

```bash
nestvault rereplicate \
  --server http://192.168.1.100:8000
```

| Opção | Descrição |
|-------|-----------|
| `--server` | URL do servidor |

Exemplo de saída:

```
Iniciando re-replicacao de conteudos sub-replicados...
Re-replicacao concluida: 312 arquivo(s) replicado(s), 0 pulado(s) (fonte inacessivel) — alvo: 2 copia(s)
```

Se `skipped > 0`, significa que alguns arquivos têm a única cópia em um volume `degraded` — eles não puderam ser replicados. Recupere o disco e execute o comando novamente.

---

### reconcile-replication

Reconcilia o acervo inteiro com o `REPLICATION_FACTOR` atual do servidor, resolvendo **ambas** as direções:

- **Sub-replicados** (fator aumentou ou disco foi adicionado): cria cópias faltantes
- **Sobre-replicados** (fator diminuiu): remove cópias excedentes do disco e do banco

```bash
nestvault reconcile-replication \
  --server http://192.168.1.100:8000
```

| Opção | Descrição |
|-------|-----------|
| `--server` | URL do servidor |

Exemplo de saída:

```
Reconciliacao concluida: 40 replicado(s), 80 copia(s) excedente(s) removida(s), 0 pulado(s) — alvo: 1 copia(s)
```

Se `skipped > 0`, algum arquivo tem a única cópia em volume `degraded`. Recupere o disco e execute novamente.

---

### encrypt-existing

Cifra todos os arquivos físicos que ainda não foram criptografados. Use após ativar `ENCRYPTION_ENABLED=true` no servidor para migrar um acervo existente. Requer que o servidor esteja rodando com `ENCRYPTION_ENABLED=true`.

```bash
nestvault encrypt-existing \
  --server http://192.168.1.100:8000
```

| Opção | Descrição |
|-------|-----------|
| `--server` | URL do servidor |

Exemplo de saída:

```
Iniciando criptografia de arquivos existentes...
  Arquivos criptografados : 1842
  Bytes processados       : 38.6 GB
  Já criptografados       : 0 (pulados)
  Tempo                   : 312.4s
```

**Notas:**

- Arquivos em volumes `degraded` são pulados e contados em "pulados" — rode novamente após recuperar o disco.
- A operação é **idempotente**: arquivos já cifrados são ignorados automaticamente.
- Em caso de interrupção, os arquivos já processados permanecem cifrados — reprocessar os restantes é seguro.
- Novos uploads feitos com `ENCRYPTION_ENABLED=true` já chegam cifrados; o `encrypt-existing` trata apenas o acervo pré-v3.1.

> Requer NestVault v3.1+ no servidor. Em servidores mais antigos, retorna `404` com mensagem de erro clara.

---

### Limpeza automática por espaço em disco

O servidor verifica automaticamente o espaço livre **ao finalizar cada backup** (status → `done`). Se o espaço livre no disco estiver abaixo de **5%**, versões antigas são apagadas até que o espaço seja normalizado. Essa verificação ocorre **em background** — o cliente recebe a confirmação do backup imediatamente, sem esperar o scan de disco.

Da mesma forma, ao excluir um label (`DELETE /backups/{label}`) ou uma versão (`DELETE /backups/{label}/versions/{key}`), a remoção dos registros no banco é imediata, mas a limpeza dos arquivos físicos órfãos ocorre em background.

**Comportamento:**

- Com múltiplos discos (`STORAGE_DIRS`), verifica o **menor** percentual livre entre todos os volumes — o cleanup dispara se **qualquer** disco estiver abaixo de 5%
- Com disco único (`STORAGE_DIR`), verifica o espaço do filesystem onde o storage está montado
- Apaga as versões mais antigas primeiro, distribuindo entre todos os labels
- **Nunca apaga a versão mais recente** de cada label — cada label sempre terá ao menos 1 versão
- Após cada deleção, reavalia o espaço e para assim que atingir 5%
- Registra no terminal do servidor cada versão apagada e o espaço livre atualizado

**Logs de exemplo (com dois discos):**

```
[auto-cleanup] Espaço livre mínimo: 3.2% — abaixo de 5%, iniciando limpeza...
[auto-cleanup] Removida notebook-joao/2026-03-01T02:00:00 — 4 arquivo(s) do storage — livre mín: 3.8%
[auto-cleanup] Removida servidor-web/2026-03-05T03:00:00 — 2 arquivo(s) do storage — livre mín: 4.3%
[auto-cleanup] Removida notebook-joao/2026-03-08T02:00:00 — 7 arquivo(s) do storage — livre mín: 5.1%
[auto-cleanup] Espaço normalizado (5.1%), encerrando.
```

> Essa limpeza é um mecanismo de segurança para evitar disco cheio. Para controle previsível de retenção, use o comando [`cleanup`](#cleanup) agendado via cron.

---

### Agendar com cron

```cron
# Backup todo dia às 02:00
0 2 * * * BACKUP_API_KEY=sua-chave \
  /home/usuario/client/.venv/bin/python \
  /home/usuario/client/nestvault.py backup ~/docs \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000 \
  --exclude node_modules .git \
  --workers 4 \
  >> /var/log/backup.log 2>&1

# Cleanup semanal — manter 10 versões em todos os labels
0 3 * * 0 BACKUP_API_KEY=sua-chave \
  /home/usuario/client/.venv/bin/python \
  /home/usuario/client/nestvault.py cleanup \
  --all --keep 10 \
  --server http://192.168.1.100:8000 \
  >> /var/log/backup-cleanup.log 2>&1
```

---

## 🧪 Testes

A suíte cobre helpers internos (unitários) e todos os endpoints da API (integração), usando SQLite in-memory e diretórios temporários — sem depender de nenhum serviço externo.

### Instalar dependências de desenvolvimento

```bash
pip install pytest pytest-asyncio httpx
```

### Executar todos os testes

```bash
pytest tests/ -v
```

### Executar um módulo específico

```bash
pytest tests/test_upload.py -v
pytest tests/test_cleanup.py -v
```

### Com relatório de cobertura (opcional)

```bash
pip install pytest-cov
pytest tests/ --cov=server --cov-report=term-missing
```

### O que cada módulo testa

| Arquivo | O que cobre |
|---|---|
| `test_helpers.py` | `_pick_volume`, `_content_path`, `_min_disk_free_percent` (mocks de disco); `_expected_stored_size` (fórmula AES-GCM para plain/cifrado, matches tamanho real); `_copy_with_sha256` (hash-during-copy) |
| `test_backups.py` | CRUD de backups e versões — criação, listagem, finalização, deleção |
| `test_check.py` | `/check` e `/check/batch` — 3 branches: novo, conteúdo existente, já registrado |
| `test_upload.py` | `/upload` — upload novo, deduplicação (mesmo sha256), modo register-only |
| `test_files.py` | `GET /files` e download — listagem ordenada, 404 e 410 (arquivo físico ausente) |
| `test_compare.py` | `GET /compare` — added, deleted, modified, unchanged, size_delta |
| `test_cleanup.py` | `/cleanup`, `/maintenance/cleanup-orphans` — remoção de versões e arquivos órfãos |
| `test_storage.py` | `GET /storage/info` — volume único e agregação de dois volumes; `reclaimable_bytes` |
| `test_auth.py` | Rejeição sem chave, rejeição com chave errada, acesso liberado com chave válida |
| `test_replication.py` | `/maintenance/rereplicate` e `/maintenance/reconcile-replication` — sub-replicação e sobre-replicação |
| `test_disks.py` | `GET /storage/disks` — status de volumes, contagem de cópias físicas por volume |

---

## ☁️ Cloud Backup

Existem duas formas independentes de configurar cloud backup. Ambas podem coexistir no mesmo servidor.

| | OAuth nativo | Via rclone |
|---|---|---|
| **Provedores** | Google Drive, OneDrive | 70+ (Drive, OneDrive, S3, B2, Dropbox…) |
| **Setup** | Registrar app no Google Cloud / Azure | Instalar rclone e rodar `rclone config` |
| **Tokens** | Armazenados no banco (cifrados) | Gerenciados pelo rclone |
| **Endpoints** | `/cloud/*` | `/rclone/*` |

---

### Opção 1 — OAuth nativo (Google Drive / OneDrive)

Para ativar, registre um aplicativo OAuth2 em cada provedor:

**Google Drive:**
1. [Google Cloud Console](https://console.cloud.google.com/) → APIs & Services → Credentials → Create OAuth 2.0 Client ID
2. Tipo: **Web application**
3. Authorized redirect URI: `http://<ip-do-servidor>:8000/cloud/callback/gdrive`
4. Copie Client ID e Client Secret → env vars `GDRIVE_CLIENT_ID` / `GDRIVE_CLIENT_SECRET`

**OneDrive:**
1. [Azure Portal](https://portal.azure.com/) → App registrations → New registration
2. Authentication → Add a platform → **Mobile and desktop applications** (public client — requerido pelo PKCE)
3. Redirect URI: `http://<ip-do-servidor>:8000/cloud/callback/onedrive`
4. Marque "Allow public client flows" → Save
5. Copie o Application (client) ID → env var `ONEDRIVE_CLIENT_ID` (nenhum client secret é necessário)

**Fluxo de uso:**

1. Abra o dashboard (`http://<ip>:8000/`) → seção **Cloud Backup**
2. Clique em **+ Google Drive** ou **+ OneDrive** — o browser redireciona para o OAuth do provedor
3. Após autorizar, a conta aparece na lista — pode conectar múltiplas contas de ambos os provedores
4. Clique em **+ Job** na conta — selecione a pasta de origem e o label de destino; configure o cron (opcional)
5. Use **▶ Run** para executar manualmente ou aguarde o próximo disparo agendado

---

### Opção 2 — Via rclone *(v7.0)*

Não requer registro de app. O rclone gerencia toda a autenticação OAuth localmente; o NestVault apenas chama o binário para listar e baixar arquivos.

#### Pré-requisito: instalar o rclone

```bash
# Linux / Raspberry Pi
sudo apt install rclone

# macOS
brew install rclone

# Ou via script oficial (todas as plataformas)
curl https://rclone.org/install.sh | sudo bash
```

#### Configurar Google Drive no rclone

Execute no servidor (requer browser ou acesso ao URL exibido):

```bash
rclone config
```

Siga o assistente interativo:

```
n) New remote
name> gdrive                    # nome que você escolhe — usado no NestVault

Storage> drive                  # ou digite o número da opção "Google Drive"

# Client ID e Secret: deixe em branco para usar o app público do rclone
# (funciona para uso pessoal; para volume corporativo, crie seu próprio app)
client_id>
client_secret>

scope> 1                        # drive (acesso completo) — ou 2 para read-only

# Demais opções: Enter para aceitar os padrões

# Autenticação:
# - Se você está no servidor com browser: o rclone abre o browser automaticamente
# - Se é um servidor remoto sem browser (Raspberry Pi, VPS):
Use web browser to authenticate rclone? (y/n) n
# O rclone exibe um URL — abra no seu computador, autorize, copie o código e cole aqui
```

Após concluir:

```bash
rclone lsd gdrive:              # lista pastas na raiz — confirma que funciona
rclone ls gdrive:Fotos/2024     # lista arquivos em uma pasta
```

#### Configurar OneDrive no rclone

```bash
rclone config
```

```
n) New remote
name> onedrive                  # nome que você escolhe

Storage> onedrive               # ou "Microsoft OneDrive"

# Client ID e Secret: deixe em branco para usar o app público do rclone
client_id>
client_secret>

# Autenticação — mesmo fluxo do Google Drive acima
# Para servidor sem browser: copie o URL, autorize no PC, cole o código

# Tipo de conta:
Your choice> 1                  # OneDrive Personal (ou 2 para Business/SharePoint)

# O rclone detecta os drives disponíveis e pede para confirmar:
Found 1 drives, selecting the first one...
```

Teste:

```bash
rclone lsd onedrive:            # lista pastas na raiz
rclone ls onedrive:Documentos   # lista arquivos em uma pasta
```

#### Configurar Dropbox, S3, Backblaze B2 e outros

O processo é o mesmo: `rclone config` → escolher o provedor → seguir o assistente. Consulte a [documentação do rclone](https://rclone.org/docs/) para cada provedor. Após configurado, o nome do remote funciona igual no NestVault.

#### Verificar remotes configurados

```bash
rclone listremotes
# gdrive:
# onedrive:
# mys3:
```

O endpoint `GET /rclone/remotes` retorna a mesma lista via API.

#### Criar um job de backup rclone

Via API:

```bash
curl -X POST http://<ip>:8000/rclone/jobs \
  -H "X-API-Key: <sua-chave>" \
  -H "Content-Type: application/json" \
  -d '{
    "remote_name": "gdrive",
    "remote_path": "Fotos/2024",
    "display_name": "Google Drive – Fotos 2024",
    "target_label": "fotos-gdrive",
    "cron_expr": "0 3 * * *",
    "enabled": true
  }'
```

Campos:

| Campo | Descrição |
|---|---|
| `remote_name` | Nome do remote configurado no rclone (ex: `gdrive`, `onedrive`) |
| `remote_path` | Caminho dentro do remote a ser copiado (ex: `Fotos/2024`). Deixe vazio para a raiz |
| `display_name` | Nome amigável exibido nos logs |
| `target_label` | Label NestVault de destino (criado automaticamente se não existir) |
| `cron_expr` | Expressão cron com 5 campos (veja [Cron](#cron)). Omita para execução manual |
| `enabled` | `true` ativa o agendamento |

Executar manualmente:

```bash
curl -X POST http://<ip>:8000/rclone/jobs/1/run \
  -H "X-API-Key: <sua-chave>"
```

Verificar status:

```bash
curl http://<ip>:8000/rclone/jobs/1/status \
  -H "X-API-Key: <sua-chave>"
```

#### Servidor sem browser (Raspberry Pi / VPS)

O rclone tem suporte nativo para autenticação em máquinas headless. Na etapa `Use web browser to authenticate rclone?`, responda `n`. O rclone exibe uma URL — abra no seu computador pessoal, autorize, e o rclone no servidor aguarda o código ser colado no terminal.

Alternativamente, configure o rclone no seu computador pessoal e copie o arquivo de configuração para o servidor:

```bash
# No computador pessoal
rclone config        # configura gdrive, onedrive, etc.
cat ~/.config/rclone/rclone.conf

# Copie o conteúdo para o servidor
scp ~/.config/rclone/rclone.conf pi@192.168.1.100:~/.config/rclone/rclone.conf
```

O NestVault usa o `rclone.conf` padrão do usuário que roda o servidor. Para um path customizado, exporte `RCLONE_CONFIG=/path/to/rclone.conf` no ambiente do serviço.

---

### Funcionamento interno (ambas as opções)

- O servidor lista recursivamente a pasta configurada e baixa cada arquivo para storage local
- Arquivos idênticos (mesmo SHA-256) são detectados por deduplicação — nenhum byte extra no disco
- Criptografia e replicação funcionam normalmente — o backup cloud é tratado igual ao backup via cliente CLI
- Arquivos com `mtime` inalterado em relação à versão anterior são ignorados sem re-download — runs recorrentes em pastas estáticas são significativamente mais rápidos
- Erros por arquivo são tolerados — o job continua e registra o erro na última mensagem
- *(OAuth nativo)* Tokens são renovados automaticamente; refresh_tokens são armazenados criptografados no banco via Fernet
- *(rclone)* Tokens são gerenciados pelo rclone em `~/.config/rclone/rclone.conf` — NestVault não os armazena

### Cron

Cron usa **5 campos** no formato padrão: `minuto hora dia_mes mês dia_semana`.

| Expressão | Significado |
|---|---|
| `0 2 * * *` | Todo dia às 02:00 UTC |
| `0 */6 * * *` | A cada 6 horas |
| `30 1 * * 0` | Domingos à 01:30 UTC |
| `0 3 1 * *` | Dia 1 de cada mês às 03:00 UTC |

Deixar o campo vazio desabilita o agendamento (execução manual apenas).

---

## 🖥️ Dashboard Web

Acessível pelo browser, servido diretamente pelo FastAPI:

```
http://<ip-da-pi>:8000/
```

Na primeira visita com autenticação ativada, o browser pedirá a API Key — salva no `localStorage`. Para trocar, clique em **⌀ API Key** no header.

**O que o dashboard exibe:**

- **Stats globais** — total de backups, versões, arquivos, storage total
- **Disco livre** — espaço disponível no disco montado com barra visual de uso e percentual *(v2.7)*
- **Espaço liberável** — quanto seria recuperado apagando versões antigas (mantendo 1 por label) *(v2.7)*
- **Tabela de backups** — clique em um label para expandir as versões
- **Versões** — clique em uma versão para ver os arquivos
- **Comparação de versões** — selecione duas versões com as checkboxes e clique em ⇄ Comparar: veja arquivos adicionados, removidos, modificados e o delta de tamanho de cada um
- **Cloud Backup** *(v4.0)* — conecte contas Google Drive e OneDrive, gerencie jobs de backup agendados e execute manualmente
- **Manutenção** — página dedicada a operações administrativas de storage:
  - **Limpeza de Órfãos** — remove arquivos físicos sem referência em nenhuma versão ativa
  - **Re-replicar** — cria cópias faltantes para conteúdos com menos réplicas que `REPLICATION_FACTOR`
  - **Reconciliar Replicação** — remove cópias excedentes e preenche faltantes em uma só operação
  - **Cifrar Existentes** — cifra arquivos não criptografados (requer confirmar digitando `CIFRAR` — irreversível)
  - **Limpar Versões Antigas** — mantém apenas N versões mais recentes de um label escolhido
  - **Excluir Versões por Data** *(v5.0)* — exibe preview por label de quantas versões serão removidas antes de uma data; a versão `done` mais recente de cada label é sempre preservada
  - **Excluir Label Completo** — exclui um label e todas as suas versões (requer digitar o nome do label)
- **Discos** — página `/disks` com painel de volumes: espaço total/livre/usado, arquivos físicos por volume e status (ok/degraded)
- **Explorer de arquivos** — navegação e download de arquivos de uma versão específica via `/explorer`
- **Backups em tempo real** — indicador no cabeçalho com contagem de backups em andamento; polling automático a cada 3 s com botão ⏸ para pausar

---

## ⚡ Otimizações

### v7.0.0

| Componente | Mudança |
|---|---|
| **`cloud/rclone_runner.py`** | Novo runner paralelo ao OAuth: lista via `rclone lsjson --recursive`, baixa via `rclone cat` com SHA-256 calculado em single pass durante o stream — sem buffer completo em memória |
| **`cloud/rclone_runner.py` — skip por mtime** | Mesma lógica do runner OAuth: arquivos com `mtime` inalterado não são baixados; `prev_files` carregado da última versão `done` (+ versão `incomplete`/`failed` para resume) |
| **`cloud/rclone_runner.py` — producer-consumer** | Reutiliza `_process_file_sync` e `_register_version_file_sync` importados de `cloud/runner.py` — deduplicação, criptografia, replicação e registro no banco idênticos ao fluxo OAuth |
| **`cloud/rclone_runner.py` — subprocess seguro** | Todos os comandos rclone usam `asyncio.create_subprocess_exec` (lista de args, sem `shell=True`) — sem risco de injection; `remote_name` validado com regex `[a-zA-Z0-9_-]{1,64}` |
| **`cloud/rclone_router.py`** | Novos endpoints em `/rclone/*`: `GET /remotes`, `GET /remotes/{name}/browse`, CRUD de jobs, `POST /jobs/{id}/run`, `GET /jobs/{id}/status` — mesmo padrão de lock por job do router OAuth |
| **`database.py` — `RcloneBackupJob`** | Nova tabela `rclone_backup_jobs` sem FK para `cloud_credentials` — rclone gerencia tokens externamente; criada automaticamente pelo `init_db()` no startup |
| **`scheduler.py`** | `add_or_update_rclone_job` / `remove_rclone_job` / `reload_rclone_jobs_from_db` — agendamento cron idêntico ao dos jobs OAuth, com prefixo de ID `rclone_job_{id}` para não colidir |

### v6.1.0

| Componente | Mudança |
|---|---|
| **`main.py` — upload novo** | `_store_new_content` (move, cifra, verifica, replica) extraída para função síncrona e chamada via `asyncio.to_thread` — event loop liberado durante uploads pesados |
| **`main.py` — dedup** | Verificação de integridade leve: `_expected_stored_size(plain_size, encrypted)` calcula o tamanho esperado pela fórmula AES-256-GCM (`12 + plain_size + ⌈plain_size/1MB⌉ × 20`) sem decifrar o arquivo. Integridade profunda continua com o job `validate-integrity` |
| **`main.py` — `_ensure_replicas` no dedup/register** | Chamadas a `_ensure_replicas` (I/O bloqueante entre volumes) movidas para `asyncio.to_thread` |
| **`main.py` — `_build_fast_data`** | `_safe_disk_usage` chamado uma vez por volume em vez de duas (storage + disks) — elimina statvfs duplicado |
| **`storage.py` — SSD cache move** | `_copy_with_sha256`: lê a origem uma única vez em chunks de 1 MB calculando hash e escrevendo o destino simultaneamente — 2 leituras em vez de 3 por move (~33% menos I/O) |
| **`cloud/runner.py` — consumer** | `_process_file_sync` e `_register_version_file_sync` extraídas como funções síncronas; consumer chama ambas via `asyncio.to_thread` — downloads do producer não travam mais enquanto o consumer processa |
| **`cloud/runner.py` — `httpx.AsyncClient`** | Um único cliente compartilhado por job via `async with httpx.AsyncClient(...)` em `run_cloud_backup_job` — elimina handshake TCP/TLS por arquivo baixado |
| **`cloud/base.py`, `gdrive.py`, `onedrive.py`** | `download_file_to` aceita parâmetro opcional `client: httpx.AsyncClient \| None` — usa o cliente compartilhado do runner quando fornecido, cria um próprio se `None` (retrocompatível) |
| **`nestvault.py` — pool HTTP** | `HTTPAdapter(pool_connections=4, pool_maxsize=32)` — pool dimensionado para `--workers` altos sem descartar conexões |
| **`nestvault.py` — retry** | `_with_retries(fn, what)` com backoff exponencial (1 s, 2 s) aplicado em upload, register e check — tolera erros transientes (429, 5xx, falhas de rede) sem abortar o backup |

### v4.7.0

| Componente | Mudança |
|---|---|
| **`storage.py` — `ensure_replicas`** | Replicação paralela via `ThreadPoolExecutor`: todas as cópias para volumes adicionais são feitas simultaneamente; operações de DB permanecem na thread principal após o pool terminar |
| **`main.py` — `cleanup_orphans` / `encrypt_existing`** | Eliminadas N+1 queries: SHA-256s válidos buscados em uma query `.in_()` antes do loop; `encrypt_existing` busca todas as cópias em lote e agrupa por sha256 em memória em vez de uma query por arquivo |
| **`main.py` — `storage_disks`** | `content_files` e `content_bytes` por volume calculados por `GROUP BY` em vez de um COUNT+SUM por volume |
| **`main.py` — `GET /backups/disk-summary`** | Novo endpoint batch que retorna espaço total/livre/usado de todos os discos em uma chamada; dashboard substituiu N fetches paralelos por esta chamada única |
| **`database.py` — novos índices** | `idx_label_status_key` (atualizado para cobrir `version_key`), `idx_version_created`, `idx_version_finished` em `backup_versions`; `idx_cbj_last_run` em `cloud_backup_jobs` |
| **`cloud/router.py` — `list_jobs`** | `joinedload(CloudBackupJob.credential)` elimina N+1 na listagem de jobs |
| **`explorer.html` — filtro** | Debounce de 250 ms no campo de busca — evita queries redundantes a cada keystroke |
| **`main.py` — logs de upload** | Todos os 4 caminhos de upload (`nova`, `nova cifrada`, `dedup`, `registrada`) logam `[upload] label/version_key ← path — modo sha256… (MB)` com contexto de label e versão — antes era `[integrity]` sem correlação |
| **`main.py` — logs de versão** | `create_version` loga `[versao] label/key criada`; `finish_version` loga `[versao] label/key → status` para todos os status (inclusive `error`/`incomplete`, antes silenciosos) |
| **`cloud/runner.py` — logs de progresso** | `_producer` loga `[cloud-runner] [i/total] path` a cada ~25% do total para jobs com ≥ 10 arquivos |

### v5.0

| Componente | Mudança |
|---|---|
| **`main.py` — `GET /maintenance/cleanup-by-date/preview`** | Novo endpoint de preview: retorna contagem de versões elegíveis para remoção agrupadas por label, filtradas por `before` (data de corte) e `label` opcional. Versões `running` e a versão `done` mais recente de cada label são excluídas do conjunto via subquery `max(id) GROUP BY backup_label` |
| **`main.py` — `POST /maintenance/cleanup-by-date`** | Novo endpoint de execução: deleta versões elegíveis (mesmas regras do preview), remove `VersionFile`s explicitamente (SQLite sem FK cascade por padrão), executa `_cleanup_orphan_contents()` e retorna estatísticas por label |
| **`maintenance.html` — card "Excluir Versões por Data"** | Novo card na grade de manutenção com dropdown de label (Todos os labels / label específico) e input de data; preview carrega automaticamente ao mudar qualquer campo e exibe tabela por label com total em vermelho; botão habilitado apenas quando `total > 0`; após execução atualiza preview automaticamente |
| **`nestvault.py` — `_AuthSession` / `_prompt_api_key`** | Subclasse de `requests.Session` que intercepta respostas 401: solicita a API Key via `getpass.getpass()` (sem eco no terminal) e retenta a requisição original com a nova chave — transparente para todos os comandos sem nenhuma alteração nos call sites |

### v4.8.0

| Componente | Mudança |
|---|---|
| **`nestvault.py` — `restore --exclude`** | Comando `restore` passou a aceitar `--exclude` com múltiplos valores, filtrando arquivos cujo caminho relativo contenha o componente de diretório especificado — comportamento idêntico ao `--exclude` do `backup` |
| **Versionamento unificado** | Client e server passam a compartilhar o mesmo número de versão a partir de `v4.8.0` |

### v4.5.1

| Componente | Mudança |
|---|---|
| **`cloud/runner.py` (server)** | Pipeline producer-consumer via `asyncio.Queue(maxsize=4)`: producer faz download em streaming enquanto consumer processa (dedup, store, encrypt, replicate, DB) simultaneamente. `asyncio.gather(producer, consumer)` substitui o loop sequencial anterior |
| **`crypto.encrypt_stream` (server)** | Movida para `loop.run_in_executor` no consumer — operação CPU-bound não bloqueia mais o event loop durante a criptografia de arquivos |

### v4.0

| Componente | Mudança |
|---|---|
| **`cloud/` (server)** | Novo módulo com abstração `CloudProvider`, implementações `GoogleDriveProvider` e `OneDriveProvider`. OAuth2 manual via `httpx` — sem SDKs de terceiros (Google Auth, MSAL) |
| **`scheduler.py` (server)** | APScheduler `AsyncIOScheduler` integrado ao lifespan do FastAPI. Jobs persistidos no banco e restaurados no startup. `add_or_update_job`, `remove_job`, `reload_jobs_from_db` |
| **`storage.py` (server)** | Helpers de storage extraídos de `main.py` para eliminar importação circular com `cloud/`. `pick_volume`, `content_path`, `ensure_replicas`, `healthy_volumes`, `volume_health_monitor` — compartilhados entre `main.py` e `cloud/runner.py` |
| **`database.py` — novas tabelas** | `CloudCredential` (conta cloud + tokens OAuth) e `CloudBackupJob` (configuração de job: conta, pasta, label, cron). Tokens criptografados com Fernet; chave derivada do `BACKUP_API_KEY` via SHA-256 |
| **Runner (server)** | `run_cloud_backup_job` — lista pasta recursivamente, baixa arquivo a arquivo em streaming com SHA-256 single-pass, reutiliza pipeline de deduplicação/criptografia/replicação existente. Token renovado a cada 100 arquivos. Erros por arquivo tolerados |
| **`/cloud/*` (server)** | 12 novos endpoints para gerenciar contas e jobs. `POST /cloud/jobs/{id}/run` dispara execução via `asyncio.create_task` — resposta 202 imediata |
| **`index.html`** | Seção "Cloud Backup" no dashboard: conectar contas OAuth, tabela de jobs, execução manual e acompanhamento de status |
| **Novas dependências** | `httpx>=0.27.0` e `apscheduler>=3.10.0` — apenas 2 pacotes adicionados |

### v3.1

| Componente | Mudança |
|---|---|
| **`ENCRYPTION_ENABLED` / `ENCRYPTION_KEY` (server)** | Novas env vars. Padrão `false` — compatível com discos que já têm criptografia própria (LUKS, ZFS, FileVault). Chave validada no startup; falha imediata se inválida |
| **`crypto.py` (server)** | Novo módulo. AES-256-GCM em chunks de 1 MB: `encrypt_stream(src, dst, key)` e `decrypt_chunks(path, key)`. Formato: `[12 bytes nonce][4 bytes len][ciphertext+tag]` repetido. Nonce único por chunk via XOR com índice |
| **`FileContent.encrypted` (DB)** | Nova coluna `INTEGER NOT NULL DEFAULT 0`. Migração automática via `ALTER TABLE` no startup — sem downtime. Distingue arquivos pré-v3.1 (plaintext) de arquivos novos (cifrados) |
| **Upload (server)** | Após gravar no disco, cifra o arquivo antes de replicar. Cópias nos outros volumes já chegam cifradas |
| **Download (server)** | Se `fc.encrypted=True`: `StreamingResponse` que decifra chunk a chunk. Se `False`: `FileResponse` direto (zero overhead para arquivos não cifrados) |
| **`POST /maintenance/encrypt-existing` (server)** | Novo endpoint para migração do acervo existente. Cifra in-place cada cópia física, atualiza `encrypted=True` e faz commit por arquivo — interrupção não perde progresso |
| **`encrypt-existing` (client)** | Novo subcomando que chama o endpoint com timeout de 600 s e exibe progresso |

### v3.0

| Componente | Mudança |
|---|---|
| **`REPLICATION_FACTOR` (server)** | Nova env var. Padrão `1` = comportamento anterior (sem replicação, compatível com RAID físico/ZFS). `2+` = replicação síncrona no upload para N volumes. `0` = espelhar para todos os volumes saudáveis |
| **`FileContentCopy` (DB)** | Nova tabela rastreia o path físico de cada cópia por volume (`sha256`, `stored_at`, `volume_path`) |
| **Upload (server)** | Após gravar a cópia primária, `_ensure_replicas()` copia para volumes adicionais antes de confirmar. Volumes degraded são pulados |
| **Download (server)** | Tenta cada cópia em ordem, pulando volumes degraded — 503 apenas se todas as cópias estão em volumes degraded, 410 se o dado sumiu |
| **Cleanup (server)** | Remove todas as cópias físicas de um conteúdo órfão antes de apagar o registro |
| **Re-replicação (server)** | `_volume_health_monitor` detecta recovery e copia arquivos sub-replicados em background via `_rereplicate_to_volume` |
| **`/storage/disks` (server)** | Contagem de arquivos por volume via tabela `file_content_copies` (mais precisa que LIKE anterior) |
| **Backfill (server)** | No startup, `_backfill_content_copies` migra entradas `FileContent` existentes para a nova tabela — sem downtime |

### v2.9

| Componente | Mudança |
|---|---|
| **Hashing (client)** | `ThreadPoolExecutor` → `ProcessPoolExecutor` para SHA-256: bypassa o GIL, paralelismo real de CPU em todos os núcleos |
| **`_hash_item` (client)** | Função top-level de módulo (necessário para serialização do `ProcessPoolExecutor`) com `chunksize` dinâmico para minimizar overhead de IPC |
| **`hash_workers` (client)** | Novo parâmetro independente de `workers`; padrão `os.cpu_count()`, separando o tunning de upload (rede) do de hashing (CPU) |
| **`--hash-workers` (CLI)** | Novo argumento para controle manual do número de processos de hash |

**Ganho esperado:**

| Cenário | v2.8 (4 threads) | v2.9 (N processos) |
|---|---|---|
| 200k arquivos, 8 núcleos | linha base | ~4–6× mais rápido |
| 200k arquivos, 16 núcleos | linha base | ~8–12× mais rápido |
| 2ª execução (cache hits) | sem leitura de disco | sem mudança (já ótimo) |

> O ganho é maior em arquivos de tamanho médio (1 KB–10 MB) onde o SHA-256 domina. Para arquivos muito pequenos (<1 KB), o overhead de IPC pode reduzir o ganho; para arquivos muito grandes, o gargalo vira I/O de disco.

### v2.8

| Componente | Mudança |
|---|---|
| **Config — `STORAGE_DIRS`** | Nova env var aceita lista de paths separados por vírgula. `STORAGE_DIR` legado continua funcionando como antes (retrocompatível) |
| **`_pick_volume()` (server)** | Novo helper que escolhe o volume com mais bytes livres no momento de cada upload |
| **Upload (server)** | Tmp e conteúdo final escritos no mesmo volume escolhido — evita `shutil.move` cross-device |
| **`/storage/info` (server)** | `total_bytes`, `used_bytes` e `free_bytes` agora somam todos os volumes; `reclaimable_bytes` calculado via DB como antes |
| **Auto-cleanup (server)** | Usa o menor % livre entre todos os volumes — cleanup dispara se qualquer disco estiver crítico |
| **Cliente** | Nenhuma alteração — completamente transparente |

### v2.7

| Componente | Mudança |
|---|---|
| **`GET /storage/info` (server)** | Novo endpoint que retorna `total_bytes`, `used_bytes`, `free_bytes` via `shutil.disk_usage(STORAGE_DIR)` e `reclaimable_bytes` via subquery: soma o `size` dos `FileContent`s cujo `sha256` não é referenciado por nenhuma versão "done" mais recente de qualquer label |
| **Dashboard — stat boxes (6)** | Dois novos boxes no stats bar: "Disco Livre" (espaço + percentual + barra visual) e "Liberável" (bytes recuperáveis com cleanup de versões antigas) |
| **Dashboard — barra de disco** | Barra de progresso visual dentro do box "Disco Livre" — muda de cor conforme ocupação (verde → âmbar → vermelho ao ultrapassar 80%/90%) |

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
                └── FileContent (sha256, stored_at, encrypted) ← primeiro path + flag de cifra
                      └── FileContentCopy (sha256, stored_at, volume_path) ← todas as cópias
```

**Storage físico — disco único:**
```
storage/
└── _content/
    ├── ab/
    │   └── abcd1234ef567890...   ← conteúdo único por sha256
    └── f7/
        └── f7a923bc11d24e5f...
```

**Storage físico — dois discos (`STORAGE_DIRS=/mnt/disk1,/mnt/disk2`):**
```
/mnt/disk1/
└── _content/
    ├── ab/
    │   └── abcd1234ef567890...   ← arquivos novos vão para o disco com mais espaço livre
    └── f7/
        └── f7a923bc11d24e5f...

/mnt/disk2/
└── _content/
    └── 3c/
        └── 3ca812de55f09b1a...   ← cada FileContent.stored_at guarda o path absoluto
```

O conteúdo de cada arquivo é armazenado **uma única vez por sha256**, independente de quantas versões ou labels o referenciem. Com `REPLICATION_FACTOR=1` (padrão), cada conteúdo fica em um único volume. Com `REPLICATION_FACTOR=2`, uma cópia adicional é gravada em outro volume:

**Storage físico — replicação ativa (`REPLICATION_FACTOR=2`):**
```
/mnt/disk1/
└── _content/
    ├── ab/
    │   └── abcd1234ef567890...   ← cópia primária
    └── f7/
        └── f7a923bc11d24e5f...

/mnt/disk2/
└── _content/
    ├── ab/
    │   └── abcd1234ef567890...   ← réplica (mesmo conteúdo, path diferente)
    └── f7/
        └── f7a923bc11d24e5f...
```

Download tenta cada cópia automaticamente — se disk1 falhar, disk2 serve o arquivo sem intervenção.

---

## 🔌 Endpoints da API

### Dashboard e Health

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `GET` | `/` | Dashboard web |
| `GET` | `/health` | Status do servidor e versão |
| `GET` | `/maintenance` | Página de manutenção (HTML) |
| `GET` | `/explorer` | Explorer de arquivos (HTML) |

### Backups

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/backups` | Cria backup — idempotente |
| `GET` | `/backups` | Lista todos os backups — `?client_name=` filtra por cliente |
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
| `POST` | `/backups/{label}/versions/{key}/absorb` | Herda arquivos ausentes de outra versão (modo acumulativo) |
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

### Storage

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `GET` | `/storage/info` | Espaço total/livre/usado do disco e bytes liberáveis ao apagar versões antigas |
| `GET` | `/storage/disks` | Status e contagem de arquivos físicos por volume |
| `GET` | `/disks` | Dashboard de discos (HTML) |

### Manutenção

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/maintenance/cleanup-orphans` | Remove todos os arquivos físicos não referenciados por nenhuma versão |
| `POST` | `/maintenance/rereplicate` | Re-replica conteúdos com menos cópias que `REPLICATION_FACTOR` |
| `POST` | `/maintenance/reconcile-replication` | Reconcilia replicação: remove cópias excedentes e preenche faltantes conforme `REPLICATION_FACTOR` |
| `POST` | `/maintenance/encrypt-existing` | Cifra arquivos físicos ainda não criptografados (requer `ENCRYPTION_ENABLED=true`) |
| `GET` | `/maintenance/cleanup-by-date/preview` | Preview de versões elegíveis para remoção antes de uma data (`?before=YYYY-MM-DD[&label=X]`) |
| `POST` | `/maintenance/cleanup-by-date` | Remove versões anteriores a uma data; preserva última versão `done` por label e versões `running` (`?before=YYYY-MM-DD[&label=X]`) |

### Cloud Backup *(v4.0)*

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `GET` | `/cloud/accounts` | Lista contas cloud conectadas |
| `GET` | `/cloud/accounts/{provider}/auth` | Gera URL de autenticação OAuth2 (`provider`: `gdrive` ou `onedrive`) |
| `DELETE` | `/cloud/accounts/{id}` | Desconecta conta (remove credenciais e jobs associados) |
| `GET` | `/cloud/accounts/{id}/folders` | Lista pastas raiz da conta |
| `GET` | `/cloud/accounts/{id}/folders/{folder_id}` | Lista subpastas de uma pasta |
| `GET` | `/cloud/jobs` | Lista todos os jobs de cloud backup |
| `POST` | `/cloud/jobs` | Cria job de backup (conta, pasta, label destino, cron) |
| `GET` | `/cloud/jobs/{id}` | Detalhes de um job |
| `PATCH` | `/cloud/jobs/{id}` | Atualiza job (pasta, label, cron, enabled) |
| `DELETE` | `/cloud/jobs/{id}` | Remove job |
| `POST` | `/cloud/jobs/{id}/run` | Inicia execução manual do job (async, retorna 202 imediatamente) |
| `GET` | `/cloud/jobs/{id}/status` | Status da última execução (last_run_at, status, message) |

> O callback OAuth (`GET /cloud/callback/{provider}`) é chamado pelo provedor — não é chamado diretamente pelo usuário.
>
> `POST /cloud/jobs/{id}/run` retorna `{ "status": "started", "job_id": N }` — a execução ocorre em background. Use `GET /cloud/jobs/{id}/status` para acompanhar.

> `/maintenance/cleanup-orphans` — retorna `{ "files_removed": N, "bytes_freed": N }`. Útil após deleções em massa. Operação **síncrona**.
>
> `/maintenance/rereplicate` — retorna `{ "replicated": N, "skipped": N, "target_copies": N }`. `replicated` = arquivos que receberam ao menos uma nova cópia. `skipped` = arquivos cuja única cópia está em volume `degraded`. Operação **síncrona** — pode demorar em acervos grandes.
>
> `/maintenance/reconcile-replication` — retorna `{ "replicated": N, "skipped": N, "cleaned": N, "target_copies": N }`. Remove cópias excedentes e preenche arquivos sub-replicados em uma única chamada. Útil ao reduzir ou aumentar `REPLICATION_FACTOR`. Operação **síncrona** — pode demorar em acervos grandes.
>
> `/maintenance/encrypt-existing` — retorna `{ "files_encrypted": N, "bytes_processed": N, "skipped": N }`. `skipped` inclui arquivos sem cópia acessível (volume degraded) e erros de I/O. Operação **síncrona** — use timeout longo em acervos grandes (cliente usa 600 s). Retorna `400` se `ENCRYPTION_ENABLED=false`.

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

#### `AbsorbRequest`
```json
{
  "source_version_key": "2026-03-01T02:00:00"  // versão da qual herdar arquivos ausentes
}
```

---

### Schemas de Response

#### `HealthResponse`
```json
{
  "status":  "ok",
  "version": "4.2.0",
  "time":    "2026-04-25T10:42:31.123456"
}
```

#### `BackupInfo`
Stats agregados refletem a **última versão `done`** do backup.
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
  "duration_seconds": 161.0,                // null se ainda em andamento
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

#### `EncryptExistingResponse`
```json
{
  "files_encrypted": 1842,   // arquivos cifrados com sucesso nesta execução
  "bytes_processed": 38654705664,
  "skipped": 3               // arquivos pulados (volume degraded ou erro de I/O)
}
```

#### `AbsorbResponse`
```json
{
  "inherited": 100,  // VersionFiles copiados da versão fonte para a versão destino
  "skipped": 20      // arquivos da fonte que já existiam no destino (pelo original_path)
}
```

#### `StorageInfoResponse`
```json
{
  "total_bytes":      500107862016,
  "used_bytes":       214748364800,
  "free_bytes":       285359497216,
  "reclaimable_bytes": 6442450944
}
```

`total_bytes`, `used_bytes` e `free_bytes` são a **soma de todos os volumes** configurados em `STORAGE_DIRS` (ou o volume único de `STORAGE_DIR`).

#### `DiskVolumeInfo`
```json
{
  "path":          "/mnt/disk1/backups",
  "total_bytes":   500107862016,
  "used_bytes":    214748364800,
  "free_bytes":    285359497216,
  "content_files": 1842,
  "content_bytes": 38654705664,
  "status":        "ok"
}
```

`status` pode ser `"ok"` ou `"degraded"` (volume inacessível). Em estado degraded, `total_bytes`, `used_bytes` e `free_bytes` são `0`. `content_files` e `content_bytes` contam as cópias físicas **presentes neste volume** — com replicação ativa, o mesmo arquivo aparece em múltiplos volumes.

`reclaimable_bytes` = tamanho total dos `FileContent`s referenciados **exclusivamente** por versões antigas (não pela versão "done" mais recente de nenhum label). É o espaço que seria recuperado rodando `cleanup --keep 1 --all`.

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
| `GET /backups` | query: `client_name` (opcional) | `list[BackupInfo]` |
| `GET /backups/{label}` | — | `BackupInfo` |
| `DELETE /backups/{label}` | — | `BackupDeletedResponse` |
| `POST /backups/{label}/versions` | `VersionCreate` | `VersionCreatedResponse` |
| `GET /backups/{label}/versions` | — | `list[VersionInfo]` |
| `GET /backups/{label}/versions/{key}` | — | `VersionInfo` |
| `PATCH /backups/{label}/versions/{key}` | `VersionFinish` | `VersionInfo` |
| `DELETE /backups/{label}/versions/{key}` | — | `VersionDeletedResponse` |
| `POST /backups/{label}/versions/{key}/absorb` | `AbsorbRequest` | `AbsorbResponse` |
| `POST /backups/{label}/cleanup` | `CleanupRequest` | `CleanupResponse` |
| `GET /backups/{label}/compare` | query: `v1`, `v2` | `CompareResponse` |
| `POST /check` | `CheckRequest` | `CheckResponse` |
| `POST /check/batch` | `CheckBatchRequest` | `list[CheckBatchResultItem]` |
| `POST /upload` | binary stream + headers `X-*` | `UploadResponse` |
| `POST /sync` | `SyncRequest` | `SyncResponse` |
| `GET /files` | query: `backup_label`, `version_key` | `list[FileInfo]` |
| `GET /files/{id}/download` | — | binary stream |
| `GET /storage/info` | — | `StorageInfoResponse` |
| `GET /storage/disks` | — | `list[DiskVolumeInfo]` |
| `POST /maintenance/cleanup-orphans` | — | `OrphanCleanupResponse` |
| `POST /maintenance/rereplicate` | — | `RereplicateResponse` |
| `POST /maintenance/reconcile-replication` | — | `ReconcileResponse` |
| `POST /maintenance/encrypt-existing` | — | `EncryptExistingResponse` |
| `GET /maintenance/cleanup-by-date/preview` | query: `before`, `label` (opcional) | `{ total, per_label: [{label, count}] }` |
| `POST /maintenance/cleanup-by-date` | query: `before`, `label` (opcional) | `{ total_deleted, per_label: [{label, deleted}], storage_files_removed, bytes_freed }` |

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

---

## 🐘 PostgreSQL (opcional) — v7.1

Por padrão o NestVault usa **SQLite**, que é ideal para uso doméstico e NAS. Se você tiver muitos uploads simultâneos ou quiser eliminar completamente qualquer possibilidade de lock, é possível usar o **PostgreSQL** como backend alternativo.

### Quando usar cada um

| Cenário | Recomendação | Motivo |
|---|---|---|
| Raspberry Pi (qualquer modelo) | **SQLite** | PostgreSQL consome 50–150 MB RAM extra e desgasta mais o SD com writes contínuos |
| NAS doméstico | **SQLite** | NestVault é single-process; WAL já elimina locks sem servidor externo |
| Servidor x86 com SSD, uploads intensos | **PostgreSQL** | MVCC real compensa; I/O abundante, RAM sobrando |
| Múltiplas instâncias compartilhando o banco | **PostgreSQL** | Único cenário onde múltiplos writers simultâneos existem de verdade |

### Instalando o driver Python (psycopg2)

O driver PostgreSQL **não é instalado por padrão** (para não impactar quem usa SQLite, especialmente no Raspberry Pi 32-bit onde a compilação do driver pode falhar).

Instale apenas quando for usar PostgreSQL:

```bash
# Qualquer Linux com pip (Raspberry Pi 64-bit, x86, etc.)
pip install -r requirements-postgres.txt

# Raspberry Pi 32-bit (armhf) — prefira o pacote do sistema para evitar compilação
sudo apt install -y python3-psycopg2
```

### Instalando o PostgreSQL no Linux

```bash
sudo apt update
sudo apt install -y postgresql postgresql-contrib
sudo systemctl enable --now postgresql
```

Verifique que o serviço está rodando:

```bash
sudo systemctl status postgresql
```

### Criando usuário e banco de dados

```bash
sudo -u postgres psql <<'EOF'
CREATE USER nestvault WITH PASSWORD 'sua_senha_aqui';
CREATE DATABASE nestvault OWNER nestvault;
\q
EOF
```

Teste a conexão:

```bash
psql -U nestvault -h localhost -d nestvault -c "SELECT version();"
```

### Configurando o NestVault para usar PostgreSQL

Em vez de `DB_PATH`, defina `DATABASE_URL`:

```bash
export DATABASE_URL="postgresql://nestvault:sua_senha_aqui@localhost/nestvault"
```

> **Nota:** `DB_PATH` é ignorado quando `DATABASE_URL` está definido.

Se estiver usando systemd, adicione a variável ao arquivo de serviço:

```ini
[Service]
Environment="DATABASE_URL=postgresql://nestvault:sua_senha_aqui@localhost/nestvault"
# Remova ou comente a linha DB_PATH se existir
```

Reinicie o serviço após a alteração:

```bash
sudo systemctl daemon-reload
sudo systemctl restart nestvault
```

O NestVault cria as tabelas automaticamente na primeira inicialização.

### Migrando dados do SQLite para PostgreSQL

Se já possui dados no SQLite e quer migrar para PostgreSQL, use o script incluído:

```bash
cd server
python migrate_to_postgres.py \
  --sqlite /caminho/para/backup.db \
  --postgres "postgresql://nestvault:sua_senha_aqui@localhost/nestvault"
```

O script:
- Cria as tabelas no PostgreSQL (caso ainda não existam)
- Copia os dados em lotes de 500 registros
- É **idempotente**: registros já existentes no destino são ignorados, então pode ser re-executado com segurança
- Exibe progresso por tabela e resumo final com contagem de registros

Para verificar a conexão sem migrar dados:

```bash
python migrate_to_postgres.py \
  --sqlite /caminho/para/backup.db \
  --postgres "postgresql://nestvault:sua_senha_aqui@localhost/nestvault" \
  --dry-run
```

Após a migração bem-sucedida, configure `DATABASE_URL` e reinicie o servidor. Verifique o dashboard para confirmar que os backups aparecem normalmente.

### Revertendo para SQLite

Se quiser voltar ao SQLite (ou criar um backup portátil do banco PostgreSQL), use o script reverso:

```bash
cd server
python migrate_to_sqlite.py \
  --postgres "postgresql://nestvault:sua_senha_aqui@localhost/nestvault" \
  --sqlite   /caminho/para/backup_restored.db
```

Após a migração:
1. Remova `DATABASE_URL` do ambiente (ou do arquivo systemd)
2. Configure `DB_PATH=/caminho/para/backup_restored.db` (ou mova o arquivo para o local padrão)
3. Reinicie o servidor

Para verificar sem migrar dados:

```bash
python migrate_to_sqlite.py \
  --postgres "postgresql://nestvault:sua_senha_aqui@localhost/nestvault" \
  --sqlite   /caminho/para/backup_restored.db \
  --dry-run
```
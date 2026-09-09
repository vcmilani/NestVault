# 🗄️ NestVault  `v9.0.2`

Sistema de backup com **versionamento**, **deduplicação de conteúdo** e **backup por usuário** — cada conta só cria, lista e restaura seus próprios backups.

Cada execução de backup cria uma nova versão dentro do label. O servidor armazena o conteúdo físico apenas uma vez por sha256 — versões diferentes que compartilham arquivos idênticos não duplicam o storage.

Projetado para consumir poucos recursos: roda bem em **Raspberry Pi** e em **computadores antigos**, inclusive com discos externos USB.

📜 Histórico completo de versões em [CHANGELOG.md](CHANGELOG.md).

---

## Estrutura

```
NestVault/
├── server/
│   ├── main.py                  ← API FastAPI
│   ├── config.py                ← Configuração persistida em arquivo (v8.0)
│   ├── database.py              ← Modelos SQLite/SQLAlchemy
│   ├── storage.py               ← Helpers de storage (dedup, replicação, volumes)
│   ├── crypto.py                ← Criptografia AES-256-GCM (v3.1)
│   ├── auth.py                  ← Autenticação por usuário (v7.9)
│   ├── scheduler.py             ← APScheduler para jobs rclone (v4.0)
│   ├── daily_digest.py          ← Resumo diário via Telegram (v4.5)
│   ├── nightly_cleanup.py       ← Limpeza noturna automática (v5.2)
│   ├── cache_state.py           ← Cache de estado para polling de atividade
│   ├── cloud/                   ← Módulo de cloud backup via rclone (v7.0)
│   │   ├── rclone_runner.py     ← Lógica de execução via rclone
│   │   └── rclone_router.py     ← Endpoints /rclone/*
│   ├── requirements.txt
│   ├── requirements-postgres.txt ← Dependências opcionais para PostgreSQL (v7.1)
│   ├── config.json              ← Configuração do servidor (gerado no 1º boot, gitignored)
│   └── static/
│       ├── index.html           ← Dashboard web
│       └── settings.html        ← Tela de configurações (v8.0)
├── client/
│   ├── nestvault.py             ← Cliente de backup/restore
│   └── requirements.txt
├── tools/
│   ├── migrate_to_postgres.py       ← Migração SQLite → PostgreSQL (v7.1)
│   ├── migrate_to_sqlite.py         ← Migração reversa PostgreSQL → SQLite (v7.1)
│   └── migrate_drop_oauth_tables.sql ← Drop das tabelas OAuth obsoletas (v7.3)
├── .gitignore
├── README.md
└── CHANGELOG.md                 ← Histórico de versões
```

---

## 📱 Clientes disponíveis

| Cliente | Plataforma | Repositório |
|---------|------------|-------------|
| **nestvault.py** | Linux / macOS / Windows (CLI Python) | este repositório — `client/nestvault.py` |
| **NestVault para macOS** | macOS (app nativo SwiftUI) | [github.com/vcmilani/NestVault_Xcode](https://github.com/vcmilani/NestVault_Xcode) |

O servidor expõe uma API REST padrão — qualquer cliente que implemente o [contrato da API](#-endpoints-da-api) funciona sem modificações no servidor.

---

## ⚠️ Atualizando da v7.8 para v7.9

A v7.9 introduz **backup por usuário**. Nenhuma ação manual é necessária para o servidor continuar no ar — a migração roda sozinha no primeiro boot — mas o comportamento da autenticação muda de forma relevante:

- **`BACKUP_API_KEY` deixa de ter o modo "sem autenticação".** Antes, omitir a variável desabilitava a checagem de chave; agora autenticação é **sempre obrigatória**. Se você rodava o servidor sem `BACKUP_API_KEY` definida, defina uma antes de atualizar — sem isso nenhum client conseguirá se autenticar.
- **No primeiro boot com a v7.9**, se ainda não existir nenhum usuário no banco, o servidor cria automaticamente uma conta `admin` cuja chave é a própria `BACKUP_API_KEY` do ambiente. Todo backup já existente é atribuído a esse admin (`owner_user_id`). Nada quebra: os clients que já usavam essa chave continuam funcionando exatamente como antes, agora como admin.
- **Endpoints antes abertos para qualquer chave válida** (`/storage/*`, `/maintenance/*`, `/api/stats`, `/api/activity`, `/rclone/*`) passam a exigir uma conta com `role=admin`. Uma chave de usuário comum recebe `403` nesses endpoints.
- **Para criar contas por pessoa/máquina**: acesse `/manage-users` com a chave de admin, crie um usuário e distribua a chave gerada (exibida **uma única vez**). Configure essa chave como `BACKUP_API_KEY` no client dessa pessoa.
- **Labels criados antes da migração pertencem ao admin bootstrap.** Se quiser que um usuário novo assuma um label antigo que já era "dele" na prática, reatribua o dono em **Manutenção → Reatribuir Dono** (ou `PATCH /backups/{label}/owner`) — só depois disso a chave dessa pessoa consegue acessar aquele label. Tentar criar/acessar um label que já pertence a outra conta retorna `403 Voce nao tem permissao sobre este backup`.

Nenhuma coluna precisa ser migrada manualmente — `init_db()` cria a tabela `users` e adiciona `backup_ids.owner_user_id` automaticamente, com o mesmo padrão idempotente já usado nas migrações anteriores.

---

## ⚠️ Atualizando da v7.2 para v7.3

A v7.3 **remove** o sistema de cloud backup OAuth (Google Drive e OneDrive direto). O rclone é agora o único backend. Nenhuma ação é obrigatória — o servidor simplesmente não criará mais nem acessará as tabelas `cloud_credentials` e `cloud_backup_jobs`.

**Para limpar o banco (opcional, mas recomendado):**

```bash
# Backup antes de dropar
cp backup.db backup.db.bak

# SQLite
sqlite3 backup.db < tools/migrate_drop_oauth_tables.sql

# PostgreSQL
psql "$DATABASE_URL" -f tools/migrate_drop_oauth_tables.sql
```

O script já inclui instruções completas para ambos os backends.

Se você ainda não usa o rclone, configure-o agora (veja [Cloud Backup](#️-cloud-backup)):

```bash
rclone config   # configure gdrive, onedrive ou qualquer outro provedor
```

---

## ⚠️ Atualizando da v3.x para v4.0

A v4.0 adicionou duas tabelas ao banco: `cloud_credentials` e `cloud_backup_jobs`. Essas tabelas existem em bancos criados entre v4.0 e v7.2 e podem ser dropadas a partir da v7.3 (ver seção acima).

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

# Cole a chave em Configurações → Storage (ou direto no config.json) e reinicie:
#   "storage": { "encryption_enabled": true, "encryption_key": "<chave-gerada-acima>" }
sudo systemctl restart backup-server
```

> A partir da v8.0.0 a tela `/settings` faz isso sem SSH: os dois campos ficam no cartão **Storage**, marcados como `requer reinício`, e a própria tela oferece o botão de reiniciar. Alterar a criptografia com conteúdo já gravado pede confirmação por palavra-chave.

> **Guarde a chave em local seguro.** Se perdida, arquivos cifrados se tornam irrecuperáveis. Rotação de chave não está disponível na v3.1.

**Migrar arquivos existentes** (após ativar `storage.encryption_enabled`):

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

Em Configurações → Storage, ajuste **Fator de replicação** para `2` — vale na hora, sem reiniciar. Direto no arquivo, seria:

```json
"storage": { "replication_factor": 2 }
```

Novos uploads serão replicados. Conteúdos existentes **não** são re-replicados automaticamente retroativamente — apenas quando sofrem novo upload ou quando um volume degraded se recupera.

### Adicionando um disco novo ao cluster

Se você adicionar um novo ponto de montagem a `storage.dirs`, o servidor o reconhece como volume saudável imediatamente — mas **não re-replica os arquivos existentes para ele**. Apenas novos uploads passarão a usar o disco novo.

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
usuário monta o disco novo em /mnt/disk3 e adiciona a storage.dirs
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

### 2. Configuração (`config.json`)

A partir da **v8.0.0** todos os parâmetros do servidor vivem em um arquivo de configuração — não mais em variáveis de ambiente. O arquivo é criado sozinho no primeiro boot e pode ser editado pela tela **[Configurações](#-tela-de-configurações)** (`/settings`, restrita a admins) ou à mão.

| | |
|---|---|
| **Local padrão** | `server/config.json` (ao lado de `main.py`) |
| **Como mudar o local** | variável de ambiente `NESTVAULT_CONFIG=/caminho/config.json` |
| **Permissão** | `0600` — o arquivo guarda segredos (chave de criptografia, tokens, DSN do Postgres) |
| **Precedência** | arquivo > padrão. As variáveis de ambiente **só semeiam** o arquivo no primeiro boot |

A única variável de ambiente que continua obrigatória é **`BACKUP_API_KEY`**: é o segredo de bootstrap que vira o primeiro usuário admin. Ela não vai para o `config.json` de propósito — persistir uma chave de administrador em texto no disco não compensa, e a rotação já é feita pela tela `/manage-users`.

```bash
export BACKUP_API_KEY="uma-chave-secreta-forte-aqui"   # obrigatória — vira a chave do admin no primeiro boot
uvicorn main:app --host 0.0.0.0 --port 8000            # gera server/config.json com os padrões
```

Exemplo do arquivo gerado (só as chaves que você quiser mudar precisam estar presentes — o que faltar cai no padrão):

```json
{
  "_schema_version": 1,
  "storage": {
    "dirs": ["/mnt/disk1/backups", "/mnt/disk2/backups"],
    "replication_factor": 2,
    "fallback_threshold_gb": 10.0,
    "encryption_enabled": false,
    "encryption_key": ""
  },
  "ssd_cache": { "enabled": false, "dir": "", "max_gb": 20.0 },
  "database":  { "url": "", "path": "/mnt/disk1/backup.db" },
  "db_backup": { "enabled": true, "retention": 7, "hour": 1, "minute": 0 },
  "digest": {
    "hour": 18,
    "telegram_bot_token": "", "telegram_chat_id": "",
    "anthropic_api_key": "",
    "ollama_url": "http://localhost:11434", "ollama_model": "llama3"
  },
  "rclone": { "config_path": "" }
}
```

#### Migrando de uma instalação anterior à v8.0.0

Não é preciso reconfigurar nada à mão. Suba a v8.0.0 **uma vez com as `Environment=` ainda no lugar**: como não existe `config.json`, o servidor gera o arquivo a partir das variáveis atuais e loga o que migrou.

```
[config] migrando 6 variavel(is) de ambiente: STORAGE_DIRS, REPLICATION_FACTOR, DB_PATH, ...
[config] /home/pi/backup_system/server/config.json gerado a partir do ambiente
```

Depois disso: confira os valores em `/settings`, remova as linhas `Environment=` da unit systemd (menos `BACKUP_API_KEY`) e faça `daemon-reload`. A partir daí o arquivo é a única fonte da verdade — variáveis de ambiente deixadas para trás passam a ser **ignoradas**, não sobrescrevem o arquivo.

#### Referência dos parâmetros

Os parâmetros marcados com **↻** só passam a valer depois de reiniciar o servidor (volumes, engine do banco e chave de criptografia são fixados no import). Os demais são aplicados na hora ao salvar, inclusive o reagendamento dos jobs cron.

**`storage`** — armazenamento e replicação

| Chave | Tipo | Padrão | ↻ | Descrição |
|---|---|---|:-:|---|
| `dirs` | lista | `["./storage"]` | ↻ | Volumes em ordem de prioridade. Um único diretório também é válido |
| `replication_factor` | int | `1` | | `1` = sem replicação; `2` = espelha em 2 volumes; `0` = todos os volumes saudáveis |
| `fallback_threshold_gb` | float | `10.0` | | Piso de espaço livre por disco antes de passar para o próximo da lista |
| `encryption_enabled` | bool | `false` | ↻ | Criptografia AES-256-GCM em repouso. Omitir se o disco já é criptografado (LUKS, ZFS, FileVault) |
| `encryption_key` | str 🔒 | `""` | ↻ | 32 bytes em Base64. Obrigatória quando `encryption_enabled` é `true` |

Gere a chave com:

```bash
python3 -c 'import os,base64; print(base64.b64encode(os.urandom(32)).decode())'
```

**`ssd_cache`** — cache tier em SSD

| Chave | Tipo | Padrão | ↻ | Descrição |
|---|---|---|:-:|---|
| `enabled` | bool | `false` | ↻ | Habilita o staging de uploads no SSD |
| `dir` | str | `""` | ↻ | Diretório **no SSD**. Obrigatório quando `enabled` é `true` |
| `max_gb` | float | `20.0` | | Limite de uso do SSD pela fila pendente (GB) |

Quando habilitado, uploads são escritos no SSD e o servidor responde ao cliente imediatamente; a movimentação para o HDD ocorre em background. Se o SSD atingir o limite ou tiver menos de 2 GB livres, o upload recai silenciosamente para o HDD. Moves pendentes sobrevivem a reinicializações (persistidos em `ssd_cache_pending_moves` no banco).

> **Não use MicroSD como `ssd_cache.dir`.** Write sequencial de cartões rápidos (~130 MB/s) é marginalmente melhor que HDD, mas sofrem throttling térmico sob carga e têm endurance muito inferior a um SSD real. O benefício é nulo e o desgaste é alto.

**`database`** — backend do banco

| Chave | Tipo | Padrão | ↻ | Descrição |
|---|---|---|:-:|---|
| `url` | str 🔒 | `""` | ↻ | DSN do PostgreSQL (`postgresql://user:pass@host/db`). Vazio = SQLite |
| `path` | str | `"./backup.db"` | ↻ | Arquivo do SQLite, usado quando `url` está vazio |

**`db_backup`** — backup automático do próprio banco

| Chave | Tipo | Padrão | ↻ | Descrição |
|---|---|---|:-:|---|
| `enabled` | bool | `true` | | Habilita o backup automático do banco |
| `retention` | int | `7` | | Número máximo de backups mantidos por volume |
| `hour` | int | `1` | | Hora de execução (0–23, horário local) |
| `minute` | int | `0` | | Minuto de execução (0–59) |

O backup exporta o banco para `_db_backups/` em **cada volume saudável** listado em `storage.dirs`. Para PostgreSQL usa `pg_dump --format=custom` (requer `pg_dump` no PATH); para SQLite usa `sqlite3.backup()` — cópia consistente sem travar leituras em andamento. Cada arquivo recebe timestamp no nome (`nestvault_db_YYYYMMDD_HHMMSS.dump|db`). Backups além do limite de retenção são removidos automaticamente.

> **Por que isso importa?** Com 1 SSD + N HDDs, o SSD guarda o banco (mapa sha256 → caminhos físicos, versões, labels). Se o SSD falhar, os arquivos dos HDDs ficam intactos mas irrecuperáveis sem o banco. O backup automático resolve isso exportando o banco para os próprios HDDs.

**`digest`** — resumo diário via Telegram

| Chave | Tipo | Padrão | ↻ | Descrição |
|---|---|---|:-:|---|
| `hour` | int | `18` | | Hora de envio (0–23, horário local) |
| `telegram_bot_token` | str 🔒 | `""` | | Token do bot gerado pelo @BotFather |
| `telegram_chat_id` | str | `""` | | ID do chat que receberá o digest |
| `anthropic_api_key` | str 🔒 | `""` | | Usa Claude Haiku para gerar o resumo ([console.anthropic.com](https://console.anthropic.com)) |
| `ollama_url` | str | `http://localhost:11434` | | Fallback local quando não há `anthropic_api_key` |
| `ollama_model` | str | `llama3` | | Modelo Ollama a usar |

**Como obter o `telegram_chat_id`:** crie o bot com @BotFather, mande qualquer mensagem para ele e acesse `https://api.telegram.org/bot<TOKEN>/getUpdates` no browser — o campo `chat.id` no JSON é o valor a usar.

Sem `telegram_bot_token` e `telegram_chat_id` o digest é gerado internamente mas não enviado. Sem chave de IA o servidor envia um resumo estruturado com os dados brutos do banco.

**`rclone`** — cloud backup

| Chave | Tipo | Padrão | ↻ | Descrição |
|---|---|---|:-:|---|
| `config_path` | str | `""` | | Caminho do `rclone.conf`, repassado ao binário via `RCLONE_CONFIG`. Vazio usa `~/.config/rclone/rclone.conf` do usuário que executa o processo |

Útil quando o servidor roda como serviço systemd com usuário diferente do que configurou o rclone. O rclone precisa estar instalado e acessível no `PATH`.

#### 🔒 Segredos

Os campos marcados com 🔒 nunca são devolvidos em texto puro pelo `GET /api/settings` — a API responde com uma máscara (`••••••5678`) e um flag `is_set`. Na tela, deixar um campo de segredo em branco **mantém** o valor atual; só um valor novo e não-vazio substitui o que está gravado.


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
# Único segredo que continua no ambiente: cria o primeiro admin no boot inicial.
Environment="BACKUP_API_KEY=sua-chave-aqui"
# Opcional — só se o config.json não estiver em WorkingDirectory/config.json
# Environment="NESTVAULT_CONFIG=/etc/nestvault/config.json"
# Todo o resto (volumes, replicação, criptografia, SSD cache, digest, backup do
# banco) vive em config.json e é editável em /settings — ver "2. Configuração".
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

O `Restart=always` é o que faz o botão **Reiniciar servidor** da tela de Configurações funcionar: o endpoint encerra o processo e o systemd o sobe de novo. Sem supervisor, o servidor simplesmente para.

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

> **v7.9 — autenticação por usuário:** a chave configurada aqui identifica uma conta específica no servidor (admin ou usuário comum), não mais uma senha global. Peça ao administrador do servidor para criar uma conta em `/manage-users` e usar a chave gerada — cada conta só enxerga e restaura seus próprios backups (admins continuam com acesso a tudo).
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

# Ajustar a válvula de segurança do smart skip (padrão: 7 dias)
nestvault backup ~/documentos \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000 \
  --full-rescan-days 3
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
| `--full-rescan-days` | | Idade máxima (dias) da última versão `done` para usar o Smart Skip — veja [Smart Skip](#smart-skip) (padrão: `7`) |
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

Quando o Smart Skip é acionado (nada mudou desde a última versão), a linha `Herdados` aparece com `(smart skip)` em vez de `(modo acumulativo)`, e todos os arquivos aparecem em `Cacheados`:

```
=======================================================
  Backup      : [notebook-joao]
  Versao      : 2026-04-25T11:00:00
  Verificados : 142
  Enviados    : 0
  Registrados : 0
  Cacheados   : 142
  Ignorados   : 0
  Erros       : 0
  Herdados    : 142  ← smart skip: nada mudou, um único /absorb
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

**Otimização client-side (v7.8+):** clientes com `--accumulate`/`accumulate` identificam arquivos inalterados cujo path + sha256 já estão na versão anterior e os retiram do pipeline de registro — eles são herdados pelo mesmo `/absorb` final em vez de gerar um request de registro cada. Isso reduz ainda mais o número de requests num backup incremental típico, sem mudar a semântica do absorb no servidor.

---

### Smart Skip

*(v7.11+)* Muitos backups agendados via cron rodam repetidamente sobre diretórios que **não mudam** entre execuções (ex.: um backup noturno de uma pasta que só recebe arquivos novos ocasionalmente). Nesses casos, gastar hash + `/check/batch` + registro para centenas ou milhares de arquivos que já estão idênticos no servidor é desperdício puro de CPU e round-trips de rede.

**Como funciona:**

O cliente sempre precisa varrer o diretório local (`os.walk` + `stat`) para detectar deleções — isso não tem como pular. Mas se essa varredura mostrar que **nada mudou** desde a última versão `done` (nenhum arquivo novo, modificado ou deletado — o conjunto de paths é idêntico), o backup inteiro vira uma única chamada a `/absorb` que clona a versão anterior por completo, em vez de hashear/checar/registrar arquivo por arquivo.

```
Backup 1 — diretório com 142 arquivos:
  hash + check + upload de tudo (primeira vez)
  versão 2026-04-25T10:42:31 → 142 arquivos

Backup 2 — mesmo diretório, nada mudou:
  varre o diretório, confirma que os 142 paths batem com a versão anterior
  smart skip: um único /absorb, sem hash/check/register individual
  versão 2026-04-25T11:00:00 → 142 arquivos (herdados)
```

**Válvula de segurança:** o smart skip só é usado se a última versão `done` tiver no máximo `--full-rescan-days` dias (padrão `7`). Depois desse prazo, o backup volta a fazer a verificação completa normalmente, mesmo que nada pareça ter mudado — evita uma cadeia indefinida de `/absorb` sobre `/absorb` sem nunca reconferir o conteúdo real dos arquivos no disco.

**Quando o smart skip *não* é usado:**

| Situação | Comportamento |
|---|---|
| Algum arquivo novo, modificado ou deletado | Backup normal (pipeline de hash/check/upload) |
| `--accumulate` ativo | Segue a semântica própria do modo acumulativo (veja acima) |
| Nenhuma versão `done` anterior (primeiro backup) | Backup normal |
| Última versão `done` mais velha que `--full-rescan-days` | Backup normal, mesmo sem mudanças aparentes |
| Um arquivo sumiu entre a varredura do diretório e a leitura do `stat` (race condition) | Backup normal — o smart skip é desabilitado nesse run para não arriscar clonar um estado com uma deleção não capturada |

**Cache local de hash:** para tornar a varredura em si mais barata, o cliente também mantém um cache local por label (`~/.cache/nestvault` no Linux, `~/Library/Caches/nestvault` no macOS, `%LOCALAPPDATA%\nestvault` no Windows) com o índice `path → {sha256, size, mtime}` da última versão enviada por essa máquina. Se o `version_key` desse cache local ainda bater com o último "done" do servidor, o cliente evita o `GET /files` completo (que baixa e faz parse do índice inteiro da versão anterior) e usa o cache local direto. Cache ausente ou desatualizado simplesmente volta ao fetch completo de sempre — nunca é tratado como fonte de verdade não verificada.

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
| `--workers` | | Downloads paralelos (padrão: `4`) |
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

Força a re-replicação de todos os arquivos que possuem menos cópias físicas do que o `storage.replication_factor` configurado no servidor. Use após:

- Adicionar um disco novo ao cluster (arquivos existentes não são replicados automaticamente)
- Recuperar um disco que ficou `degraded` por um longo período
- Aumentar o valor de `storage.replication_factor`

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

Reconcilia o acervo inteiro com o `storage.replication_factor` atual do servidor, resolvendo **ambas** as direções:

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

Cifra todos os arquivos físicos que ainda não foram criptografados. Use após ativar `storage.encryption_enabled` no servidor para migrar um acervo existente. Requer que o servidor esteja rodando com a criptografia habilitada.

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
- Novos uploads feitos com `storage.encryption_enabled` já chegam cifrados; o `encrypt-existing` trata apenas o acervo pré-v3.1.

> Requer NestVault v3.1+ no servidor. Em servidores mais antigos, retorna `404` com mensagem de erro clara.

---

### Limpeza automática por espaço em disco

O servidor verifica automaticamente o espaço livre **ao finalizar cada backup** (status → `done`). Se o espaço livre no disco estiver abaixo de **5%**, versões antigas são apagadas até que o espaço seja normalizado. Essa verificação ocorre **em background** — o cliente recebe a confirmação do backup imediatamente, sem esperar o scan de disco.

Da mesma forma, ao excluir um label (`DELETE /backups/{label}`) ou uma versão (`DELETE /backups/{label}/versions/{key}`), a remoção dos registros no banco é imediata, mas a limpeza dos arquivos físicos órfãos ocorre em background.

**Comportamento:**

- Com múltiplos discos (`storage.dirs`), verifica o **menor** percentual livre entre todos os volumes — o cleanup dispara se **qualquer** disco estiver abaixo de 5%
- Com um único volume em `storage.dirs`, verifica o espaço do filesystem onde o storage está montado
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
| `test_auth.py` | Rejeição sem chave, rejeição com chave errada, acesso liberado com chave válida, `403` de usuário comum em endpoint admin, usuário desativado perde acesso *(v7.9)* |
| `test_user_isolation.py` *(v7.9)* | Backup por usuário: listagem escopada por dono, bloqueio de leitura/escrita/download cruzado entre usuários, admin com acesso irrestrito |
| `test_replication.py` | `/maintenance/rereplicate` e `/maintenance/reconcile-replication` — sub-replicação e sobre-replicação |
| `test_disks.py` | `GET /storage/disks` — status de volumes, contagem de cópias físicas por volume |
| `test_rclone_walk.py` | Walk incremental do rclone — conclusão + limpeza de checkpoint, resume de diretório falho, falha de listagem isolada, skip por mtime, dispatch por backend, override de `strategy`, `_MAX_RESUMES`, batching cross-directory, pastas protegidas |

---

## ☁️ Cloud Backup

O cloud backup do NestVault usa o **rclone** como único backend. O rclone suporta 70+ provedores (Google Drive, OneDrive, S3, Backblaze B2, Dropbox e outros) e gerencia toda a autenticação localmente — sem necessidade de registrar apps no Google Cloud Console ou Azure Portal.

| | Via rclone |
|---|---|
| **Provedores** | 70+ (Drive, OneDrive, S3, B2, Dropbox…) |
| **Setup** | Instalar rclone e rodar `rclone config` |
| **Tokens** | Gerenciados pelo rclone em `~/.config/rclone/rclone.conf` |
| **Endpoints** | `/rclone/*` |

O NestVault não armazena tokens — o rclone gerencia autenticação externamente. Isso elimina a necessidade de registrar aplicativos OAuth nos portais dos provedores.

### Pré-requisito: instalar o rclone

```bash
# Linux / Raspberry Pi
sudo apt install rclone

# macOS
brew install rclone

# Ou via script oficial (todas as plataformas)
curl https://rclone.org/install.sh | sudo bash
```

### Configurar Google Drive no rclone

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

### Configurar OneDrive no rclone

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

### Configurar iCloud no rclone

```bash
rclone config
```

```
n) New remote
name> icloud                    # nome que você escolhe

Storage> iclouddrive            # ou "iCloud Drive"

apple_id> voce@icloud.com
password>                       # senha da conta Apple (não é senha de app)

service> drive                  # "drive" para arquivos; "photos" para a fototeca

# 2FA: o rclone pede o código de 6 dígitos exibido no seu dispositivo confiável
```

Teste:

```bash
rclone lsd icloud:              # lista pastas na raiz
```

Três particularidades do backend iCloud que afetam a operação do NestVault:

**O `trust_token` expira em 30 dias.** Passado esse prazo, todos os jobs desse remote passam a falhar com erros de autenticação (`HTTP error 421`, `Invalid session token`). A renovação é manual e exige o código 2FA:

```bash
rclone config reconnect icloud:
```

**O `rclone.conf` precisa ser gravável pelo usuário que roda o servidor.** Diferente do Google Drive e do OneDrive, o backend iCloud regrava cookies e `trust_token` no arquivo a cada renovação de sessão. Se o arquivo for somente-leitura para o usuário do systemd, a sessão é perdida a cada run e o job falha de forma intermitente com `421 (Invalid global session)`.

**Um job por remote de cada vez.** O NestVault serializa os runs por `remote_name` — dois processos rclone reautenticando em paralelo sobrescrevem os cookies um do outro e invalidam a sessão. Um run (agendado ou manual) disparado enquanto outro está ativo no mesmo remote é descartado com aviso no log; o cron volta no horário seguinte.

> **Packages do macOS** (`.pages`, `.numbers`, `.key`, `.playgroundbook`, `.xcodeproj`) são pastas que o iCloud entrega como **zip**, mas cujo tamanho é reportado descompactado. O rclone acusa `corrupted on transfer: sizes differ` e descarta o arquivo; o NestVault detecta esse caso específico e refaz o lote com `--ignore-size`. O arquivo é armazenado como o zip que a Apple entrega — é a única forma disponível pela API.

### Configurar Dropbox, S3, Backblaze B2 e outros

O processo é o mesmo: `rclone config` → escolher o provedor → seguir o assistente. Consulte a [documentação do rclone](https://rclone.org/docs/) para cada provedor. Após configurado, o nome do remote funciona igual no NestVault.

### Verificar remotes configurados

```bash
rclone listremotes
# gdrive:
# onedrive:
# mys3:
```

O endpoint `GET /rclone/remotes` retorna a mesma lista via API.

### Criar um job de backup rclone

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

### Servidor sem browser (Raspberry Pi / VPS)

O rclone tem suporte nativo para autenticação em máquinas headless. Na etapa `Use web browser to authenticate rclone?`, responda `n`. O rclone exibe uma URL — abra no seu computador pessoal, autorize, e o rclone no servidor aguarda o código ser colado no terminal.

Alternativamente, configure o rclone no seu computador pessoal e copie o arquivo de configuração para o servidor:

```bash
# No computador pessoal
rclone config        # configura gdrive, onedrive, etc.
cat ~/.config/rclone/rclone.conf

# Copie o conteúdo para o servidor
scp ~/.config/rclone/rclone.conf pi@192.168.1.100:~/.config/rclone/rclone.conf
```

O NestVault usa o `rclone.conf` padrão do usuário que roda o servidor. Para um path customizado, defina `rclone.config_path` em Configurações — o valor é repassado ao binário via `RCLONE_CONFIG`.

---

### Funcionamento interno

Cada job usa uma de duas estratégias de listagem, escolhida automaticamente por backend ou forçada pelo campo **Estratégia** do job (`auto` | `fast`/recursiva | `walk`/incremental):

**Caminho rápido (`fast`)** — OneDrive, Google Drive, iCloud Drive e demais backends com listagem recursiva eficiente:
- Uma única `rclone lsjson --recursive --fast-list` varre a pasta inteira num só processo
- Download em **lotes** via `rclone copy --files-from` (até 250 arquivos ou 3 GB por lote), escopado à raiz do job — resolve nomes unicode/acentuados que falhavam com paths explícitos
- Pipeline producer/consumer: enquanto um lote baixa, o anterior é hasheado/registrado

**Walk incremental (`walk`)** — iCloud Photos (lento, com rate-limit, listagem recursiva não completa):
- Lista **um diretório por vez** (`rclone lsjson` não recursivo), enfileirando subpastas
- O download reaproveita o mesmo mecanismo de lotes do caminho rápido, agrupando arquivos de qualquer diretório
- **Checkpoint resumível** em `BackupVersion.progress_json` (salvo a cada 5 min): um run interrompido retoma na mesma versão sem re-listar diretórios concluídos; diretórios com falha são re-tentados no próximo resume; após 3 resumes sem concluir, a versão é abandonada e uma nova é criada

Comum às duas estratégias:
- SHA-256 calculado após o download do lote; arquivos idênticos são detectados por deduplicação — nenhum byte extra no disco
- Criptografia e replicação funcionam normalmente — o backup cloud é tratado igual ao backup via cliente CLI
- Arquivos com `mtime` inalterado em relação à versão anterior são ignorados sem re-download — runs recorrentes em pastas estáticas são significativamente mais rápidos
- Erros por arquivo são tolerados — o job continua e registra o erro na última mensagem
- Tokens gerenciados pelo rclone em `~/.config/rclone/rclone.conf` — NestVault não os armazena nem os acessa

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

Na primeira visita, o browser pedirá a API Key — salva no `localStorage`. Para trocar, clique em **⌀ API Key** no header.

> **v7.9 — dashboard é admin-only.** O painel web (stats, discos, manutenção, atividade, rclone, usuários) exige uma chave com `role=admin`; uma chave de usuário comum recebe a tela de login novamente com "Esta chave não tem permissão de administrador." O caminho de backup/restore do usuário comum é o CLI (`nestvault.py`), cuja API (`/backups`, `/files`, etc.) já é escopada por dono.

**O que o dashboard exibe:**

- **Stats globais** — total de backups, versões, arquivos, storage total
- **Disco livre** — espaço disponível no disco montado com barra visual de uso e percentual *(v2.7)*
- **Espaço liberável** — quanto seria recuperado apagando versões antigas (mantendo 1 por label) *(v2.7)*
- **Tabela de backups** — clique em um label para expandir as versões; coluna **Usuário** mostra o dono de cada backup *(v7.9)*
- **Versões** — clique em uma versão para ver os arquivos
- **Comparação de versões** — selecione duas versões com as checkboxes e clique em ⇄ Comparar: veja arquivos adicionados, removidos, modificados e o delta de tamanho de cada um
- **Cloud Backup (rclone)** — gerencie jobs de backup rclone agendados e execute manualmente via `/rclone-jobs`
- **Manutenção** — página dedicada a operações administrativas de storage:
  - **Limpeza de Órfãos** — remove arquivos físicos sem referência em nenhuma versão ativa
  - **Re-replicar** — cria cópias faltantes para conteúdos com menos réplicas que `storage.replication_factor`
  - **Reconciliar Replicação** — remove cópias excedentes e preenche faltantes em uma só operação
  - **Cifrar Existentes** — cifra arquivos não criptografados (requer confirmar digitando `CIFRAR` — irreversível)
  - **Limpar Versões Antigas** — mantém apenas N versões mais recentes de um label escolhido
  - **Excluir Versões por Data** *(v5.0)* — exibe preview por label de quantas versões serão removidas antes de uma data; a versão `done` mais recente de cada label é sempre preservada
  - **Excluir Label Completo** — exclui um label e todas as suas versões (requer digitar o nome do label)
  - **Reatribuir Dono** *(v7.9)* — transfere a posse de um backup para outro usuário; necessário para labels criados antes da migração para backup por usuário (ficam com o admin) ou ao reorganizar contas
- **Usuários** *(v7.9)* — página `/manage-users`: cria contas (admin ou usuário comum), gira chaves e ativa/desativa acesso. A chave gerada é exibida uma única vez
- **Configurações** *(v8.0)* — página `/settings`, ver abaixo
- **Discos** — página `/disks` com painel de volumes: espaço total/livre/usado, arquivos físicos por volume e status (ok/degraded)
- **Explorer de arquivos** — navegação e download de arquivos de uma versão específica via `/explorer`
- **Backups em tempo real** — indicador no cabeçalho com contagem de backups em andamento; polling automático a cada 3 s com botão ⏸ para pausar

### ⚒ Tela de Configurações

`/settings` — restrita a admins. Edita o `config.json` descrito em [2. Configuração](#2-configuração-configjson) sem SSH.

Os campos são renderizados a partir do schema devolvido pelo `GET /api/settings`, então um parâmetro novo em `server/config.py` aparece na tela automaticamente, com o rótulo, a ajuda e a validação que o schema declara.

- **Um cartão por grupo**, com botão de salvar próprio — você envia só o grupo que mexeu
- **Badge `requer reinício`** nos parâmetros estruturais (volumes, criptografia, banco, diretório do SSD cache); os demais valem no instante em que você salva, incluindo o reagendamento dos jobs de digest e backup do banco
- **Faixa de reinício pendente** aparece quando algum parâmetro ↻ foi alterado, com o botão **⏻ Reiniciar servidor** (pede a palavra `REINICIAR`). A página fica aguardando o `/health` responder e recarrega sozinha quando o servidor volta
- **Segredos** mostram máscara e a nota *"deixe em branco para manter"* — o valor real nunca chega ao browser
- **Erros de validação** aparecem no rodapé do próprio cartão, com o nome do campo e a faixa aceita

Mudar a criptografia com conteúdo já gravado abre uma confirmação por palavra-chave antes de enviar: os arquivos existentes ficam ilegíveis com a chave nova.

---

## ⚡ Otimizações

### v7.14.0

| Componente | Mudança |
|---|---|
| **`server/sysmetrics.py`** | Novo módulo — amostra CPU/memória/swap/load/temperatura/uptime via `/proc` e `/sys/class/thermal` (stdlib puro, sem `psutil`), com histórico em ring buffer de 60 pontos (~5 min a 5s/amostra) para o sparkline. CPU calculada por delta contra a amostra anterior, tratando `iowait` como tempo ocioso (convenção do `top`/`htop`) |
| **`server/main.py` — `_system_metrics_loop` / `ActivityResponse.system`** | Task de background amostra a cada 5s fora do caminho do request; `GET /api/activity` expõe o resultado sem I/O adicional por chamada. Campo opcional — cobre cold start e hosts sem `/proc` sem derrubar o endpoint |
| **`server/static/activity.html` — seção "Sistema"** | Cards de CPU e Memória com sparkline SVG e barras por núcleo, tiles de Load/Swap/Temperatura/Uptime. `render()` passou a excluir `system` do diff de `_lastRenderKey` para essas métricas (que mudam a cada poll) não recriarem o DOM das demais seções da página |
| **`tests/test_sysmetrics.py`** | Novo — 8 casos cobrindo parsing de `/proc/stat`/`/proc/meminfo`, cálculo de delta de CPU, proteção contra divisão por zero, cap do histórico e degradação graciosa sem `/proc` |

### v7.13.0

| Componente | Mudança |
|---|---|
| **`server/nightly_cleanup.py` — `_version_fingerprint`** | Hash sha256 do conjunto `(original_path, sha256)` de uma versão `done`, lido com `yield_per(1000)` — identifica quando duas versões consecutivas do mesmo label têm conteúdo idêntico |
| **`server/nightly_cleanup.py` — `_prune_unchanged_versions`** | Roda depois da retenção temporal sobre as versões sobreviventes: remove as `done` com fingerprint igual à anterior no label, mantendo a primeira de cada bloco igual e sempre a última `done` do label |
| **`server/nightly_cleanup.py` — `run_nightly_cleanup`** | `survivors` passa a ser derivado de `keep_ids` (calculado antes de `_delete_versions`), não reacessando atributos de versões já deletadas — o `commit()` da deleção expira todos os objetos da sessão, e reler um atributo de uma instância apagada explode com `ObjectDeletedError` |
| **`tests/test_nightly_cleanup.py`** | Novo — 10 casos cobrindo fingerprint, poda em blocos, casos sem alteração real, as faixas da retenção existente e dois testes de ponta a ponta via `run_nightly_cleanup()` (nenhum teste cobria este módulo antes) |

### v7.12.0

| Componente | Mudança |
|---|---|
| **`server/main.py` — `_build_stats_data` (alterações por dia)** | Agregação feita no banco em 3 queries de contagem, sem trazer nenhuma linha de `version_files` para o Python: `LAG(id)` dá a versão predecessora e as contagens saem de `added = total − same_path`, `removed = total_anterior − same_path`, `modified = same_path − same_both`. A versão anterior comparava os dicionários de arquivos em Python e não terminava em 10 min |
| **`server/main.py` — `_get_reclaimable_bytes`** | Anti-join `LEFT JOIN` + `IS NULL` sobre `file_contents` inteiro trocado por `total − retido`, com a varredura partindo do conjunto pequeno (shas retidos) e entrando em `file_contents` pela primary key. O formato anterior prendia um core por mais de 15 min |
| **`server/main.py` — `reclaimable_by_label` (Q11)** | Mesmo anti-join reescrito com `NOT EXISTS`, coberto por `idx_sha256` — de >120 s (abortado) para ~25 s |
| **`server/main.py` — `_refresh_stats_async` / `GET /api/stats`** | Stats saem do caminho do request: cache vencido é servido na hora e recalculado numa thread de background (uma por vez, via `threading.Lock`), com aquecimento no `lifespan`. Antes, o primeiro request após o TTL de 300 s pagava a agregação inteira |
| **`server/static/sw.js`** | Cache versionado (`nestvault-static-v2`) — os assets de `/static/` são cache-first e o HTML vem sempre da rede, então sem o bump o navegador combinava HTML novo com `theme.css` antigo e as variáveis `--chart-*` ficavam indefinidas (gráficos invisíveis) |
| **Medição** | Banco sintético de 105 versões / 525k `version_files` num Raspberry Pi: `_build_stats_data` de >15 min para 30,7 s; `/api/stats` responde em ~10 ms; `/api/activity` mantém 13 ms durante o recálculo |

### v7.11.0

| Componente | Mudança |
|---|---|
| **`client/nestvault.py` — `backup_directory` (modo batch)** | Reescrito de fases estanques (hash tudo → check tudo → upload tudo) para pipeline sobreposto: hashing consumido incrementalmente via `ProcessPoolExecutor` + `as_completed` em vez de `pool.map()` bloqueante; buffer de resultados dispara `/check/batch` a cada `--batch-size` hashes prontos, sem esperar o restante |
| **`client/nestvault.py` — pool de check dedicado** | Chunks de `/check/batch` passam a rodar em `ThreadPoolExecutor(min(4, workers))` próprio, em paralelo — antes era um `for` sequencial bloqueante antes de qualquer upload começar |
| **`client/nestvault.py` — cache hits imediatos** | Arquivos inalterados (mtime+size) não dependem do hashing dos demais: disparam para `/register/batch` assim que identificados, em paralelo com o hashing dos arquivos novos/modificados |
| **`client/nestvault.py` — `_smart_skip_eligible` / Smart Skip** | Quando a varredura confirma que nada mudou desde a última versão `done` (sem novos/modificados/deletados) e ela tem no máximo `--full-rescan-days` dias (padrão 7), o backup vira uma única chamada `/absorb` em vez de hash/check/register por arquivo — veja [Smart Skip](#smart-skip) |
| **`client/nestvault.py` — cache local de hash** | `_load_local_hash_cache`/`_save_local_hash_cache` espelham localmente o índice `path → {sha256, size, mtime}` da última versão enviada por label; usado no lugar do `GET /files` completo quando o `version_key` local bate com o último "done" do servidor |
| **`client/nestvault.py` — `_local_cache_dir`** | Diretório de cache detectado por SO: `~/Library/Caches` no macOS, `%LOCALAPPDATA%` no Windows, `$XDG_CACHE_HOME`/`~/.cache` no Linux |
| **`tests/test_client_pipeline.py`** | Novo — 18 casos cobrindo `_smart_skip_eligible`, `_version_age_days`, `_chunked` e o round-trip do cache local de hash (incluindo os três ramos de SO) |
| **Origem** | Estratégias portadas do cliente macOS (`NestVaultClient`), que já usava um pipeline `AsyncStream` producer/consumer equivalente — sem mudanças na API do servidor |

### v7.10.0

| Componente | Mudança |
|---|---|
| **`server/static/explorer.html`** | Reescrito: navegador em colunas (Miller columns) substitui a árvore + lista de arquivos; `openPath` (array de segmentos) substitui `selectedPath` e passa a ser refletido em `?path=` na URL |
| **`server/static/explorer.html` — `restoreOpenPathForVersion()`** | Nova função única usada nos três pontos que trocam de versão (Anterior/Próxima, `popstate`, carga inicial) — mantém o maior prefixo do caminho que ainda existe na versão carregada, com aviso (`.explorer-hint`) quando há fallback para um ancestral |
| **`server/static/explorer.html` — breadcrumb** | Passa a incluir o caminho da pasta aberta, com cada segmento clicável (`renderBreadcrumbPath()`) |
| **`server/static/explorer.html` — `#verSelect`** | Novo dropdown de versões ao lado do Anterior/Próxima, compartilhando a lógica de troca em `goToVersion()` |

### v7.9.0

| Componente | Mudança |
|---|---|
| **`server/database.py` — `User`** | Nova tabela `users`: `username` (unique), `api_key_hash` (SHA-256, nunca a chave em texto puro), `role` (`admin`/`user`), `is_active`. `hash_api_key()` centraliza o hashing |
| **`server/database.py` — `BackupID.owner_user_id`** | Nova coluna (FK nullable para `users.id`) — migração idempotente via `ALTER TABLE` no `init_db()`, mesmo padrão de `progress_json`/`strategy`/`encrypted` |
| **`server/database.py` — `bootstrap_admin_user` / `_backfill_backup_owners`** | Rodam no boot: se não existe nenhum `User`, cria o admin a partir de `BACKUP_API_KEY` e atribui todo `BackupID` sem dono a ele — migração sem downtime |
| **`server/auth.py`** | Reescrito: `get_current_user` (resolve `User` a partir do hash da `X-API-Key`), `require_admin` (403 se `role != "admin"`), `require_owner_or_admin` (403 se não é dono nem admin) |
| **`server/main.py` — `_get_backup_or_404` / `_get_version_or_404`** | Ganham parâmetro opcional `user` — quando presente, aplicam `require_owner_or_admin`; propaga a checagem de posse para quase todos os endpoints que já usavam essas funções |
| **`server/main.py` — `GET /files/{id}/download`** | Passa a fazer JOIN `VersionFile → BackupVersion → BackupID` para checar o dono antes de servir o arquivo — antes não havia checagem nenhuma nesse endpoint |
| **`server/main.py` — `/users`, `/users/{id}/rotate-key`, `/users/{id}`, `/backups/{label}/owner`** | Novos endpoints admin-only para criar/listar/desativar usuários, rotacionar chave e reatribuir dono de um backup |
| **`server/cloud/rclone_router.py`** | Todos os endpoints `/rclone/*` passam a exigir `require_admin` |
| **`server/static/users.html`** | Nova tela `/manage-users`: criar usuário (chave exibida uma única vez), girar chave, ativar/desativar |
| **`server/static/maintenance.html`** | Novo card "Reatribuir Dono" (`PATCH /backups/{label}/owner`) |
| **`server/static/*.html`** | Todas as páginas passam a tratar `403` (chave válida sem permissão de admin) além do `401` já existente |
| **`client/nestvault.py` — `_AuthSession`** | Intercepta `403` e levanta `HTTPError` com mensagem "Acesso negado: `<detail>`" em vez do erro genérico do `raise_for_status()` |
| **`tests/conftest.py`** | `client` passa a autenticar como admin por padrão (chave fixa); novo fixture `two_users` (admin + dois usuários comuns no mesmo banco) para testar isolamento |
| **`tests/test_user_isolation.py`** | 8 casos novos cobrindo listagem escopada, escrita/leitura/download cruzados entre usuários e bypass do admin |

### v7.8.0

| Componente | Mudança |
|---|---|
| **`server/main.py` — `POST /register/batch`** | Novo endpoint: registra até 500 arquivos por request. Duas queries `IN` (conteúdos existentes no storage + `VersionFile`s já registrados nesta versão para os paths do lote) substituem N queries; upsert dividido em update (path já existente na versão) + `bulk_insert_mappings` (novos), respeitando `uq_version_path` sem depender de `ON CONFLICT` — portável entre SQLite e PostgreSQL; **um único `db.commit()` por lote** |
| **`server/main.py` — schemas** | Novos `RegisterBatchItem`, `RegisterBatchRequest`, `RegisterBatchResultItem`, `RegisterBatchResponse` |
| **`server/main.py` — `_bg_ensure_replicas_batch`** | Réplicas dos conteúdos do lote garantidas via `BackgroundTasks` após a resposta — mesmo padrão do `/upload`, que cria réplicas fora do write-lock |
| **Semântica de erro parcial** | Item cujo sha256 não existe no storage volta `registered: false` sem abortar o lote (o cliente escala para upload); versão não-`running`: 409, como o `/absorb` |
| **`tests/test_register_batch.py`** | 7 casos novos: lote todo novo, conteúdo ausente no meio do lote, upsert de path existente, path duplicado no lote (último vence), versão não-running, versão inexistente, lote vazio |
| **`client/nestvault.py`** | CLI adota o endpoint: registers (cache hits + conteúdo existente) coalescidos em lotes de `--batch-size`; gate único `_server_version()` (substitui `_server_supports_batch`) alimenta os gates de `/check/batch` (≥2.6) e `/register/batch` (≥7.8); lote que falha cai para registro individual por arquivo |
| **Cliente macOS (`NestVaultClient`)** | Mesma adoção — registers do pipeline coalescidos em lotes de 200, compartilhando o pool de workers com os uploads; fallback simétrico ao do CLI |

### v7.1.0

| Componente | Mudança |
|---|---|
| **`server/database.py` — dual backend** | Detecção automática de `DATABASE_URL`: PostgreSQL com `pool_pre_ping`; SQLite com WAL + NullPool (comportamento anterior preservado integralmente) |
| **`server/database.py` — `BigInteger`** | `FileContent.size` alterado de `Integer` para `BigInteger` — suporte a arquivos > 2 GB no PostgreSQL (SQLite ignora a distinção) |
| **`server/requirements-postgres.txt`** | Novo arquivo opcional com `psycopg2-binary`; não incluído no `requirements.txt` principal para não quebrar Raspberry Pi 32-bit sem wheel pré-compilado |
| **`tools/migrate_to_postgres.py`** | Script SQLite → PostgreSQL: coerção de booleanos (0/1 → bool), correção automática de `INTEGER → BIGINT` em tabelas já criadas, fault-tolerance com divisão binária de batches para contornar corrupção física no SQLite |
| **`tools/migrate_to_sqlite.py`** | Script reverso PostgreSQL → SQLite: permite voltar ao modo leve ou criar backup portátil do banco |
| **`README.md`** | Nova seção `## 🐘 PostgreSQL (opcional)` com instalação, configuração, migração e reversão |

### v7.3.0

| Componente | Mudança |
|---|---|
| **`cloud/base.py`, `gdrive.py`, `onedrive.py`, `router.py`, `runner.py`** | Deletados — sistema OAuth removido; rclone é o único backend de cloud backup |
| **`cloud/rclone_runner.py`** | Absorveu `_process_file_sync` e `_register_version_file_sync` (antes importadas do deletado `runner.py`); adicionados imports de `shutil`, `crypto`, `FileContent`, `FileContentCopy` |
| **`main.py`** | Removidos schemas `RunningJobInfo`, `RecentJobInfo`, `CloudJobStat`; removidos campos `running_jobs`, `recent_jobs` de `ActivityResponse` e `cloud_jobs` de `StatsResponse`; removido router `/cloud/*` |
| **`scheduler.py`** | Removidas `add_or_update_job`, `remove_job`, `reload_jobs_from_db` (OAuth); mantidas apenas as variantes `rclone_*` |
| **`database.py`** | Removidas classes `CloudCredential`, `CloudBackupJob` e funções de token encryption (`_token_cipher`, `encrypt_token`, `decrypt_token`) |
| **`daily_digest.py`** | Removida query de cloud jobs — digest passa a reportar apenas backups locais e armazenamento |
| **`static/index.html`** | Removidas seção "Cloud Backup" OAuth, modais de autenticação e todo o JS associado |
| **`tools/migrate_drop_oauth_tables.sql`** | Novo script para dropar `cloud_backup_jobs` e `cloud_credentials` em bancos existentes (SQLite e PostgreSQL) |

### v7.0.0

| Componente | Mudança |
|---|---|
| **`cloud/rclone_runner.py`** | Novo runner: lista via `rclone lsjson --recursive`, baixa via `rclone cat` com SHA-256 calculado em single pass durante o stream — sem buffer completo em memória |
| **`cloud/rclone_runner.py` — skip por mtime** | Arquivos com `mtime` inalterado não são baixados; `prev_files` carregado da última versão `done` (+ versão `incomplete`/`failed` para resume) |
| **`cloud/rclone_runner.py` — producer-consumer** | Pipeline producer-consumer com `_process_file_sync` e `_register_version_file_sync` — deduplicação, criptografia, replicação e registro no banco |
| **`cloud/rclone_runner.py` — subprocess seguro** | Todos os comandos rclone usam `asyncio.create_subprocess_exec` (lista de args, sem `shell=True`) — sem risco de injection; `remote_name` validado com regex `[a-zA-Z0-9_-]{1,64}` |
| **`cloud/rclone_router.py`** | Endpoints em `/rclone/*`: `GET /remotes`, `GET /remotes/{name}/browse`, CRUD de jobs, `POST /jobs/{id}/run`, `GET /jobs/{id}/status` |
| **`database.py` — `RcloneBackupJob`** | Nova tabela `rclone_backup_jobs`; rclone gerencia tokens externamente — nenhum token no banco |
| **`scheduler.py`** | `add_or_update_rclone_job` / `remove_rclone_job` / `reload_rclone_jobs_from_db` — agendamento cron com prefixo `rclone_job_{id}` |

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
| **`cloud/gdrive.py`, `onedrive.py`** | `download_file_to` aceita parâmetro opcional `client: httpx.AsyncClient \| None` — usa o cliente compartilhado do runner quando fornecido, cria um próprio se `None` (retrocompatível) *(removido na v7.3)* |
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

**Storage físico — dois discos (`storage.dirs = ["/mnt/disk1", "/mnt/disk2"]`):**
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

O conteúdo de cada arquivo é armazenado **uma única vez por sha256**, independente de quantas versões ou labels o referenciem. Com `storage.replication_factor = 1` (padrão), cada conteúdo fica em um único volume. Com `2`, uma cópia adicional é gravada em outro volume:

**Storage físico — replicação ativa (`storage.replication_factor = 2`):**
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

> **v7.9 — dois níveis de acesso.** Toda rota exige `X-API-Key` de uma conta válida. Rotas em **Backups/Versões/Arquivos** funcionam para qualquer usuário autenticado, mas são **escopadas por dono**: um usuário comum só enxerga/cria/altera labels em que é `owner_user_id`; tentar acessar um label de outro usuário retorna `403`. Admin não tem essa restrição. Rotas em **Storage/Manutenção/Cloud Backup/Usuários/Configuração** exigem `role=admin` — uma chave de usuário comum recebe `403` nelas.

### Dashboard e Health

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `GET` | `/` | Dashboard web *(admin)* |
| `GET` | `/health` | Status do servidor e versão |
| `GET` | `/maintenance` | Página de manutenção (HTML, admin) |
| `GET` | `/explorer` | Explorer de arquivos (HTML, admin) |
| `GET` | `/manage-users` | Gerenciamento de usuários (HTML, admin) *(v7.9)* |
| `GET` | `/settings` | Tela de configurações (HTML, admin) *(v8.0)* |

### Configuração (admin) *(v8.0)*

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `GET` | `/api/settings` | Devolve todos os parâmetros agrupados, com tipo, faixa, ajuda, se exige reinício e se há reinício pendente. Segredos vêm mascarados |
| `PUT` | `/api/settings` | Atualização parcial (`{"storage": {"replication_factor": 2}}`). Valida, persiste e aplica a quente o que não exige reinício. `400` com a mensagem do campo em caso de valor inválido |
| `POST` | `/api/settings/restart` | Encerra o processo para que o supervisor o suba de novo — única forma de aplicar os parâmetros marcados com ↻ sem SSH |

> Alterar `storage.encryption_enabled` ou `storage.encryption_key` com conteúdo já gravado retorna `409`; para prosseguir, reenvie com `"confirm_encryption_change": true` no corpo. Um segredo enviado vazio mantém o valor atual — a tela nunca reenvia o valor real, só a máscara.

### Usuários (admin) *(v7.9)*

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/users` | Cria usuário — retorna a API key gerada **uma única vez** |
| `GET` | `/users` | Lista usuários (sem expor as chaves) |
| `PATCH` | `/users/{id}` | Ativa/desativa o acesso (`is_active`) — histórico de backups é preservado |
| `POST` | `/users/{id}/rotate-key` | Gera nova chave para o usuário e invalida a anterior — retorna a nova chave **uma única vez** |
| `PATCH` | `/backups/{label}/owner` | Reatribui o dono de um backup (`owner_user_id`) |

> As chaves nunca são armazenadas em texto puro — o banco guarda apenas o SHA-256 da chave (`users.api_key_hash`).

### Backups

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/backups` | Cria backup — idempotente; o criador vira o dono (`owner_user_id`) |
| `GET` | `/backups` | Lista backups do usuário autenticado (admin vê todos) — `?client_name=` filtra por cliente |
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
| `POST` | `/register/batch` | Registra em lote (até 500) conteúdo já existente no storage — um commit por lote (v7.8+) |
| `POST` | `/upload` | Registra arquivo na versão |
| `POST` | `/sync` | Confirma sincronização da versão com o cliente |
| `GET` | `/files` | Lista arquivos de uma versão |
| `GET` | `/files/{id}/download` | Faz download |

> Paths com caracteres especiais são transmitidos em **base64** no header `X-Original-Path`.

### Storage (admin)

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `GET` | `/storage/info` | Espaço total/livre/usado do disco e bytes liberáveis ao apagar versões antigas |
| `GET` | `/storage/disks` | Status e contagem de arquivos físicos por volume |
| `GET` | `/disks` | Dashboard de discos (HTML) |

### Manutenção (admin)

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/maintenance/cleanup-orphans` | Remove todos os arquivos físicos não referenciados por nenhuma versão |
| `POST` | `/maintenance/rereplicate` | Re-replica conteúdos com menos cópias que `storage.replication_factor` |
| `POST` | `/maintenance/reconcile-replication` | Reconcilia replicação: remove cópias excedentes e preenche faltantes conforme `storage.replication_factor` |
| `POST` | `/maintenance/encrypt-existing` | Cifra arquivos físicos ainda não criptografados (requer `storage.encryption_enabled`) |
| `GET` | `/maintenance/cleanup-by-date/preview` | Preview de versões elegíveis para remoção antes de uma data (`?before=YYYY-MM-DD[&label=X]`) |
| `POST` | `/maintenance/cleanup-by-date` | Remove versões anteriores a uma data; preserva última versão `done` por label e versões `running` (`?before=YYYY-MM-DD[&label=X]`) |

### Cloud Backup / rclone (admin)

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `GET` | `/rclone/remotes` | Lista remotes configurados no rclone |
| `GET` | `/rclone/remotes/{name}/browse` | Lista pastas de um remote (`?path=` opcional) |
| `GET` | `/rclone/jobs` | Lista todos os jobs rclone |
| `POST` | `/rclone/jobs` | Cria job (remote_name, remote_path, label destino, cron) |
| `GET` | `/rclone/jobs/{id}` | Detalhes de um job |
| `PATCH` | `/rclone/jobs/{id}` | Atualiza job (path, label, cron, enabled) |
| `DELETE` | `/rclone/jobs/{id}` | Remove job |
| `POST` | `/rclone/jobs/{id}/run` | Inicia execução manual (async, retorna 202 imediatamente) |
| `GET` | `/rclone/jobs/{id}/status` | Status da última execução (last_run_at, status, message) |

> `POST /rclone/jobs/{id}/run` retorna `{ "status": "started", "job_id": N }` — a execução ocorre em background. Use `GET /rclone/jobs/{id}/status` para acompanhar.

> `/maintenance/cleanup-orphans` — retorna `{ "files_removed": N, "bytes_freed": N }`. Útil após deleções em massa. Operação **síncrona**.
>
> `/maintenance/rereplicate` — retorna `{ "replicated": N, "skipped": N, "target_copies": N }`. `replicated` = arquivos que receberam ao menos uma nova cópia. `skipped` = arquivos cuja única cópia está em volume `degraded`. Operação **síncrona** — pode demorar em acervos grandes.
>
> `/maintenance/reconcile-replication` — retorna `{ "replicated": N, "skipped": N, "cleaned": N, "target_copies": N }`. Remove cópias excedentes e preenche arquivos sub-replicados em uma única chamada. Útil ao reduzir ou aumentar `storage.replication_factor`. Operação **síncrona** — pode demorar em acervos grandes.
>
> `/maintenance/encrypt-existing` — retorna `{ "files_encrypted": N, "bytes_processed": N, "skipped": N }`. `skipped` inclui arquivos sem cópia acessível (volume degraded) e erros de I/O. Operação **síncrona** — use timeout longo em acervos grandes (cliente usa 600 s). Retorna `400` se `storage.encryption_enabled` for `false`.

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

`total_bytes`, `used_bytes` e `free_bytes` são a **soma de todos os volumes** configurados em `storage.dirs`.

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
| `POST /register/batch` | `RegisterBatchRequest` | `RegisterBatchResponse` |
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

Preencha `database.url` em Configurações → Banco de dados, ou direto no `config.json`:

```json
"database": {
  "url": "postgresql://nestvault:sua_senha_aqui@localhost/nestvault",
  "path": "./backup.db"
}
```

> **Nota:** `database.path` é ignorado quando `database.url` está preenchido. O DSN é tratado como segredo: a API devolve apenas uma máscara.

O par é marcado como `requer reinício` — a engine do SQLAlchemy é criada no import do módulo:

```bash
sudo systemctl restart nestvault
```

O NestVault cria as tabelas automaticamente na primeira inicialização.

### Dimensionando o pool de conexões

Três campos em Configurações → Banco de dados controlam o pool do SQLAlchemy (ignorados no SQLite, que usa `NullPool`):

| Campo | Padrão | O que é |
|---|---|---|
| `database.pool_size` | `10` | Conexões mantidas abertas permanentemente. |
| `database.max_overflow` | `20` | Conexões extras abertas sob pico, acima do pool permanente. |
| `database.pool_timeout_seconds` | `30` | Espera de um request por uma conexão livre antes de falhar com `500`. |

O teto real de conexões é `pool_size + max_overflow` (padrão: 30) **por processo uvicorn** — mantenha-o abaixo do `max_connections` do PostgreSQL (padrão: 100). Os três exigem reinício.

Se o log mostrar `QueuePool limit of size N overflow M reached, connection timed out`, o servidor está aceitando mais requests simultâneos do que o pool comporta: aumente `max_overflow` — ou reduza o paralelismo do cliente (`nestvault backup --workers`).

### Migrando dados do SQLite para PostgreSQL

Se já possui dados no SQLite e quer migrar para PostgreSQL, use o script incluído:

```bash
python tools/migrate_to_postgres.py \
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
python tools/migrate_to_postgres.py \
  --sqlite /caminho/para/backup.db \
  --postgres "postgresql://nestvault:sua_senha_aqui@localhost/nestvault" \
  --dry-run
```

Após a migração bem-sucedida, preencha `database.url` e reinicie o servidor. Verifique o dashboard para confirmar que os backups aparecem normalmente.

### Revertendo para SQLite

Se quiser voltar ao SQLite (ou criar um backup portátil do banco PostgreSQL), use o script reverso:

```bash
python tools/migrate_to_sqlite.py \
  --postgres "postgresql://nestvault:sua_senha_aqui@localhost/nestvault" \
  --sqlite   /caminho/para/backup_restored.db
```

Após a migração:
1. Esvazie `database.url` no `config.json` (ou em Configurações → Banco de dados)
2. Aponte `database.path` para `/caminho/para/backup_restored.db` (ou mova o arquivo para o local padrão)
3. Reinicie o servidor

Para verificar sem migrar dados:

```bash
python tools/migrate_to_sqlite.py \
  --postgres "postgresql://nestvault:sua_senha_aqui@localhost/nestvault" \
  --sqlite   /caminho/para/backup_restored.db \
  --dry-run
```
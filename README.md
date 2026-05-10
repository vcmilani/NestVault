# 🗄️ NestVault  `v2.9`

Sistema de backup com **versionamento**, **deduplicação de conteúdo** e **isolamento por label**.

Cada execução de backup cria uma nova versão dentro do label. O servidor armazena o conteúdo físico apenas uma vez por sha256 — versões diferentes que compartilham arquivos idênticos não duplicam o storage.

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
```

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

# Aumentar paralelismo de upload (padrão: 4 workers)
python backup_client.py backup ~/documentos \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000 \
  --workers 8

# Controlar processos de hashing (padrão: os.cpu_count())
python backup_client.py backup ~/documentos \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000 \
  --hash-workers 16

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

# Modo acumulativo — acumula todos os arquivos já vistos entre execuções (ideal para galerias de fotos)
python backup_client.py backup /Volumes/HD/Fotos \
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
| `test_helpers.py` | `_pick_volume`, `_content_path`, `_min_disk_free_percent` (mocks de disco) |
| `test_backups.py` | CRUD de backups e versões — criação, listagem, finalização, deleção |
| `test_check.py` | `/check` e `/check/batch` — 3 branches: novo, conteúdo existente, já registrado |
| `test_upload.py` | `/upload` — upload novo, deduplicação (mesmo sha256), modo register-only |
| `test_files.py` | `GET /files` e download — listagem ordenada, 404 e 410 (arquivo físico ausente) |
| `test_compare.py` | `GET /compare` — added, deleted, modified, unchanged, size_delta |
| `test_cleanup.py` | `/cleanup`, `/maintenance/cleanup-orphans` — remoção de versões e arquivos órfãos |
| `test_storage.py` | `GET /storage/info` — volume único e agregação de dois volumes; `reclaimable_bytes` |
| `test_auth.py` | Rejeição sem chave, rejeição com chave errada, acesso liberado com chave válida |

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

---

## ⚡ Otimizações

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
                └── FileContent (sha256, stored_at) ← arquivo físico único por conteúdo
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

O conteúdo de cada arquivo é armazenado **uma única vez**, independente de quantas versões ou labels o referenciem. O campo `stored_at` no banco registra o path absoluto de cada conteúdo, de modo que leitura, download e restore funcionam independentemente de em qual disco o arquivo está.

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
  "version": "2.9.0",
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
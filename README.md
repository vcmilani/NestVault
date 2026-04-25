# 🗄️ Backup Files — Raspberry Pi  `v2.0`

Sistema de backup com **versionamento**, **deduplicação de conteúdo** e **isolamento por label**.

Cada execução de backup cria uma nova versão dentro do label. O servidor armazena o conteúdo físico apenas uma vez por sha256 — versões diferentes que compartilham arquivos idênticos não duplicam o storage. Arquivos deletados são marcados na versão, nunca apagados do storage diretamente.

---

## Estrutura

```
backup_system/
├── server/
│   ├── main.py              ← API FastAPI (roda na Raspberry Pi)
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

**Opções:**

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

**Opções:**

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

Use o valor da coluna **VERSAO** para restaurar um estado específico.

---

### restore

Baixa os arquivos de uma **versão específica** e reconstrói a estrutura de pastas no destino. Apenas arquivos com status `active` são restaurados — deletados são ignorados.

```bash
# Restaurar uma versão específica
python backup_client.py restore /tmp/restore \
  --label "notebook-joao" \
  --version "2026-04-25T10:42:31" \
  --server http://192.168.1.100:8000

# Restaurar apenas um subdiretório da versão
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

# Sobrescrever arquivos existentes no destino
python backup_client.py restore /tmp/restore \
  --label "notebook-joao" \
  --version "2026-04-25T10:42:31" \
  --overwrite
```

**Opções:**

| Opção | Obrigatório | Descrição |
|-------|:-----------:|-----------|
| `--label` | ✅ | Label do backup |
| `--version` | ✅ | Chave da versão (obtida via `versions`) |
| `--server` | | URL do servidor |
| `--prefix` | | Restaurar apenas arquivos com esse prefixo |
| `--overwrite` | | Sobrescreve arquivos existentes no destino |
| `--dry-run` | | Apenas lista, não baixa |

A integridade de cada arquivo é validada após o download pelo SHA-256. Se não bater, o arquivo é removido e marcado como erro.

---

### cleanup

Remove versões antigas de um ou todos os backups, mantendo apenas as `N` mais recentes. Arquivos físicos órfãos (não referenciados por nenhuma versão remanescente) são apagados do storage automaticamente.

```bash
# Limpar um label específico, manter 5 versões
python backup_client.py cleanup \
  --label "notebook-joao" \
  --keep 5 \
  --server http://192.168.1.100:8000

# Limpar TODOS os labels de uma vez, manter 5 versões cada
python backup_client.py cleanup \
  --all \
  --keep 5 \
  --server http://192.168.1.100:8000

# Manter apenas 3 versões em todos os backups
python backup_client.py cleanup \
  --all \
  --keep 3 \
  --server http://192.168.1.100:8000
```

**Opções:**

| Opção | Descrição |
|-------|-----------|
| `--label` | Label específico a limpar (mutuamente exclusivo com `--all`) |
| `--all` | Limpa todos os labels do servidor de uma vez |
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

```bash
crontab -e
```

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
- **Auto-refresh** a cada 30 segundos, ou manual pelo botão ↻

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

O conteúdo de cada arquivo é armazenado **uma única vez**, independente de quantas versões ou labels o referenciem. Isso garante deduplicação real — dois labels com um arquivo idêntico compartilham o mesmo bloco de storage.

---

## 🔒 Isolamento por label

Cada label tem seu próprio conjunto de versões e VersionFiles. As operações de `check`, `upload`, `sync` e `restore` são sempre escopadas ao label — um backup nunca interfere em outro.

---

## 🔌 Endpoints da API

Todos os endpoints (exceto `/health` e `/`) exigem `X-API-Key` quando autenticação está ativada.

### Dashboard e Health

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `GET` | `/` | Dashboard web |
| `GET` | `/health` | Status do servidor e versão |

### Backups

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/backups` | Cria backup — idempotente se label já existe |
| `GET` | `/backups` | Lista todos os backups com stats |
| `GET` | `/backups/{label}` | Detalhes de um backup |
| `DELETE` | `/backups/{label}` | Remove backup e todas as suas versões |

### Versões

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/backups/{label}/versions` | Cria nova versão |
| `GET` | `/backups/{label}/versions` | Lista versões do label |
| `GET` | `/backups/{label}/versions/{version_key}` | Detalhes de uma versão |
| `PATCH` | `/backups/{label}/versions/{version_key}` | Finaliza versão (done/failed) |
| `DELETE` | `/backups/{label}/versions/{version_key}` | Remove versão e limpa órfãos |
| `POST` | `/backups/{label}/cleanup` | Remove versões antigas, mantém `keep` mais recentes |

### Arquivos

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/check` | Verifica se arquivo precisa upload na versão atual |
| `POST` | `/upload` | Registra arquivo na versão (com ou sem upload de conteúdo) |
| `POST` | `/sync` | Marca como deleted arquivos ausentes no cliente |
| `GET` | `/files` | Lista arquivos de uma versão (`backup_label` + `version_key` obrigatórios) |
| `GET` | `/files/{id}/download` | Faz download de um arquivo |

> Paths com caracteres especiais são transmitidos em **base64** no header `X-Original-Path`.

---

## 📊 Documentação automática

Com o servidor rodando:
- **Dashboard**: `http://<ip-da-pi>:8000/`
- **Swagger UI**: `http://<ip-da-pi>:8000/docs`
- **ReDoc**: `http://<ip-da-pi>:8000/redoc`
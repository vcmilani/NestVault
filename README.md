# 🗄️ Backup Files — Raspberry Pi

Sistema de backup de arquivos com deduplicação por metadados, isolamento por identificador e uploads paralelos.
Cada backup possui um **label único** — arquivos de backups diferentes são completamente isolados no storage físico, no banco de dados e nas operações de sync.

---

## Estrutura

```
backup_system/
├── server/
│   ├── main.py          ← API FastAPI (roda na Raspberry Pi)
│   ├── database.py      ← Modelos SQLite/SQLAlchemy
│   └── requirements.txt
├── client/
│   ├── backup_client.py ← Script de backup/restore (roda no cliente)
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
export BACKUP_API_KEY="uma-chave-secreta-forte-aqui"
export STORAGE_DIR="/mnt/hd-externo/backups"   # onde os arquivos serão salvos
export DB_PATH="/mnt/hd-externo/backup.db"      # banco SQLite
```

> **Dica:** Coloque essas variáveis no arquivo de serviço do systemd ou em `/etc/environment`.

### 3. Iniciar o servidor

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

### 4. Rodar como serviço (systemd)

Crie `/etc/systemd/system/backup-server.service`:

```ini
[Unit]
Description=Backup Server
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

---

## 🚀 Comandos

O cliente opera com três subcomandos: `backup`, `backups` e `restore`.
O argumento `--label` é **obrigatório** nos comandos `backup` e `restore`.

---

### backup

Envia arquivos locais para o servidor sob um identificador único (`--label`). Se o backup ainda não existir no servidor, é criado automaticamente. Arquivos com metadados idênticos são ignorados sem trafegar nenhum byte. Os uploads são feitos em paralelo para melhor performance.

```bash
# Backup simples
python backup_client.py backup ~/documentos \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000

# Com prefixo de path no servidor
python backup_client.py backup ~/documentos \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000 \
  --prefix /home/joao/documentos

# Ignorar subpastas específicas
python backup_client.py backup ~/projeto \
  --label "projeto-alpha" \
  --server http://192.168.1.100:8000 \
  --exclude node_modules .git __pycache__ .venv dist build

# Aumentar workers para mais performance (padrão: 4)
python backup_client.py backup ~/documentos \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000 \
  --workers 8

# Verificar sem enviar (dry-run)
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
| `--workers` | | Número de uploads paralelos (padrão: `4`) |
| `--dry-run` | | Apenas verifica, não envia nada e não cria o backup |

**Recomendação de workers por cenário:**

| Cenário | Workers sugerido |
|---------|:----------------:|
| Pi com cartão SD | 2 |
| Pi com HD externo USB | 4–6 |
| Pi com SSD | 6–8 |
| Muitos arquivos pequenos | 8+ |
| Arquivos grandes (>100 MB) | 2–3 |

> Em `--dry-run` o número de workers é ignorado e o processamento é sequencial para manter o output legível.

Durante o upload, cada thread exibe sua própria barra de progresso:

```
10:42:31  INFO     Workers   : 4 threads paralelas
10:42:31  INFO     UPLOAD /home/joao/videos/aula.mp4  (342.7 MB)  [Arquivo novo]
  aula.mp4                |████████████░░░░░░| 187.3M/342.7M [4.2MB/s]
10:42:31  INFO     UPLOAD /home/joao/docs/relatorio.pdf  (1.2 MB)  [Arquivo novo]
10:42:31  INFO     SKIP   /home/joao/docs/notas.txt
```

Ao final, arquivos que existem no servidor mas foram removidos localmente são apagados automaticamente do backup (sync). O sync é isolado ao label — nunca remove arquivos de outro backup.

```
==================================================
  Backup      : [notebook-joao]
  Verificados : 42
  Enviados    : 5
  Ignorados   : 37
  Erros       : 0
  Removidos   : 1
==================================================
```

---

### backups

Lista todos os backups registrados no servidor com contagem de arquivos, tamanho total e data do último run.

```bash
# Listar todos os backups
python backup_client.py backups \
  --server http://192.168.1.100:8000

# Filtrar por nome do cliente
python backup_client.py backups \
  --server http://192.168.1.100:8000 \
  --client "notebook-joao"
```

**Opções:**

| Opção | Descrição |
|-------|-----------|
| `--server` | URL do servidor |
| `--client` | Filtrar por nome do cliente |

Exemplo de saída:

```
LABEL                           CLIENTE               STATUS    ARQUIVOS     TAMANHO  ULTIMO RUN
---------------------------------------------------------------------------------------------------------
notebook-joao                   notebook-joao         active         142      1.2 GB  2026-04-22 02:00
servidor-web                    servidor-web          active          38     320.5 MB  2026-04-21 03:00
projeto-alpha                   notebook-joao         active         891      4.7 GB  2026-04-20 14:30
```

Use o valor da coluna **LABEL** para fazer restore de um backup específico.

---

### restore

Baixa os arquivos de um backup identificado pelo `--label` e reconstrói a estrutura de pastas no destino.

```bash
# Restaurar um backup pelo label
python backup_client.py restore /tmp/restore \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000

# Restaurar apenas arquivos de um subdiretório específico
python backup_client.py restore /tmp/restore \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000 \
  --prefix /home/joao/documentos

# Ver o que seria restaurado sem baixar nada
python backup_client.py restore /tmp/restore \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000 \
  --dry-run

# Sobrescrever arquivos que já existem no destino
python backup_client.py restore /tmp/restore \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000 \
  --overwrite
```

**Opções:**

| Opção | Obrigatório | Descrição |
|-------|:-----------:|-----------|
| `--label` | ✅ | Identificador do backup a restaurar |
| `--server` | | URL do servidor |
| `--prefix` | | Restaurar apenas arquivos com esse prefixo de path |
| `--overwrite` | | Sobrescreve arquivos existentes no destino |
| `--dry-run` | | Apenas lista os arquivos, não baixa nada |

O restore valida a integridade de cada arquivo após o download comparando o SHA-256 com o valor registrado no servidor. Se não bater, o arquivo é removido localmente e marcado como erro.

---

### Agendar com cron

```bash
crontab -e
```

```cron
# Backup todo dia às 02:00 com 4 workers
0 2 * * * BACKUP_API_KEY=sua-chave \
  /home/usuario/client/.venv/bin/python \
  /home/usuario/client/backup_client.py backup ~/docs \
  --label "notebook-joao" \
  --server http://192.168.1.100:8000 \
  --exclude node_modules .git \
  --workers 4 \
  >> /var/log/backup.log 2>&1
```

---

## 🔒 Isolamento por label

Cada label possui seu próprio espaço completamente isolado no servidor:

**Storage físico separado:**
```
storage/
├── notebook-joao/
│   └── ab/ab12ef34_relatorio.pdf
├── servidor-web/
│   └── cd/cd56gh78_index.html
└── projeto-alpha/
    └── ef/ef90ij12_config.yaml
```

**Regras de isolamento:**
- `/check` — só consulta arquivos do label informado
- `/upload` — armazena fisicamente sob a pasta do label
- `/sync` — só remove arquivos do label informado, nunca toca outros backups
- `/restore` — só lista e baixa arquivos do label informado

---

## 🔒 Lógica de Deduplicação

Dentro de um mesmo backup, o `/check` compara **4 campos simultaneamente**:

| Campo | O que representa |
|-------|-----------------|
| `original_path` | Identidade do arquivo |
| `sha256` | Hash do conteúdo — garante integridade |
| `size` | Tamanho em bytes |
| `mtime` | Última modificação (epoch) |

Se todos os 4 forem idênticos → **sem upload**.
Se o path existe mas qualquer outro campo mudou → **arquivo modificado, será atualizado**.
Se o path não existe neste backup → **arquivo novo, upload necessário**.

---

## 🔌 Endpoints da API

Todos os endpoints (exceto `/health`) exigem o header `X-API-Key`.

### Backups (identificadores)

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/backups` | Cria um backup — idempotente se label já existe |
| `GET` | `/backups` | Lista todos os backups com stats |
| `GET` | `/backups/{label}` | Detalhes de um backup |
| `DELETE` | `/backups/{label}` | Remove o backup e todos os seus arquivos |

### Arquivos

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `GET` | `/health` | Status do servidor |
| `POST` | `/check` | Verifica se arquivo precisa ser enviado (escopado ao label) |
| `POST` | `/upload` | Envia arquivo — requer header `X-Backup-Label` |
| `POST` | `/sync` | Remove arquivos ausentes no cliente (escopado ao label) |
| `GET` | `/files?backup_label=` | Lista arquivos de um backup (`backup_label` obrigatório) |
| `GET` | `/files/{id}/download` | Faz download de um arquivo |
| `DELETE` | `/files/{id}` | Remove um arquivo do backup |

> Paths com caracteres especiais (acentos, cedilha) são transmitidos em **base64** no header `X-Original-Path` e decodificados automaticamente pelo servidor.

---

## 📊 Documentação automática

Com o servidor rodando, acesse:
- **Swagger UI**: `http://<ip-da-pi>:8000/docs`
- **ReDoc**: `http://<ip-da-pi>:8000/redoc`
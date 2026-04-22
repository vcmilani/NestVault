# 🗄️ Backup Server — Raspberry Pi

Sistema de backup de arquivos com deduplicação por metadados e controle de sessões.
O cliente só envia o arquivo se o servidor ainda não tiver uma cópia idêntica, e cada execução de backup gera uma sessão identificada — permitindo restaurar exatamente o estado de um backup específico.

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

> **Dica:** Coloque essas variáveis em `/etc/environment` ou no arquivo de serviço do systemd.

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

O cliente opera com três subcomandos: `backup`, `sessions` e `restore`.

---

### backup

Envia arquivos locais para o servidor. Cada execução cria uma **sessão** identificada por um UUID. Arquivos com metadados idênticos ao último backup são ignorados automaticamente.

```bash
# Backup simples
python backup_client.py backup ~/documentos \
  --server http://192.168.1.100:8000

# Com prefixo e label descritivo
python backup_client.py backup ~/documentos \
  --server http://192.168.1.100:8000 \
  --prefix /backups/meu-notebook \
  --label "pre-atualizacao-ubuntu"

# Ignorar subpastas específicas
python backup_client.py backup ~/projeto \
  --server http://192.168.1.100:8000 \
  --exclude node_modules .git __pycache__ .venv dist build

# Definir nome do cliente manualmente (padrão: hostname da máquina)
python backup_client.py backup ~/documentos \
  --server http://192.168.1.100:8000 \
  --client "notebook-joao"

# Só verificar sem enviar (dry-run)
python backup_client.py backup ~/documentos \
  --server http://192.168.1.100:8000 \
  --dry-run
```

**Opções:**

| Opção | Descrição |
|-------|-----------|
| `--server` | URL do servidor (padrão: `http://localhost:8000`) |
| `--prefix` | Prefixo do path no servidor (ex: `/backups/meu-pc`) |
| `--label` | Nome amigável para a sessão (ex: `pre-atualizacao`) |
| `--client` | Nome do cliente — padrão é o hostname da máquina |
| `--exclude` | Subpastas a ignorar — aceita múltiplos valores |
| `--dry-run` | Apenas verifica, não envia nada e não cria sessão |

Durante o upload, uma barra de progresso exibe o andamento em tempo real:

```
10:42:31  INFO     UPLOAD /backups/notebook/videos/aula.mp4  (342.7 MB)
  aula.mp4                |████████████░░░░░░| 187.3M/342.7M [4.2MB/s]
10:42:53  INFO       OK  id=87  sha256=a3f9c12d8b01...
```

Ao final, o resumo inclui o ID da sessão criada:

```
==================================================
  Sessao      : a3f9c12d-8b01-4e2f-9c3d-1a2b3c4d5e6f
  Verificados : 42
  Enviados    : 5
  Ignorados   : 37
  Erros       : 0
  Removidos   : 1
==================================================
```

Arquivos que existem no servidor mas foram removidos do cliente são apagados automaticamente ao final (sync).

---

### sessions

Lista todas as sessões de backup registradas no servidor, com contagem de arquivos, tamanho total e status.

```bash
# Listar todas as sessões
python backup_client.py sessions \
  --server http://192.168.1.100:8000

# Filtrar por cliente
python backup_client.py sessions \
  --server http://192.168.1.100:8000 \
  --client "notebook-joao"

# Filtrar por status
python backup_client.py sessions \
  --server http://192.168.1.100:8000 \
  --status done
```

**Opções:**

| Opção | Descrição |
|-------|-----------|
| `--server` | URL do servidor |
| `--client` | Filtrar por nome do cliente |
| `--status` | Filtrar por status: `done`, `failed`, `running` |

Exemplo de saída:

```
ID                                    LABEL                 CLIENTE               STATUS    ARQUIVOS     TAMANHO  INICIO
------------------------------------------------------------------------------------------------------------------------
a3f9c12d-...  pre-atualizacao-ubuntu  notebook-joao         done          42      198.3 MB  2026-04-21 02:00
b7e1d890-...  backup-semanal          notebook-joao         done          40      195.1 MB  2026-04-14 02:00
c1d2e3f4-...  -                       servidor-web          failed         0         0.0 B  2026-04-10 03:00
```

Use o ID da sessão desejada para fazer um restore seletivo.

---

### restore

Baixa os arquivos do servidor e reconstrói a estrutura de pastas no destino. Pode restaurar uma sessão específica ou todos os arquivos disponíveis.

```bash
# Restaurar uma sessão específica (recomendado)
python backup_client.py restore /tmp/restore \
  --server http://192.168.1.100:8000 \
  --session a3f9c12d-8b01-4e2f-9c3d-1a2b3c4d5e6f

# Restaurar apenas arquivos de um prefixo dentro da sessão
python backup_client.py restore /tmp/restore \
  --server http://192.168.1.100:8000 \
  --session a3f9c12d-8b01-4e2f-9c3d-1a2b3c4d5e6f \
  --prefix /backups/notebook/home/usuario/documentos

# Restaurar tudo (sem filtro de sessão)
python backup_client.py restore /tmp/restore \
  --server http://192.168.1.100:8000

# Ver o que seria restaurado sem baixar nada
python backup_client.py restore /tmp/restore \
  --server http://192.168.1.100:8000 \
  --session a3f9c12d-... \
  --dry-run

# Sobrescrever arquivos que já existem no destino
python backup_client.py restore /tmp/restore \
  --server http://192.168.1.100:8000 \
  --session a3f9c12d-... \
  --overwrite
```

**Opções:**

| Opção | Descrição |
|-------|-----------|
| `--server` | URL do servidor |
| `--session` | ID da sessão a restaurar (obtido via `sessions`) |
| `--prefix` | Restaurar apenas arquivos com esse prefixo |
| `--overwrite` | Sobrescreve arquivos existentes no destino |
| `--dry-run` | Apenas lista, não baixa nada |

O restore valida a integridade de cada arquivo após o download comparando o SHA-256 com o valor registrado no servidor. Se não bater, o arquivo é removido e marcado como erro.

---

### Agendar com cron

```bash
crontab -e
```

```cron
# Backup todo dia às 02:00 com label da data
0 2 * * * BACKUP_API_KEY=sua-chave \
  /home/usuario/client/.venv/bin/python \
  /home/usuario/client/backup_client.py backup ~/docs \
  --server http://192.168.1.100:8000 \
  --prefix /backups/meu-pc \
  --label "auto-$(date +\%Y-\%m-\%d)" \
  --exclude node_modules .git \
  >> /var/log/backup.log 2>&1
```

---

## 🔌 Endpoints da API

Todos os endpoints (exceto `/health`) exigem o header `X-API-Key`.

### Sessões

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/sessions` | Cria uma nova sessão de backup |
| `GET` | `/sessions` | Lista todas as sessões com stats |
| `GET` | `/sessions/{id}` | Detalhes de uma sessão |
| `PATCH` | `/sessions/{id}` | Finaliza uma sessão (done/failed) |
| `DELETE` | `/sessions/{id}` | Remove sessão e todos os seus arquivos |

### Arquivos

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `GET` | `/health` | Status do servidor |
| `POST` | `/check` | Verifica se arquivo precisa ser enviado |
| `POST` | `/upload` | Envia arquivo (aceita `X-Session-Id`) |
| `POST` | `/sync` | Remove arquivos que sumiram do cliente |
| `GET` | `/files` | Lista arquivos (filtra por `session_id` e/ou `path_prefix`) |
| `GET` | `/files/{id}/download` | Faz download de um arquivo |
| `DELETE` | `/files/{id}` | Remove um arquivo do backup |

> Paths com caracteres especiais (acentos, cedilha) são transmitidos em **base64** no header `X-Original-Path` e decodificados automaticamente pelo servidor.

---

## 🔒 Lógica de Deduplicação

O `/check` compara **4 campos simultaneamente**:

| Campo | O que representa |
|-------|-----------------|
| `original_path` | Identidade do arquivo (de onde veio) |
| `sha256` | Hash do conteúdo — garante integridade |
| `size` | Tamanho em bytes |
| `mtime` | Última modificação (epoch) |

Se todos os 4 forem idênticos → **sem upload**.
Se o path existe mas qualquer outro campo mudou → **arquivo modificado, nova versão**.
Se o path não existe → **arquivo novo**.

---

## 🖥️ Múltiplos clientes

Use `--prefix` para separar o espaço de cada máquina no servidor. O sync e o restore respeitam o prefixo e nunca interferem uns nos outros.

```
storage/
├── /backups/notebook-joao/home/joao/docs/...
├── /backups/notebook-maria/home/maria/docs/...
└── /backups/servidor-web/var/www/...
```

Ao listar sessões, use `--client` para ver apenas os backups de uma máquina:

```bash
python backup_client.py sessions --client "notebook-joao"
```

---

## 📊 Documentação automática

Com o servidor rodando, acesse:
- **Swagger UI**: `http://<ip-da-pi>:8000/docs`
- **ReDoc**: `http://<ip-da-pi>:8000/redoc`
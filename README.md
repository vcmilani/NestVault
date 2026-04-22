# 🗄️ Backup Files — Raspberry Pi

Sistema de backup de arquivos com deduplicação por metadados.
O cliente só envia o arquivo se o servidor ainda não tiver uma cópia idêntica.

---

## Estrutura

```
backup_system/
├── server/
│   ├── main.py          ← API FastAPI (roda na Raspberry Pi)
│   ├── database.py      ← Modelos SQLite/SQLAlchemy
│   └── requirements.txt
└── client/
    ├── backup_client.py ← Script de backup/restore (roda no cliente)
    └── requirements.txt
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

> **Dica:** Coloque essas variáveis em `/etc/environment` ou num arquivo `.env` lido pelo systemd.

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

O cliente opera com dois subcomandos: `backup` e `restore`.

### backup

Envia arquivos locais para o servidor. Arquivos com metadados idênticos são ignorados automaticamente.

```bash
# Backup simples
python backup_client.py backup ~/documentos --server http://192.168.1.100:8000

# Com prefixo de path no servidor (recomendado para múltiplos clientes)
python backup_client.py backup ~/documentos \
  --server http://192.168.1.100:8000 \
  --prefix /backups/meu-notebook

# Ignorar subpastas específicas
python backup_client.py backup ~/projeto \
  --server http://192.168.1.100:8000 \
  --exclude node_modules .git __pycache__ .venv dist build

# Só verificar sem enviar (dry-run)
python backup_client.py backup ~/documentos \
  --server http://192.168.1.100:8000 \
  --dry-run
```

**Opções do backup:**

| Opção | Descrição |
|-------|-----------|
| `--server` | URL do servidor (padrão: `http://localhost:8000`) |
| `--prefix` | Prefixo do path no servidor (ex: `/backups/meu-pc`) |
| `--exclude` | Subpastas a ignorar — aceita múltiplos valores |
| `--dry-run` | Apenas verifica, não envia nada |

Durante o upload, uma barra de progresso exibe o andamento em tempo real:

```
10:42:31  INFO     UPLOAD /backups/notebook/videos/aula.mp4  (342,7 MB)
  aula.mp4                |████████████░░░░░░| 187.3M/342.7M [4.2MB/s]
10:42:53  INFO       OK  sha256=a3f9c12d8b01...
```

Ao final, arquivos que existem no servidor mas foram removidos do cliente são apagados automaticamente do backup (sync).

### restore

Baixa todos os arquivos do servidor e reconstrói a estrutura de pastas no destino.

```bash
# Restaurar tudo
python backup_client.py restore /tmp/restore \
  --server http://192.168.1.100:8000

# Restaurar apenas um prefixo específico
python backup_client.py restore /tmp/restore \
  --server http://192.168.1.100:8000 \
  --prefix /backups/meu-notebook

# Ver o que seria restaurado sem baixar nada
python backup_client.py restore /tmp/restore \
  --server http://192.168.1.100:8000 \
  --dry-run

# Sobrescrever arquivos que já existem no destino
python backup_client.py restore /tmp/restore \
  --server http://192.168.1.100:8000 \
  --overwrite
```

**Opções do restore:**

| Opção | Descrição |
|-------|-----------|
| `--server` | URL do servidor (padrão: `http://localhost:8000`) |
| `--prefix` | Restaurar apenas arquivos com esse prefixo |
| `--overwrite` | Sobrescreve arquivos existentes no destino |
| `--dry-run` | Apenas lista, não baixa nada |

O restore valida a integridade de cada arquivo após o download comparando o SHA-256 com o valor registrado no servidor. Se não bater, o arquivo é removido e marcado como erro.

### Agendar com cron

```bash
crontab -e
```

```cron
# Backup todo dia às 02:00 ignorando node_modules e .git
0 2 * * * BACKUP_API_KEY=sua-chave /home/usuario/client/.venv/bin/python \
  /home/usuario/client/backup_client.py backup ~/docs \
  --server http://192.168.1.100:8000 \
  --prefix /backups/meu-pc \
  --exclude node_modules .git \
  >> /var/log/backup.log 2>&1
```

---

## 🔌 Endpoints da API

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `GET` | `/health` | Status do servidor |
| `POST` | `/check` | Verifica se arquivo precisa ser enviado |
| `POST` | `/upload` | Envia arquivo |
| `POST` | `/sync` | Remove arquivos que sumiram do cliente |
| `GET` | `/files` | Lista todos os arquivos |
| `GET` | `/files/{id}/download` | Faz download de um arquivo |
| `DELETE` | `/files/{id}` | Remove um arquivo do backup |

Todos os endpoints (exceto `/health`) exigem o header `X-API-Key`.

> Paths com caracteres especiais (acentos, cedilha) são transmitidos em **base64** no header `X-Original-Path` e decodificados automaticamente pelo servidor.

### Exemplo de `/check`

```json
POST /check
{
  "original_path": "/home/pi/docs/relatorio.pdf",
  "sha256": "abc123...",
  "size": 204800,
  "mtime": 1713700000.0
}
```

Resposta:
```json
{ "needs_upload": false, "reason": "Metadados idênticos — arquivo já está no backup", "file_id": 42 }
```

### Exemplo de `/sync`

```json
POST /sync
{
  "existing_paths": ["/backups/notebook/docs/a.pdf", "/backups/notebook/docs/b.pdf"],
  "path_prefix": "/backups/notebook"
}
```

Resposta:
```json
{ "deleted": ["/backups/notebook/docs/antigo.pdf"], "deleted_count": 1 }
```

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

> O hash SHA-256 garante que dois arquivos com mesmo nome mas conteúdo diferente nunca sejam confundidos.

---

## 🖥️ Múltiplos clientes

Use `--prefix` para separar o espaço de cada máquina no servidor. O sync e o restore respeitam o prefixo e nunca interferem uns nos outros.

```
storage/
├── /backups/notebook-a/home/usuario/docs/...
├── /backups/notebook-b/home/usuario/docs/...
└── /backups/servidor-web/var/www/...
```

---

## 📊 Documentação automática

Com o servidor rodando, acesse:
- **Swagger UI**: `http://<ip-da-pi>:8000/docs`
- **ReDoc**: `http://<ip-da-pi>:8000/redoc`
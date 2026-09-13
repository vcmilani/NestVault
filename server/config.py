"""
Configuração persistida em arquivo — v8.0.0

Substitui a leitura direta de variáveis de ambiente espalhada pelos módulos.
A fonte da verdade é um JSON (default: server/config.json, sobrescrevível por
NESTVAULT_CONFIG), editável pela tela /settings.

Precedência: arquivo > default. As variáveis de ambiente antigas são usadas
apenas para *semear* o arquivo no primeiro boot (migração de instalações que
hoje configuram tudo via systemd Environment=); depois disso são ignoradas.

Exceção deliberada: BACKUP_API_KEY continua vindo do ambiente. É segredo de
bootstrap, usado uma única vez para criar o primeiro admin — persistir uma
chave de administrador em texto no disco não compensa, e a rotação já é
coberta pela tela /manage-users.
"""

import base64
import json
import logging
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger("backup-server")

SCHEMA_VERSION = 1


class ConfigError(ValueError):
    """Valor inválido para um campo de configuração — vira HTTP 400 nos endpoints."""


# Este módulo é carregado no import de database/storage, ou seja, ANTES do
# logging.basicConfig() de main.py — o que aconteceu no boot (migração do
# ambiente, valor inválido no arquivo) sairia sem formatação ou nem apareceria.
# Guardamos as mensagens e main.py as descarrega no lifespan.
_boot_log: list[tuple[int, str]] = []


def _boot(level: int, msg: str) -> None:
    _boot_log.append((level, msg))


def flush_boot_log() -> None:
    """Emite o que ficou pendente do import. Chamado pelo lifespan de main.py."""
    while _boot_log:
        level, msg = _boot_log.pop(0)
        log.log(level, msg)


# -- Schema -------------------------------------------------------------------

@dataclass(frozen=True)
class Field:
    key: str                       # "storage.replication_factor"
    env: Optional[str]             # variável de ambiente equivalente (seeding)
    type: str                      # "str" | "int" | "float" | "bool" | "list[str]"
    default: Any
    label: str
    help: str
    requires_restart: bool = False
    secret: bool = False
    min: Optional[float] = None
    max: Optional[float] = None

    @property
    def group(self) -> str:
        return self.key.split(".", 1)[0]

    @property
    def name(self) -> str:
        return self.key.split(".", 1)[1]


GROUP_LABELS = {
    "storage":   "Storage",
    "ssd_cache": "SSD Cache",
    "database":  "Banco de dados",
    "db_backup": "Backup do banco",
    "digest":    "Digest diário",
    "rclone":    "rclone",
}

SCHEMA: list[Field] = [
    # -- storage --
    Field("storage.dirs", "STORAGE_DIRS", "list[str]", ["./storage"],
          "Volumes de storage",
          "Diretórios de armazenamento em ordem de prioridade, separados por vírgula.",
          requires_restart=True),
    Field("storage.replication_factor", "REPLICATION_FACTOR", "int", 1,
          "Fator de replicação",
          "Nº de cópias de cada arquivo; 0 = todos os volumes saudáveis.",
          min=0),
    Field("storage.fallback_threshold_gb", "STORAGE_FALLBACK_THRESHOLD_GB", "float", 10.0,
          "Limiar de espaço livre (GB)",
          "Piso de espaço livre por disco; abaixo disso o volume é considerado esgotado para escrita.",
          min=0),
    Field("storage.auto_rebalance_enabled", "STORAGE_AUTO_REBALANCE_ENABLED", "bool", False,
          "Rebalanceamento automático",
          "Move arquivos de discos abaixo do limiar de espaço livre para discos com espaço sobrando, periodicamente."),
    Field("storage.rebalance_check_interval_minutes", "STORAGE_REBALANCE_CHECK_INTERVAL_MINUTES", "int", 15,
          "Intervalo do rebalanceamento (min)",
          "Frequência da checagem automática de rebalanceamento entre discos.",
          min=5),
    Field("storage.encryption_enabled", "ENCRYPTION_ENABLED", "bool", False,
          "Criptografia em repouso",
          "Cifra os arquivos com AES-256-GCM. Alterar depois de gravar dados torna o conteúdo existente ilegível.",
          requires_restart=True),
    Field("storage.encryption_key", "ENCRYPTION_KEY", "str", "",
          "Chave de criptografia",
          "32 bytes em Base64. Obrigatória quando a criptografia está habilitada.",
          requires_restart=True, secret=True),

    # -- ssd_cache --
    Field("ssd_cache.enabled", "SSD_CACHE_ENABLED", "bool", False,
          "SSD cache habilitado",
          "Usa um SSD como área de staging antes de mover para os HDs.",
          requires_restart=True),
    Field("ssd_cache.dir", "SSD_CACHE_DIR", "str", "",
          "Diretório do SSD cache",
          "Caminho do staging no SSD. Vazio desabilita o cache.",
          requires_restart=True),
    Field("ssd_cache.max_gb", "SSD_CACHE_MAX_GB", "float", 20.0,
          "Teto do SSD cache (GB)",
          "Tamanho máximo da fila pendente no SSD.",
          min=0),
    Field("ssd_cache.idle_delay_minutes", "SSD_CACHE_IDLE_DELAY_MINUTES", "float", 5.0,
          "Atraso antes de mover com espaço sobrando (min)",
          "Se o SSD cache tem espaço livre, espera esse tempo sem nenhum backup ativo antes de mover para os HDs.",
          min=0),

    # -- database --
    Field("database.url", "DATABASE_URL", "str", "",
          "DATABASE_URL (PostgreSQL)",
          "DSN do PostgreSQL (ex: postgresql://user:pass@host/db). Vazio = SQLite.",
          requires_restart=True, secret=True),
    Field("database.path", "DB_PATH", "str", "./backup.db",
          "Caminho do SQLite",
          "Arquivo do banco quando DATABASE_URL está vazio.",
          requires_restart=True),
    Field("database.pool_size", "DB_POOL_SIZE", "int", 10,
          "Conexões permanentes do pool",
          "Só PostgreSQL. Conexões mantidas abertas. O default do SQLAlchemy (5) fica "
          "abaixo do nº de requests simultâneos que o servidor aceita e causa "
          "'QueuePool limit ... timed out' sob backup paralelo.",
          requires_restart=True, min=1),
    Field("database.max_overflow", "DB_MAX_OVERFLOW", "int", 20,
          "Conexões extras do pool",
          "Só PostgreSQL. Conexões abertas sob pico, acima do pool permanente. "
          "O teto real é pool_size + max_overflow — mantenha-o abaixo do "
          "max_connections do PostgreSQL.",
          requires_restart=True, min=0),
    Field("database.pool_timeout_seconds", "DB_POOL_TIMEOUT", "float", 30.0,
          "Timeout de espera por conexão (s)",
          "Só PostgreSQL. Tempo que um request espera por uma conexão livre antes de "
          "falhar com 500.",
          requires_restart=True, min=1),

    # -- db_backup --
    Field("db_backup.enabled", "DB_BACKUP_ENABLED", "bool", True,
          "Backup do banco habilitado",
          "Copia o banco para os volumes de storage diariamente."),
    Field("db_backup.retention", "DB_BACKUP_RETENTION", "int", 7,
          "Retenção (nº de backups)",
          "Quantidade de backups do banco mantidos por volume.",
          min=1),
    Field("db_backup.hour", "DB_BACKUP_HOUR", "int", 1,
          "Hora do backup",
          "Hora local de execução do backup do banco.",
          min=0, max=23),
    Field("db_backup.minute", "DB_BACKUP_MINUTE", "int", 0,
          "Minuto do backup",
          "Minuto local de execução do backup do banco.",
          min=0, max=59),

    # -- digest --
    Field("digest.hour", "DIGEST_HOUR", "int", 18,
          "Hora do digest",
          "Hora local de envio do resumo diário.",
          min=0, max=23),
    Field("digest.telegram_bot_token", "TELEGRAM_BOT_TOKEN", "str", "",
          "Token do bot Telegram",
          "Sem token o digest e os alertas não são enviados.",
          secret=True),
    Field("digest.telegram_chat_id", "TELEGRAM_CHAT_ID", "str", "",
          "Chat ID do Telegram",
          "Destinatário do digest e dos alertas."),
    Field("digest.anthropic_api_key", "ANTHROPIC_API_KEY", "str", "",
          "Chave da API Anthropic",
          "Gera o resumo com Claude. Sem ela, usa o Ollama local.",
          secret=True),
    Field("digest.ollama_url", "OLLAMA_URL", "str", "http://localhost:11434",
          "URL do Ollama",
          "Endpoint do Ollama usado como fallback de IA."),
    Field("digest.ollama_model", "OLLAMA_MODEL", "str", "llama3",
          "Modelo do Ollama",
          "Nome do modelo usado no fallback local."),

    # -- rclone --
    Field("rclone.config_path", "RCLONE_CONFIG", "str", "",
          "Caminho do rclone.conf",
          "Repassado ao binário rclone via RCLONE_CONFIG. Vazio usa o padrão do rclone."),
]

BY_KEY: dict[str, Field] = {f.key: f for f in SCHEMA}

GROUPS: list[str] = list(dict.fromkeys(f.group for f in SCHEMA))


# -- Coerção e validação ------------------------------------------------------

def _coerce(f: Field, raw: Any) -> Any:
    """Converte o valor bruto (arquivo, env ou JSON do PUT) para o tipo do campo."""
    if f.type == "bool":
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() in ("true", "1", "yes", "on")

    if f.type == "list[str]":
        if isinstance(raw, (list, tuple)):
            items = [str(p).strip() for p in raw]
        else:
            items = [p.strip() for p in str(raw).split(",")]
        return [p for p in items if p]

    if f.type == "int":
        try:
            return int(str(raw).strip())
        except (TypeError, ValueError):
            raise ConfigError(f"{f.key}: valor inteiro invalido ('{raw}')")

    if f.type == "float":
        try:
            return float(str(raw).strip())
        except (TypeError, ValueError):
            raise ConfigError(f"{f.key}: valor numerico invalido ('{raw}')")

    return "" if raw is None else str(raw)


def _check(f: Field, value: Any) -> None:
    """Regras por campo, aplicadas depois da coerção."""
    if f.type in ("int", "float"):
        if f.min is not None and value < f.min:
            raise ConfigError(f"{f.key}: valor minimo e {f.min:g} (recebido {value:g})")
        if f.max is not None and value > f.max:
            raise ConfigError(f"{f.key}: valor maximo e {f.max:g} (recebido {value:g})")

    if f.key == "storage.dirs" and not value:
        raise ConfigError("storage.dirs: informe ao menos um volume de storage")

    if f.key == "storage.encryption_key" and value:
        try:
            decoded = base64.b64decode(value, validate=True)
        except Exception:
            raise ConfigError("storage.encryption_key: deve estar em Base64")
        if len(decoded) != 32:
            raise ConfigError(
                f"storage.encryption_key: deve ter 32 bytes apos decodificacao (atual: {len(decoded)})"
            )


# -- Estado do módulo ---------------------------------------------------------

def config_path() -> Path:
    return Path(os.getenv("NESTVAULT_CONFIG") or (Path(__file__).parent / "config.json"))


_data: dict[str, dict[str, Any]] = {}
_path: Optional[Path] = None
_boot_snapshot: dict[str, Any] = {}


def _defaults() -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {g: {} for g in GROUPS}
    for f in SCHEMA:
        out[f.group][f.name] = f.default
    return out


def _seed_from_env() -> dict[str, dict[str, Any]]:
    """Monta a config inicial a partir das variáveis de ambiente antigas."""
    data = _defaults()
    seeded: list[str] = []
    for f in SCHEMA:
        if not f.env:
            continue
        raw = os.getenv(f.env)
        # STORAGE_DIR é o nome legado, com precedência menor que STORAGE_DIRS
        if raw is None and f.key == "storage.dirs":
            raw = os.getenv("STORAGE_DIR")
        if raw is None or raw == "":
            continue
        try:
            value = _coerce(f, raw)
            _check(f, value)
        except ConfigError as e:
            _boot(logging.WARNING, f"[config] {f.env} ignorada na migracao: {e}")
            continue
        data[f.group][f.name] = value
        seeded.append(f.env)
    if seeded:
        _boot(logging.INFO, f"[config] migrando {len(seeded)} variavel(is) de ambiente: {', '.join(seeded)}")
    return data


def _merge(raw: dict) -> dict[str, dict[str, Any]]:
    """Aplica os defaults sobre o que veio do arquivo — chave ausente nunca falha."""
    data = _defaults()
    for f in SCHEMA:
        group = raw.get(f.group)
        if not isinstance(group, dict) or f.name not in group:
            continue
        try:
            value = _coerce(f, group[f.name])
            _check(f, value)
        except ConfigError as e:
            _boot(logging.WARNING, f"[config] valor invalido no arquivo, usando o padrao: {e}")
            continue
        data[f.group][f.name] = value
    return data


def _write(path: Path, data: dict) -> None:
    """Escrita atômica com permissão 0600 — o arquivo guarda segredos."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"_schema_version": SCHEMA_VERSION, **data}
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".config-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, ensure_ascii=False)
            fh.write("\n")
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    except Exception:
        Path(tmp).unlink(missing_ok=True)
        raise


def load(path: Optional[Path] = None) -> dict:
    """Carrega o arquivo (gerando-o a partir do ambiente se não existir)."""
    global _data, _path, _boot_snapshot
    _path = Path(path) if path else config_path()

    if _path.exists():
        try:
            raw = json.loads(_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            _boot(logging.ERROR, f"[config] {_path} ilegivel ({e}) — usando os padroes")
            raw = {}
        _data = _merge(raw if isinstance(raw, dict) else {})
    else:
        _data = _seed_from_env()
        _write(_path, _data)
        _boot(logging.INFO, f"[config] {_path} gerado a partir do ambiente")

    _boot_snapshot = _restart_values()
    return _data


def get(key: str) -> Any:
    f = BY_KEY[key]
    if not _data:
        load()
    return _data[f.group][f.name]


def path() -> str:
    return str(_path or config_path())


def _restart_values() -> dict[str, Any]:
    return {f.key: _data[f.group][f.name] for f in SCHEMA if f.requires_restart}


def pending_restart() -> bool:
    """True se algum campo estrutural mudou desde o boot."""
    return _restart_values() != _boot_snapshot


def _mask(value: str) -> str:
    if not value:
        return ""
    return "••••••" + value[-4:] if len(value) > 8 else "••••••"


def public() -> dict:
    """Payload consumido por GET /api/settings e pela tela — segredos mascarados."""
    if not _data:
        load()
    groups = []
    for g in GROUPS:
        fields = []
        for f in SCHEMA:
            if f.group != g:
                continue
            value = _data[g][f.name]
            fields.append({
                "key": f.key,
                "name": f.name,
                "label": f.label,
                "help": f.help,
                "type": f.type,
                "value": _mask(value) if f.secret else value,
                "default": f.default,
                "requires_restart": f.requires_restart,
                "secret": f.secret,
                "is_set": bool(value) if f.secret else None,
                "min": f.min,
                "max": f.max,
            })
        groups.append({"name": g, "label": GROUP_LABELS.get(g, g), "fields": fields})
    return {
        "groups": groups,
        "pending_restart": pending_restart(),
        "config_path": path(),
    }


# -- Escrita ------------------------------------------------------------------

def normalize(updates: dict) -> dict[str, Any]:
    """Valida um dict {grupo: {campo: valor}} e devolve {dotted_key: valor coagido}.

    Segredo com valor vazio é descartado (mantém o atual) — a tela nunca reenvia
    o valor real, só a máscara.
    """
    if not _data:
        load()
    if not isinstance(updates, dict):
        raise ConfigError("payload invalido: esperado um objeto por grupo")

    resolved: dict[str, Any] = {}
    for group, fields in updates.items():
        if group not in GROUPS:
            raise ConfigError(f"grupo desconhecido: '{group}'")
        if not isinstance(fields, dict):
            raise ConfigError(f"grupo '{group}': esperado um objeto de campos")
        for name, raw in fields.items():
            key = f"{group}.{name}"
            f = BY_KEY.get(key)
            if f is None:
                raise ConfigError(f"parametro desconhecido: '{key}'")
            if f.secret and (raw is None or str(raw).strip() == ""):
                continue
            value = _coerce(f, raw)
            _check(f, value)
            if value != _data[group][name]:
                resolved[key] = value
    return resolved


def save(updates: dict) -> dict[str, Any]:
    """Valida, persiste e aplica a quente. Devolve {dotted_key: novo valor} do que mudou."""
    resolved = normalize(updates)
    if not resolved:
        return {}
    for key, value in resolved.items():
        f = BY_KEY[key]
        _data[f.group][f.name] = value
    _write(_path or config_path(), _data)
    apply_runtime()
    log.info(f"[config] atualizado: {', '.join(sorted(resolved))}")
    return resolved


def touches_encryption(updates: dict) -> bool:
    """True se o payload realmente altera criptografia (usado pelo guardrail do PUT)."""
    try:
        resolved = normalize(updates)
    except ConfigError:
        return False
    return any(k.startswith("storage.encryption") for k in resolved)


# -- Aplicação a quente -------------------------------------------------------

def apply_runtime() -> None:
    """Propaga para os módulos os parâmetros que não exigem reinício.

    Os módulos leem seus globais dentro das funções, então reatribuí-los aqui
    vale imediatamente. Os campos requires_restart ficam de fora de propósito:
    volumes, engine do banco e chave de criptografia são fixados no import.
    """
    import storage
    import db_backup
    import daily_digest

    storage.REPLICATION_FACTOR = get("storage.replication_factor")
    storage.STORAGE_FALLBACK_THRESHOLD_GB = get("storage.fallback_threshold_gb")
    storage.SSD_CACHE_MAX_GB = get("ssd_cache.max_gb")
    storage.SSD_CACHE_IDLE_DELAY_MINUTES = get("ssd_cache.idle_delay_minutes")
    storage.AUTO_REBALANCE_ENABLED = get("storage.auto_rebalance_enabled")
    storage.REBALANCE_CHECK_INTERVAL_MINUTES = get("storage.rebalance_check_interval_minutes")

    db_backup.DB_BACKUP_ENABLED = get("db_backup.enabled")
    db_backup.DB_BACKUP_RETENTION = get("db_backup.retention")
    db_backup.DB_BACKUP_HOUR = get("db_backup.hour")
    db_backup.DB_BACKUP_MINUTE = get("db_backup.minute")

    daily_digest.TELEGRAM_BOT_TOKEN = get("digest.telegram_bot_token")
    daily_digest.TELEGRAM_CHAT_ID = get("digest.telegram_chat_id")
    daily_digest.ANTHROPIC_API_KEY = get("digest.anthropic_api_key")
    daily_digest.OLLAMA_URL = get("digest.ollama_url")
    daily_digest.OLLAMA_MODEL = get("digest.ollama_model")

    # Reagenda os jobs cron cujo horário pode ter mudado. O scheduler só existe
    # depois do lifespan; fora dele (testes, import) não há o que reagendar.
    try:
        import scheduler as sched
        if sched.scheduler.running:
            sched.schedule_daily_digest()
            sched.schedule_db_backup()
            sched.schedule_disk_rebalance_check()
    except Exception as e:  # pragma: no cover - reagendamento é best-effort
        log.warning(f"[config] falha ao reagendar jobs: {e}")


load()

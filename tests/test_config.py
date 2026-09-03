"""Testa a camada de configuração persistida em arquivo (server/config.py).

Cobre: defaults, migração a partir do ambiente no primeiro boot, round-trip de
escrita, permissão do arquivo, validação por campo, mascaramento de segredos e
o cálculo de pending_restart.
"""
import json
import os
import stat

import pytest

import config as cfg
import daily_digest
import db_backup
import storage as storage_mod

from conftest import ADMIN_KEY


@pytest.fixture(autouse=True)
def isolated_config(tmp_path, monkeypatch):
    """Cada teste carrega sua própria config e devolve o estado global ao final.

    config.py é um singleton de módulo e save() propaga para storage/db_backup/
    daily_digest — sem restaurar, um teste daqui contaminaria os demais.
    """
    saved = (cfg._data, cfg._path, cfg._boot_snapshot)
    saved_runtime = (
        storage_mod.REPLICATION_FACTOR,
        storage_mod.STORAGE_FALLBACK_THRESHOLD_GB,
        storage_mod.SSD_CACHE_MAX_GB,
        storage_mod.AUTO_REBALANCE_ENABLED,
        storage_mod.REBALANCE_CHECK_INTERVAL_MINUTES,
        db_backup.DB_BACKUP_ENABLED,
        db_backup.DB_BACKUP_RETENTION,
        db_backup.DB_BACKUP_HOUR,
        db_backup.DB_BACKUP_MINUTE,
        daily_digest.TELEGRAM_BOT_TOKEN,
        daily_digest.OLLAMA_MODEL,
    )
    monkeypatch.setenv("NESTVAULT_CONFIG", str(tmp_path / "config.json"))
    yield tmp_path / "config.json"
    cfg._data, cfg._path, cfg._boot_snapshot = saved
    (storage_mod.REPLICATION_FACTOR,
     storage_mod.STORAGE_FALLBACK_THRESHOLD_GB,
     storage_mod.SSD_CACHE_MAX_GB,
     storage_mod.AUTO_REBALANCE_ENABLED,
     storage_mod.REBALANCE_CHECK_INTERVAL_MINUTES,
     db_backup.DB_BACKUP_ENABLED,
     db_backup.DB_BACKUP_RETENTION,
     db_backup.DB_BACKUP_HOUR,
     db_backup.DB_BACKUP_MINUTE,
     daily_digest.TELEGRAM_BOT_TOKEN,
     daily_digest.OLLAMA_MODEL) = saved_runtime


# -- Criação e defaults -------------------------------------------------------

def test_creates_file_with_defaults_when_absent(isolated_config):
    path = isolated_config
    assert not path.exists()
    cfg.load(path)
    assert path.exists()

    raw = json.loads(path.read_text())
    assert raw["_schema_version"] == cfg.SCHEMA_VERSION
    assert cfg.get("storage.replication_factor") == 1
    assert cfg.get("storage.dirs") == ["./storage"]
    assert cfg.get("db_backup.enabled") is True
    assert cfg.get("digest.ollama_model") == "llama3"


def test_file_is_written_with_0600(isolated_config):
    cfg.load(isolated_config)
    mode = stat.S_IMODE(os.stat(isolated_config).st_mode)
    assert mode == 0o600, f"config.json guarda segredos, esperado 0600 (atual {oct(mode)})"


def test_missing_keys_fall_back_to_defaults(isolated_config):
    isolated_config.write_text(json.dumps({"storage": {"replication_factor": 3}}))
    cfg.load(isolated_config)
    assert cfg.get("storage.replication_factor") == 3
    assert cfg.get("db_backup.hour") == 1          # grupo inteiro ausente
    assert cfg.get("storage.fallback_threshold_gb") == 10.0  # campo ausente no grupo


def test_invalid_value_in_file_falls_back_instead_of_crashing(isolated_config):
    isolated_config.write_text(json.dumps({"storage": {"replication_factor": "abc"}}))
    cfg.load(isolated_config)
    assert cfg.get("storage.replication_factor") == 1


def test_corrupt_file_falls_back_to_defaults(isolated_config):
    isolated_config.write_text("{ nao e json")
    cfg.load(isolated_config)
    assert cfg.get("storage.replication_factor") == 1


# -- Migração a partir do ambiente --------------------------------------------

def test_seeds_from_env_on_first_boot(isolated_config, monkeypatch, tmp_path):
    v1, v2 = tmp_path / "v1", tmp_path / "v2"
    monkeypatch.setenv("STORAGE_DIRS", f"{v1},{v2}")
    monkeypatch.setenv("REPLICATION_FACTOR", "2")
    monkeypatch.setenv("DIGEST_HOUR", "20")
    monkeypatch.setenv("ENCRYPTION_ENABLED", "true")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "tok-123")

    cfg.load(isolated_config)

    assert cfg.get("storage.dirs") == [str(v1), str(v2)]
    assert cfg.get("storage.replication_factor") == 2
    assert cfg.get("digest.hour") == 20
    assert cfg.get("storage.encryption_enabled") is True
    assert cfg.get("digest.telegram_bot_token") == "tok-123"


def test_legacy_storage_dir_is_accepted(isolated_config, monkeypatch, tmp_path):
    monkeypatch.setenv("STORAGE_DIR", str(tmp_path / "solo"))
    cfg.load(isolated_config)
    assert cfg.get("storage.dirs") == [str(tmp_path / "solo")]


def test_storage_dirs_wins_over_legacy_storage_dir(isolated_config, monkeypatch):
    monkeypatch.setenv("STORAGE_DIRS", "/a,/b")
    monkeypatch.setenv("STORAGE_DIR", "/legado")
    cfg.load(isolated_config)
    assert cfg.get("storage.dirs") == ["/a", "/b"]


def test_env_is_ignored_once_the_file_exists(isolated_config, monkeypatch):
    cfg.load(isolated_config)                       # gera com os defaults
    monkeypatch.setenv("REPLICATION_FACTOR", "9")
    cfg.load(isolated_config)                       # segundo boot
    assert cfg.get("storage.replication_factor") == 1, "o arquivo e a fonte da verdade"


def test_invalid_env_is_skipped_during_migration(isolated_config, monkeypatch):
    monkeypatch.setenv("DB_BACKUP_HOUR", "99")
    cfg.load(isolated_config)
    assert cfg.get("db_backup.hour") == 1


# -- Escrita ------------------------------------------------------------------

def test_save_round_trips_to_disk(isolated_config):
    cfg.load(isolated_config)
    changed = cfg.save({"storage": {"replication_factor": 3}})
    assert changed == {"storage.replication_factor": 3}

    cfg.load(isolated_config)
    assert cfg.get("storage.replication_factor") == 3


def test_save_ignores_unchanged_values(isolated_config):
    cfg.load(isolated_config)
    assert cfg.save({"storage": {"replication_factor": 1}}) == {}


def test_save_applies_hot_values_to_the_modules(isolated_config):
    cfg.load(isolated_config)
    cfg.save({
        "storage":   {"replication_factor": 2, "fallback_threshold_gb": 25.0},
        "db_backup": {"retention": 14},
        "digest":    {"ollama_model": "mistral"},
    })
    assert storage_mod.REPLICATION_FACTOR == 2
    assert storage_mod.STORAGE_FALLBACK_THRESHOLD_GB == 25.0
    assert db_backup.DB_BACKUP_RETENTION == 14
    assert daily_digest.OLLAMA_MODEL == "mistral"


def test_save_keeps_permissions_on_rewrite(isolated_config):
    cfg.load(isolated_config)
    cfg.save({"db_backup": {"hour": 5}})
    assert stat.S_IMODE(os.stat(isolated_config).st_mode) == 0o600


def test_save_does_not_leave_temp_files_behind(isolated_config):
    cfg.load(isolated_config)
    cfg.save({"db_backup": {"hour": 5}})
    leftovers = [p.name for p in isolated_config.parent.iterdir() if p.name.startswith(".config-")]
    assert leftovers == []


# -- Validação ----------------------------------------------------------------

@pytest.mark.parametrize("updates", [
    {"db_backup": {"hour": 99}},                       # acima do max
    {"db_backup": {"minute": -1}},                     # abaixo do min
    {"db_backup": {"retention": 0}},                   # min 1
    {"storage": {"replication_factor": -1}},           # min 0
    {"storage": {"replication_factor": "abc"}},        # tipo
    {"storage": {"fallback_threshold_gb": "muito"}},   # tipo
    {"storage": {"dirs": []}},                         # lista vazia
    {"storage": {"encryption_key": "nao-e-base64!!"}},  # base64 inválido
    {"storage": {"encryption_key": "YWJj"}},           # base64 válido, 3 bytes
    {"grupo_inexistente": {"x": 1}},
    {"storage": {"campo_inexistente": 1}},
])
def test_rejects_invalid_updates(isolated_config, updates):
    cfg.load(isolated_config)
    with pytest.raises(cfg.ConfigError):
        cfg.save(updates)


def test_rejected_update_does_not_touch_the_file(isolated_config):
    cfg.load(isolated_config)
    before = isolated_config.read_text()
    with pytest.raises(cfg.ConfigError):
        cfg.save({"db_backup": {"hour": 99}})
    assert isolated_config.read_text() == before


def test_accepts_a_valid_32_byte_key(isolated_config):
    import base64
    key = base64.b64encode(b"x" * 32).decode()
    cfg.load(isolated_config)
    cfg.save({"storage": {"encryption_key": key}})
    assert cfg.get("storage.encryption_key") == key


# -- Segredos -----------------------------------------------------------------

def test_public_masks_secrets(isolated_config):
    cfg.load(isolated_config)
    cfg.save({"digest": {"telegram_bot_token": "12345678abcd"}})

    fields = {f["key"]: f for g in cfg.public()["groups"] for f in g["fields"]}
    token = fields["digest.telegram_bot_token"]
    assert token["secret"] is True
    assert token["is_set"] is True
    assert "12345678" not in token["value"]
    assert token["value"].startswith("••••••")


def test_public_reports_unset_secrets(isolated_config):
    cfg.load(isolated_config)
    fields = {f["key"]: f for g in cfg.public()["groups"] for f in g["fields"]}
    assert fields["digest.anthropic_api_key"]["is_set"] is False
    assert fields["digest.anthropic_api_key"]["value"] == ""


def test_empty_secret_keeps_the_current_value(isolated_config):
    cfg.load(isolated_config)
    cfg.save({"digest": {"telegram_bot_token": "tok-abc"}})
    cfg.save({"digest": {"telegram_bot_token": "", "telegram_chat_id": "42"}})
    assert cfg.get("digest.telegram_bot_token") == "tok-abc"
    assert cfg.get("digest.telegram_chat_id") == "42"


def test_non_secret_field_can_be_cleared(isolated_config):
    cfg.load(isolated_config)
    cfg.save({"digest": {"telegram_chat_id": "42"}})
    cfg.save({"digest": {"telegram_chat_id": ""}})
    assert cfg.get("digest.telegram_chat_id") == ""


# -- pending_restart ----------------------------------------------------------

def test_pending_restart_is_false_after_load(isolated_config):
    cfg.load(isolated_config)
    assert cfg.pending_restart() is False


def test_hot_field_does_not_require_restart(isolated_config):
    cfg.load(isolated_config)
    cfg.save({"storage": {"replication_factor": 2}})
    assert cfg.pending_restart() is False


def test_structural_field_requires_restart(isolated_config, tmp_path):
    cfg.load(isolated_config)
    cfg.save({"storage": {"dirs": [str(tmp_path / "novo")]}})
    assert cfg.pending_restart() is True


def test_pending_restart_clears_on_next_load(isolated_config, tmp_path):
    cfg.load(isolated_config)
    cfg.save({"database": {"path": str(tmp_path / "outro.db")}})
    assert cfg.pending_restart() is True
    cfg.load(isolated_config)
    assert cfg.pending_restart() is False


# -- Coerência do schema ------------------------------------------------------

def test_every_field_is_reachable_and_typed(isolated_config):
    cfg.load(isolated_config)
    for f in cfg.SCHEMA:
        value = cfg.get(f.key)
        expected = {"str": str, "int": int, "float": float, "bool": bool, "list[str]": list}[f.type]
        assert isinstance(value, expected), f"{f.key} deveria ser {f.type}"


def test_schema_keys_are_unique():
    keys = [f.key for f in cfg.SCHEMA]
    assert len(keys) == len(set(keys))


def test_every_group_has_a_label():
    for group in cfg.GROUPS:
        assert group in cfg.GROUP_LABELS


# -- Endpoints ----------------------------------------------------------------

def test_get_settings_returns_all_groups(auth_client):
    r = auth_client.get("/api/settings", headers={"X-API-Key": ADMIN_KEY})
    assert r.status_code == 200
    body = r.json()
    assert [g["name"] for g in body["groups"]] == cfg.GROUPS
    assert body["pending_restart"] is False
    assert body["config_path"]


def test_put_settings_persists_and_returns_new_state(auth_client):
    r = auth_client.put("/api/settings", headers={"X-API-Key": ADMIN_KEY},
                        json={"db_backup": {"retention": 21}})
    assert r.status_code == 200
    fields = {f["key"]: f for g in r.json()["groups"] for f in g["fields"]}
    assert fields["db_backup.retention"]["value"] == 21
    assert cfg.get("db_backup.retention") == 21


def test_put_settings_rejects_invalid_value(auth_client):
    r = auth_client.put("/api/settings", headers={"X-API-Key": ADMIN_KEY},
                        json={"db_backup": {"hour": 99}})
    assert r.status_code == 400
    assert "db_backup.hour" in r.json()["detail"]


def test_put_settings_never_echoes_a_secret(auth_client):
    r = auth_client.put("/api/settings", headers={"X-API-Key": ADMIN_KEY},
                        json={"digest": {"telegram_bot_token": "supersecreto123"}})
    assert r.status_code == 200
    assert "supersecreto123" not in r.text


def test_encryption_change_is_allowed_while_there_is_no_content(auth_client):
    r = auth_client.put("/api/settings", headers={"X-API-Key": ADMIN_KEY},
                        json={"storage": {"encryption_enabled": True}})
    assert r.status_code == 200
    assert cfg.get("storage.encryption_enabled") is True

"""Saúde SMART (v10.2.0): parse do JSON do smartctl (ATA, USB-SAT, NVMe),
detecção de piora, alertas, volume indisponível e os endpoints /storage/smart."""
import subprocess

import pytest

import config
import database as db_mod
import main as m
import notify
import smart


def _ata(passed=True, temp=38, realloc=0, pending=0, uncorr=0, hours=1200):
    return {
        "model_name": "WDC WD40EFRX",
        "smart_status": {"passed": passed},
        "temperature": {"current": temp},
        "power_on_time": {"hours": hours},
        "ata_smart_attributes": {"table": [
            {"id": 5, "raw": {"value": realloc}},
            {"id": 197, "raw": {"value": pending}},
            {"id": 198, "raw": {"value": uncorr}},
        ]},
    }


NVME = {
    "model_name": "Samsung SSD 970",
    "smart_status": {"passed": True},
    "nvme_smart_health_information_log": {
        "temperature": 41, "power_on_hours": 900, "media_errors": 0, "critical_warning": 0,
    },
}


def test_parse_ata():
    p = smart.parse(_ata(temp=40, realloc=3, hours=5000))
    assert p == {"passed": True, "temp_c": 40, "power_on_hours": 5000, "reallocated": 3,
                 "pending": 0, "uncorrectable": 0, "model": "WDC WD40EFRX"}
    assert smart.health(p) == "warn"
    assert smart.health(smart.parse(_ata())) == "ok"
    assert smart.health(smart.parse(_ata(pending=1))) == "fail"
    assert smart.health(smart.parse(_ata(passed=False))) == "fail"


def test_parse_nvme():
    p = smart.parse(NVME)
    assert p["temp_c"] == 41 and p["power_on_hours"] == 900
    assert p["uncorrectable"] == 0 and p["reallocated"] is None and p["passed"] is True


def test_parent_disk_fallback(monkeypatch):
    monkeypatch.setattr(smart.shutil, "which", lambda name: None)
    assert smart.parent_disk("/dev/sda1") == "/dev/sda"
    assert smart.parent_disk("/dev/nvme0n1p2") == "/dev/nvme0n1"
    assert smart.parent_disk("/dev/mmcblk0p2") == "/dev/mmcblk0"
    assert smart.parent_disk("/dev/sdb") == "/dev/sdb"


def test_overrides(monkeypatch):
    monkeypatch.setitem(config._data["smart"], "device_overrides",
                        ["/mnt/hd1=/dev/sdc:sat", "/mnt/hd2/=/dev/sdd", "lixo"])
    assert smart.resolve_device("/mnt/hd1") == ("/dev/sdc", "sat")
    assert smart.resolve_device("/mnt/hd2") == ("/dev/sdd", None)


def test_worsened():
    prev = db_mod.SmartSnapshot(passed=True, temp_c=40, reallocated=0, pending=0, uncorrectable=0)
    assert smart.worsened(prev, smart.parse(_ata(temp=41))) == []
    reasons = smart.worsened(prev, smart.parse(_ata(realloc=2, temp=56)))
    assert any("realocados: 0 → 2" in r for r in reasons)
    assert any("temperatura 56" in r for r in reasons)
    # Temperatura só alerta na subida.
    hot = db_mod.SmartSnapshot(passed=True, temp_c=57, reallocated=2, pending=0, uncorrectable=0)
    assert smart.worsened(hot, smart.parse(_ata(realloc=2, temp=58))) == []
    assert smart.worsened(None, smart.parse(_ata(passed=False))) == ["autoteste SMART reprovado"]


@pytest.fixture
def fake_disk(tmp_vol, monkeypatch):
    """Volume de teste mapeado para /dev/sdz e smartctl respondendo `state['data']`."""
    state = {"data": _ata(), "calls": []}
    monkeypatch.setitem(config._data["smart"], "device_overrides", [f"{tmp_vol}=/dev/sdz"])
    monkeypatch.setitem(config._data["smart"], "use_sudo", False)
    monkeypatch.setattr(smart.shutil, "which", lambda name: f"/usr/sbin/{name}")
    # Resolvido na chamada: m.SessionLocal só aponta para o banco do teste
    # depois que a fixture client/two_users roda.
    monkeypatch.setattr(smart, "SessionLocal", lambda: m.SessionLocal())

    def fake_run(cmd):
        import json
        state["calls"].append(cmd)
        return subprocess.CompletedProcess(cmd, 0, json.dumps(state["data"]), "")
    monkeypatch.setattr(smart, "_run", fake_run)
    sent = []
    monkeypatch.setattr(notify, "send_sync", lambda text, **kw: sent.append(text) or True)
    state["sent"] = sent
    return state


def test_run_check_stores_and_alerts(client, tmp_vol, fake_disk):
    assert smart.run_smart_check() == {str(tmp_vol): "ok"}
    assert fake_disk["calls"][0] == ["smartctl", "-j", "-a", "/dev/sdz"]
    assert fake_disk["sent"] == []

    fake_disk["data"] = _ata(realloc=8)
    assert smart.run_smart_check() == {str(tmp_vol): "warn"}
    assert len(fake_disk["sent"]) == 1 and "realocados: 0 → 8" in fake_disk["sent"][0]

    # Mesma contagem: nada novo, nada enviado.
    smart.run_smart_check()
    assert len(fake_disk["sent"]) == 1

    r = client.get("/storage/smart")
    assert r.status_code == 200
    vol = r.json()["volumes"][0]
    assert vol["health"] == "warn" and vol["latest"]["reallocated"] == 8
    assert vol["latest"]["device"] == "/dev/sdz" and len(vol["temps"]) == 3


def test_usb_bridge_retries_with_sat(client, tmp_vol, fake_disk, monkeypatch):
    import json

    def run(cmd):
        fake_disk["calls"].append(cmd)
        if "-d" in cmd:
            return subprocess.CompletedProcess(cmd, 0, json.dumps(_ata()), "")
        return subprocess.CompletedProcess(cmd, 1, json.dumps(
            {"smartctl": {"messages": [{"string": "Unknown USB bridge"}]}}), "")
    monkeypatch.setattr(smart, "_run", run)
    assert smart.run_smart_check() == {str(tmp_vol): "ok"}
    assert fake_disk["calls"][-1] == ["smartctl", "-j", "-a", "-d", "sat", "/dev/sdz"]


def test_unavailable_volume(client, tmp_vol, fake_disk, monkeypatch):
    import json
    monkeypatch.setattr(smart, "_run", lambda cmd: subprocess.CompletedProcess(
        cmd, 2, json.dumps({"smartctl": {"messages": [{"string": "No such device"}]}}), ""))
    res = smart.run_smart_check()
    assert res[str(tmp_vol)].startswith("indisponível") and "No such device" in res[str(tmp_vol)]
    vol = client.get("/storage/smart").json()["volumes"][0]
    assert vol["health"] == "unknown" and "No such device" in vol["error"]


def test_refresh_endpoint_admin_only(two_users, fake_disk):
    admin, alice, _bob = two_users
    assert alice.post("/storage/smart/refresh").status_code == 403
    r = admin.post("/storage/smart/refresh")
    assert r.status_code == 200 and r.json()["volumes"][0]["health"] == "ok"

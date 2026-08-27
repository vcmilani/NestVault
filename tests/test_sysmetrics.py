import sysmetrics


def test_parse_cpu_stat_aggregate_and_per_core():
    text = (
        "cpu  100 0 200 700 10 0 0 0 0 0\n"
        "cpu0 50 0 100 350 5 0 0 0 0 0\n"
        "cpu1 50 0 100 350 5 0 0 0 0 0\n"
        "intr 12345 0 0\n"
    )
    rows = sysmetrics.parse_cpu_stat(text)
    assert len(rows) == 3
    idle, total = rows[0]
    assert idle == 700 + 10  # idle + iowait
    assert total == sum([100, 0, 200, 700, 10, 0, 0, 0, 0, 0])
    idle0, total0 = rows[1]
    assert idle0 == 350 + 5
    assert total0 == sum([50, 0, 100, 350, 5, 0, 0, 0, 0, 0])


def test_parse_meminfo_converts_kb_to_bytes():
    text = (
        "MemTotal:        3886904 kB\n"
        "MemFree:          129920 kB\n"
        "MemAvailable:     725880 kB\n"
        "SwapTotal:        102396 kB\n"
        "SwapFree:         102396 kB\n"
    )
    vals = sysmetrics.parse_meminfo(text)
    assert vals["MemTotal"] == 3886904 * 1024
    assert vals["MemAvailable"] == 725880 * 1024
    assert vals["SwapTotal"] == 102396 * 1024
    assert vals["SwapFree"] == 102396 * 1024
    assert "MemFree" not in vals


def test_parse_meminfo_missing_fields_are_absent():
    assert sysmetrics.parse_meminfo("Weird: 1 kB\n") == {}


def test_sample_delta_produces_known_cpu_pct(monkeypatch):
    sysmetrics.reset()

    stat_texts = [
        "cpu  0 0 0 1000 0 0 0 0 0 0\n",
        "cpu  400 0 0 1600 0 0 0 0 0 0\n",  # +400 busy, +600 idle over 1000 total delta -> 40% busy
    ]
    call = {"i": 0}

    def fake_read_cpu_stat():
        text = stat_texts[min(call["i"], len(stat_texts) - 1)]
        call["i"] += 1
        return sysmetrics.parse_cpu_stat(text)

    monkeypatch.setattr(sysmetrics, "_read_cpu_stat", fake_read_cpu_stat)
    monkeypatch.setattr(sysmetrics, "_read_meminfo", lambda: {
        "MemTotal": 1000, "MemAvailable": 400, "SwapTotal": 100, "SwapFree": 100,
    })
    monkeypatch.setattr(sysmetrics, "_read_uptime", lambda: 12345.0)
    monkeypatch.setattr(sysmetrics, "_read_loadavg", lambda: (0.1, 0.2, 0.3))
    monkeypatch.setattr(sysmetrics, "_read_temp_c", lambda: 42.0)

    assert sysmetrics.snapshot() is None  # nada amostrado ainda

    sysmetrics.sample()  # primeira amostra: sem _prev_cpu, cpu_pct ainda None
    snap = sysmetrics.snapshot()
    assert snap["cpu_pct"] is None
    assert snap["mem_pct"] == 60.0
    assert snap["history"] == []  # só entra no histórico quando cpu_pct existe

    sysmetrics.sample()  # segunda amostra: delta = 400 busy / 1000 total -> 40% cpu
    snap = sysmetrics.snapshot()
    assert snap["cpu_pct"] == 40.0
    assert snap["mem_pct"] == 60.0
    assert snap["swap_pct"] == 0.0
    assert snap["temp_c"] == 42.0
    assert snap["uptime_seconds"] == 12345
    assert len(snap["history"]) == 1
    assert snap["history"][0]["cpu"] == 40.0
    assert snap["history"][0]["mem"] == 60.0


def test_sample_zero_total_delta_does_not_divide_by_zero(monkeypatch):
    sysmetrics.reset()
    same_text = "cpu  10 0 0 90 0 0 0 0 0 0\n"

    monkeypatch.setattr(sysmetrics, "_read_cpu_stat", lambda: sysmetrics.parse_cpu_stat(same_text))
    monkeypatch.setattr(sysmetrics, "_read_meminfo", lambda: None)
    monkeypatch.setattr(sysmetrics, "_read_uptime", lambda: None)
    monkeypatch.setattr(sysmetrics, "_read_loadavg", lambda: None)
    monkeypatch.setattr(sysmetrics, "_read_temp_c", lambda: None)

    sysmetrics.sample()
    sysmetrics.sample()  # mesmo texto -> delta total == 0
    snap = sysmetrics.snapshot()
    assert snap["cpu_pct"] == 0.0
    assert snap["mem_pct"] is None
    assert snap["load_1"] is None


def test_history_caps_at_history_points(monkeypatch):
    sysmetrics.reset()
    counter = {"n": 0}

    def fake_read_cpu_stat():
        counter["n"] += 1
        busy = counter["n"] * 10
        return sysmetrics.parse_cpu_stat(f"cpu  {busy} 0 0 {1000 - busy} 0 0 0 0 0 0\n")

    monkeypatch.setattr(sysmetrics, "_read_cpu_stat", fake_read_cpu_stat)
    monkeypatch.setattr(sysmetrics, "_read_meminfo", lambda: {"MemTotal": 1000, "MemAvailable": 500})
    monkeypatch.setattr(sysmetrics, "_read_uptime", lambda: 1.0)
    monkeypatch.setattr(sysmetrics, "_read_loadavg", lambda: (0, 0, 0))
    monkeypatch.setattr(sysmetrics, "_read_temp_c", lambda: None)

    for _ in range(sysmetrics.HISTORY_POINTS + 10):
        sysmetrics.sample()

    snap = sysmetrics.snapshot()
    assert len(snap["history"]) == sysmetrics.HISTORY_POINTS


def test_sample_degrades_gracefully_when_proc_unavailable(monkeypatch):
    """Host sem /proc (não-Linux): sample() não deve levantar, e snapshot()
    passa a devolver um dict com todos os campos em None em vez de None puro
    (só é None antes da primeira chamada a sample() — cold start)."""
    sysmetrics.reset()
    assert sysmetrics.snapshot() is None

    monkeypatch.setattr(sysmetrics, "_read_cpu_stat", lambda: None)
    monkeypatch.setattr(sysmetrics, "_read_meminfo", lambda: None)
    monkeypatch.setattr(sysmetrics, "_read_uptime", lambda: None)
    monkeypatch.setattr(sysmetrics, "_read_loadavg", lambda: None)
    monkeypatch.setattr(sysmetrics, "_read_temp_c", lambda: None)

    sysmetrics.sample()
    snap = sysmetrics.snapshot()
    assert snap is not None
    assert snap["cpu_pct"] is None
    assert snap["mem_pct"] is None
    assert snap["temp_c"] is None


def test_activity_endpoint_exposes_system_key(client):
    r = client.get("/api/activity")
    assert r.status_code == 200
    data = r.json()
    assert "system" in data
    # sem lifespan rodando (TestClient sobe o lifespan, mas sem amostra prévia
    # o primeiro valor pode vir None) — a chave deve sempre existir e, se
    # presente, respeitar o schema.
    if data["system"] is not None:
        assert "cpu_pct" in data["system"]
        assert "history" in data["system"]

"""The SNMP community string reaches every SET (replay, start reset, end reset)."""

import asyncio
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import pytest

import signal_replay as sr
from signal_replay import replay as replay_mod

DEVICE = "dev"


def _events(n: int = 6) -> pd.DataFrame:
    t0 = datetime(2026, 1, 5, 8, 0, 0)
    rows = []
    for k in range(n):
        rows.append((t0 + timedelta(seconds=0.2 * k), 82, 2))
        rows.append((t0 + timedelta(seconds=0.2 * k + 0.1), 81, 2))
    df = pd.DataFrame(rows, columns=["timestamp", "event_id", "parameter"])
    df["device_id"] = DEVICE
    return df


@pytest.fixture
def recorded(monkeypatch):
    calls: List[tuple] = []

    async def fake_send(ip_port, group, state, dtype, community="public", timeout=2.0, *, snmp_engine):
        calls.append(("send", community, int(state)))

    async def fake_reset(ip_port, community="public", debug=False, timeout=2.0, *, snmp_engine, **_kw):
        calls.append(("reset_all", community, 0))

    monkeypatch.setattr(replay_mod, "async_send_ntcip", fake_send)
    monkeypatch.setattr(replay_mod, "async_reset_all_detectors", fake_reset)
    return calls


def test_default_community_is_public(recorded):
    config = sr.SignalConfig(device_id=DEVICE, ip="127.0.0.1", udp_port=1025)
    assert config.snmp_community == "public"
    config.events = _events()
    sr.SignalReplay(config).run()
    assert recorded and {c[1] for c in recorded} == {"public"}


def test_custom_community_used_for_replay_and_resets(recorded):
    config = sr.SignalConfig(device_id=DEVICE, ip="127.0.0.1", udp_port=1025, snmp_community="bench")
    config.events = _events()
    replay = sr.SignalReplay(config)
    replay.run()

    kinds = {c[0] for c in recorded}
    assert kinds == {"send", "reset_all"}
    assert {c[1] for c in recorded} == {"bench"}
    # The closing reset (state 0 sends after the replay) used it too.
    assert replay.detectors_reset is True
    assert any(c[0] == "send" and c[2] == 0 for c in recorded)


def test_blocking_safety_net_reset_uses_community(recorded):
    config = sr.SignalConfig(device_id=DEVICE, ip="127.0.0.1", udp_port=1025, snmp_community="bench")
    config.events = _events()
    replay = sr.SignalReplay(config)
    replay.touched_keys.add(("Vehicle", 1))
    assert replay.reset_touched_groups_blocking(budget=2.0) is True
    assert recorded and {c[1] for c in recorded} == {"bench"}


@pytest.mark.parametrize("bad", ["", None, 5])
def test_invalid_community_rejected(bad):
    with pytest.raises(ValueError):
        sr.SignalConfig(device_id=DEVICE, ip="127.0.0.1", udp_port=1025, snmp_community=bad)


def test_batch_runner_scenario_then_suite_community(tmp_path):
    scenarios = [
        sr.TestScenario(scenario_id="A", database_name="a.bin", events_source="a.csv",
                        test_type=sr.TestType.SIMILARITY, snmp_community="scenario-c"),
        sr.TestScenario(scenario_id="B", database_name="b.bin", events_source="b.csv",
                        test_type=sr.TestType.SIMILARITY),
    ]
    suite = sr.SoftwareTestSuite(
        suite_name="s", software_version="1", baseline_version="1", scenarios=scenarios,
        batches=[sr.TestBatch(batch_id="b1", assignments={"A": "127.0.0.1:1025", "B": "127.0.0.1:1026"})],
        snmp_community="suite-c",
    )
    with sr.BatchRunner(suite, work_dir=tmp_path, run_log=False, interactive=False) as runner:
        assert runner._signal_config(scenarios[0], "127.0.0.1:1025").snmp_community == "scenario-c"
        assert runner._signal_config(scenarios[1], "127.0.0.1:1026").snmp_community == "suite-c"

    default_suite = sr.SoftwareTestSuite(
        suite_name="s", software_version="1", baseline_version="1", scenarios=scenarios[1:],
        batches=[sr.TestBatch(batch_id="b1", assignments={"B": "127.0.0.1:1026"})],
    )
    with sr.BatchRunner(default_suite, work_dir=tmp_path / "d", run_log=False, interactive=False) as runner:
        assert runner._signal_config(scenarios[1], "127.0.0.1:1026").snmp_community == "public"

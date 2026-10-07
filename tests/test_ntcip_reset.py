"""reset_all_detectors error handling (SNMP is mocked; nothing is sent)."""

import logging
from unittest.mock import patch

import pytest

import signal_replay as sr


def _failing_send(message):
    calls = []

    async def send(ip_port, group, state, dtype, community="public", timeout=2.0, *, snmp_engine):
        calls.append((dtype, group))
        raise RuntimeError(message)

    return send, calls


def test_reset_all_detectors_logs_and_returns_by_default(caplog):
    send, calls = _failing_send("SNMP error: No SNMP response received before timeout")
    with patch("signal_replay.ntcip.async_send_ntcip", send), caplog.at_level(
        logging.WARNING, logger="signal_replay.ntcip"
    ):
        assert sr.reset_all_detectors(("192.0.2.1", 161), timeout=0.1) is None
    assert calls == [("Vehicle", 1)]  # remaining resets skipped
    assert any("Reset failed" in r.getMessage() for r in caplog.records)


def test_reset_all_detectors_raise_on_error_is_a_connectivity_check():
    send, _calls = _failing_send("SNMP error: No SNMP response received before timeout")
    with patch("signal_replay.ntcip.async_send_ntcip", send):
        with pytest.raises(RuntimeError, match="timeout"):
            sr.reset_all_detectors(("192.0.2.1", 161), timeout=0.1, raise_on_error=True)


def test_reset_all_detectors_raise_on_error_still_skips_missing_groups():
    sent = []

    async def send(ip_port, group, state, dtype, community="public", timeout=2.0, *, snmp_engine):
        if group > 2:
            raise RuntimeError("SNMP error: noSuchName")
        sent.append((dtype, group))

    with patch("signal_replay.ntcip.async_send_ntcip", send):
        sr.reset_all_detectors(("192.0.2.1", 161), raise_on_error=True)
    assert sent == [(t, g) for t in ("Vehicle", "Ped", "Preempt") for g in (1, 2)]

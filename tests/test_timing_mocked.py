"""
Unit tests for simulation timing that use mocks for CI environments.

These tests verify the orchestrator timing logic is correct without
requiring a live device. They mock SNMP and HTTP calls.
"""

import pytest
import time
import asyncio
import threading
from datetime import datetime, timedelta
from typing import List
from unittest.mock import patch, MagicMock
import pandas as pd

import signal_replay as sr


def create_synthetic_events(
    duration_seconds: float,
    events_per_second: float = 2.0,
    detector_groups: List[int] = [1, 2, 3],
    device_id: str = 'test_device'
) -> pd.DataFrame:
    """Create synthetic event data with known duration."""
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    total_events = int(duration_seconds * events_per_second)
    
    events = []
    for i in range(total_events):
        timestamp = base_time + timedelta(seconds=i / events_per_second)
        detector = detector_groups[i % len(detector_groups)]
        event_id = 82 if (i // len(detector_groups)) % 2 == 0 else 81
        
        events.append({
            'timestamp': timestamp,
            'event_id': event_id,
            'parameter': detector,
            'device_id': device_id
        })
    
    return pd.DataFrame(events)


class TestOrchestratorTimingWithMocks:
    """
    Tests for orchestrator timing using mocks.
    
    These tests verify that there is no "double-wait" bug where the
    orchestrator waits for the replay to complete AND THEN sleeps
    for the estimated duration again.
    """
    
    @pytest.fixture
    def mock_ip(self):
        return '127.0.0.1'
    
    @pytest.fixture
    def mock_port(self):
        return 1025
    
    @pytest.fixture
    def short_events(self):
        """Create short 5-second event sequence."""
        return create_synthetic_events(duration_seconds=5.0, events_per_second=2.0)
    
    @patch('signal_replay.collector.fetch_output_data')
    @patch('signal_replay.replay.send_ntcip')
    @patch('signal_replay.replay.reset_all_detectors')
    def test_no_double_wait_single_run(
        self,
        mock_reset: MagicMock,
        mock_send: MagicMock,
        mock_fetch: MagicMock,
        mock_ip,
        mock_port,
        short_events,
        temp_db_path
    ):
        """
        Verify orchestrator does NOT wait twice for each run.
        
        The bug: _run_all_signals() blocks for ~N seconds, then code
        sleeps for estimated_duration + 5 seconds = another ~N seconds.
        Total: 2N instead of N.
        
        This test fails if actual time > 1.5x expected duration.
        """
        mock_fetch.return_value = pd.DataFrame(columns=['TimeStamp', 'EventTypeID', 'Parameter'])
        
        signal_config = sr.SignalConfig(
            device_id='test_device',
            ip=mock_ip,
            udp_port=mock_port,
            cycle_length=0,
            incompatible_pairs=[]
        )
        
        sim_config = sr.SimulationConfig(
            signals=[signal_config],
            events=short_events,
            simulation_replays=1,
            stop_on_conflict=False,
            db_path=temp_db_path,
            simulation_speed=1.0
        )
        
        # Get expected duration from replay
        # Manually set events on signal config for replay duration calculation
        signal_config.events = short_events
        replay = sr.SignalReplay(signal_config, simulation_speed=1.0)
        expected_duration = replay.get_run_duration()
        
        start = time.time()
        sim = sr.ATCSimulation(sim_config, debug=False)
        results = sim.run()
        actual_duration = time.time() - start
        
        # With mocks (no real SNMP), overhead should be minimal
        # Account for post_replay_settle_seconds (default 10s) and thread scheduling
        # If double-wait exists, actual will be ~2x expected (FAIL)
        settle = sim_config.post_replay_settle_seconds
        max_allowed = expected_duration + settle + 3.0
        
        assert actual_duration <= max_allowed, (
            f"Single-run took {actual_duration:.1f}s but expected ~{expected_duration:.1f}s "
            f"(+{settle:.0f}s settle). "
            f"Max allowed: {max_allowed:.1f}s. "
            f"Ratio: {actual_duration / expected_duration:.2f}x. "
            f"This suggests the double-wait bug where orchestrator waits for "
            f"replay completion AND THEN sleeps for estimated_duration."
        )
        
        assert len(results['completed_runs']) == 1
    
    @patch('signal_replay.collector.fetch_output_data')
    @patch('signal_replay.replay.send_ntcip')
    @patch('signal_replay.replay.reset_all_detectors')
    def test_no_double_wait_multiple_runs(
        self,
        mock_reset: MagicMock,
        mock_send: MagicMock,
        mock_fetch: MagicMock,
        mock_ip,
        mock_port,
        short_events,
        temp_db_path
    ):
        """
        Verify multiple runs scale linearly, not 2x per run.
        
        If double-wait exists: 2 runs of 5s events = 20s (2 * 2 * 5)
        Correct behavior: 2 runs of 5s events = ~10s + overhead
        """
        num_runs = 2
        mock_fetch.return_value = pd.DataFrame(columns=['TimeStamp', 'EventTypeID', 'Parameter'])
        
        signal_config = sr.SignalConfig(
            device_id='test_device',
            ip=mock_ip,
            udp_port=mock_port,
            cycle_length=0,
            incompatible_pairs=[]
        )
        
        sim_config = sr.SimulationConfig(
            signals=[signal_config],
            events=short_events,
            simulation_replays=num_runs,
            stop_on_conflict=False,
            db_path=temp_db_path,
            simulation_speed=1.0
        )
        
        # Manually set events on signal config for replay duration calculation
        signal_config.events = short_events
        replay = sr.SignalReplay(signal_config, simulation_speed=1.0)
        expected_per_run = replay.get_run_duration()
        expected_total = expected_per_run * num_runs
        
        start = time.time()
        sim = sr.ATCSimulation(sim_config, debug=False)
        results = sim.run()
        actual_duration = time.time() - start
        
        # With mocks, overhead should be minimal
        # Account for post_replay_settle_seconds (default 10s) per run
        # Double-wait bug would make ratio ~2x (FAIL)
        settle = sim_config.post_replay_settle_seconds
        max_allowed = expected_total + (settle * num_runs) + (3.0 * num_runs)
        
        assert actual_duration <= max_allowed, (
            f"Multi-run took {actual_duration:.1f}s "
            f"(avg {actual_duration / num_runs:.1f}s/run) but expected "
            f"~{expected_total:.1f}s total ({expected_per_run:.1f}s/run +{settle:.0f}s settle). "
            f"Max allowed: {max_allowed:.1f}s. "
            f"Ratio: {actual_duration / expected_total:.2f}x. "
            f"This suggests the double-wait bug."
        )
        
        assert len(results['completed_runs']) == num_runs


class TestSignalReplayTiming:
    """Tests for the SignalReplay class timing."""
    
    @pytest.fixture
    def mock_ip(self):
        return '127.0.0.1'
    
    @pytest.fixture
    def mock_port(self):
        return 1025
    
    @pytest.fixture
    def short_events(self):
        return create_synthetic_events(duration_seconds=3.0, events_per_second=2.0)
    
    def test_expected_duration_calculation(self, mock_ip, mock_port, short_events):
        """Verify get_run_duration() returns correct value."""
        config = sr.SignalConfig(
            device_id='test',
            ip=mock_ip,
            udp_port=mock_port,
            cycle_length=0,
            incompatible_pairs=[]
        )
        
        # Manually set events on signal config for replay duration calculation
        config.events = short_events
        replay = sr.SignalReplay(config, simulation_speed=1.0)
        expected = replay.get_run_duration()
        
        # Should be approximately 3 seconds
        assert 2.0 <= expected <= 4.0, f"Expected ~3s, got {expected}"
    
    @patch('signal_replay.replay.send_ntcip')
    @patch('signal_replay.replay.reset_all_detectors')
    def test_replay_runs_in_expected_time(
        self,
        mock_reset: MagicMock,
        mock_send: MagicMock,
        mock_ip,
        mock_port,
        short_events
    ):
        """Verify replay completes in approximately expected time."""
        config = sr.SignalConfig(
            device_id='test',
            ip=mock_ip,
            udp_port=mock_port,
            cycle_length=0,
            incompatible_pairs=[]
        )
        
        # Manually set events on signal config for replay
        config.events = short_events
        
        replay = sr.SignalReplay(config, simulation_speed=1.0)
        expected = replay.get_run_duration()
        
        start = time.time()
        replay.run()
        actual = time.time() - start
        
        # Should complete within expected time + small overhead
        assert actual <= expected + 1.0, (
            f"Replay took {actual:.2f}s but expected {expected:.2f}s"
        )
        # Should not be too fast (must wait for events)
        assert actual >= expected - 0.5, (
            f"Replay too fast: {actual:.2f}s vs expected {expected:.2f}s"
        )

    def test_empty_activation_feed_raises_clear_error(self, mock_ip, mock_port):
        """Non-detector-only logs should fail early with a clear error."""
        events = pd.DataFrame({
            "timestamp": [datetime(2024, 1, 1, 12, 0, 0), datetime(2024, 1, 1, 12, 0, 1)],
            "event_id": [1, 10],  # No replayable detector events
            "parameter": [1, 1],
            "device_id": ["test", "test"],
        })

        config = sr.SignalConfig(
            device_id="test",
            ip=mock_ip,
            udp_port=mock_port,
            cycle_length=0,
            incompatible_pairs=[],
        )
        config.events = events

        with pytest.raises(ValueError, match="has no replayable detector events"):
            sr.SignalReplay(config, simulation_speed=1.0)

    @patch("signal_replay.replay.reset_all_detectors")
    @patch("signal_replay.replay.send_ntcip")
    def test_run_waits_for_pending_executor_sends_inside_event_loop(
        self,
        mock_send: MagicMock,
        mock_reset: MagicMock,
        mock_ip,
        mock_port,
    ):
        """run() should return only after fire-and-forget sends finish."""
        events = create_synthetic_events(duration_seconds=2.0, events_per_second=20.0, detector_groups=[1])
        config = sr.SignalConfig(
            device_id="test",
            ip=mock_ip,
            udp_port=mock_port,
            cycle_length=0,
            incompatible_pairs=[],
            http_port=None,
        )
        config.events = events

        sent_counter = {"count": 0}
        lock = threading.Lock()

        def slow_send(*_args, **_kwargs):
            time.sleep(0.05)
            with lock:
                sent_counter["count"] += 1

        mock_send.side_effect = slow_send

        replay = sr.SignalReplay(config, simulation_speed=1.0)
        expected_calls = len(replay.activation_feed)

        async def _run_inside_loop():
            replay.run()

        asyncio.run(_run_inside_loop())

        assert sent_counter["count"] == expected_calls

    @patch("signal_replay.replay.send_ntcip")
    def test_send_command_uses_configured_snmp_timeout(
        self,
        mock_send: MagicMock,
        mock_ip,
        mock_port,
        short_events,
    ):
        config = sr.SignalConfig(
            device_id="test",
            ip=mock_ip,
            udp_port=mock_port,
            cycle_length=0,
            incompatible_pairs=[],
        )
        config.events = short_events
        replay = sr.SignalReplay(config, simulation_speed=1.0, snmp_timeout_seconds=2.0)

        row = replay.activation_feed.iloc[0]
        replay._send_command_sync(row)

        assert mock_send.call_count == 1
        assert mock_send.call_args.kwargs["timeout"] == 2.0

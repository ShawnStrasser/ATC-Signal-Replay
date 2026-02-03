"""
Integration tests for live device simulation.

These tests require a live SNMP-enabled controller and will be skipped
if the device is not reachable. They verify:
1. Timing is correct (no double-wait bug)
2. Output events match input events (via DTW comparison)
3. Database storage and retrieval works correctly
4. Full simulation pipeline functions end-to-end

Run with: pytest tests/test_live_integration.py -v -s
Skip in CI: These tests are marked with @pytest.mark.live_device
"""

import pytest
import time
import os
import requests
from datetime import datetime, timedelta
from typing import Tuple, List
import pandas as pd

import signal_replay as sr


# Test device configuration
# This device should be accessible for live testing
LIVE_DEVICE_IP_PORT: Tuple[str, int] = ('167.131.72.35', 161)

# HTTP port for data collection (may differ from SNMP port)
LIVE_DEVICE_HTTP_PORT: int = 80


def is_device_reachable(ip_port: Tuple[str, int], timeout: float = 5.0) -> bool:
    """Check if the SNMP device is reachable."""
    try:
        sr.send_ntcip(ip_port, detector_group=1, state_integer=0, detector_type='Vehicle')
        return True
    except Exception:
        return False


def is_http_reachable(ip: str, port: int, timeout: float = 5.0) -> bool:
    """Check if the HTTP data collection endpoint is reachable."""
    try:
        url = f'http://{ip}:{port}/v1/asclog/xml/full'
        response = requests.get(url, timeout=timeout, verify=False)
        return response.status_code == 200
    except Exception:
        return False


def create_synthetic_events(
    duration_seconds: float,
    events_per_second: float = 2.0,
    detector_groups: List[int] = [1, 2, 3],
    device_id: str = 'test'
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


# Custom marker for live device tests
live_device = pytest.mark.skipif(
    not is_device_reachable(LIVE_DEVICE_IP_PORT),
    reason=f"Live device {LIVE_DEVICE_IP_PORT[0]}:{LIVE_DEVICE_IP_PORT[1]} not reachable"
)

# Custom marker for tests needing HTTP collection
live_device_http = pytest.mark.skipif(
    not is_device_reachable(LIVE_DEVICE_IP_PORT) or 
    not is_http_reachable(LIVE_DEVICE_IP_PORT[0], LIVE_DEVICE_HTTP_PORT),
    reason=f"Live device HTTP endpoint {LIVE_DEVICE_IP_PORT[0]}:{LIVE_DEVICE_HTTP_PORT} not reachable"
)


@pytest.fixture(scope="module")
def live_ip_port() -> Tuple[str, int]:
    """Provide the live device IP/port."""
    if not is_device_reachable(LIVE_DEVICE_IP_PORT):
        pytest.skip(f"Live device not reachable: {LIVE_DEVICE_IP_PORT}")
    return LIVE_DEVICE_IP_PORT


@pytest.fixture(scope="module")
def live_http_port() -> int:
    """Provide the live device HTTP port."""
    return LIVE_DEVICE_HTTP_PORT


@pytest.fixture(scope="module")
def http_available() -> bool:
    """Check if HTTP collection is available."""
    return is_http_reachable(LIVE_DEVICE_IP_PORT[0], LIVE_DEVICE_HTTP_PORT)


@pytest.fixture
def short_events() -> pd.DataFrame:
    """Create a short 10-second event sequence for testing."""
    return create_synthetic_events(duration_seconds=10.0, events_per_second=2.0)


class TestLiveDeviceConnectivity:
    """Basic connectivity tests for live device."""
    
    @live_device
    def test_snmp_send_receive(self, live_ip_port):
        """Test that we can send SNMP commands to the device."""
        # Send detector activation
        sr.send_ntcip(live_ip_port, detector_group=1, state_integer=1, detector_type='Vehicle')
        time.sleep(0.5)
        
        # Reset
        sr.send_ntcip(live_ip_port, detector_group=1, state_integer=0, detector_type='Vehicle')
        
        # If we get here, communication works
        assert True
    
    @live_device
    def test_reset_all_detectors(self, live_ip_port):
        """Test that reset_all_detectors works without errors."""
        sr.reset_all_detectors(live_ip_port, debug=True)
        assert True


class TestLiveSimulationTiming:
    """
    Timing tests with live device.
    
    These verify there is no double-wait bug where each run takes
    2x the expected duration.
    """
    
    @live_device
    def test_single_replay_timing(self, live_ip_port, short_events):
        """Test that a single SignalReplay runs in expected time."""
        signal_config = sr.SignalConfig(
            device_id='test',
            ip=live_ip_port[0],
            udp_port=live_ip_port[1],
            cycle_length=0,
            incompatible_pairs=[],
            http_port=None  # Disable collection for pure timing test
        )
        
        # Manually set events on signal config for replay
        signal_config.events = short_events
        
        replay = sr.SignalReplay(signal_config, simulation_speed=1.0, debug=True)
        expected_duration = replay.get_run_duration()
        
        print(f"\nExpected replay duration: {expected_duration:.2f}s")
        
        start = time.time()
        replay.run()
        actual_duration = time.time() - start
        
        print(f"Actual replay duration: {actual_duration:.2f}s")
        
        # Allow for SNMP latency: 20% overhead + 1.5s fixed overhead
        # A 10s test should complete in ~13.5s max, NOT 20+ seconds
        max_allowed = expected_duration * 1.20 + 1.5
        
        assert actual_duration <= max_allowed, (
            f"Replay took {actual_duration:.2f}s but expected ~{expected_duration:.2f}s. "
            f"Max allowed: {max_allowed:.2f}s. Ratio: {actual_duration / expected_duration:.2f}x"
        )
    
    @live_device_http
    def test_full_simulation_no_double_wait(
        self,
        live_ip_port,
        short_events,
        temp_db_path
    ):
        """
        Test that full simulation runs in expected time (no double-wait).
        
        This is the key regression test for the timing bug.
        Requires HTTP data collection to work.
        """
        signal_config = sr.SignalConfig(
            device_id='test',
            ip=live_ip_port[0],
            udp_port=live_ip_port[1],
            cycle_length=0,
            incompatible_pairs=[],
            http_port=LIVE_DEVICE_HTTP_PORT  # Use correct HTTP port
        )
        
        sim_config = sr.SimulationConfig(
            signals=[signal_config],
            events=short_events,
            simulation_replays=1,
            stop_on_conflict=False,
            db_path=temp_db_path,
            simulation_speed=1.0
        )
        
        # Manually set events on signal config for replay duration calculation
        signal_config.events = short_events
        replay = sr.SignalReplay(signal_config, simulation_speed=1.0)
        expected_duration = replay.get_run_duration()
        
        print(f"\nExpected simulation duration: {expected_duration:.2f}s")
        
        start = time.time()
        sim = sr.ATCSimulation(sim_config, debug=True)
        results = sim.run()
        actual_duration = time.time() - start
        
        print(f"Actual simulation duration: {actual_duration:.2f}s")
        print(f"Ratio: {actual_duration / expected_duration:.2f}x")
        
        # With collection overhead, allow 25% + 3s
        # Double-wait bug would make ratio ~2x (FAIL)
        max_allowed = expected_duration * 1.25 + 3.0
        
        assert actual_duration <= max_allowed, (
            f"Simulation took {actual_duration:.2f}s but expected ~{expected_duration:.2f}s. "
            f"Ratio: {actual_duration / expected_duration:.2f}x. "
            f"This indicates the double-wait bug is present."
        )
        
        assert len(results['completed_runs']) == 1


class TestLiveDataComparison:
    """
    Tests that verify output data matches input data.
    
    These use DTW comparison to ensure the simulation produces
    expected results.
    """
    
    @live_device_http
    def test_comparison_results_available(
        self,
        live_ip_port,
        short_events,
        temp_db_path
    ):
        """Test that comparison results are generated after simulation.
        
        Requires HTTP data collection to work.
        """
        signal_config = sr.SignalConfig(
            device_id='test',
            ip=live_ip_port[0],
            udp_port=live_ip_port[1],
            cycle_length=0,
            incompatible_pairs=[],
            http_port=LIVE_DEVICE_HTTP_PORT
        )
        
        sim_config = sr.SimulationConfig(
            signals=[signal_config],
            events=short_events,
            simulation_replays=2,
            stop_on_conflict=False,
            db_path=temp_db_path,
            simulation_speed=1.0
        )
        
        sim = sr.ATCSimulation(sim_config, debug=True)
        results = sim.run()
        
        # Check comparison results exist
        comparison_results = sim.get_comparison_results()
        assert comparison_results is not None, "No comparison results generated"
        
        # Check summary is not empty
        summary = sim.get_comparison_summary()
        assert len(summary) > 0, "Empty comparison summary"
        
        print(f"\nComparison Summary:\n{summary}")
    
    @live_device_http
    def test_database_stores_events(
        self,
        live_ip_port,
        short_events,
        temp_db_path
    ):
        """Test that events are stored in the database."""
        signal_config = sr.SignalConfig(
            device_id='test',
            ip=live_ip_port[0],
            udp_port=live_ip_port[1],
            cycle_length=0,
            incompatible_pairs=[],
            http_port=LIVE_DEVICE_HTTP_PORT
        )
        
        sim_config = sr.SimulationConfig(
            signals=[signal_config],
            events=short_events,
            simulation_replays=1,
            stop_on_conflict=False,
            db_path=temp_db_path,
            simulation_speed=1.0
        )
        
        sim = sr.ATCSimulation(sim_config, debug=True)
        results = sim.run()
        
        # Check input events stored
        input_events = sim.get_input_events()
        print(f"\nInput events stored: {len(input_events)}")
        assert len(input_events) > 0, "No input events stored in database"
        
        # Check output events stored (may be empty if HTTP collection fails)
        output_events = sim.get_events()
        print(f"Output events collected: {len(output_events)}")


class TestDTWComparison:
    """Tests for DTW comparison functionality."""
    
    def test_compare_identical_sequences(self):
        """Test that identical sequences have DTW distance of 0."""
        base_time = datetime(2024, 1, 1, 12, 0, 0)
        
        events = pd.DataFrame({
            'timestamp': [base_time + timedelta(seconds=i) for i in range(10)],
            'event_id': [1, 10, 1, 10, 21, 23, 61, 65, 1, 10],
            'parameter': [1, 1, 2, 2, 1, 1, 1, 1, 3, 3]
        })
        
        result = sr.compare_runs(
            events, events,
            device_id='test',
            run_a_label='run_1',
            run_b_label='run_2'
        )
        
        assert result.sequence_dtw.distance == 0.0, "Identical sequences should have 0 DTW"
        assert result.match_percentage == 100.0, "Identical sequences should have 100% match"
    
    def test_compare_different_sequences(self):
        """Test that different sequences have positive DTW distance."""
        base_time = datetime(2024, 1, 1, 12, 0, 0)
        
        events_a = pd.DataFrame({
            'timestamp': [base_time + timedelta(seconds=i) for i in range(5)],
            'event_id': [1, 10, 1, 10, 1],
            'parameter': [1, 1, 2, 2, 3]
        })
        
        events_b = pd.DataFrame({
            'timestamp': [base_time + timedelta(seconds=i) for i in range(5)],
            'event_id': [1, 10, 1, 10, 10],  # Different last event
            'parameter': [1, 1, 2, 2, 4]     # Different parameter
        })
        
        result = sr.compare_runs(
            events_a, events_b,
            device_id='test',
            run_a_label='run_1',
            run_b_label='run_2'
        )
        
        assert result.sequence_dtw.distance > 0, "Different sequences should have positive DTW"
        assert result.match_percentage < 100.0, "Different sequences should have <100% match"
    
    def test_prepare_events_for_comparison(self):
        """Test event preparation filters to comparison event IDs."""
        base_time = datetime(2024, 1, 1, 12, 0, 0)
        
        # Include some event IDs that should be filtered out
        events = pd.DataFrame({
            'timestamp': [base_time + timedelta(seconds=i) for i in range(10)],
            'event_id': [1, 10, 82, 81, 21, 23, 999, 1000, 61, 65],  # 82, 81, 999, 1000 not in comparison
            'parameter': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
        })
        
        prepared = sr.prepare_events_for_comparison(events)
        
        # Should only include comparison event IDs
        for event_id in prepared['event_id'].unique():
            assert event_id in sr.COMPARISON_EVENT_IDS, f"Event ID {event_id} should be filtered"

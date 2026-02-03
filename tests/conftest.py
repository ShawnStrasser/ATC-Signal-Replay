"""
Pytest configuration and fixtures for signal_replay tests.

This module provides:
- Fixtures for live device testing (skipped in CI if device unreachable)
- Common test utilities and synthetic data generators
"""

import pytest
from typing import Tuple


# Live test device configuration
# Update this to match your test controller
LIVE_DEVICE_IP_PORT: Tuple[str, int] = ('167.131.72.35', 161)


def is_device_reachable(ip_port: Tuple[str, int], timeout: float = 5.0) -> bool:
    """
    Check if a device is reachable via SNMP.
    
    Attempts to send a simple SNMP command to verify connectivity.
    """
    try:
        import signal_replay as sr
        sr.send_ntcip(ip_port, detector_group=1, state_integer=0, detector_type='Vehicle')
        return True
    except Exception:
        return False


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "live_device: marks tests as requiring live device (skipped if unreachable)"
    )


@pytest.fixture(scope="session")
def live_device_available() -> bool:
    """Check if live device is available for the entire test session."""
    return is_device_reachable(LIVE_DEVICE_IP_PORT)


@pytest.fixture(scope="session")
def live_device_ip_port(live_device_available) -> Tuple[str, int]:
    """
    Fixture providing the live test device IP/port.
    
    Skips the test if the device is not reachable.
    """
    if not live_device_available:
        pytest.skip(
            f"Live device {LIVE_DEVICE_IP_PORT[0]}:{LIVE_DEVICE_IP_PORT[1]} not reachable. "
            f"Set LIVE_DEVICE_IP_PORT in conftest.py to run live tests."
        )
    return LIVE_DEVICE_IP_PORT


@pytest.fixture
def temp_db_path(tmp_path):
    """Provide a temporary database path for tests and clean up after."""
    db_file = tmp_path / "test_simulation.duckdb"
    db_path = str(db_file)
    yield db_path
    
    # Cleanup after test
    import os
    try:
        if os.path.exists(db_path):
            os.remove(db_path)
        # Also remove wal and tmp files if they exist
        for ext in ['.wal', '.tmp']:
            if os.path.exists(db_path + ext):
                os.remove(db_path + ext)
    except Exception as e:
        print(f"Warning: Could not clean up {db_path}: {e}")


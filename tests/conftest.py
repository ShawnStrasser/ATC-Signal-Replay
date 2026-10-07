"""
Pytest configuration and fixtures for signal_replay tests.

This module provides:
- Fixtures for live device testing (opt-in; see below)
- Common test utilities and synthetic data generators

Live tests are marked ``live`` and deselected by default (``addopts`` in
pyproject.toml). No device address is stored in the repository. To run them
against your own test controller set the environment variables and select the
marker explicitly::

    TEST_CONTROLLER_IP=192.168.1.100:161 TEST_CONTROLLER_HTTP_PORT=80 pytest -m live
"""

import os
from typing import Optional, Tuple

import matplotlib

# Headless plotting for the whole test session. The package itself never uses pyplot,
# but software_validation/software_validate.py (exercised by some tests) does, and the
# interactive Tk backend that Windows picks by default is not safe from worker threads.
matplotlib.use("Agg")

import pytest


def live_device_ip_port_from_env() -> Optional[Tuple[str, int]]:
    """Parse TEST_CONTROLLER_IP ("ip" or "ip:snmp_port"); None when unset."""
    raw = os.environ.get("TEST_CONTROLLER_IP", "").strip()
    if not raw:
        return None
    ip, _, port = raw.partition(":")
    return (ip, int(port) if port else 161)


def live_device_http_port_from_env() -> int:
    """HTTP port of the live device's event log endpoint (TEST_CONTROLLER_HTTP_PORT, default 80)."""
    return int(os.environ.get("TEST_CONTROLLER_HTTP_PORT", "80") or 80)


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


@pytest.fixture(scope="session")
def live_device_ip_port() -> Tuple[str, int]:
    """
    Fixture providing the live test device IP/port from TEST_CONTROLLER_IP.

    Skips the test if the variable is unset or the device is not reachable.
    """
    ip_port = live_device_ip_port_from_env()
    if ip_port is None:
        pytest.skip("Live device not configured. Set TEST_CONTROLLER_IP to run live tests.")
    if not is_device_reachable(ip_port):
        pytest.skip(f"Live device {ip_port[0]}:{ip_port[1]} not reachable.")
    return ip_port


@pytest.fixture(scope="session")
def live_device_http_port() -> int:
    """Fixture providing the live device HTTP port (TEST_CONTROLLER_HTTP_PORT)."""
    return live_device_http_port_from_env()


@pytest.fixture
def temp_db_path(tmp_path):
    """Provide a temporary database path for tests and clean up after."""
    db_file = tmp_path / "test_simulation.db"
    db_path = str(db_file)
    yield db_path

    # Cleanup after test
    try:
        if os.path.exists(db_path):
            os.remove(db_path)
        # Also remove wal and tmp files if they exist
        for ext in ['.wal', '.tmp']:
            if os.path.exists(db_path + ext):
                os.remove(db_path + ext)
    except Exception as e:
        print(f"Warning: Could not clean up {db_path}: {e}")

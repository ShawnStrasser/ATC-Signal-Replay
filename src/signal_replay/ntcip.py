"""
NTCIP/SNMP communication module for sending detector states to controllers.
"""

from pysnmp.hlapi import (
    SnmpEngine, CommunityData, UdpTransportTarget, ContextData,
    ObjectType, ObjectIdentity, Integer, setCmd
)
from typing import Tuple, Literal
import threading

# Create one SnmpEngine per thread to avoid concurrency issues
_thread_local = threading.local()


def get_engine() -> SnmpEngine:
    """Get or create a thread-local SNMP engine."""
    if not hasattr(_thread_local, 'engine'):
        _thread_local.engine = SnmpEngine()
    return _thread_local.engine


def send_ntcip(
    ip_port: Tuple[str, int],
    detector_group: int,
    state_integer: int,
    detector_type: Literal['Vehicle', 'Ped', 'Preempt'],
    community: str = 'public',
    timeout: float = 2.0
) -> None:
    """
    Send an NTCIP SET command to update detector states on a controller.
    
    Args:
        ip_port: Tuple of (IP address, port) for the controller
        detector_group: Detector group number (1-16)
        state_integer: Bitmask of detector states (0-255)
        detector_type: Type of detector ('Vehicle', 'Ped', or 'Preempt')
        community: SNMP community string (default: 'public')
        timeout: SNMP response timeout in seconds (default: 2.0)
    
    Raises:
        RuntimeError: If SNMP communication fails
        ValueError: If detector_type is invalid
    """
    # OIDs per NTCIP 1202 v3 section 5.3.11.3
    if detector_type == 'Vehicle':
        oid = ObjectIdentity(f'1.3.6.1.4.1.1206.4.2.1.2.12.1.2.{detector_group}')
    elif detector_type == 'Ped':
        oid = ObjectIdentity(f'1.3.6.1.4.1.1206.4.2.1.2.13.1.2.{detector_group}')
    elif detector_type == 'Preempt':
        oid = ObjectIdentity(f'1.3.6.1.4.1.1206.4.2.1.6.3.1.2.{detector_group}')
    else:
        raise ValueError(f"Invalid detector_type: {detector_type}. Must be 'Vehicle', 'Ped', or 'Preempt'")

    # timeout is in 1/100th seconds for pysnmp UdpTransportTarget
    timeout_hundredths = max(1, int(timeout * 100))

    error_indication, error_status, error_index, var_binds = next(
        setCmd(
            get_engine(),
            CommunityData(community, mpModel=0),
            UdpTransportTarget(ip_port, timeout=timeout_hundredths / 100, retries=0),
            ContextData(),
            ObjectType(oid, Integer(state_integer))
        )
    )

    if error_indication:
        raise RuntimeError(f'SNMP error: {error_indication}')
    elif error_status:
        raise RuntimeError(f'SNMP error: {error_status.prettyPrint()} at {error_index}')


def reset_all_detectors(
    ip_port: Tuple[str, int],
    community: str = 'public',
    debug: bool = False,
    timeout: float = 2.0,
) -> None:
    """
    Reset all detector states to 0 for a controller.
    
    Args:
        ip_port: Tuple of (IP address, port) for the controller
        community: SNMP community string (default: 'public')
        debug: If True, print debug messages
        timeout: SNMP response timeout in seconds for each reset command
    
    Note:
        Silently skips detectors that don't exist (noSuchName errors).
    """
    for detector_type in ['Vehicle', 'Ped', 'Preempt']:
        for detector_group in range(1, 17):  # Detector groups range from 1 to 16
            try:
                send_ntcip(ip_port, detector_group, 0, detector_type, community, timeout=timeout)
            except RuntimeError as e:
                err_msg = str(e)
                if "noSuchName" in err_msg:
                    # Non-existent detector — skip remaining groups for this type
                    if debug:
                        print(f"Detector group {detector_group} of type {detector_type} "
                              f"does not exist for {ip_port}.")
                    break
                else:
                    # Timeout or other SNMP error — skip entire reset for this controller
                    # (don't hammer a non-responsive controller with 48 more timeout waits)
                    print(f"Warning: reset failed for {ip_port} ({err_msg}), skipping remaining resets")
                    return
    
    if debug:
        print(f"Detector states reset successfully for {ip_port}")

"""
NTCIP/SNMP communication module for sending detector states to controllers.

Uses pysnmp 7.x async API (``pysnmp.hlapi.v3arch.asyncio``).  All public
functions are synchronous wrappers around the underlying async implementation
so that callers that are *not* inside an event loop (CLI scripts, tests,
``reset_all_detectors``) keep working unchanged.  The replay module calls
the ``async_*`` variants directly to avoid thread-pool overhead.

A single :class:`SnmpEngine` must be reused for all SNMP operations within
an event-loop lifetime.  Creating a new engine per call leaks MIB builder
caches, UDP transports, and asyncio dispatcher tasks — the root cause of
the 300 MB → 21 GB growth observed 2025-06.
"""

import asyncio
import logging
from typing import Tuple, Literal

from pysnmp.hlapi.v3arch.asyncio import (
    SnmpEngine,
    CommunityData,
    UdpTransportTarget,
    ContextData,
    ObjectType,
    ObjectIdentity,
    Integer,
    set_cmd,
)

from ._logging import debug_level

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# OID helpers
# ---------------------------------------------------------------------------

_OID_PREFIX = {
    'Vehicle': '1.3.6.1.4.1.1206.4.2.1.2.12.1.2',
    'Ped':     '1.3.6.1.4.1.1206.4.2.1.2.13.1.2',
    'Preempt': '1.3.6.1.4.1.1206.4.2.1.6.3.1.2',
}


def _oid_for(detector_type: str, detector_group: int) -> ObjectIdentity:
    prefix = _OID_PREFIX.get(detector_type)
    if prefix is None:
        raise ValueError(
            f"Invalid detector_type: {detector_type}. "
            f"Must be 'Vehicle', 'Ped', or 'Preempt'"
        )
    return ObjectIdentity(f'{prefix}.{detector_group}')


# ---------------------------------------------------------------------------
# Async implementation (preferred – called from the asyncio event loop)
# ---------------------------------------------------------------------------

async def async_send_ntcip(
    ip_port: Tuple[str, int],
    detector_group: int,
    state_integer: int,
    detector_type: Literal['Vehicle', 'Ped', 'Preempt'],
    community: str = 'public',
    timeout: float = 2.0,
    *,
    snmp_engine: SnmpEngine,
) -> None:
    """
    Send an NTCIP SET command to update detector states on a controller.

    This is the async version — prefer this from inside an event loop.

    Args:
        ip_port: Tuple of (IP address, port) for the controller
        detector_group: Detector group number (1-16)
        state_integer: Bitmask of detector states (0-255)
        detector_type: Type of detector ('Vehicle', 'Ped', or 'Preempt')
        community: SNMP community string (default: 'public')
        timeout: SNMP response timeout in seconds (default: 2.0)
        snmp_engine: Reusable SnmpEngine instance.  The caller owns the
            engine lifecycle and must call ``close_dispatcher()`` when done.

    Raises:
        RuntimeError: If SNMP communication fails
        ValueError: If detector_type is invalid
    """
    oid = _oid_for(detector_type, detector_group)

    error_indication, error_status, error_index, var_binds = await set_cmd(
        snmp_engine,
        CommunityData(community, mpModel=0),
        await UdpTransportTarget.create(ip_port, timeout=timeout, retries=0),
        ContextData(),
        ObjectType(oid, Integer(state_integer)),
    )

    if error_indication:
        raise RuntimeError(f'SNMP error: {error_indication}')
    if error_status:
        raise RuntimeError(
            f'SNMP error: {error_status.prettyPrint()} at {error_index}'
        )


async def async_reset_all_detectors(
    ip_port: Tuple[str, int],
    community: str = 'public',
    debug: bool = False,
    timeout: float = 2.0,
    *,
    snmp_engine: SnmpEngine,
    raise_on_error: bool = False,
) -> None:
    """
    Reset all detector states to 0 for a controller (async version).

    Args:
        ip_port: Tuple of (IP address, port) for the controller
        community: SNMP community string (default: 'public')
        debug: If True, print debug messages
        timeout: SNMP response timeout in seconds for each reset command
        snmp_engine: Reusable SnmpEngine instance.
        raise_on_error: If True, a failed reset (no answer, timeout, any
            error other than noSuchName) raises RuntimeError. If False (the
            default) it is logged at WARNING and the remaining resets are
            skipped.

    Raises:
        RuntimeError: Only with ``raise_on_error=True``.

    Note:
        Silently skips detectors that don't exist (noSuchName errors).
    """
    for detector_type in ['Vehicle', 'Ped', 'Preempt']:
        for detector_group in range(1, 17):
            try:
                await async_send_ntcip(
                    ip_port, detector_group, 0, detector_type, community,
                    timeout=timeout,
                    snmp_engine=snmp_engine,
                )
            except RuntimeError as e:
                err_msg = str(e)
                if "noSuchName" in err_msg:
                    logger.log(
                        debug_level(debug),
                        "Detector group %s of type %s does not exist for %s.",
                        detector_group, detector_type, ip_port,
                    )
                    break
                else:
                    if raise_on_error:
                        raise
                    logger.warning(
                        "Reset failed for %s (%s), skipping remaining resets",
                        ip_port, err_msg,
                    )
                    return

    logger.log(debug_level(debug), "Detector states reset successfully for %s", ip_port)


# ---------------------------------------------------------------------------
# Sync wrappers (for CLI scripts, tests, and code not inside an event loop)
# ---------------------------------------------------------------------------

async def _with_engine(coro_factory):
    """Create a temporary SnmpEngine, run *coro_factory(engine)*, then close."""
    engine = SnmpEngine()
    try:
        return await coro_factory(engine)
    finally:
        engine.close_dispatcher()


def send_ntcip(
    ip_port: Tuple[str, int],
    detector_group: int,
    state_integer: int,
    detector_type: Literal['Vehicle', 'Ped', 'Preempt'],
    community: str = 'public',
    timeout: float = 2.0,
) -> None:
    """Synchronous wrapper around :func:`async_send_ntcip`."""
    asyncio.run(
        _with_engine(lambda eng: async_send_ntcip(
            ip_port, detector_group, state_integer, detector_type,
            community, timeout,
            snmp_engine=eng,
        ))
    )


def reset_all_detectors(
    ip_port: Tuple[str, int],
    community: str = 'public',
    debug: bool = False,
    timeout: float = 2.0,
    raise_on_error: bool = False,
) -> None:
    """Synchronous wrapper around :func:`async_reset_all_detectors`.

    With ``raise_on_error=True`` it doubles as a connectivity check: it
    raises RuntimeError when the controller does not answer SNMP.
    """
    asyncio.run(
        _with_engine(lambda eng: async_reset_all_detectors(
            ip_port, community, debug, timeout,
            snmp_engine=eng, raise_on_error=raise_on_error,
        ))
    )

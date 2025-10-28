from pysnmp.hlapi import SnmpEngine, CommunityData, UdpTransportTarget, ContextData, ObjectType, ObjectIdentity, Integer, setCmd
import time
from typing import Tuple, Literal

import threading

# Create one SnmpEngine per thread to avoid concurrency issues
_thread_local = threading.local()

def get_engine():
    if not hasattr(_thread_local, 'engine'):
        _thread_local.engine = SnmpEngine()
    return _thread_local.engine

def send_ntcip(
    ip_port: Tuple[str, int],
    detector_group: int,
    state_integer: int,
    type: Literal['Vehicle', 'Ped', 'Preempt'],
    community: str = 'public'
) -> None:
    if type == 'Vehicle':
        oid = ObjectIdentity(f'1.3.6.1.4.1.1206.4.2.1.2.12.1.2.{detector_group}')
    elif type == 'Ped':
        oid = ObjectIdentity(f'1.3.6.1.4.1.1206.4.2.1.2.13.1.2.{detector_group}')
    elif type == 'Preempt':
        oid = ObjectIdentity(f'1.3.6.1.4.1.1206.4.2.1.6.3.1.2.{detector_group}')

    error_indication, error_status, error_index, var_binds = next(
        setCmd(
            get_engine(),  # Get thread-specific engine
            CommunityData(community, mpModel=0),
            UdpTransportTarget(ip_port, timeout=3, retries=0),
            ContextData(),
            ObjectType(oid, Integer(state_integer))
        )
    )

    if error_indication:
        raise RuntimeError(f'SNMP error: {error_indication}')
    elif error_status:
        raise RuntimeError(f'SNMP error: {error_status.prettyPrint()} at {error_index}')
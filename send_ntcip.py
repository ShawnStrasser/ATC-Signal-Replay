from pysnmp.hlapi import SnmpEngine, CommunityData, UdpTransportTarget, ContextData, ObjectType, ObjectIdentity, Integer, setCmd
import time
from typing import Tuple, Literal

def send_ntcip(
    ip_port: Tuple[str, int],
    detector_group: int,
    state_integer: int,
    type: Literal['Vehicle', 'Ped', 'Preempt']
) -> None:
    """Set the state of a detector group using NTCIP protocol.
    
    Args:
        ip_port: Tuple of (IP address, port number)
        detector_group: Detector group number (expected range: 1-16)
        state_integer: State value (expected range: 0-255)
        type: Type of detector ('Vehicle', 'Ped', or 'Preempt')
    """
    # From NTCIP 1202 v3 section 5.3.11.3 - Vehicle Detector Control Group Actuation
    if type == 'Vehicle':
        oid = ObjectIdentity(f'1.3.6.1.4.1.1206.4.2.1.2.12.1.2.{detector_group}')
    elif type == 'Ped':
        oid = ObjectIdentity(f'1.3.6.1.4.1.1206.4.2.1.2.13.1.2.{detector_group}')
    elif type == 'Preempt':
        oid = ObjectIdentity(f'1.3.6.1.4.1.1206.4.2.1.6.3.1.2.{detector_group}')

    error_indication, error_status, error_index, var_binds = next(
        setCmd(
            SnmpEngine(),
            CommunityData('public', mpModel=0),
            UdpTransportTarget(ip_port),
            ContextData(),
            ObjectType(oid, Integer(state_integer))
        )
    )
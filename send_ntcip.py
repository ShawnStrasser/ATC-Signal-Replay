from pysnmp.hlapi import SnmpEngine, CommunityData, UdpTransportTarget, ContextData, ObjectType, ObjectIdentity, Integer, setCmd
import time

# This function will set the state of a vehicle detector group
def send_ntcip(ip_port, detector_group, state_integer, type):
    try:
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

        if error_indication:
            print(f"Error at {time.ctime()}: {error_indication}")
        elif error_status:
            print(f"Error at {time.ctime()} at {error_index}: {error_status.prettyPrint()}")

    except Exception as e:
        print(f"An exception occurred: {e}")
# ATC-Signal-Replay
Python script for replaying high-resolution event logs from ATC signal controllers back to test controllers using NTCIP.

For now, it is set up to work with the MAXTIME emulator, but can be adapted to any controller due to using NTCIP.

## How it Works

1. Read vehicle/pedestrian/preempt inputs from hi-res data and converts them into detector group state integers per NTCIP 1202 v3 section 5.3.11.3 (page 275)
2. State integers are then fed back to a test controller with SNMP
3. Read the new event log from the test controller
4. Check for phase/overlap conflicts (virtual conflict monitor)
5. Repeat until a conflict is found, save events to .csv when conflict is found

Check out the [Example notebook](./Example.ipynb) in this repo for an example useage.

## Cool Features

- For coordinated signals, enter the cycle length to wait until the right time in the next cycle to begin the simulation.
- Set the simulation speed to run faster than real-time.

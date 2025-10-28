# atc_replay.py
import duckdb
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from send_ntcip import send_ntcip
import asyncio
import threading
import time
from datetime import datetime, timedelta
import math
from jinja2 import Template

class ATCReplay:
    def __init__(self, event_log, device_mapping, incompatible_pairs=None, simulation_speed=1, cycle_length=0, limit_rows=0, debug=False):
        self.event_log = event_log
        self.incompatible_pairs = incompatible_pairs
        self.activation_feed = None
        self.input_data = None
        self.output_data = None
        self.log_path = None
        self.ip_mapping = device_mapping
        self.cycle_length = cycle_length
        self.original_start_time = None
        self.limit_rows = limit_rows
        self.debug = debug

        # If event log is .db then load the maxtime and lastfail from the database
        if event_log.endswith('.db'):
            self._load_maxtime_lastfail()
        else:
            self._load_from_path()

        self._generate_activation_feed(simulation_speed)

    def _load_maxtime_lastfail(self):
        if self.debug:
            print("Loading data from SQLite database")
        # Connect to the SQLite database using DuckDB
        con = duckdb.connect()
        con.execute(f"ATTACH DATABASE '{self.event_log}' AS LastFail (TYPE SQLITE)")
        con.execute("USE LastFail")

        # Read SQL from 'load_event_log.sql'
        sql_file = 'SQL/load_maxtime_db.sql'
        with open(sql_file, 'r') as f:
            sql = f.read()

        self.input_data = con.execute(sql).df()
        con.close()

        # NOTE: This does not explicitly order the data! Come back to this later.
        # ~~~ Nothing is as permanent as a temporary solution ~~~ 
        if self.limit_rows > 0:
            self.input_data = self.input_data.tail(self.limit_rows)
        if self.debug:
            print("Data loaded successfully")

    def _load_from_path(self):
        if self.debug:
            print("Loading data from path")
        # Variables to replace
        template_vars = {
            'timestamp': 'timestamp',
            'eventid': 'event_id',
            'parameter': 'parameter',
            'from_path': self.event_log
        }
        # Read SQL template
        with open('SQL/load_from_path.sql', 'r') as f:
            template = Template(f.read())
        # Render SQL with variables
        sql = template.render(**template_vars)
        self.input_data = duckdb.sql(sql).df()
        # NOTE: This does not explicitly order the data! Come back to this later.
        # ~~~ Nothing is as permanent as a temporary solution ~~~ 
        if self.limit_rows > 0:
            self.input_data = self.input_data.tail(self.limit_rows)
        if self.debug:
            print("Data loaded successfully")

    def _generate_activation_feed(self, simulation_speed):
        if self.debug:
            print("Generating activation feed")
        con = duckdb.connect()
        # Register raw_data as a table in DuckDB
        con.register('raw_data', self.input_data)

        # Read SQL from 'impute_actuations.sql'
        with open('SQL/impute_actuations.sql', 'r') as f:
            sql_impute_actuations = f.read()

        imputed = con.execute(sql_impute_actuations).df()

        # Register imputed as a table in DuckDB
        con.register('imputed', imputed)

        # Read SQL from 'generate_activation_feed.sql'
        with open('SQL/generate_activation_feed.sql', 'r') as f:
            sql_generate_activation_feed = f.read()

        self.activation_feed = con.execute(sql_generate_activation_feed).df()
        con.close()

        # Add sleep_time_cumulative column
        self.activation_feed['sleep_time_cumulative'] = self.activation_feed['sleep_time'].cumsum() / simulation_speed
        # for timing when the simulation starts if it is in coord
        self.original_start_time = self.activation_feed['TimeStamp'].min()
        if self.debug:
            print("Activation feed generated successfully")


    async def _send_command(self, row, start_time):
        """
        Coroutine to send a single SNMP command after waiting for the required delay.
        """
        current_time = asyncio.get_event_loop().time()
        delay = row.sleep_time_cumulative - (current_time - start_time)
        if delay > 0:
            await asyncio.sleep(delay)
        # Send the command asynchronously using run_in_executor to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            send_ntcip,
            self.ip_mapping[row.DeviceId],
            row.group_number,
            row.state_integer,
            row.DetectorType
        )

    async def _run_async(self):
        """
        Asynchronous runner that schedules all SNMP commands based on their cumulative sleep times.
        """
        activation_feed = self.activation_feed.copy()
        start_time = asyncio.get_event_loop().time()

        tasks = [
            asyncio.create_task(self._send_command(row, start_time))
            for _, row in activation_feed.iterrows()
        ]

        # Wait for all tasks to complete
        await asyncio.gather(*tasks)

    def run(self):
        """
        Synchronous method to start the asynchronous SNMP command replay.
        It handles cases where an event loop is already running by running the async tasks in a separate thread.
        """
        self.start_run = datetime.now().strftime('%m-%d-%Y %H:%M:%S.%f')[:-5]

        try:
            # Try to get the current running loop
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No event loop is running, safe to use asyncio.run
            asyncio.run(self._run_async())
        else:
            # An event loop is already running, run the async code in a new thread
            thread = threading.Thread(target=self._run_in_thread)
            thread.start()
            thread.join()  # Wait for the thread to finish

    def _run_in_thread(self):
        """
        Runs the asynchronous _run_async method in a new event loop within a separate thread.
        """
        self.reset_detector_states()
        self.wait_until_next_cycle()
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        new_loop.run_until_complete(self._run_async())
        new_loop.close()

    def reset_detector_states(self):
            """
            Resets the detector states for all types to 0.
            Catches and handles 'noSuchName' errors that occur when a detector doesn't exist.
            """
            if self.debug:
                print("Resetting detector states to 0")
            # Get all unique IP/port combinations from the mapping
            ip_ports = set(self.ip_mapping.values())
            if self.debug:
                print(f"IP/port combinations: {ip_ports}")

            for ip_port in ip_ports:
                for detector_type in ['Vehicle', 'Ped', 'Preempt']:
                    for detector_group in range(1, 17):  # Assuming detector groups range from 1 to 16
                        try:
                            send_ntcip(ip_port, detector_group, 0, detector_type)
                        except Exception as e:
                            # Skip 'noSuchName' errors as they just indicate non-existent detectors
                            if "noSuchName" in str(e):
                                print(f"Detector {detector_group} of type {detector_type} does not exist for {ip_port}.")
                                break
                            else:
                                print(f"Error resetting detector {detector_group} of type {detector_type} for {ip_port}: {e}")
                                raise e
            if self.debug:
                print("Detector states reset successfully")


    # This function will wait until the next cycle starts to start the simulation
    def wait_until_next_cycle(self):
        # pass if cycle_length is 0
        if self.cycle_length == 0:
            return
        # Calculate the difference in seconds between current_time and start_time
        delta = datetime.now() - self.original_start_time
        delta_seconds = delta.total_seconds()

        # Calculate the number of cycles that have passed
        cycles_passed = delta_seconds / self.cycle_length

        # Find the next whole cycle
        next_cycle = math.ceil(cycles_passed) * self.cycle_length

        # Calculate the next multiple time
        next_time = self.original_start_time + timedelta(seconds=next_cycle)

        # Calculate the sleep time from current_time to next_time
        sleep_time = next_time - datetime.now()
        sleep_time = sleep_time.total_seconds()
        print(f'Waiting for {sleep_time} seconds until the next cycle starts')
        time.sleep(sleep_time)


    def get_output_data(self):
        # Get the output data from the ASC log
        # Get the first device IP and port from the mapping
        first_device_id = list(self.ip_mapping.keys())[0]
        ip_port = self.ip_mapping[first_device_id]  # It's already a tuple
        # Get the output data from the ASC log
        url = f'http://{ip_port[0]}:{ip_port[1]}/v1/asclog/xml/full'

        response = requests.get(url, verify=False)
        # Parse the XML
        root = ET.fromstring(response.text)
        # Create a list of dictionaries from the Event elements
        data = [event.attrib for event in root.findall('.//Event')]
        # Create a DataFrame from the list of dictionaries
        df = pd.DataFrame(data).drop(columns='ID')
        df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
        df['EventTypeID'] = df['EventTypeID'].astype(int)
        df['Parameter'] = df['Parameter'].astype(int)
        self.output_data = df

    def conflict_check(self):
        if self.incompatible_pairs is None:
            return "Conflict check cannot be performed: no incompatible pairs provided"
        
        con = duckdb.connect()
        con.register('Event', self.output_data)

        # Read SQL from 'load_conflict_events.sql'
        sql_file = 'SQL/load_conflict_events.sql'
        with open(sql_file, 'r') as f:
            sql = f.read()

        raw_data = con.sql(sql).df()
        con.close()

        # Get the list of parameters
        parameters = raw_data['Parameter'].unique().tolist()

        # Initialize the state dictionary with default state 0 for each parameter
        current_states = {param: 0 for param in parameters}

        # List to store the state snapshots
        state_records = []

        # Iterate through the DataFrame and update states
        for index, row in raw_data.iterrows():
            param = row['Parameter']
            state = row['state_integer']
            current_states[param] = state

            # Create a snapshot: copy the current state
            snapshot = {'TimeStamp': row['TimeStamp']}
            for p in parameters:
                snapshot[p] = current_states[p]

            state_records.append(snapshot)

        # Create the final DataFrame
        final_df = pd.DataFrame(state_records)

        # Check for conflicts
        final_df['Conflicts'] = final_df.apply(lambda row: self._check_incompatibilities(row, self.incompatible_pairs), axis=1)
        final_df['Has_Conflict'] = final_df['Conflicts'].apply(lambda x: len(x) > 0)
        final_df['Conflict_Details'] = final_df['Conflicts'].apply(
            lambda x: '; '.join([f"{a} & {b}" for a, b in x]) if x else ""
        )

        # Drop duplicates of TimeStamp, keeping the last row
        final_df = final_df.drop_duplicates(subset='TimeStamp', keep='last')

        conflict_df = final_df[final_df['Has_Conflict']][['TimeStamp', 'Conflict_Details']]

        return conflict_df

    def _check_incompatibilities(self, row, pairs):
        conflicts = []
        for (param1, param2) in pairs:
            if row.get(param1, 0) == 1 and row.get(param2, 0) == 1:
                conflicts.append((param1, param2))
        return conflicts
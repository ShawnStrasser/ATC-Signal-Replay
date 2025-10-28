from atc_replay import ATCReplay
port = int(input("Enter the port number: "))
rows = int(input("Enter the number of rows to use (0 to use all): "))
ip_port = ('127.0.0.1', port)
db_path = 'LastFailEventLog.db'
cycle_length = 100 # for timing when to start the simulation

# Your virtual conflict monitor :)
incompatible_pairs = [
  # Phases
  ('Ph1', 'Ph2'),
  ('Ph1', 'Ph4'),
  ('Ph1', 'Ph8'),
  ('Ph2', 'Ph4'),
  ('Ph2', 'Ph8'),
  ('Ph4', 'Ph5'),
  ('Ph4', 'Ph6'),
  ('Ph5', 'Ph6'),
  ('Ph5', 'Ph8'),
  ('Ph6', 'Ph8'),
  # Overlaps
  ('O1', 'Ph4'),
  ('O1', 'Ph8'),
  ('O2', 'Ph1'),
  ('O2', 'Ph4'),
  ('O2', 'Ph8'),
  ('O5', 'Ph4'),
  ('O5', 'Ph8'),
  ('O6', 'Ph4'),
  ('O6', 'Ph8'),
]

# Instantiate ATCReplay (loads event log and converts it to activation feed for NTICP)
replay = ATCReplay(event_log=db_path,
                   ip_port=ip_port,
                   incompatible_pairs=incompatible_pairs,
                   simulation_speed=1,
                   cycle_length=cycle_length,
                   limit_rows=rows)

# Print simulation run time
run_time = round(replay.activation_feed['sleep_time_cumulative'].max(),1)
minutes, seconds = divmod(run_time, 60)
print(f'Simulation run time: {minutes} minutes and {seconds} seconds')

# Rerun the trial until a conflict is found
replay_attempts = 30
for i in range(replay_attempts):
    print(f'ATCReplay attempt {i+1}/{replay_attempts}')
    replay.run()
    replay.get_output_data()
    conflict_df = replay.conflict_check()
    if not conflict_df.empty:
        print(conflict_df)
        # Save event log output to a csv file
        replay.output_data.to_csv(f'simulated_failure_log_{port}.csv', index=False)
        replay.output_data.to_parquet(f'simulated_failure_log_{port}.parquet', index=False)
        break
    
input('Complete, press Enter to exit')
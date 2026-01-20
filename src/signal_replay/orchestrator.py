"""
Main orchestrator for ATC Signal Replay simulations.
"""

import threading
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

from .config import SimulationConfig, SignalConfig
from .replay import SignalReplay, create_replays
from .collector import DatabaseManager, DataCollector, fetch_output_data, check_conflicts
from .comparison import compare_all_runs, format_comparison_summary, ComparisonResult


class ATCSimulation:
    """
    Main orchestrator for multi-signal ATC replay simulations.
    
    Manages replay processes, data collection, conflict detection,
    and post-simulation comparison analysis.
    """
    
    def __init__(self, config: SimulationConfig, debug: bool = False):
        """
        Initialize the ATC simulation.
        
        Args:
            config: SimulationConfig with all simulation parameters
            debug: Enable debug output
        """
        self.config = config
        self.debug = debug
        
        # Initialize database
        self.db = DatabaseManager(config.db_path)
        
        # Store input events for comparison
        self._store_input_events()
        
        # State tracking
        self._current_run: int = 0
        self._simulation_start_time: Optional[datetime] = None
        self._stop_event: threading.Event = threading.Event()
        self._conflicts_found: List[Dict[str, Any]] = []
        self._completed_runs: List[int] = []
        self._comparison_results: Optional[Dict[str, List[ComparisonResult]]] = None
    
    def _store_input_events(self) -> None:
        """Store input events for each signal for later comparison."""
        for signal_config in self.config.signals:
            replay = SignalReplay(
                signal_config,
                simulation_speed=self.config.simulation_speed,
                debug=self.debug
            )
            
            # Store the input data
            if replay.input_data is not None and not replay.input_data.empty:
                # Convert to format expected by database
                input_df = replay.input_data.copy()
                input_df = input_df.rename(columns={
                    'TimeStamp': 'timestamp',
                    'EventId': 'event_id',
                    'Detector': 'parameter'
                })
                self.db.insert_input_events(input_df, signal_config.device_id)
    
    def _run_single_signal(self, signal_config: SignalConfig) -> datetime:
        """Run replay for a single signal and return start time."""
        replay = SignalReplay(
            signal_config,
            simulation_speed=self.config.simulation_speed,
            debug=self.debug
        )
        return replay.run()
    
    def _run_all_signals(self) -> Dict[str, datetime]:
        """Run replay for all signals in parallel and return start times."""
        start_times = {}
        
        with ThreadPoolExecutor(max_workers=len(self.config.signals)) as executor:
            futures = {
                executor.submit(self._run_single_signal, sig): sig.device_id
                for sig in self.config.signals
            }
            
            for future in as_completed(futures):
                device_id = futures[future]
                start_time = future.result()
                start_times[device_id] = start_time
        
        return start_times
    
    def _get_estimated_duration(self) -> float:
        """Get estimated simulation duration in seconds."""
        max_duration = 0.0
        
        for signal_config in self.config.signals:
            replay = SignalReplay(
                signal_config,
                simulation_speed=self.config.simulation_speed,
                debug=self.debug
            )
            duration = replay.get_run_duration()
            if duration > max_duration:
                max_duration = duration
        
        return max_duration
    
    def _on_conflict_detected(self, conflicts: List) -> None:
        """Callback when conflicts are detected."""
        for conflict in conflicts:
            self._conflicts_found.append({
                'device_id': conflict.device_id,
                'run_number': conflict.run_number,
                'timestamp': conflict.timestamp,
                'conflict_details': conflict.conflict_details
            })
        
        if self.config.stop_on_conflict:
            self._stop_event.set()
    
    def run(self) -> Dict[str, Any]:
        """
        Run the complete simulation.
        
        Executes all configured replay runs, collects data,
        checks for conflicts, and runs comparison analysis.
        
        Returns:
            Dict with simulation results including:
            - completed_runs: List of completed run numbers
            - conflicts: List of detected conflicts
            - stopped_early: Whether simulation stopped due to conflict
            - comparison_summary: DTW comparison summary string
        """
        print(f"Starting ATC simulation with {len(self.config.signals)} signals, "
              f"{self.config.simulation_replays} replays")
        
        estimated_duration = self._get_estimated_duration()
        print(f"Estimated duration per run: {estimated_duration:.1f} seconds")
        
        # Build device configs for collector
        device_configs = {
            sig.device_id: (sig.ip_port, sig.incompatible_pairs)
            for sig in self.config.signals
        }
        
        # Create data collector
        collector = DataCollector(
            db_path=self.config.db_path,
            device_configs=device_configs,
            collection_interval_minutes=self.config.collection_interval_minutes,
            stop_on_conflict=self.config.stop_on_conflict,
            debug=self.debug
        )
        
        stopped_early = False
        
        for run_num in range(1, self.config.simulation_replays + 1):
            if self._stop_event.is_set():
                stopped_early = True
                print(f"\nSimulation stopped early due to conflict after run {run_num - 1}")
                break
            
            print(f"\n--- Starting Run {run_num}/{self.config.simulation_replays} ---")
            self._current_run = run_num
            
            # Reset stop event for this run
            run_stop_event = threading.Event()
            
            # Start data collection in background thread
            collection_thread = threading.Thread(
                target=collector.run_collection_loop,
                args=(run_num, datetime.now(), run_stop_event, self._on_conflict_detected),
                daemon=True
            )
            collection_thread.start()
            
            # Run all signals
            start_times = self._run_all_signals()
            self._simulation_start_time = min(start_times.values()) if start_times else datetime.now()
            
            # Wait for simulation to complete
            time.sleep(estimated_duration + 5)  # Add buffer
            
            # Stop collection for this run
            run_stop_event.set()
            collection_thread.join(timeout=30)
            
            # Final data collection for this run
            collector.collect_once(run_num, self._simulation_start_time)
            
            self._completed_runs.append(run_num)
            print(f"Run {run_num} completed")
            
            # Check if we should stop
            if self._conflicts_found and self.config.stop_on_conflict:
                stopped_early = True
                print("\nConflict detected! Stopping simulation.")
                break
        
        # Run comparison analysis
        print("\n--- Running Comparison Analysis ---")
        self._run_comparison()
        
        # Build results
        results = {
            'completed_runs': self._completed_runs,
            'conflicts': self._conflicts_found,
            'stopped_early': stopped_early,
            'comparison_summary': self.get_comparison_summary()
        }
        
        # Print summary
        self._print_summary()
        
        return results
    
    def _run_comparison(self) -> None:
        """Run DTW comparison analysis on all collected data."""
        device_ids = [sig.device_id for sig in self.config.signals]
        
        self._comparison_results = compare_all_runs(
            self.db,
            device_ids,
            self._completed_runs,
            include_input_comparison=True
        )
    
    def _print_summary(self) -> None:
        """Print final simulation summary."""
        print("\n" + "=" * 60)
        print("SIMULATION COMPLETE")
        print("=" * 60)
        
        print(f"\nCompleted Runs: {len(self._completed_runs)}")
        print(f"Conflicts Found: {len(self._conflicts_found)}")
        
        if self._conflicts_found:
            print("\nConflicts:")
            for conflict in self._conflicts_found:
                print(f"  [{conflict['device_id']}] Run {conflict['run_number']}: "
                      f"{conflict['conflict_details']} at {conflict['timestamp']}")
        
        if self._comparison_results:
            print("\n" + self.get_comparison_summary())
    
    def get_events(
        self,
        device_id: Optional[str] = None,
        run_number: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Get collected events from the database.
        
        Args:
            device_id: Optional filter by device
            run_number: Optional filter by run number
        
        Returns:
            DataFrame of events
        """
        return self.db.get_events(device_id=device_id, run_number=run_number)
    
    def get_conflicts(
        self,
        device_id: Optional[str] = None,
        run_number: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Get detected conflicts from the database.
        
        Args:
            device_id: Optional filter by device
            run_number: Optional filter by run number
        
        Returns:
            DataFrame of conflicts
        """
        return self.db.get_conflicts(device_id=device_id, run_number=run_number)
    
    def get_comparison_results(self) -> Optional[Dict[str, List[ComparisonResult]]]:
        """Get the raw comparison results."""
        return self._comparison_results
    
    def get_comparison_summary(self) -> str:
        """Get formatted comparison summary."""
        if not self._comparison_results:
            return "No comparison results available."
        return format_comparison_summary(self._comparison_results)
    
    def get_input_events(self, device_id: Optional[str] = None) -> pd.DataFrame:
        """Get stored input events for comparison."""
        return self.db.get_input_events(device_id=device_id)


def quick_run(
    signals: List[Dict[str, Any]],
    simulation_replays: int = 1,
    stop_on_conflict: bool = False,
    db_path: str = "./atc_replay.duckdb",
    simulation_speed: float = 1.0,
    debug: bool = False
) -> Dict[str, Any]:
    """
    Quick helper to run a simulation from a list of signal dictionaries.
    
    Args:
        signals: List of dicts with keys:
            - device_id: str
            - ip_port: tuple (host, port)
            - cycle_length: int
            - incompatible_pairs: list of tuples
            - events: DataFrame, Arrow Table, or file path
        simulation_replays: Number of replay runs
        stop_on_conflict: Stop when conflict detected
        db_path: Path to DuckDB database
        simulation_speed: Speed multiplier
        debug: Enable debug output
    
    Returns:
        Simulation results dict
    
    Example:
        >>> results = quick_run([
        ...     {
        ...         'device_id': 'signal_1',
        ...         'ip_port': ('127.0.0.1', 1025),
        ...         'cycle_length': 100,
        ...         'incompatible_pairs': [('Ph1', 'Ph5')],
        ...         'events': 'events.csv'
        ...     }
        ... ], simulation_replays=3)
    """
    signal_configs = [
        SignalConfig(
            device_id=s['device_id'],
            ip_port=s['ip_port'],
            cycle_length=s['cycle_length'],
            incompatible_pairs=s.get('incompatible_pairs', []),
            events=s['events']
        )
        for s in signals
    ]
    
    config = SimulationConfig(
        signals=signal_configs,
        simulation_replays=simulation_replays,
        stop_on_conflict=stop_on_conflict,
        db_path=db_path,
        simulation_speed=simulation_speed
    )
    
    sim = ATCSimulation(config, debug=debug)
    return sim.run()

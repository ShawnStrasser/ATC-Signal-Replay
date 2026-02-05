"""
Main orchestrator for ATC Signal Replay simulations.
"""

import threading
import time
from copy import copy
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import pandas as pd

from .config import SimulationConfig, SignalConfig
from .replay import SignalReplay, create_replays
from .collector import DatabaseManager, DataCollector, fetch_output_data, check_conflicts
from .comparison import (
    compare_all_runs, 
    format_comparison_summary, 
    ComparisonResult,
    ComparisonThresholds,
    compare_runs,
    compare_and_visualize,
    store_comparison_result,
    generate_timeline,
    prepare_events_for_comparison,
    HAS_ATSPM
)


def _load_events(events: Union[pd.DataFrame, str, Path]) -> pd.DataFrame:
    """Load events from DataFrame or file path."""
    if isinstance(events, pd.DataFrame):
        return events
    elif isinstance(events, (str, Path)):
        path = Path(events)
        if path.suffix.lower() == '.parquet':
            return pd.read_parquet(path)
        else:
            return pd.read_csv(path)
    else:
        raise ValueError(f"events must be DataFrame or file path, got {type(events)}")


def _normalize_device_id_column(df: pd.DataFrame) -> str:
    """Find and return the device_id column name (normalized)."""
    for col in df.columns:
        if col.lower() in ('device_id', 'deviceid'):
            return col
    raise ValueError(
        "Centralized events must have a 'device_id' column to map events to signals. "
        f"Found columns: {list(df.columns)}"
    )


def _distribute_events(
    signals: List[SignalConfig],
    centralized_events: Union[pd.DataFrame, str, Path]
) -> None:
    """
    Distribute centralized events to all signals by filtering on device_id.
    
    Args:
        signals: List of SignalConfig objects (modified in place)
        centralized_events: DataFrame or path with device_id column
    """
    # Load the centralized events
    all_events = _load_events(centralized_events)
    device_id_col = _normalize_device_id_column(all_events)
    
    # Get unique device_ids in the events
    available_device_ids = set(all_events[device_id_col].astype(str).unique())
    
    for signal in signals:
        # Filter centralized events for this device
        device_id_str = str(signal.device_id)
        if device_id_str not in available_device_ids:
            raise ValueError(
                f"Signal '{signal.device_id}' not found in events. "
                f"Available device_ids: {sorted(available_device_ids)}"
            )
        
        # Filter events for this device
        mask = all_events[device_id_col].astype(str) == device_id_str
        signal_events = all_events[mask].copy()
        
        if signal_events.empty:
            raise ValueError(
                f"No events found for device_id '{signal.device_id}'"
            )
        
        # Assign events directly (using object.__setattr__ for frozen dataclass compatibility)
        object.__setattr__(signal, 'events', signal_events)


class ATCSimulation:
    """
    Main orchestrator for multi-signal ATC replay simulations.
    
    Manages replay processes, data collection, conflict detection,
    and post-simulation comparison analysis.
    
    Can be initialized in two ways:
    
    1. Legacy (with SimulationConfig):
        config = SimulationConfig(signals=[...], simulation_replays=5)
        sim = ATCSimulation(config)
    
    2. Streamlined (with kwargs):
        sim = ATCSimulation(
            signals=[...],
            events='all_events.csv',  # Centralized, filtered by device_id
            replays=5
        )
    
    Comparison Thresholds:
        Set comparison_thresholds to control when plots are generated.
        If a comparison exceeds thresholds and output_dir is set, 
        Gantt charts will be automatically generated.
    """
    
    def __init__(
        self,
        config: Optional[SimulationConfig] = None,
        *,
        signals: Optional[List[SignalConfig]] = None,
        events: Union[pd.DataFrame, str, Path, None] = None,
        replays: int = 1,
        stop_on_conflict: bool = False,
        db_path: str = "./atc_replay.duckdb",
        simulation_speed: float = 1.0,
        comparison_thresholds: Optional[ComparisonThresholds] = None,
        output_dir: Optional[Union[str, Path]] = None,
        debug: bool = False
    ):
        """
        Initialize the ATC simulation.
        
        Args:
            config: SimulationConfig with all simulation parameters (legacy pattern)
            signals: List of SignalConfig objects (streamlined pattern)
            events: REQUIRED. Centralized event source with device_id column.
                Events are automatically filtered and distributed to signals by device_id.
            replays: Number of simulation runs (streamlined pattern)
            stop_on_conflict: Stop simulation on first conflict
            db_path: Path to DuckDB database
            simulation_speed: Speed multiplier (1.0 = real-time)
            comparison_thresholds: Thresholds for triggering comparison alerts.
                If None, uses defaults (sequence=0.05, timing=0.02, match=95%).
            output_dir: Directory to save comparison plots when thresholds exceeded.
                If None, no plots are generated.
            debug: Enable debug output
        """
        self.debug = debug
        self.comparison_thresholds = comparison_thresholds or ComparisonThresholds()
        self.output_dir = Path(output_dir) if output_dir else None
        
        # Handle legacy vs streamlined initialization
        if config is not None:
            # Legacy pattern: SimulationConfig provided
            if signals is not None:
                raise ValueError("Cannot specify both 'config' and 'signals'. Use one or the other.")
            # Distribute events to all signals
            _distribute_events(config.signals, config.events)
            self.config = config
        else:
            # Streamlined pattern: kwargs provided
            if signals is None:
                raise ValueError("Must provide either 'config' or 'signals'")
            if events is None:
                raise ValueError("Must provide 'events' (centralized event source with device_id column)")
            
            # Distribute centralized events to all signals
            _distribute_events(signals, events)
            
            # Create SimulationConfig internally
            self.config = SimulationConfig(
                signals=signals,
                events=events,
                simulation_replays=replays,
                stop_on_conflict=stop_on_conflict,
                db_path=db_path,
                simulation_speed=simulation_speed,
            )
        
        # Initialize database
        self.db = DatabaseManager(self.config.db_path)
        
        # Determine starting run number if database already has runs
        self._run_offset = self.db.get_max_run_number()
        if self._run_offset > 0:
            print(f"Existing runs found in database. Starting new runs from {self._run_offset + 1}")
        
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
        """Store source comparison events (phase/overlap events) for each signal.
        
        This stores the original phase/overlap events from the source data,
        not the detector actuations. This allows meaningful comparison between
        the source data and the replay output events.
        """
        for signal_config in self.config.signals:
            replay = SignalReplay(
                signal_config,
                simulation_speed=self.config.simulation_speed,
                debug=self.debug
            )
            
            # Get source comparison events (phase/overlap events)
            comparison_events = replay.get_source_comparison_events()
            
            if comparison_events is not None and not comparison_events.empty:
                # Data is already in the correct format (timestamp, event_id, parameter)
                self.db.insert_input_events(comparison_events, signal_config.device_id)
    
    def _run_single_signal(self, signal_config: SignalConfig) -> datetime:
        """Run replay for a single signal and return start time."""
        replay = SignalReplay(
            signal_config,
            simulation_speed=self.config.simulation_speed,
            debug=self.debug
        )
        return replay.run()
    
    def _run_all_signals(self) -> Dict[str, datetime]:
        """
        Run replay for all signals in parallel and return start times.
        
        This method BLOCKS until all signal replays have completed.
        Multiple signals run in parallel via ThreadPoolExecutor, but this
        method waits for all of them to finish before returning.
        """
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
            # Create conflict dict
            conflict_dict = {
                'device_id': conflict.device_id,
                'run_number': conflict.run_number,
                'timestamp': conflict.timestamp,
                'conflict_details': conflict.conflict_details
            }
            
            # Only add if not already in the list (avoid duplicates in memory)
            if conflict_dict not in self._conflicts_found:
                self._conflicts_found.append(conflict_dict)
        
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
        print(f"Estimated duration per run: {int(estimated_duration // 60):02d}:{int(estimated_duration % 60):02d}")
        
        # Build device configs for collector
        # Structure: {device_id: (ip_port, incompatible_pairs, http_port)}
        device_configs = {
            sig.device_id: (sig.ip_port, sig.incompatible_pairs, sig.http_port)
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
        
        for run_idx in range(1, self.config.simulation_replays + 1):
            run_num = run_idx + self._run_offset
            
            if self._stop_event.is_set():
                stopped_early = True
                print(f"\nSimulation stopped early due to conflict after run {run_num - 1}")
                break
            
            print(f"\n--- Starting Run {run_num}/{self.config.simulation_replays + self._run_offset} ---")
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
            
            # Run all signals - this BLOCKS until all replays complete
            # No additional sleep needed since _run_all_signals waits for completion
            start_times = self._run_all_signals()
            self._simulation_start_time = min(start_times.values()) if start_times else datetime.now()
            
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
        """Run DTW comparison analysis on all collected data with threshold checks."""
        device_ids = [sig.device_id for sig in self.config.signals]
        
        self._comparison_results = compare_all_runs(
            self.db,
            device_ids,
            self._completed_runs,
            include_input_comparison=True
        )
        
        # Check thresholds and generate plots for each comparison
        self._check_thresholds_and_plot()
    
    def _check_thresholds_and_plot(self) -> None:
        """Check comparison thresholds and generate plots when exceeded."""
        if not self._comparison_results:
            return
        
        for device_id, comparisons in self._comparison_results.items():
            for result in comparisons:
                # Add threshold info to result
                result.thresholds = self.comparison_thresholds
                exceeded, reason = self.comparison_thresholds.exceeds_threshold(
                    result.sequence_dtw.normalized_distance,
                    result.timing_dtw.normalized_distance,
                    result.match_percentage
                )
                result.exceeds_threshold = exceeded
                result.threshold_reason = reason
                
                # Store result in database
                try:
                    store_comparison_result(self.config.db_path, result)
                except Exception as e:
                    if self.debug:
                        print(f"Failed to store comparison result: {e}")
                
                # Generate plot if threshold exceeded and output_dir configured
                if exceeded and self.output_dir:
                    self._generate_comparison_plot(device_id, result)
                elif exceeded:
                    print(f"\n⚠️  Threshold exceeded for {device_id} ({result.run_a} vs {result.run_b}):")
                    print(f"    {reason}")
                    print(f"    Set output_dir to generate comparison plots.")
    
    def _generate_comparison_plot(self, device_id: str, result: ComparisonResult) -> None:
        """Generate Gantt chart for a comparison that exceeded thresholds."""
        if not HAS_ATSPM:
            if self.debug:
                print("Cannot generate plots: 'atspm' package not installed")
            return
        if not HAS_PLOTLY:
            if self.debug:
                print("Cannot generate plots: 'plotly' package not installed")
            return
        
        try:
            # Get events for both runs
            if result.run_a == "input":
                events_a = self.db.get_input_events(device_id=device_id)
            else:
                events_a = self.db.get_events(device_id=device_id, run_number=int(result.run_a))
            
            events_b = self.db.get_events(device_id=device_id, run_number=int(result.run_b))
            
            if events_a.empty or events_b.empty:
                if self.debug:
                    print(f"No events to plot for {device_id}")
                return
            
            # Find divergence time
            divergence_start = None
            divergence_end = None
            
            if result.divergence_windows:
                df_a_prep = prepare_events_for_comparison(events_a)
                if not df_a_prep.empty:
                    first_div = result.divergence_windows[0]
                    if first_div.start_index_a < len(df_a_prep):
                        divergence_start = df_a_prep.iloc[first_div.start_index_a]['timestamp']
                    if first_div.end_index_a < len(df_a_prep):
                        divergence_end = df_a_prep.iloc[first_div.end_index_a]['timestamp']
            
            # Generate timelines
            timeline_a = generate_timeline(events_a, device_id=device_id)
            timeline_b = generate_timeline(events_b, device_id=device_id)
            
            # Create output filename
            label_a = str(result.run_a)
            label_b = str(result.run_b)
            output_name = f"{device_id}_{label_a}_vs_{label_b}".replace(' ', '_')
            output_path = self.output_dir / f"{output_name}.png"
            
            # Create Gantt chart
            if HAS_MATPLOTLIB:
                from .comparison import create_comparison_gantt_matplotlib
                import matplotlib.pyplot as plt
                
                fig = create_comparison_gantt_matplotlib(
                    timeline_a=timeline_a,
                    timeline_b=timeline_b,
                    label_a=f"Run {label_a}" if label_a != "input" else "Input",
                    label_b=f"Run {label_b}",
                    title=f"Device {device_id}: {label_a} vs {label_b}",
                    divergence_start=divergence_start,
                    divergence_end=divergence_end,
                    output_path=output_path,
                    window_minutes=5.0
                )
                if fig:
                    plt.close(fig)
            
            result.plot_path = str(output_path)
            
            if self.debug:
                print(f"Generated comparison plot: {output_path}")
                
        except Exception as e:
            if self.debug:
                print(f"Failed to generate plot for {device_id}: {e}")
    
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
            
            # Print threshold alerts
            alerts = []
            for device_id, comparisons in self._comparison_results.items():
                for result in comparisons:
                    if hasattr(result, 'exceeds_threshold') and result.exceeds_threshold:
                        alerts.append((device_id, result))
            
            if alerts:
                print("\n⚠️  THRESHOLD ALERTS:")
                for device_id, result in alerts:
                    print(f"  [{device_id}] {result.run_a} vs {result.run_b}: {result.threshold_reason}")
                    if result.plot_path:
                        print(f"      Plot: {result.plot_path}")
    
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

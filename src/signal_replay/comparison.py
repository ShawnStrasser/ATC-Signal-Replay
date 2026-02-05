"""
DTW-based comparison module for analyzing hi-res event sequences.

DTW Metrics Explained:
----------------------
- **Sequence DTW (normalized_distance)**: Measures how different the sequence of 
  (event_id, parameter) pairs is using categorical distance (0=exact match, 1=mismatch).
  The normalized value divides total mismatches by path length.
  **Lower is better** (0.0 = identical sequences, 1.0 = completely different).
  
- **Timing DTW (normalized_distance)**: Measures how different the timing patterns are
  using Euclidean distance on normalized time deltas.
  **Lower is better** (0.0 = identical timing patterns).

- **Match Percentage**: Percentage of aligned events that match exactly along the
  DTW warping path. **Higher is better** (100% = all events match).

Threshold Guidelines:
---------------------
- sequence_threshold: 0.05 = 5% mismatch rate along the path (reasonable default)
- timing_threshold: 0.02 = small timing differences allowed (reasonable default)
- Match < 95% often indicates significant behavioral differences
"""

import pandas as pd
import numpy as np
from typing import List, Tuple, Dict, Optional, Union, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from pathlib import Path
import warnings
import duckdb

try:
    from dtaidistance import dtw
    HAS_DTAIDISTANCE = True
except ImportError:
    HAS_DTAIDISTANCE = False

try:
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    from matplotlib.colors import to_rgba
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

try:
    from atspm import SignalDataProcessor
    HAS_ATSPM = True
except ImportError:
    HAS_ATSPM = False


# Event IDs to include in comparison
COMPARISON_EVENT_IDS = [
    1, 7, 8, 9, 10, 11, 21, 22, 23, 32, 33, 55, 56,
    61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72,
    105, 106, 107, 111, 112, 113, 114, 115, 118, 119
]

# Default thresholds for comparison alerts
DEFAULT_SEQUENCE_THRESHOLD = 0.05  # 5% mismatch rate
DEFAULT_TIMING_THRESHOLD = 0.02    # 2% timing deviation


@dataclass
class ComparisonThresholds:
    """Thresholds for triggering comparison alerts and visualizations.
    
    Attributes:
        sequence_threshold: Maximum allowed sequence DTW normalized distance.
            Values above this trigger alerts/plots. Default 0.05 (5% mismatch).
        timing_threshold: Maximum allowed timing DTW normalized distance.
            Values above this trigger alerts/plots. Default 0.02.
        match_threshold: Minimum required match percentage.
            Values below this trigger alerts. Default 95.0 (95% match).
    """
    sequence_threshold: float = DEFAULT_SEQUENCE_THRESHOLD
    timing_threshold: float = DEFAULT_TIMING_THRESHOLD
    match_threshold: float = 95.0
    
    def exceeds_threshold(self, sequence_dtw: float, timing_dtw: float, match_pct: float) -> Tuple[bool, str]:
        """Check if any threshold is exceeded and return reason."""
        reasons = []
        if sequence_dtw > self.sequence_threshold:
            reasons.append(f"Sequence DTW ({sequence_dtw:.4f}) > threshold ({self.sequence_threshold})")
        if timing_dtw > self.timing_threshold:
            reasons.append(f"Timing DTW ({timing_dtw:.4f}) > threshold ({self.timing_threshold})")
        if match_pct < self.match_threshold:
            reasons.append(f"Match ({match_pct:.1f}%) < threshold ({self.match_threshold}%)")
        
        return len(reasons) > 0, "; ".join(reasons)


@dataclass
class DTWResult:
    """Result of a DTW comparison."""
    distance: float
    normalized_distance: float
    warping_path: List[Tuple[int, int]]
    sequence_length_a: int
    sequence_length_b: int


@dataclass 
class DivergenceWindow:
    """A window where two sequences diverge significantly."""
    start_index_a: int
    end_index_a: int
    start_index_b: int
    end_index_b: int
    start_time_delta_a: float
    end_time_delta_a: float
    start_time_delta_b: float
    end_time_delta_b: float
    start_timestamp_a: Optional[datetime] = None
    end_timestamp_a: Optional[datetime] = None
    start_timestamp_b: Optional[datetime] = None
    end_timestamp_b: Optional[datetime] = None


@dataclass
class ComparisonResult:
    """Complete comparison result between two runs."""
    device_id: str
    run_a: Union[int, str]  # run number or "input"
    run_b: Union[int, str]
    sequence_dtw: DTWResult
    timing_dtw: DTWResult
    divergence_windows: List[DivergenceWindow]
    match_percentage: float
    # Optional: threshold info for storage
    thresholds: Optional[ComparisonThresholds] = None
    exceeds_threshold: bool = False
    threshold_reason: str = ""
    # Optional: plot path if generated
    plot_path: Optional[str] = None


@dataclass
class ComparisonAnalysis:
    """Extended analysis including timeline data and visualization info."""
    result: ComparisonResult
    events_a: pd.DataFrame
    events_b: pd.DataFrame
    timeline_a: Optional[pd.DataFrame] = None
    timeline_b: Optional[pd.DataFrame] = None
    divergence_start_time: Optional[datetime] = None
    divergence_end_time: Optional[datetime] = None


def prepare_events_for_comparison(
    events_df: pd.DataFrame,
    start_time: Optional[datetime] = None,
    event_ids: Optional[List[int]] = None
) -> pd.DataFrame:
    """
    Prepare events for DTW comparison.
    
    Filters to comparison event IDs and normalizes timestamps to time deltas.
    
    Args:
        events_df: DataFrame with timestamp, event_id, parameter columns
        start_time: Optional start time for delta calculation (uses min timestamp if None)
    
    Returns:
        Prepared DataFrame with time_delta, event_id, parameter, encoded_sequence
    """
    # Normalize column names
    df = events_df.copy()
    col_map = {}
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in ('timestamp', 'time_stamp'):
            col_map[col] = 'timestamp'
        elif col_lower in ('event_id', 'eventid', 'eventtypeid'):
            col_map[col] = 'event_id'
        elif col_lower in ('parameter', 'param'):
            col_map[col] = 'parameter'
    
    df = df.rename(columns=col_map)
    
    # Filter to comparison event IDs
    if event_ids is None:
        event_ids = COMPARISON_EVENT_IDS
    if event_ids:
        df = df[df['event_id'].isin(event_ids)].copy()
    
    if df.empty:
        return df
    
    # Ensure timestamp is datetime
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Sort by timestamp
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    # Calculate time delta from start
    if start_time is None:
        start_time = df['timestamp'].min()
    
    df['time_delta'] = (df['timestamp'] - start_time).dt.total_seconds()
    
    return df


def encode_categorical_sequence(
    df: pd.DataFrame,
    encoding_map: Optional[Dict[Tuple[int, int], int]] = None
) -> Tuple[np.ndarray, Dict[Tuple[int, int], int]]:
    """
    Encode (event_id, parameter) pairs as integer sequence for categorical comparison.
    
    Each unique (event_id, parameter) combination is assigned a unique integer.
    These are NOT normalized because we use a custom binary distance function
    for categorical data: distance = 0 if values match exactly, 1 if they differ.
    
    Args:
        df: DataFrame with event_id and parameter columns
        encoding_map: Optional existing encoding map to use for consistency
    
    Returns:
        Tuple of (encoded integer array, encoding map)
    """
    if df.empty:
        return np.array([]), {}
    
    # Create (event_id, parameter) pairs
    pairs = list(zip(df['event_id'].values, df['parameter'].values))
    
    # Build or use encoding map
    if encoding_map is None:
        unique_pairs = sorted(set(pairs))
        encoding_map = {pair: i for i, pair in enumerate(unique_pairs)}
    
    # Encode pairs - use integer encoding, NOT normalized
    # The distance function will treat these as categorical (match=0, mismatch=1)
    max_val = max(encoding_map.values()) if encoding_map else 0
    encoded = np.array([encoding_map.get(p, max_val + 1) for p in pairs], dtype=float)
    
    return encoded, encoding_map


def compute_dtw(
    seq_a: np.ndarray,
    seq_b: np.ndarray,
    categorical: bool = False
) -> DTWResult:
    """
    Compute DTW distance and warping path between two sequences.
    
    Args:
        seq_a: First sequence
        seq_b: Second sequence
        categorical: If True, use binary distance (0=exact match, 1=mismatch).
                    This is appropriate for encoded categorical data where
                    numeric proximity has no meaning. If False, use standard
                    Euclidean distance (appropriate for timing data).
    
    Returns:
        DTWResult with distance, path, and lengths
    """
    if not HAS_DTAIDISTANCE:
        raise ImportError(
            "dtaidistance package is required for DTW comparison. "
            "Install with: pip install dtaidistance"
        )
    
    if len(seq_a) == 0 or len(seq_b) == 0:
        return DTWResult(
            distance=float('inf'),
            normalized_distance=float('inf'),
            warping_path=[],
            sequence_length_a=len(seq_a),
            sequence_length_b=len(seq_b)
        )
    
    if categorical:
        # For categorical data, we transform the problem so that Euclidean
        # DTW produces the correct alignment for categorical matching.
        #
        # Problem: Standard DTW uses |seq_a[i] - seq_b[j]| as distance.
        # With integer encoding, adjacent categories (e.g., 4 and 5) would
        # appear similar when they're actually completely different categories.
        #
        # Solution: Use one-hot encoding. For each position, create a vector
        # where only the index matching the category value is 1, rest are 0.
        # Then Euclidean distance between matching categories = 0, 
        # and between different categories = sqrt(2) ≈ 1.41.
        #
        # For efficiency with many categories, we use a simpler approach:
        # compute the distance matrix directly using binary distance.
        
        n, m = len(seq_a), len(seq_b)
        
        # Build distance matrix: 0 if values match, 1 if they differ
        dist_matrix = np.ones((n, m), dtype=np.float64)
        for i in range(n):
            for j in range(m):
                if seq_a[i] == seq_b[j]:
                    dist_matrix[i, j] = 0.0
        
        # Compute DTW using the distance matrix
        # dtaidistance doesn't directly support distance matrices,
        # so we compute the cumulative cost matrix and path ourselves
        
        # Initialize cumulative cost matrix
        cum_cost = np.full((n + 1, m + 1), np.inf)
        cum_cost[0, 0] = 0.0
        
        for i in range(1, n + 1):
            for j in range(1, m + 1):
                cost = dist_matrix[i - 1, j - 1]
                cum_cost[i, j] = cost + min(
                    cum_cost[i - 1, j],      # insertion
                    cum_cost[i, j - 1],      # deletion  
                    cum_cost[i - 1, j - 1]   # match
                )
        
        distance = float(cum_cost[n, m])
        
        # Backtrack to find the warping path
        path = []
        i, j = n, m
        while i > 0 or j > 0:
            path.append((i - 1, j - 1))  # Convert to 0-indexed
            if i == 0:
                j -= 1
            elif j == 0:
                i -= 1
            else:
                # Find which direction we came from
                candidates = [
                    (cum_cost[i - 1, j - 1], i - 1, j - 1),
                    (cum_cost[i - 1, j], i - 1, j),
                    (cum_cost[i, j - 1], i, j - 1),
                ]
                _, i, j = min(candidates, key=lambda x: x[0])
        
        path.reverse()
        
        # Normalize by path length (gives proportion of mismatches)
        normalized_distance = distance / len(path) if path else float('inf')
    else:
        # Standard Euclidean DTW for numerical/timing data
        distance = dtw.distance(seq_a, seq_b)
        path = dtw.warping_path(seq_a, seq_b)
        
        # Normalize by path length
        normalized_distance = distance / len(path) if path else float('inf')
    
    return DTWResult(
        distance=distance,
        normalized_distance=normalized_distance,
        warping_path=path,
        sequence_length_a=len(seq_a),
        sequence_length_b=len(seq_b)
    )


def find_divergence_windows(
    warping_path: List[Tuple[int, int]],
    time_deltas_a: np.ndarray,
    time_deltas_b: np.ndarray,
    jump_threshold: int = 3
) -> List[DivergenceWindow]:
    """
    Find windows where sequences diverge based on warping path.
    
    A divergence occurs when the warping path shows large jumps,
    indicating one sequence has events not matched in the other.
    
    Args:
        warping_path: DTW warping path as list of (i, j) tuples
        time_deltas_a: Time deltas for sequence A
        time_deltas_b: Time deltas for sequence B
        jump_threshold: Minimum jump size to consider a divergence
    
    Returns:
        List of DivergenceWindow objects
    """
    if not warping_path or len(warping_path) < 2:
        return []
    
    divergences = []
    in_divergence = False
    divergence_start = None
    
    for k in range(1, len(warping_path)):
        prev_i, prev_j = warping_path[k - 1]
        curr_i, curr_j = warping_path[k]
        
        jump_i = curr_i - prev_i
        jump_j = curr_j - prev_j
        
        # Detect divergence: one index jumps while other stays (or small step)
        is_divergent = (
            (jump_i >= jump_threshold and jump_j <= 1) or
            (jump_j >= jump_threshold and jump_i <= 1)
        )
        
        if is_divergent and not in_divergence:
            # Start of divergence
            in_divergence = True
            divergence_start = (prev_i, prev_j, k - 1)
        elif not is_divergent and in_divergence:
            # End of divergence
            in_divergence = False
            start_i, start_j, _ = divergence_start
            
            divergences.append(DivergenceWindow(
                start_index_a=start_i,
                end_index_a=prev_i,
                start_index_b=start_j,
                end_index_b=prev_j,
                start_time_delta_a=float(time_deltas_a[start_i]) if start_i < len(time_deltas_a) else 0.0,
                end_time_delta_a=float(time_deltas_a[prev_i]) if prev_i < len(time_deltas_a) else 0.0,
                start_time_delta_b=float(time_deltas_b[start_j]) if start_j < len(time_deltas_b) else 0.0,
                end_time_delta_b=float(time_deltas_b[prev_j]) if prev_j < len(time_deltas_b) else 0.0,
            ))
    
    # Handle divergence at end of path
    if in_divergence and divergence_start:
        start_i, start_j, _ = divergence_start
        last_i, last_j = warping_path[-1]
        
        divergences.append(DivergenceWindow(
            start_index_a=start_i,
            end_index_a=last_i,
            start_index_b=start_j,
            end_index_b=last_j,
            start_time_delta_a=float(time_deltas_a[start_i]) if start_i < len(time_deltas_a) else 0.0,
            end_time_delta_a=float(time_deltas_a[last_i]) if last_i < len(time_deltas_a) else 0.0,
            start_time_delta_b=float(time_deltas_b[start_j]) if start_j < len(time_deltas_b) else 0.0,
            end_time_delta_b=float(time_deltas_b[last_j]) if last_j < len(time_deltas_b) else 0.0,
        ))
    
    return divergences


def compare_runs(
    events_a: pd.DataFrame,
    events_b: pd.DataFrame,
    device_id: str,
    run_a_label: Union[int, str] = "A",
    run_b_label: Union[int, str] = "B",
    start_time_a: Optional[datetime] = None,
    start_time_b: Optional[datetime] = None,
    event_ids: Optional[List[int]] = None
) -> ComparisonResult:
    """
    Compare two runs using DTW on both sequence and timing.
    
    Args:
        events_a: Events from first run
        events_b: Events from second run
        device_id: Device identifier
        run_a_label: Label for first run (e.g., run number or "input")
        run_b_label: Label for second run
        start_time_a: Start time for run A (for time delta calculation)
        start_time_b: Start time for run B
    
    Returns:
        ComparisonResult with DTW distances and divergence windows
    """
    # Prepare events
    df_a = prepare_events_for_comparison(events_a, start_time_a, event_ids=event_ids)
    df_b = prepare_events_for_comparison(events_b, start_time_b, event_ids=event_ids)
    
    if df_a.empty or df_b.empty:
        return ComparisonResult(
            device_id=device_id,
            run_a=run_a_label,
            run_b=run_b_label,
            sequence_dtw=DTWResult(
                distance=float('inf'),
                normalized_distance=float('inf'),
                warping_path=[],
                sequence_length_a=len(df_a),
                sequence_length_b=len(df_b)
            ),
            timing_dtw=DTWResult(
                distance=float('inf'),
                normalized_distance=float('inf'),
                warping_path=[],
                sequence_length_a=len(df_a),
                sequence_length_b=len(df_b)
            ),
            divergence_windows=[],
            match_percentage=0.0
        )
    
    # Build unified encoding map for both sequences
    all_pairs = (
        list(zip(df_a['event_id'].values, df_a['parameter'].values)) +
        list(zip(df_b['event_id'].values, df_b['parameter'].values))
    )
    unique_pairs = sorted(set(all_pairs))
    encoding_map = {pair: i for i, pair in enumerate(unique_pairs)}
    
    # Encode sequences
    seq_a, _ = encode_categorical_sequence(df_a, encoding_map)
    seq_b, _ = encode_categorical_sequence(df_b, encoding_map)
    
    # Get timing sequences
    time_a = df_a['time_delta'].values.astype(float)
    time_b = df_b['time_delta'].values.astype(float)
    
    # Normalize timing to [0, 1] for fair comparison
    max_time = max(time_a.max() if len(time_a) > 0 else 0, 
                   time_b.max() if len(time_b) > 0 else 0)
    if max_time > 0:
        time_a_norm = time_a / max_time
        time_b_norm = time_b / max_time
    else:
        time_a_norm = time_a
        time_b_norm = time_b
    
    # Compute DTW for sequences (categorical - binary match/mismatch distance)
    sequence_dtw = compute_dtw(seq_a, seq_b, categorical=True)
    
    # Compute DTW for timing (numerical - Euclidean distance)
    timing_dtw = compute_dtw(time_a_norm, time_b_norm, categorical=False)
    
    # Find divergence windows (use sequence path as primary)
    divergences = find_divergence_windows(
        sequence_dtw.warping_path,
        time_a,
        time_b
    )
    
    # Calculate match percentage based on warping path alignment
    # For each pair in the warping path, check if values match exactly.
    # We use path_length as denominator since it represents the actual
    # number of alignment decisions made by DTW.
    path_length = len(sequence_dtw.warping_path)
    if path_length > 0:
        # Count exact matches along the warping path
        matching_values = sum(
            1 for i, j in sequence_dtw.warping_path
            if seq_a[i] == seq_b[j]
        )
        # Match percentage = matches / path length
        # This gives the proportion of aligned positions that match exactly
        match_percentage = (matching_values / path_length) * 100
    else:
        match_percentage = 0.0
    
    return ComparisonResult(
        device_id=device_id,
        run_a=run_a_label,
        run_b=run_b_label,
        sequence_dtw=sequence_dtw,
        timing_dtw=timing_dtw,
        divergence_windows=divergences,
        match_percentage=match_percentage
    )


def compare_all_runs(
    db_manager,  # DatabaseManager from collector module
    device_ids: List[str],
    run_numbers: List[int],
    include_input_comparison: bool = True
) -> Dict[str, List[ComparisonResult]]:
    """
    Compare all runs for all devices.
    
    Args:
        db_manager: DatabaseManager instance
        device_ids: List of device IDs to compare
        run_numbers: List of run numbers to compare
        include_input_comparison: Whether to compare against input events
    
    Returns:
        Dict mapping device_id to list of ComparisonResult
    """
    from .collector import DatabaseManager
    from .replay import SignalReplay
    
    results = {}
    
    for device_id in device_ids:
        results[device_id] = []
        
        # Compare all run pairs
        for i in range(len(run_numbers) - 1):
            run_a = run_numbers[i]
            run_b = run_numbers[i + 1]
            
            events_a = db_manager.get_events(device_id=device_id, run_number=run_a)
            events_b = db_manager.get_events(device_id=device_id, run_number=run_b)
            
            result = compare_runs(
                events_a,
                events_b,
                device_id=device_id,
                run_a_label=run_a,
                run_b_label=run_b
            )
            results[device_id].append(result)
        
        # Compare input events to first run if requested
        if include_input_comparison and run_numbers:
            input_events = db_manager.get_input_events(device_id=device_id)
            run_events = db_manager.get_events(device_id=device_id, run_number=run_numbers[0])
            
            result = compare_runs(
                input_events,
                run_events,
                device_id=device_id,
                run_a_label="input",
                run_b_label=run_numbers[0]
            )
            results[device_id].append(result)
    
    return results


def format_comparison_summary(results: Dict[str, List[ComparisonResult]]) -> str:
    """
    Format comparison results into a readable summary string.
    
    Args:
        results: Dict mapping device_id to list of ComparisonResult
    
    Returns:
        Formatted summary string
    """
    lines = ["Comparison Summary:", ""]
    
    for device_id, comparisons in results.items():
        lines.append(f"Device: {device_id}")
        
        for result in comparisons:
            lines.append(
                f"  {result.run_a} vs {result.run_b}: "
                f"Sequence DTW={result.sequence_dtw.normalized_distance:.4f}, "
                f"Timing DTW={result.timing_dtw.normalized_distance:.4f}, "
                f"Match={result.match_percentage:.1f}%"
            )
        
        lines.append("")
    
    return "\n".join(lines)

def compare_event_sequences(
    events_a: pd.DataFrame,
    events_b: pd.DataFrame,
    label_a: str = "A",
    label_b: str = "B",
    device_id: str = "device",
    event_ids: Optional[List[int]] = None,
    print_summary: bool = True
) -> ComparisonResult:
    """
    Compare two event sequences directly using DTW.
    
    This is the main entry point for manual comparison of event data from
    different sources (e.g., comparing runs across different simulations,
    comparing logs from different time periods, etc.).
    
    Args:
        events_a: First event DataFrame with columns: timestamp, event_id, parameter
        events_b: Second event DataFrame with same columns
        label_a: Label for first sequence (default "A")
        label_b: Label for second sequence (default "B")
        device_id: Optional device identifier (default "device")
        event_ids: List of event IDs to include. If None, uses COMPARISON_EVENT_IDS
        print_summary: If True, print a summary of the comparison
    
    Returns:
        ComparisonResult with DTW distances, match percentage, and divergence windows
    
    Example:
        >>> import pandas as pd
        >>> import signal_replay as sr
        >>> 
        >>> # Load events from different sources
        >>> run1_events = pd.read_csv('run1_events.csv')
        >>> run2_events = pd.read_csv('run2_events.csv')
        >>> 
        >>> # Compare them
        >>> result = sr.compare_event_sequences(
        ...     run1_events, 
        ...     run2_events,
        ...     label_a="Run 1",
        ...     label_b="Run 2"
        ... )
        >>> 
        >>> # Access detailed results
        >>> print(f"Match: {result.match_percentage:.1f}%")
        >>> print(f"Sequence DTW: {result.sequence_dtw.normalized_distance:.4f}")
        >>> print(f"Divergences: {len(result.divergence_windows)}")
    """
    result = compare_runs(
        events_a=events_a,
        events_b=events_b,
        device_id=device_id,
        run_a_label=label_a,
        run_b_label=label_b,
        event_ids=event_ids
    )
    
    if print_summary:
        print(f"\n{'='*60}")
        print(f"DTW Comparison: {label_a} vs {label_b}")
        print(f"{'='*60}")
        print(f"  Sequence DTW (normalized): {result.sequence_dtw.normalized_distance:.4f}")
        print(f"  Timing DTW (normalized):   {result.timing_dtw.normalized_distance:.4f}")
        print(f"  Match Percentage:          {result.match_percentage:.1f}%")
        print(f"  Divergence Windows:        {len(result.divergence_windows)}")
        print(f"  Sequence A length:         {result.sequence_dtw.sequence_length_a}")
        print(f"  Sequence B length:         {result.sequence_dtw.sequence_length_b}")
        print(f"{'='*60}\n")
        
        if result.divergence_windows:
            print("Divergence Windows (where sequences differ):")
            for i, div in enumerate(result.divergence_windows, 1):
                print(f"  {i}. A[{div.start_index_a}:{div.end_index_a}] @ "
                      f"{div.start_time_delta_a:.2f}s - {div.end_time_delta_a:.2f}s")
                print(f"     B[{div.start_index_b}:{div.end_index_b}] @ "
                      f"{div.start_time_delta_b:.2f}s - {div.end_time_delta_b:.2f}s")
            print()
    
    return result


# =============================================================================
# Event Loading Utilities
# =============================================================================

def load_events(source: Union[pd.DataFrame, str, Path]) -> pd.DataFrame:
    """
    Load events from various sources.
    
    Supports:
    - pandas DataFrame (returned as-is)
    - CSV file path
    - Parquet file path
    - SQLite .db file (looks for 'Event' or 'events' table)
    
    Args:
        source: DataFrame or file path (csv, parquet, or .db sqlite)
    
    Returns:
        DataFrame with event data
        
    Example:
        >>> events = load_events('path/to/events.csv')
        >>> events = load_events('path/to/events.parquet')
        >>> events = load_events('path/to/asclog.db')
        >>> events = load_events(my_dataframe)
    """
    if isinstance(source, pd.DataFrame):
        return source.copy()
    
    path = Path(source)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    
    suffix = path.suffix.lower()
    
    if suffix == '.csv':
        return pd.read_csv(path)
    elif suffix == '.parquet':
        return pd.read_parquet(path)
    elif suffix == '.db':
        # SQLite database - try to find events table
        con = duckdb.connect()
        con.execute(f"ATTACH '{path}' AS source_db (TYPE SQLITE)")
        
        # Try common table names
        tables = con.execute("""
            SELECT name FROM source_db.sqlite_master 
            WHERE type='table'
        """).fetchall()
        table_names = [t[0].lower() for t in tables]
        
        if 'event' in table_names:
            # MAXTIME format
            df = con.execute("""
                SELECT
                    TO_TIMESTAMP(Timestamp + (Tick / 10))::TIMESTAMP AS timestamp,
                    EventTypeID AS event_id,
                    Parameter AS parameter
                FROM source_db.Event
                ORDER BY timestamp
            """).df()
        elif 'events' in table_names:
            df = con.execute("""
                SELECT * FROM source_db.events ORDER BY timestamp
            """).df()
        else:
            con.close()
            raise ValueError(f"No 'Event' or 'events' table found in {path}. Found tables: {[t[0] for t in tables]}")
        
        con.close()
        return df
    else:
        raise ValueError(f"Unsupported file type: {suffix}. Use .csv, .parquet, or .db")


# =============================================================================
# Timeline Generation using ATSPM
# =============================================================================

def generate_timeline(
    events: pd.DataFrame,
    device_id: str = "0",
    maxtime: bool = True
) -> pd.DataFrame:
    """
    Generate timeline events from raw hi-res data using the atspm package.
    
    Args:
        events: DataFrame with timestamp, event_id, parameter columns
        device_id: Device ID to use in the timeline
        maxtime: Include MAXTIME-specific events
    
    Returns:
        Timeline DataFrame with StartTime, EndTime, Duration, EventClass, EventValue, IsValid
    """
    if not HAS_ATSPM:
        raise ImportError(
            "The 'atspm' package is required for timeline generation. "
            "Install with: pip install atspm"
        )
    
    # Normalize column names for atspm
    df = events.copy()
    col_map = {}
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in ('timestamp', 'time_stamp'):
            col_map[col] = 'TimeStamp'
        elif col_lower in ('event_id', 'eventid', 'eventtypeid'):
            col_map[col] = 'EventId'
        elif col_lower in ('parameter', 'param'):
            col_map[col] = 'Parameter'
    
    df = df.rename(columns=col_map)
    
    # Add DeviceId if not present
    if 'DeviceId' not in df.columns:
        df['DeviceId'] = device_id
    
    # Ensure required columns exist
    required = ['TimeStamp', 'EventId', 'Parameter', 'DeviceId']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns for timeline: {missing}")
    
    # Ensure TimeStamp is datetime
    if not pd.api.types.is_datetime64_any_dtype(df['TimeStamp']):
        df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
    
    params = {
        'raw_data': df,
        'bin_size': 15,  # Required by atspm - 15 minute bins
        'aggregations': [
            {
                'name': 'has_data', 
                'params': {
                    'no_data_min': 1,           # Min minutes of data required per bin
                    'min_data_points': 1        # Min events required per sub-bin
                }
            },
            {
                'name': 'timeline',
                'params': {
                    'maxtime': maxtime,
                    'min_duration': 0,
                    'cushion_time': 60
                }
            },
        ]
    }
    
    with SignalDataProcessor(**params) as processor:
        processor.load()
        processor.aggregate()
        timeline = processor.conn.query("SELECT * FROM timeline ORDER BY StartTime").df()
    
    return timeline


def generate_simple_timeline(events: pd.DataFrame, device_id: str = "0") -> pd.DataFrame:
    """
    Generate a simple timeline from raw hi-res events without atspm.
    
    This is a fallback when atspm is slow or unavailable.
    Groups events into phase/overlap on/off pairs.
    
    Args:
        events: DataFrame with timestamp, event_id, parameter columns
        device_id: Device ID
    
    Returns:
        Timeline DataFrame with StartTime, EndTime, Duration, EventClass, EventValue
    """
    # Normalize column names
    df = events.copy()
    col_map = {}
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in ('timestamp', 'time_stamp'):
            col_map[col] = 'timestamp'
        elif col_lower in ('event_id', 'eventid', 'eventtypeid'):
            col_map[col] = 'event_id'
        elif col_lower in ('parameter', 'param'):
            col_map[col] = 'parameter'
    
    df = df.rename(columns=col_map)
    
    # Ensure timestamp is datetime
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    # Event mappings: start_event -> (end_event, EventClass)
    event_pairs = {
        1: (7, 'Green'),      # Phase On -> Phase Green Termination
        7: (8, 'Yellow'),     # Phase Green Term -> Phase Yellow
        8: (9, 'Red'),        # Phase Yellow -> Phase Red  
        61: (63, 'Overlap Green'),   # Overlap On -> Overlap Green Term
        63: (64, 'Overlap Yellow'),  # Overlap Green Term -> Overlap Yellow
        64: (65, 'Overlap Red'),     # Overlap Yellow -> Overlap Off
        105: (111, 'Preempt'),       # Preempt entry -> Preempt exit
    }
    
    timeline_rows = []
    
    # Track active events by (event_class, parameter)
    active_events = {}
    
    for _, row in df.iterrows():
        event_id = row['event_id']
        param = row['parameter']
        ts = row['timestamp']
        
        # Check if this is a start event
        if event_id in event_pairs:
            end_event, event_class = event_pairs[event_id]
            key = (event_class, param)
            active_events[key] = (ts, end_event)
        
        # Check if this ends any active event
        for key, (start_ts, end_event) in list(active_events.items()):
            event_class, p = key
            if event_id == end_event and param == p:
                duration = (ts - start_ts).total_seconds()
                timeline_rows.append({
                    'DeviceId': device_id,
                    'StartTime': start_ts,
                    'EndTime': ts,
                    'Duration': duration,
                    'EventClass': event_class,
                    'EventValue': param,
                    'IsValid': True
                })
                del active_events[key]
    
    if not timeline_rows:
        return pd.DataFrame(columns=['DeviceId', 'StartTime', 'EndTime', 'Duration', 'EventClass', 'EventValue', 'IsValid'])
    
    return pd.DataFrame(timeline_rows).sort_values('StartTime').reset_index(drop=True)


# =============================================================================
# Gantt Chart Visualization
# =============================================================================

# Color mapping for signal states
SIGNAL_COLORS = {
    'Green': '#00AA00',
    'Yellow': '#FFD700',
    'Red': '#CC0000',
    'Overlap Green': '#00CC00',
    'Overlap Trail Green': '#00FF00',
    'Overlap Yellow': '#FFFF00',
    'Overlap Red': '#FF0000',
}

# Phase/parameter color palette for other event types
PARAM_COLORS = [
    '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
    '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf',
    '#aec7e8', '#ffbb78', '#98df8a', '#ff9896', '#c5b0d5',
    '#c49c94'
]


def _get_event_color(event_class: str, event_value: int) -> str:
    """Get color for an event based on class and value."""
    if event_class in SIGNAL_COLORS:
        return SIGNAL_COLORS[event_class]
    # Use parameter-based color for other events
    return PARAM_COLORS[(event_value - 1) % len(PARAM_COLORS)]


def _get_sort_key(event_class: str, event_value: int) -> Tuple[int, int, int]:
    """
    Get sort key for ordering events in the Gantt chart.
    
    Order: Green/Yellow/Red by phase, then other events by class then value.
    """
    signal_order = {
        'Green': 0, 'Yellow': 1, 'Red': 2,
        'Overlap Green': 3, 'Overlap Trail Green': 4, 
        'Overlap Yellow': 5, 'Overlap Red': 6
    }
    
    if event_class in signal_order:
        # Group signal states - same phase on same row
        # Primary sort by value (phase number), secondary by signal state
        return (0, event_value, signal_order[event_class])
    else:
        # Other event types - sort by class name then value
        return (1, hash(event_class) % 1000, event_value)


def create_comparison_gantt_matplotlib(
    timeline_a: pd.DataFrame,
    timeline_b: pd.DataFrame,
    label_a: str = "Original",
    label_b: str = "Replay",
    title: str = "Signal Comparison",
    divergence_start: Optional[datetime] = None,
    divergence_end: Optional[datetime] = None,
    output_path: Optional[Union[str, Path]] = None,
    window_minutes: float = 5.0,
    event_classes: Optional[List[str]] = None,
    dpi: int = 150
) -> Optional[Any]:
    """
    Create a Gantt chart comparing two timelines using matplotlib (reliable PNG export).
    
    This function uses matplotlib's broken_barh for Gantt charts, which reliably
    exports to PNG without the kaleido issues that affect plotly on Windows.
    
    Args:
        timeline_a: Timeline DataFrame from first source
        timeline_b: Timeline DataFrame from second source  
        label_a: Label for first timeline (default "Original")
        label_b: Label for second timeline (default "Replay")
        title: Chart title
        divergence_start: Optional start time to center the view
        divergence_end: Optional end time to center the view
        output_path: Path to save the plot (PNG, PDF, SVG supported)
        window_minutes: Time window to show around divergence (default 5 min)
        event_classes: Optional list of event classes to include
        dpi: Resolution for PNG output (default 150)
    
    Returns:
        matplotlib Figure object, or None if matplotlib not available
    """
    if not HAS_MATPLOTLIB:
        warnings.warn("Matplotlib is required for this function. Install with: pip install matplotlib")
        return None
    
    # Default event classes for signal visualization
    if event_classes is None:
        event_classes = [
            'Green', 'Yellow', 'Red',
            'Overlap Green', 'Overlap Trail Green', 'Overlap Yellow', 'Overlap Red',
            'Preempt', 'Ped Service', 'Phase Call', 'Phase Hold',
            'TSP Service', 'TSP Call'
        ]
    
    # Filter timelines to relevant event classes
    df_a = timeline_a[timeline_a['EventClass'].isin(event_classes)].copy()
    df_b = timeline_b[timeline_b['EventClass'].isin(event_classes)].copy()
    
    if df_a.empty and df_b.empty:
        warnings.warn("No matching events found in either timeline")
        return None
    
    # Determine time window
    if divergence_start is not None:
        center_time = divergence_start
    else:
        min_src_a = df_a['StartTime'].min() if not df_a.empty else pd.Timestamp.max
        min_src_b = df_b['StartTime'].min() if not df_b.empty else pd.Timestamp.max
        center_time = min(min_src_a, min_src_b)
        if center_time == pd.Timestamp.max:
            return None
    
    # Calculate window
    window_start = center_time - timedelta(minutes=window_minutes / 2)
    window_end = (divergence_end or center_time) + timedelta(minutes=window_minutes / 2)
    
    # Filter to window
    df_a = df_a[(df_a['EndTime'] >= window_start) & (df_a['StartTime'] <= window_end)].copy()
    df_b = df_b[(df_b['EndTime'] >= window_start) & (df_b['StartTime'] <= window_end)].copy()
    
    # Calculate relative time
    zero_time = window_start
    
    def to_relative_seconds(ts):
        return (ts - zero_time).total_seconds()
    
    # Prepare data
    df_a['RelStart'] = df_a['StartTime'].apply(to_relative_seconds)
    df_a['RelEnd'] = df_a['EndTime'].apply(to_relative_seconds)
    df_a['Duration'] = df_a['RelEnd'] - df_a['RelStart']
    df_a['Source'] = label_a
    
    df_b['RelStart'] = df_b['StartTime'].apply(to_relative_seconds)
    df_b['RelEnd'] = df_b['EndTime'].apply(to_relative_seconds)
    df_b['Duration'] = df_b['RelEnd'] - df_b['RelStart']
    df_b['Source'] = label_b
    
    combined_df = pd.concat([df_a, df_b], ignore_index=True)
    
    if combined_df.empty:
        warnings.warn("No events in the time window")
        return None
    
    # Create row labels
    def create_base_label(event_class, event_value):
        if event_class in ['Green', 'Yellow', 'Red']:
            return f"Ph {int(event_value)}"
        elif event_class in ['Overlap Green', 'Overlap Trail Green', 'Overlap Yellow', 'Overlap Red']:
            return f"Ovlp {int(event_value)}"
        else:
            return f"{event_class} {int(event_value)}"
    
    combined_df['BaseLabel'] = [create_base_label(ec, ev) for ec, ev in 
                                 zip(combined_df['EventClass'], combined_df['EventValue'])]
    combined_df['SortKey'] = [_get_sort_key(ec, int(ev)) for ec, ev in 
                              zip(combined_df['EventClass'], combined_df['EventValue'])]
    combined_df['RowLabel'] = combined_df['BaseLabel'] + " (" + combined_df['Source'] + ")"
    combined_df['Color'] = [_get_event_color(ec, int(ev)) for ec, ev in 
                            zip(combined_df['EventClass'], combined_df['EventValue'])]
    
    # Build row order (interleaved: label_a above label_b for each phase)
    unique_base = combined_df[['BaseLabel', 'SortKey']].drop_duplicates().sort_values('SortKey')
    row_labels = []
    for _, row in unique_base.iterrows():
        base = row['BaseLabel']
        row_labels.append(f"{base} ({label_a})")
        row_labels.append(f"{base} ({label_b})")
    
    # Map row labels to y positions
    row_to_y = {label: i for i, label in enumerate(row_labels)}
    
    # Create figure
    fig_height = max(6, 0.4 * len(row_labels))
    fig, ax = plt.subplots(figsize=(14, fig_height))
    
    # Plot bars using broken_barh
    bar_height = 0.8
    for _, event in combined_df.iterrows():
        y_pos = row_to_y.get(event['RowLabel'], 0)
        color = event['Color']
        ax.broken_barh(
            [(event['RelStart'], event['Duration'])],
            (y_pos - bar_height/2, bar_height),
            facecolors=color,
            edgecolors='black',
            linewidth=0.5
        )
    
    # Add divergence line if specified
    if divergence_start is not None:
        div_x = to_relative_seconds(divergence_start)
        ax.axvline(x=div_x, color='red', linestyle='--', linewidth=2, label='Divergence')
    
    # Configure axes
    ax.set_yticks(range(len(row_labels)))
    ax.set_yticklabels(row_labels)
    ax.set_xlabel('Time (seconds from window start)')
    ax.set_title(title)
    ax.set_xlim(0, (window_end - window_start).total_seconds())
    ax.invert_yaxis()  # Put first row at top
    ax.grid(axis='x', alpha=0.3)
    
    # Add legend for signal colors
    legend_patches = [
        mpatches.Patch(color=SIGNAL_COLORS['Green'], label='Green'),
        mpatches.Patch(color=SIGNAL_COLORS['Yellow'], label='Yellow'),
        mpatches.Patch(color=SIGNAL_COLORS['Red'], label='Red'),
    ]
    ax.legend(handles=legend_patches, loc='upper right', fontsize=8)
    
    plt.tight_layout()
    
    # Save if output path specified
    if output_path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(str(output_path), dpi=dpi, bbox_inches='tight')
        print(f"Saved comparison chart to: {output_path}")
    
    return fig


# =============================================================================
# Manual Comparison with Visualization
# =============================================================================

def compare_and_visualize(
    events_a: Union[pd.DataFrame, str, Path],
    events_b: Union[pd.DataFrame, str, Path],
    label_a: str = "Original",
    label_b: str = "Replay",
    device_id: str = "device",
    output_dir: Optional[Union[str, Path]] = None,
    output_name: Optional[str] = None,
    event_ids: Optional[List[int]] = None,
    force_plot: bool = False,
    print_summary: bool = True,
    window_minutes: float = 5.0,
    # Simple threshold parameters (no class needed)
    match_threshold: float = 95.0,
    sequence_threshold: float = DEFAULT_SEQUENCE_THRESHOLD,
    timing_threshold: float = DEFAULT_TIMING_THRESHOLD,
    # Legacy support
    thresholds: Optional[ComparisonThresholds] = None,
) -> ComparisonResult:
    """
    Compare two event logs and optionally generate visualization.
    
    This is the main entry point for manual comparison. It:
    1. Loads events from various sources (DataFrame, CSV, Parquet, SQLite .db)
    2. Performs DTW comparison
    3. If thresholds are exceeded (or force_plot=True), generates Gantt charts
    
    Args:
        events_a: First event source (DataFrame, CSV, Parquet, or .db path)
        events_b: Second event source
        label_a: Label for first source (default "Original")
        label_b: Label for second source (default "Replay")
        device_id: Device identifier for comparison
        output_dir: Directory to save plots. If None, plots are not saved.
        output_name: Custom name for output file. Auto-generated if None.
        event_ids: Event IDs to include in comparison. None = defaults.
        force_plot: Generate plot even if thresholds not exceeded
        print_summary: Print comparison summary
        window_minutes: Time window around divergence for plots
        match_threshold: Minimum match percentage (default 95.0)
        sequence_threshold: Max sequence DTW distance (default 0.05). 
            Note: This is essentially (100 - match_threshold)/100
        timing_threshold: Max timing DTW distance (default 0.02)
        thresholds: Legacy ComparisonThresholds object (optional)
    
    Returns:
        ComparisonResult with all comparison metrics
    
    Example:
        >>> result = compare_and_visualize(
        ...     'original_log.db',
        ...     'replay_log.csv',
        ...     label_a="Original",
        ...     label_b="Replay",
        ...     output_dir='./comparison_plots',
        ...     match_threshold=90.0  # Alert if match drops below 90%
        ... )
    """
    # Load events
    df_a = load_events(events_a)
    df_b = load_events(events_b)
    
    # Use threshold object if provided, otherwise create from params
    if thresholds is None:
        thresholds = ComparisonThresholds(
            sequence_threshold=sequence_threshold,
            timing_threshold=timing_threshold,
            match_threshold=match_threshold
        )
    
    # Perform comparison
    result = compare_runs(
        events_a=df_a,
        events_b=df_b,
        device_id=device_id,
        run_a_label=label_a,
        run_b_label=label_b,
        event_ids=event_ids
    )
    
    # Add threshold info to result
    result.thresholds = thresholds
    result.exceeds_threshold, result.threshold_reason = thresholds.exceeds_threshold(
        result.sequence_dtw.normalized_distance,
        result.timing_dtw.normalized_distance,
        result.match_percentage
    )
    
    if print_summary:
        print(f"\n{'='*60}")
        print(f"Comparison: {label_a} vs {label_b}")
        print(f"{'='*60}")
        print(f"  Match Percentage:  {result.match_percentage:.1f}%  (threshold: ≥{thresholds.match_threshold}%)")
        print(f"    {'⚠️  BELOW THRESHOLD' if result.match_percentage < thresholds.match_threshold else '✓ OK'}")
        print(f"  Events in A: {result.sequence_dtw.sequence_length_a}")
        print(f"  Events in B: {result.sequence_dtw.sequence_length_b}")
        print(f"  Divergences: {len(result.divergence_windows)}")
        print(f"{'='*60}")
        
        if result.exceeds_threshold:
            print(f"\n⚠️  THRESHOLD EXCEEDED")
    
    # Determine if we should generate plots
    should_plot = (result.exceeds_threshold or force_plot) and output_dir is not None
    
    if should_plot:
        # Find divergence time from the first divergence window
            divergence_start = None
            divergence_end = None
            
            if result.divergence_windows:
                # Get prepared events to find actual timestamps
                df_a_prep = prepare_events_for_comparison(df_a, event_ids=event_ids)
                df_b_prep = prepare_events_for_comparison(df_b, event_ids=event_ids)
                
                if not df_a_prep.empty and not df_b_prep.empty:
                    first_div = result.divergence_windows[0]
                    
                    # Get actual timestamps
                    if first_div.start_index_a < len(df_a_prep):
                        divergence_start = df_a_prep.iloc[first_div.start_index_a]['timestamp']
                    if first_div.end_index_a < len(df_a_prep):
                        divergence_end = df_a_prep.iloc[first_div.end_index_a]['timestamp']
            
            # If no divergence found, use the middle of the data
            if divergence_start is None:
                df_a_prep = prepare_events_for_comparison(df_a, event_ids=event_ids)
                if not df_a_prep.empty:
                    mid_idx = len(df_a_prep) // 2
                    divergence_start = df_a_prep.iloc[mid_idx]['timestamp']
                    divergence_end = divergence_start
            
            try:
                # Use atspm timeline generator
                print("Generating timelines with atspm...")
                timeline_a = generate_timeline(df_a, device_id=device_id)
                timeline_b = generate_timeline(df_b, device_id=device_id)
                
                if timeline_a.empty or timeline_b.empty:
                    warnings.warn("No timeline events generated. Check that data contains phase/overlap events.")
                else:
                    # Generate output filename
                    if output_name is None:
                        output_name = f"comparison_{label_a}_vs_{label_b}".replace(' ', '_')
                    
                    # Default to .png if not specified
                    if not output_name.lower().endswith(('.html', '.png', '.jpeg', '.jpg', '.webp', '.pdf', '.svg')):
                        output_name += ".png"
                        
                    output_path = Path(output_dir) / output_name
                    
                    print(f"Creating Gantt chart with {len(timeline_a)} events (A) and {len(timeline_b)} events (B)...")
                    
                    # Use matplotlib for all outputs (reliable export, no kaleido issues)
                    if HAS_MATPLOTLIB:
                        # Ensure we use a supported static extension
                        if not output_path.suffix.lower() in ('.png', '.pdf', '.svg', '.jpg', '.jpeg'):
                            output_path = output_path.with_suffix('.png')

                        fig = create_comparison_gantt_matplotlib(
                            timeline_a=timeline_a,
                            timeline_b=timeline_b,
                            label_a=label_a,
                            label_b=label_b,
                            title=f"Comparison: {label_a} vs {label_b}",
                            divergence_start=divergence_start,
                            divergence_end=divergence_end,
                            output_path=output_path,
                            window_minutes=window_minutes
                        )
                        plt.close(fig)  # Clean up
                    else:
                        warnings.warn("matplotlib not installed. Install with: pip install matplotlib")
                    
                    result.plot_path = str(output_path)
                
            except Exception as e:
                import traceback
                warnings.warn(f"Failed to generate visualization: {e}")
                traceback.print_exc()
    
    return result


# =============================================================================
# Database Storage for Comparison Results
# =============================================================================

def store_comparison_result(
    db_path: str,
    result: ComparisonResult
) -> None:
    """
    Store comparison result in the database.
    
    Creates comparison_results table if it doesn't exist.
    """
    con = duckdb.connect(db_path)
    
    # Create table if not exists
    con.execute("""
        CREATE TABLE IF NOT EXISTS comparison_results (
            device_id VARCHAR,
            run_a VARCHAR,
            run_b VARCHAR,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            sequence_dtw_distance DOUBLE,
            sequence_dtw_normalized DOUBLE,
            timing_dtw_distance DOUBLE,
            timing_dtw_normalized DOUBLE,
            match_percentage DOUBLE,
            num_divergences INTEGER,
            sequence_threshold DOUBLE,
            timing_threshold DOUBLE,
            match_threshold DOUBLE,
            exceeds_threshold BOOLEAN,
            threshold_reason VARCHAR,
            plot_path VARCHAR
        )
    """)
    
    # Insert result
    con.execute("""
        INSERT INTO comparison_results (
            device_id, run_a, run_b,
            sequence_dtw_distance, sequence_dtw_normalized,
            timing_dtw_distance, timing_dtw_normalized,
            match_percentage, num_divergences,
            sequence_threshold, timing_threshold, match_threshold,
            exceeds_threshold, threshold_reason, plot_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        result.device_id,
        str(result.run_a),
        str(result.run_b),
        result.sequence_dtw.distance,
        result.sequence_dtw.normalized_distance,
        result.timing_dtw.distance,
        result.timing_dtw.normalized_distance,
        result.match_percentage,
        len(result.divergence_windows),
        result.thresholds.sequence_threshold if result.thresholds else None,
        result.thresholds.timing_threshold if result.thresholds else None,
        result.thresholds.match_threshold if result.thresholds else None,
        result.exceeds_threshold,
        result.threshold_reason,
        result.plot_path
    ])
    
    con.close()
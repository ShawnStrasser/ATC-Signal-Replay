"""
DTW-based comparison module for analyzing hi-res event sequences.
"""

import pandas as pd
import numpy as np
from typing import List, Tuple, Dict, Optional, Union
from datetime import datetime
from dataclasses import dataclass

try:
    from dtaidistance import dtw
    HAS_DTAIDISTANCE = True
except ImportError:
    HAS_DTAIDISTANCE = False


# Event IDs to include in comparison
COMPARISON_EVENT_IDS = [
    1, 7, 8, 9, 10, 11, 21, 22, 23, 32, 33, 55, 56,
    61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72,
    105, 106, 107, 111, 112, 113, 114, 115, 118, 119
]


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
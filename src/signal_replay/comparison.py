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
    Encode (event_id, parameter) pairs as normalized numerical sequence.
    
    Args:
        df: DataFrame with event_id and parameter columns
        encoding_map: Optional existing encoding map to use for consistency
    
    Returns:
        Tuple of (encoded array normalized to [0,1], encoding map)
    """
    if df.empty:
        return np.array([]), {}
    
    # Create (event_id, parameter) pairs
    pairs = list(zip(df['event_id'].values, df['parameter'].values))
    
    # Build or use encoding map
    if encoding_map is None:
        unique_pairs = sorted(set(pairs))
        encoding_map = {pair: i for i, pair in enumerate(unique_pairs)}
    
    # Encode pairs
    max_val = max(encoding_map.values()) if encoding_map else 0
    encoded = np.array([encoding_map.get(p, max_val + 1) for p in pairs], dtype=float)
    
    # Normalize to [0, 1]
    if len(encoded) > 0 and encoded.max() > 0:
        encoded = encoded / encoded.max()
    
    return encoded, encoding_map


def compute_dtw(
    seq_a: np.ndarray,
    seq_b: np.ndarray
) -> DTWResult:
    """
    Compute DTW distance and warping path between two sequences.
    
    Args:
        seq_a: First sequence
        seq_b: Second sequence
    
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
    
    # Compute DTW distance and path
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
    
    # Compute DTW for sequences
    sequence_dtw = compute_dtw(seq_a, seq_b)
    
    # Compute DTW for timing
    timing_dtw = compute_dtw(time_a_norm, time_b_norm)
    
    # Find divergence windows (use sequence path as primary)
    divergences = find_divergence_windows(
        sequence_dtw.warping_path,
        time_a,
        time_b
    )
    
    # Calculate match percentage based on warping path
    # Perfect match = diagonal path, length = max(len_a, len_b)
    # Actual path length vs diagonal gives mismatch
    max_len = max(len(seq_a), len(seq_b))
    if max_len > 0 and sequence_dtw.warping_path:
        # Count how many path steps are diagonal (i+1, j+1)
        diagonal_steps = 0
        for k in range(1, len(sequence_dtw.warping_path)):
            prev_i, prev_j = sequence_dtw.warping_path[k - 1]
            curr_i, curr_j = sequence_dtw.warping_path[k]
            if curr_i == prev_i + 1 and curr_j == prev_j + 1:
                diagonal_steps += 1
        
        match_percentage = (diagonal_steps / max_len) * 100
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

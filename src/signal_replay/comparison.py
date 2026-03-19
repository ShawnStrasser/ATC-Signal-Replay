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

from dtaidistance import dtw
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.colors import to_rgba
from atspm import SignalDataProcessor


# Event IDs to include in comparison
COMPARISON_EVENT_IDS = [
    1, 7, 9, 11, 21, 22, 23, 32, 33, 55, 56,
    61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72,
    105, 106, 107, 111, 112, 113, 114, 115, 118, 119
]
# NOTE: Events 8 (Yellow Start) and 10 (Red Clear Start) removed — they always
# co-occur at the same timestamp as 7 (Green End) and 9 (Yellow End) respectively,
# so they dilute Jaccard distance without adding information.

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
    """A window where two sequences diverge significantly.
    
    Time fields:
        start_time_delta_a/b: Seconds from the ALIGNED start (after trimming)
        original_start_seconds_a/b: Seconds from the ORIGINAL run start (before trimming)
    """
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
    # Human-readable description of the divergence
    description: str = ""
    # Seconds from original run start (before alignment trimming)
    original_start_seconds_a: float = 0.0
    original_end_seconds_a: float = 0.0
    original_start_seconds_b: float = 0.0
    original_end_seconds_b: float = 0.0


@dataclass
class ChunkScore:
    """Match score for a single rolling window chunk."""
    center_seconds: float     # Center of the window in seconds from analysis start
    match_percentage: float   # DTW match % for this chunk (interior only)
    window_seconds: float     # Window duration used


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
    # Alignment offset applied (positive = skipped groups from B, negative = from A)
    alignment_offset: int = 0
    # Optional: threshold info for storage
    thresholds: Optional[ComparisonThresholds] = None
    exceeds_threshold: bool = False
    threshold_reason: str = ""
    # Optional: plot path if generated
    plot_path: Optional[str] = None
    # Timing analysis: stats on time differences between matched event groups
    timing_stats: Optional[Dict] = None
    # How much time was trimmed from each sequence during alignment
    alignment_trim_seconds_a: float = 0.0
    alignment_trim_seconds_b: float = 0.0
    # Rolling chunk scores for time-localised diagnostics
    chunk_scores: List[ChunkScore] = field(default_factory=list)
    # Temporal shift applied during alignment (seconds added to B's time_delta)
    temporal_shift_seconds: float = 0.0
    
    def format_summary(self) -> str:
        """Return a human-readable summary of the comparison."""
        lines = []
        lines.append(f"Match: {self.match_percentage:.1f}%")
        
        if self.divergence_windows:
            lines.append(f"Divergences: {len(self.divergence_windows)}")
            for i, div in enumerate(self.divergence_windows, 1):
                if div.description:
                    lines.append(f"  {i}. {div.description}")
                else:
                    m1, s1 = divmod(int(round(div.original_start_seconds_a)), 60)
                    m2, s2 = divmod(int(round(div.original_end_seconds_a)), 60)
                    lines.append(f"  {i}. {m1}:{s1:02d}\u2013{m2}:{s2:02d}")
        else:
            lines.append("Divergences: 0")
        
        if self.timing_stats:
            ts = self.timing_stats
            residual = ts.get('alignment_residual', 0.0)
            lines.append(f"Timing: jitter std={ts['std_diff']:.3f}s, "
                         f"max={ts['max_abs_diff']:.3f}s, "
                         f"95th pctl={ts['p95_abs_diff']:.3f}s"
                         + (f" (alignment residual={residual:+.1f}s)" if abs(residual) > 0.05 else ""))
        
        return "\n".join(lines)


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
    
    # Sort by timestamp, then by event_id and parameter for consistent ordering
    # Events at the same timestamp can appear in different order between runs,
    # so sorting by all three columns ensures DTW sees them in the same order.
    df = df.sort_values(['timestamp', 'event_id', 'parameter']).reset_index(drop=True)
    
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
    seq_a: np.ndarray,
    seq_b: np.ndarray,
    time_deltas_a: np.ndarray,
    time_deltas_b: np.ndarray,
    jump_threshold: int = 3,
    mismatch_window: int = 5
) -> List[DivergenceWindow]:
    """
    Find windows where sequences diverge based on the warping path.
    
    A divergence is detected in TWO ways:
    1. **Structural divergence**: Large jumps in the warping path, indicating
       one sequence has events not present in the other (insertions/deletions).
    2. **Value divergence**: A window of consecutive mismatches along the path,
       indicating the sequences have different event types at aligned positions.
    
    Args:
        warping_path: DTW warping path as list of (i, j) tuples
        seq_a: Encoded sequence A (for checking value matches)
        seq_b: Encoded sequence B
        time_deltas_a: Time deltas for sequence A
        time_deltas_b: Time deltas for sequence B
        jump_threshold: Minimum jump size to consider a structural divergence
        mismatch_window: Number of consecutive mismatches to trigger value divergence
    
    Returns:
        List of DivergenceWindow objects, sorted by start position
    """
    if not warping_path or len(warping_path) < 2:
        return []
    
    divergences = []
    
    # === Method 1: Structural divergence (jumps in path) ===
    in_structural_div = False
    structural_start = None
    
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
        
        if is_divergent and not in_structural_div:
            in_structural_div = True
            structural_start = (prev_i, prev_j, k - 1)
        elif not is_divergent and in_structural_div:
            in_structural_div = False
            start_i, start_j, _ = structural_start
            
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
    
    # Handle structural divergence at end
    if in_structural_div and structural_start:
        start_i, start_j, _ = structural_start
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
    
    # === Method 2: Value divergence (consecutive mismatches) ===
    # Track runs of mismatches along the path
    mismatch_run_start = None
    mismatch_count = 0
    
    for k, (i, j) in enumerate(warping_path):
        is_match = seq_a[i] == seq_b[j]
        
        if not is_match:
            if mismatch_run_start is None:
                mismatch_run_start = k
            mismatch_count += 1
        else:
            # End of mismatch run - check if it was long enough
            if mismatch_count >= mismatch_window:
                start_i, start_j = warping_path[mismatch_run_start]
                end_i, end_j = warping_path[k - 1]
                
                divergences.append(DivergenceWindow(
                    start_index_a=start_i,
                    end_index_a=end_i,
                    start_index_b=start_j,
                    end_index_b=end_j,
                    start_time_delta_a=float(time_deltas_a[start_i]) if start_i < len(time_deltas_a) else 0.0,
                    end_time_delta_a=float(time_deltas_a[end_i]) if end_i < len(time_deltas_a) else 0.0,
                    start_time_delta_b=float(time_deltas_b[start_j]) if start_j < len(time_deltas_b) else 0.0,
                    end_time_delta_b=float(time_deltas_b[end_j]) if end_j < len(time_deltas_b) else 0.0,
                ))
            
            mismatch_run_start = None
            mismatch_count = 0
    
    # Handle mismatch run at end of path
    if mismatch_count >= mismatch_window and mismatch_run_start is not None:
        start_i, start_j = warping_path[mismatch_run_start]
        end_i, end_j = warping_path[-1]
        
        divergences.append(DivergenceWindow(
            start_index_a=start_i,
            end_index_a=end_i,
            start_index_b=start_j,
            end_index_b=end_j,
            start_time_delta_a=float(time_deltas_a[start_i]) if start_i < len(time_deltas_a) else 0.0,
            end_time_delta_a=float(time_deltas_a[end_i]) if end_i < len(time_deltas_a) else 0.0,
            start_time_delta_b=float(time_deltas_b[start_j]) if start_j < len(time_deltas_b) else 0.0,
            end_time_delta_b=float(time_deltas_b[end_j]) if end_j < len(time_deltas_b) else 0.0,
        ))
    
    # Sort by start position in sequence A and merge overlapping windows
    if divergences:
        divergences.sort(key=lambda d: d.start_index_a)
        
        # Merge overlapping windows
        merged = [divergences[0]]
        for div in divergences[1:]:
            last = merged[-1]
            if div.start_index_a <= last.end_index_a + 1:
                # Overlapping or adjacent - merge
                merged[-1] = DivergenceWindow(
                    start_index_a=last.start_index_a,
                    end_index_a=max(last.end_index_a, div.end_index_a),
                    start_index_b=last.start_index_b,
                    end_index_b=max(last.end_index_b, div.end_index_b),
                    start_time_delta_a=last.start_time_delta_a,
                    end_time_delta_a=max(last.end_time_delta_a, div.end_time_delta_a),
                    start_time_delta_b=last.start_time_delta_b,
                    end_time_delta_b=max(last.end_time_delta_b, div.end_time_delta_b),
                )
            else:
                merged.append(div)
        divergences = merged
    
    return divergences


def _group_events_by_timestamp(df: pd.DataFrame) -> List[Tuple[float, frozenset]]:
    """
    Group events by timestamp into sets of (event_id, parameter) tuples.
    
    Events at the same timestamp may appear in different order between runs.
    Grouping them into sets makes comparison order-independent.
    
    Returns:
        List of (time_delta, frozenset_of_event_tuples)
    """
    groups = []
    for td, grp in df.groupby('time_delta'):
        events = frozenset(zip(grp['event_id'].values, grp['parameter'].values))
        groups.append((float(td), events))
    return groups


def _dtw_with_jaccard(
    groups_a: List[Tuple[float, frozenset]],
    groups_b: List[Tuple[float, frozenset]]
) -> Tuple[DTWResult, float, np.ndarray]:
    """
    Run DTW on timestamp groups using Jaccard distance.
    
    Each group is a frozenset of (event_id, parameter) tuples.
    Distance between groups: 0 if identical, Jaccard distance otherwise.
    
    Returns:
        Tuple of (DTWResult, match_percentage, distance_matrix)
    """
    n, m = len(groups_a), len(groups_b)
    
    if n == 0 or m == 0:
        return DTWResult(
            distance=float('inf'),
            normalized_distance=float('inf'),
            warping_path=[],
            sequence_length_a=n,
            sequence_length_b=m
        ), 0.0, np.array([])
    
    # Build Jaccard distance matrix
    dist_matrix = np.ones((n, m), dtype=np.float64)
    for i in range(n):
        set_a = groups_a[i][1]
        for j in range(m):
            set_b = groups_b[j][1]
            if set_a == set_b:
                dist_matrix[i, j] = 0.0
            else:
                inter = len(set_a & set_b)
                union = len(set_a | set_b)
                dist_matrix[i, j] = 1.0 - inter / union if union > 0 else 1.0
    
    # DTW with custom distance matrix
    cum_cost = np.full((n + 1, m + 1), np.inf)
    cum_cost[0, 0] = 0.0
    for i in range(1, n + 1):
        for j in range(1, m + 1):
            cost = dist_matrix[i - 1, j - 1]
            cum_cost[i, j] = cost + min(
                cum_cost[i - 1, j],
                cum_cost[i, j - 1],
                cum_cost[i - 1, j - 1]
            )
    
    distance = float(cum_cost[n, m])
    
    # Backtrack to find warping path
    path = []
    i, j = n, m
    while i > 0 or j > 0:
        path.append((i - 1, j - 1))
        if i == 0:
            j -= 1
        elif j == 0:
            i -= 1
        else:
            candidates = [
                (cum_cost[i - 1, j - 1], i - 1, j - 1),
                (cum_cost[i - 1, j], i - 1, j),
                (cum_cost[i, j - 1], i, j - 1),
            ]
            _, i, j = min(candidates, key=lambda x: x[0])
    path.reverse()
    
    normalized_distance = distance / len(path) if path else float('inf')
    matches = sum(1 for i, j in path if dist_matrix[i, j] == 0.0)
    match_pct = matches / len(path) * 100 if path else 0.0
    
    dtw_result = DTWResult(
        distance=distance,
        normalized_distance=normalized_distance,
        warping_path=path,
        sequence_length_a=n,
        sequence_length_b=m
    )
    
    return dtw_result, match_pct, dist_matrix


def _fmt_mmss(seconds: float) -> str:
    """Format seconds as M:SS for chart-consistent display."""
    m, s = divmod(int(round(seconds)), 60)
    return f"{m}:{s:02d}"


def _find_group_divergences(
    path: List[Tuple[int, int]],
    dist_matrix: np.ndarray,
    groups_a: List[Tuple[float, frozenset]],
    groups_b: List[Tuple[float, frozenset]],
    trim_seconds_a: float = 0.0,
    trim_seconds_b: float = 0.0,
    skip_threshold: int = 3,
    mismatch_window: int = 5,
    alignment_grace_seconds: float = 30.0
) -> List[DivergenceWindow]:
    """
    Find divergence windows from group-level DTW path.
    
    Detects two types of divergence:
    1. **Runs**: Consecutive horizontal/vertical DTW steps where one sequence
       advances while the other stalls (direct gap/insertion evidence). Reports
       the actual time gap from the stalled sequence's timeline.
    2. **Mismatches**: Consecutive groups that don't match (different events).
    
    Divergences in the first ``alignment_grace_seconds`` after alignment are
    suppressed — these are typically residual alignment boundary effects.
    
    Args:
        path: DTW warping path (group index pairs)
        dist_matrix: Jaccard distance matrix between groups
        groups_a/b: Timestamp groups
        trim_seconds_a/b: How much time was trimmed during alignment
        skip_threshold: Min consecutive run steps to detect as structural gap
        mismatch_window: Min consecutive mismatches for value divergence
        alignment_grace_seconds: Ignore divergences in the first N seconds
    
    Returns:
        List of DivergenceWindow with descriptions and original timestamps
    """
    if not path or len(path) < 2:
        return []
    
    divergences = []
    
    def _time_a(idx):
        return groups_a[idx][0] if idx < len(groups_a) else 0.0
    
    def _time_b(idx):
        return groups_b[idx][0] if idx < len(groups_b) else 0.0
    
    def _make_div(start_i, end_i, start_j, end_j, description=""):
        return DivergenceWindow(
            start_index_a=start_i,
            end_index_a=end_i,
            start_index_b=start_j,
            end_index_b=end_j,
            start_time_delta_a=_time_a(start_i),
            end_time_delta_a=_time_a(end_i),
            start_time_delta_b=_time_b(start_j),
            end_time_delta_b=_time_b(end_j),
            description=description,
            original_start_seconds_a=_time_a(start_i) + trim_seconds_a,
            original_end_seconds_a=_time_a(end_i) + trim_seconds_a,
            original_start_seconds_b=_time_b(start_j) + trim_seconds_b,
            original_end_seconds_b=_time_b(end_j) + trim_seconds_b,
        )
    
    # === Method 1: Structural divergence (runs of horizontal/vertical steps) ===
    # Standard DTW only steps by 0 or 1 per dimension, so we detect RUNS of
    # horizontal steps (A advances, B stalls) or vertical steps (B advances, A stalls).
    # A run of N such steps means N groups in one sequence have no match.
    run_start_k = 0
    run_type = None  # 'h' = horizontal (A advances, B stalls), 'v' = vertical, 'd' = diagonal
    
    def _classify_step(k):
        pi, pj = path[k - 1]
        ci, cj = path[k]
        di, dj = ci - pi, cj - pj
        if di >= 1 and dj == 0:
            return 'h'
        elif di == 0 and dj >= 1:
            return 'v'
        return 'd'
    
    def _emit_run(start_k, end_k, rtype):
        """Create a divergence for a run of horizontal or vertical steps.
        
        When a gap is detected, the DivergenceWindow's original_*_seconds fields
        are expanded to span the full gap duration (not just the boundary groups).
        This ensures the Gantt chart shading covers the entire missing region.
        """
        run_len = end_k - start_k
        if run_len < skip_threshold:
            return
        si, sj = path[start_k]
        ei, ej = path[end_k - 1]  # Last step in the run
        orig_a_s = _time_a(si) + trim_seconds_a
        orig_a_e = _time_a(ei) + trim_seconds_a
        orig_b_s = _time_b(sj) + trim_seconds_b
        orig_b_e = _time_b(ej) + trim_seconds_b
        if rtype == 'h':
            # A advances while B stalls → gap in Replay data
            b_stall_idx = sj
            if b_stall_idx + 1 < len(groups_b):
                b_gap = _time_b(b_stall_idx + 1) - _time_b(b_stall_idx)
            else:
                b_gap = _time_a(ei) - _time_a(si)
            desc = (f"~{b_gap:.0f}s gap in Replay at "
                    f"{_fmt_mmss(orig_a_s)}\u2013{_fmt_mmss(orig_a_s + b_gap)} "
                    f"({run_len} unmatched groups)")
            orig_a_e_expanded = orig_a_s + b_gap
        else:
            # B advances while A stalls → gap in Original data
            a_stall_idx = si
            if a_stall_idx + 1 < len(groups_a):
                a_gap = _time_a(a_stall_idx + 1) - _time_a(a_stall_idx)
            else:
                a_gap = _time_b(ej) - _time_b(sj)
            desc = (f"~{a_gap:.0f}s gap in Original at "
                    f"{_fmt_mmss(orig_b_s)}\u2013{_fmt_mmss(orig_b_s + a_gap)} "
                    f"({run_len} unmatched groups)")
            orig_a_e_expanded = orig_a_s + a_gap
        
        div = DivergenceWindow(
            start_index_a=si,
            end_index_a=ei,
            start_index_b=sj,
            end_index_b=ej,
            start_time_delta_a=_time_a(si),
            end_time_delta_a=_time_a(ei),
            start_time_delta_b=_time_b(sj),
            end_time_delta_b=_time_b(ej),
            description=desc,
            original_start_seconds_a=orig_a_s,
            original_end_seconds_a=max(orig_a_e, orig_a_e_expanded),
            original_start_seconds_b=orig_b_s,
            original_end_seconds_b=orig_b_e,
        )
        divergences.append(div)
    
    if len(path) > 1:
        run_type = _classify_step(1)
        run_start_k = 1
        for k in range(2, len(path)):
            step = _classify_step(k)
            if step != run_type:
                if run_type in ('h', 'v'):
                    _emit_run(run_start_k, k, run_type)
                run_start_k = k
                run_type = step
        # Final run
        if run_type in ('h', 'v'):
            _emit_run(run_start_k, len(path), run_type)
    
    # === Method 2: Value divergence (consecutive mismatches) ===
    mismatch_start_k = None
    mismatch_count = 0
    
    for k, (i, j) in enumerate(path):
        is_match = dist_matrix[i, j] == 0.0
        
        if not is_match:
            if mismatch_start_k is None:
                mismatch_start_k = k
            mismatch_count += 1
        else:
            if mismatch_count >= mismatch_window:
                si, sj = path[mismatch_start_k]
                ei, ej = path[k - 1]
                orig_a_start = _time_a(si) + trim_seconds_a
                orig_a_end = _time_a(ei) + trim_seconds_a
                desc = (f"Event difference: {mismatch_count} mismatched groups "
                        f"at {_fmt_mmss(orig_a_start)}\u2013{_fmt_mmss(orig_a_end)}")
                divergences.append(_make_div(si, ei, sj, ej, desc))
            mismatch_start_k = None
            mismatch_count = 0
    
    # Handle mismatch at end
    if mismatch_count >= mismatch_window and mismatch_start_k is not None:
        si, sj = path[mismatch_start_k]
        ei, ej = path[-1]
        orig_a_start = _time_a(si) + trim_seconds_a
        orig_a_end = _time_a(ei) + trim_seconds_a
        desc = (f"Event difference: {mismatch_count} mismatched groups "
                f"at {_fmt_mmss(orig_a_start)}\u2013{_fmt_mmss(orig_a_end)}")
        divergences.append(_make_div(si, ei, sj, ej, desc))
    
    # Sort and merge overlapping
    if divergences:
        divergences.sort(key=lambda d: d.start_index_a)
        merged = [divergences[0]]
        for div in divergences[1:]:
            last = merged[-1]
            if div.start_index_a <= last.end_index_a + 1:
                # Merge: prefer gap descriptions over mismatch descriptions
                # (gap descriptions include the actual time gap from the timeline)
                all_descs = [last.description, div.description]
                gap_descs = [d for d in all_descs if d and 'gap in' in d]
                best_desc = gap_descs[0] if gap_descs else (last.description or div.description)
                merged[-1] = DivergenceWindow(
                    start_index_a=last.start_index_a,
                    end_index_a=max(last.end_index_a, div.end_index_a),
                    start_index_b=last.start_index_b,
                    end_index_b=max(last.end_index_b, div.end_index_b),
                    start_time_delta_a=last.start_time_delta_a,
                    end_time_delta_a=max(last.end_time_delta_a, div.end_time_delta_a),
                    start_time_delta_b=last.start_time_delta_b,
                    end_time_delta_b=max(last.end_time_delta_b, div.end_time_delta_b),
                    description=best_desc,
                    original_start_seconds_a=last.original_start_seconds_a,
                    original_end_seconds_a=max(last.original_end_seconds_a, div.original_end_seconds_a),
                    original_start_seconds_b=last.original_start_seconds_b,
                    original_end_seconds_b=max(last.original_end_seconds_b, div.original_end_seconds_b),
                )
            else:
                merged.append(div)
        divergences = merged
    
    # Filter out divergences in the alignment grace period
    # The first few seconds after alignment often have residual mismatches
    if alignment_grace_seconds > 0:
        divergences = [
            d for d in divergences
            if d.start_time_delta_a >= alignment_grace_seconds
        ]
    
    return divergences


def _analyze_timing(
    path: List[Tuple[int, int]],
    dist_matrix: np.ndarray,
    groups_a: List[Tuple[float, frozenset]],
    groups_b: List[Tuple[float, frozenset]],
) -> Optional[Dict]:
    """
    Analyze timing differences between matched timestamp groups.
    
    For each pair in the warping path where the event sets match,
    compute the time difference. This reveals how much the timing
    varies between runs even when the event sequence is identical.
    
    Returns:
        Dict with timing statistics, or None if insufficient matches
    """
    timing_diffs = []
    for i, j in path:
        if dist_matrix[i, j] == 0.0:  # Exact set match
            t_a = groups_a[i][0]
            t_b = groups_b[j][0]
            timing_diffs.append(t_a - t_b)
    
    if len(timing_diffs) < 3:
        return None
    
    raw_diffs = np.array(timing_diffs)
    
    # The raw diffs include a constant offset from alignment granularity
    # (e.g., alignment trims to the nearest group boundary, leaving ~1s residual).
    # Subtract the median to isolate the actual timing jitter/variation.
    median_offset = float(np.median(raw_diffs))
    diffs = raw_diffs - median_offset
    abs_diffs = np.abs(diffs)
    
    return {
        'alignment_residual': median_offset,  # Systematic offset after alignment
        'mean_diff': float(np.mean(diffs)),   # Mean jitter (should be ~0)
        'std_diff': float(np.std(diffs)),      # Timing jitter std deviation
        'max_abs_diff': float(np.max(abs_diffs)),  # Worst-case jitter
        'median_diff': float(np.median(diffs)),  # Should be ~0 after correction
        'p95_abs_diff': float(np.percentile(abs_diffs, 95)),
        'n_matched_groups': len(timing_diffs),
        'n_total_path_steps': len(path),
    }


def find_alignment_offset(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    max_offset: int = 50,
    align_seconds: float = 360.0
) -> Tuple[int, float]:
    """
    Find the optimal offset to align sequence B with sequence A.
    
    Sequences often start at different points in the signal cycle (e.g., A starts
    with a "begin green" while B starts mid-cycle with an "end green"). This
    function finds the best alignment by:
    
    1. Grouping events by timestamp into SETS (order-independent matching)
    2. Trying different offsets (trimming the start of A or B)
    3. Comparing only the first `align_seconds` worth of data
    
    The set-based approach handles the common case where events at the same
    timestamp appear in different order between runs.
    
    Args:
        df_a: Prepared events DataFrame A (with event_id, parameter, time_delta)
        df_b: Prepared events DataFrame B
        max_offset: Maximum number of timestamp groups to try shifting
        align_seconds: Only use the first N seconds for alignment (default 360 = 6 min)
    
    Returns:
        Tuple of (best_offset_groups, match_percentage_at_offset)
        - If offset > 0: Skip first `offset` timestamp groups from B
        - If offset < 0: Skip first `-offset` timestamp groups from A
        The offset is in timestamp GROUPS (not individual events).
    """
    if df_a.empty or df_b.empty:
        return 0, 0.0
    
    # Group events by timestamp into sets (order-independent)
    groups_a = _group_events_by_timestamp(df_a)
    groups_b = _group_events_by_timestamp(df_b)
    
    sets_a = [events for _, events in groups_a]
    sets_b = [events for _, events in groups_b]
    times_a = [t for t, _ in groups_a]
    times_b = [t for t, _ in groups_b]
    
    # Find how many groups fit in align_seconds for each sequence
    max_groups_a = next((i for i, t in enumerate(times_a) if t > align_seconds), len(times_a))
    max_groups_b = next((i for i, t in enumerate(times_b) if t > align_seconds), len(times_b))
    max_compare = max(max_groups_a, max_groups_b)
    
    best_offset = 0
    best_match = 0.0
    
    def _jaccard_score(a_sub, b_sub, compare_len):
        """Compute average Jaccard similarity over the comparison window."""
        total_sim = 0.0
        for i in range(compare_len):
            if a_sub[i] == b_sub[i]:
                total_sim += 1.0
            else:
                inter = len(a_sub[i] & b_sub[i])
                union = len(a_sub[i] | b_sub[i])
                total_sim += inter / union if union > 0 else 0.0
        return total_sim / compare_len * 100
    
    # Small penalty per offset group to prefer smaller offsets when scores are close.
    # This prevents spurious alignment shifts from minor data perturbations.
    # Penalty of 0.15% per group means an offset of 10 costs 1.5% — significant
    # enough to prevent drift from small perturbations, but small enough that
    # genuinely different start positions still get found (a true match at offset 
    # 10 that scores 5%+ higher will still win).
    OFFSET_PENALTY_PER_GROUP = 0.15
    
    # Try positive offsets (skip timestamp groups from B)
    for offset in range(0, min(max_offset, len(sets_b))):
        a_sub = sets_a
        b_sub = sets_b[offset:]
        compare_len = min(len(a_sub), len(b_sub), max_compare)
        if compare_len < 3:
            continue
        
        match_pct = _jaccard_score(a_sub, b_sub, compare_len) - offset * OFFSET_PENALTY_PER_GROUP
        
        if match_pct > best_match:
            best_match = match_pct
            best_offset = offset
    
    # Try negative offsets (skip timestamp groups from A)
    for offset in range(1, min(max_offset, len(sets_a))):
        a_sub = sets_a[offset:]
        b_sub = sets_b
        compare_len = min(len(a_sub), len(b_sub), max_compare)
        if compare_len < 3:
            continue
        
        match_pct = _jaccard_score(a_sub, b_sub, compare_len) - offset * OFFSET_PENALTY_PER_GROUP
        
        if match_pct > best_match:
            best_match = match_pct
            best_offset = -offset
    
    return best_offset, best_match


def calculate_timeline_offset(
    events_a: pd.DataFrame,
    events_b: pd.DataFrame, 
    alignment_offset: int = 0,
    event_ids: Optional[List[int]] = None
) -> float:
    """
    Calculate the time offset (in seconds) needed to align timeline B with A.
    
    Uses the alignment offset (in timestamp groups) found by find_alignment_offset
    to determine the time difference between the starts of the aligned sequences.
    
    The offset tells us which timestamp group in B corresponds to the start of A.
    We compute the time difference between A's start and that group in B.
    
    Args:
        events_a: Original events DataFrame
        events_b: Replay events DataFrame
        alignment_offset: Offset in timestamp groups (from find_alignment_offset)
        event_ids: Optional list of event IDs to filter
    
    Returns:
        Time offset in seconds to apply to timeline B (negative = shift B left/earlier)
    """
    df_a = prepare_events_for_comparison(events_a, event_ids=event_ids)
    df_b = prepare_events_for_comparison(events_b, event_ids=event_ids)
    
    if df_a.empty or df_b.empty:
        return 0.0
    
    # Group by timestamp to get timestamp groups (same as find_alignment_offset uses)
    groups_a = _group_events_by_timestamp(df_a)
    groups_b = _group_events_by_timestamp(df_b)
    
    if alignment_offset > 0:
        # Skip first N timestamp groups from B
        # The Nth group in B corresponds to the start of A
        if alignment_offset < len(groups_b):
            time_b_at_offset = groups_b[alignment_offset][0]  # time_delta of that group
            # B needs to be shifted left by this amount so B[offset] aligns with A[0]
            return -time_b_at_offset
    elif alignment_offset < 0:
        # Skip first N timestamp groups from A
        skip = -alignment_offset
        if skip < len(groups_a):
            time_a_at_offset = groups_a[skip][0]
            # A starts later, so B needs to be shifted right
            return time_a_at_offset
    
    return 0.0


def compute_timeline_offset(
    timeline_a: pd.DataFrame,
    timeline_b: pd.DataFrame,
    max_offset_seconds: float = 300.0,
    resolution: float = 0.5,
    align_seconds: float = 360.0
) -> float:
    """
    Compute the time offset to align two atspm timelines using cross-correlation.
    
    This directly compares the phase state patterns in both timelines to find
    the time shift that best aligns them. This is more reliable than converting
    an event-count offset to a time offset.
    
    The method creates discrete time series (at `resolution` intervals) of which
    phases are in each state, then cross-correlates to find the best lag.
    
    Args:
        timeline_a: Timeline DataFrame from atspm (StartTime, EndTime, EventClass, EventValue)
        timeline_b: Timeline DataFrame from atspm
        max_offset_seconds: Maximum offset to search (both directions)
        resolution: Time resolution for discretization in seconds (default 0.5s)
        align_seconds: Only use the first N seconds for alignment (default 360s = 6 min)
    
    Returns:
        Time offset in seconds to apply to timeline B.
        Negative = shift B earlier (B started later in the signal cycle).
    """
    if timeline_a.empty or timeline_b.empty:
        return 0.0
    
    start_a = timeline_a['StartTime'].min()
    start_b = timeline_b['StartTime'].min()
    
    # Create discrete time series of green phase states
    n_bins = int(align_seconds / resolution)
    
    def discretize(tl_df, start_time):
        """Create array of frozensets: which (class, phase) are active at each time bin."""
        states = [set() for _ in range(n_bins)]
        for _, row in tl_df.iterrows():
            start_sec = (row['StartTime'] - start_time).total_seconds()
            end_sec = (row['EndTime'] - start_time).total_seconds()
            start_bin = max(0, int(start_sec / resolution))
            end_bin = min(n_bins, int(end_sec / resolution) + 1)
            ev = row['EventValue']
            key = (row['EventClass'], int(float(ev)) if pd.notna(ev) else 0)
            for b in range(start_bin, end_bin):
                states[b].add(key)
        return [frozenset(s) for s in states]
    
    states_a = discretize(timeline_a, start_a)
    states_b = discretize(timeline_b, start_b)
    
    max_lag_bins = int(max_offset_seconds / resolution)
    best_lag = 0
    best_score = -1
    
    for lag in range(-max_lag_bins, max_lag_bins + 1):
        matches = 0
        total = 0
        
        for i in range(n_bins):
            j = i - lag  # index in B that corresponds to i in A
            if 0 <= j < n_bins:
                if states_a[i] == states_b[j] and len(states_a[i]) > 0:
                    matches += 1
                if len(states_a[i]) > 0 or len(states_b[j]) > 0:
                    total += 1
        
        score = matches / total if total > 0 else 0
        if score > best_score:
            best_score = score
            best_lag = lag
    
    return best_lag * resolution


def find_temporal_offset(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    search_range: float = 200.0,
    resolution: float = 0.1,
    window_sec: float = 1800.0,
) -> float:
    """
    Find the temporal offset to align B with A using cross-correlation.

    Instead of positional matching (which can lock onto cyclic false matches),
    this searches for the time shift that maximises exact event-set matches
    within the first ``window_sec`` seconds of data.

    Uses a two-pass coarse-to-fine approach: first sweeps the full range at
    1 s resolution (~400 iterations), then refines ±2 s around the best
    coarse hit at 0.1 s resolution (~40 iterations).  Total ~440 iterations
    instead of ~4 000, giving ~10× speed-up.

    Args:
        df_a: Prepared events DataFrame A (with time_delta, event_id, parameter).
        df_b: Prepared events DataFrame B.
        search_range: Maximum shift to search in each direction (seconds).
        resolution: Final fine resolution (seconds).
        window_sec: Only use the first N seconds of each series.

    Returns:
        Best temporal shift in seconds to **add** to B's ``time_delta`` so that
        the two series are aligned.  Positive means B's events happen earlier
        than A's by that many seconds.
    """
    groups_a = _group_events_by_timestamp(df_a)
    groups_b = _group_events_by_timestamp(df_b)

    # Build quantised time -> hash lookup for A
    a_hash: Dict[int, int] = {}      # quantised key -> hash of event set
    for td, evset in groups_a:
        if td > window_sec:
            break
        a_hash[round(td / resolution)] = hash(evset)

    # Build parallel arrays for B within the window
    b_times_q: List[int] = []        # quantised time_delta values
    b_hashes: List[int] = []
    for td, evset in groups_b:
        if td > window_sec:
            break
        b_times_q.append(round(td / resolution))
        b_hashes.append(hash(evset))

    if not b_times_q or not a_hash:
        return 0.0

    def _count_matches(shift_q: int) -> int:
        count = 0
        for j in range(len(b_times_q)):
            key = b_times_q[j] + shift_q
            if key in a_hash and a_hash[key] == b_hashes[j]:
                count += 1
        return count

    # Pass 1: coarse search at 1.0s resolution
    coarse_step = round(1.0 / resolution)  # 10 quanta per coarse step
    coarse_start = round(-search_range / resolution)
    coarse_end = round(search_range / resolution)

    best_q = 0
    best_count = 0
    for sq in range(coarse_start, coarse_end + 1, coarse_step):
        c = _count_matches(sq)
        if c > best_count:
            best_count = c
            best_q = sq

    # Pass 2: fine search ±2s around best coarse hit
    fine_range = round(2.0 / resolution)  # ±20 quanta
    fine_start = best_q - fine_range
    fine_end = best_q + fine_range
    for sq in range(fine_start, fine_end + 1):
        c = _count_matches(sq)
        if c > best_count:
            best_count = c
            best_q = sq

    return best_q * resolution


# ---------------------------------------------------------------------------
# Rolling chunked DTW
# ---------------------------------------------------------------------------

def _rolling_chunk_dtw(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    window_sec: float = 2700.0,
    step_sec: float = 2400.0,
    clip_sec: float = 60.0,
) -> List[ChunkScore]:
    """
    Run DTW on rolling windows and return per-chunk match scores.

    Each window of ``window_sec`` duration slides forward by ``step_sec``.
    DTW is computed on each window, but only the interior (after clipping
    ``clip_sec`` from both edges) contributes to the match score. This avoids
    penalising chunk boundaries that split a signal cycle.

    Args:
        df_a: Prepared events (already temporally aligned).
        df_b: Prepared events (already temporally aligned).
        window_sec: Duration of each rolling window in seconds.
        step_sec: Step size between windows in seconds.
        clip_sec: Seconds to clip from each edge when scoring.

    Returns:
        List of :class:`ChunkScore` for every window.
    """
    max_time = min(df_a['time_delta'].max(), df_b['time_delta'].max())
    scores: List[ChunkScore] = []
    start = 0.0

    while start + window_sec <= max_time:
        c_start = start
        c_end = start + window_sec

        ca = df_a[(df_a['time_delta'] >= c_start) & (df_a['time_delta'] < c_end)].copy()
        cb = df_b[(df_b['time_delta'] >= c_start) & (df_b['time_delta'] < c_end)].copy()

        if ca.empty or cb.empty:
            start += step_sec
            continue

        ca['time_delta'] = ca['time_delta'] - c_start
        cb['time_delta'] = cb['time_delta'] - c_start

        ga = _group_events_by_timestamp(ca)
        gb = _group_events_by_timestamp(cb)

        if len(ga) < 5 or len(gb) < 5:
            start += step_sec
            continue

        dtw_r, match_pct, dist_m = _dtw_with_jaccard(ga, gb)

        # Score only the interior (clipped edges)
        if clip_sec > 0 and dtw_r.warping_path:
            interior_matches = 0
            interior_total = 0
            for i, j in dtw_r.warping_path:
                t_a = ga[i][0]
                t_b = gb[j][0]
                if (t_a >= clip_sec and t_a <= (window_sec - clip_sec)
                        and t_b >= clip_sec and t_b <= (window_sec - clip_sec)):
                    interior_total += 1
                    if dist_m[i, j] == 0.0:
                        interior_matches += 1
            if interior_total > 0:
                match_pct = interior_matches / interior_total * 100

        center = c_start + window_sec / 2
        scores.append(ChunkScore(
            center_seconds=center,
            match_percentage=match_pct,
            window_seconds=window_sec,
        ))
        start += step_sec

    return scores


def render_sparkline_svg(
    chunk_scores: List[ChunkScore],
    width: int = 1400,
    height: int = 220,
    pass_threshold: float = 95.0,
    warn_threshold: float = 85.0,
) -> str:
    """
    Render an inline SVG sparkline showing match percentage over time.

    Each chunk becomes a vertical bar whose colour indicates quality:
    - Green (>= pass_threshold)
    - Orange (>= warn_threshold)
    - Red   (< warn_threshold)

    A thin horizontal reference line is drawn at the pass threshold.

    Args:
        chunk_scores: Per-chunk match scores.
        width: SVG width in pixels.
        height: SVG height in pixels.
        pass_threshold: Green threshold (default 95%).
        warn_threshold: Orange/red boundary (default 85%).

    Returns:
        Self-contained SVG string suitable for embedding in HTML.
    """
    if not chunk_scores:
        return ""

    n = len(chunk_scores)
    margin_left = 64    # space for Y-axis labels
    margin_bottom = 34  # space for X-axis labels
    margin_top = 14
    margin_right = 18
    plot_w = width - margin_left - margin_right
    plot_h = height - margin_bottom - margin_top
    bar_w = max(1.0, plot_w / n)
    y_min, y_max = 0.0, 100.0  # Full 0-100% range

    def _y(pct: float) -> float:
        clamped = max(y_min, min(pct, y_max))
        return margin_top + plot_h - (clamped - y_min) / (y_max - y_min) * plot_h

    def _color(pct: float) -> str:
        if pct >= pass_threshold:
            return "#1b8a2e"
        if pct >= warn_threshold:
            return "#e8710a"
        return "#c5221f"

    bars = []
    for i, cs in enumerate(chunk_scores):
        x = margin_left + i * bar_w
        pct = cs.match_percentage
        y = _y(pct)
        h = _y(y_min) - y  # bar height from bottom of plot
        bars.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" '
            f'height="{h:.1f}" fill="{_color(pct)}" opacity="0.85">'
            f'<title>{cs.center_seconds / 3600:.1f}h: {pct:.1f}%</title></rect>'
        )

    # Reference dashed line at 100% (top)
    ref_y_100 = _y(100.0)
    ref_100 = (
        f'<line x1="{margin_left}" y1="{ref_y_100:.1f}" '
        f'x2="{width - margin_right}" y2="{ref_y_100:.1f}" '
        f'stroke="#bbb" stroke-width="0.5" stroke-dasharray="3,2" />'
    )

    # Plot area background
    bg = (
        f'<rect x="{margin_left}" y="{margin_top}" '
        f'width="{plot_w}" height="{plot_h}" fill="#f8f9fa" rx="2" />'
    )

    # Y-axis labels and grid lines
    y_labels_svg = []
    for pct_val in [0, 25, 50, 75, 100]:
        yp = _y(float(pct_val))
        y_labels_svg.append(
            f'<text x="{margin_left - 3}" y="{yp + 3:.1f}" '
            f'font-size="12" fill="#5f6368" font-family="sans-serif" '
            f'text-anchor="end">{pct_val}%</text>'
        )
        y_labels_svg.append(
            f'<line x1="{margin_left}" y1="{yp:.1f}" '
            f'x2="{width - margin_right}" y2="{yp:.1f}" '
            f'stroke="#e0e0e0" stroke-width="0.3" />'
        )

    # X-axis labels in HH:MM format at clean minute intervals
    x_labels_svg = []
    if chunk_scores:
        first_sec = chunk_scores[0].center_seconds
        last_sec = chunk_scores[-1].center_seconds
        span_sec = last_sec - first_sec
        if span_sec > 0:
            # Pick a clean minute-based interval: 1, 5, 10, 30, 60, 120, 300, 600 min
            target_labels = 6
            raw_step_min = (span_sec / 60) / target_labels
            minute_steps = [1, 5, 10, 30, 60, 120, 300, 600]
            step_min = minute_steps[0]
            for ms in minute_steps:
                if ms >= raw_step_min:
                    step_min = ms
                    break
            else:
                step_min = minute_steps[-1]
            step_s = step_min * 60
            # Start at the first clean multiple of step_s at or after first_sec
            first_label = (int(first_sec) // step_s + 1) * step_s
            label_secs = list(range(first_label, int(last_sec) + 1, step_s))
            for ls in label_secs:
                frac = (ls - first_sec) / span_sec
                xp = margin_left + frac * plot_w
                y_label = margin_top + plot_h + 14
                h_val, rem = divmod(int(ls), 3600)
                m_val = rem // 60
                x_labels_svg.append(
                    f'<text x="{xp:.1f}" y="{y_label}" '
                    f'font-size="12" fill="#5f6368" font-family="sans-serif" '
                    f'text-anchor="middle">{h_val:02d}:{m_val:02d}</text>'
                )

    # Axes lines (L-shape: left + bottom)
    axes = (
        f'<line x1="{margin_left}" y1="{margin_top}" '
        f'x2="{margin_left}" y2="{margin_top + plot_h}" '
        f'stroke="#666" stroke-width="0.8" />'
        f'<line x1="{margin_left}" y1="{margin_top + plot_h}" '
        f'x2="{width - margin_right}" y2="{margin_top + plot_h}" '
        f'stroke="#666" stroke-width="0.8" />'
    )

    svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" '
        f'preserveAspectRatio="xMidYMid meet" '
        f'style="display:block;width:100%;height:auto;vertical-align:middle;">'
        f'{bg}{"".join(y_labels_svg)}{"".join(bars)}'
        f'{ref_100}{axes}{"".join(x_labels_svg)}</svg>'
    )
    return svg


def compare_runs(
    events_a: pd.DataFrame,
    events_b: pd.DataFrame,
    device_id: str,
    run_a_label: Union[int, str] = "A",
    run_b_label: Union[int, str] = "B",
    start_time_a: Optional[datetime] = None,
    start_time_b: Optional[datetime] = None,
    event_ids: Optional[List[int]] = None,
    auto_align: bool = True,
    trim_edges_minutes: float = 0.0,
    settle_minutes: float = 0.0,
) -> ComparisonResult:
    """
    Compare two runs using group-level DTW on event sequences and timing.
    
    Events are grouped by timestamp into sets (order-independent within each
    timestamp). DTW is run on these groups using Jaccard distance:
    - 0 if the event sets are identical
    - Jaccard distance otherwise (fraction of non-overlapping events)
    
    This approach is faster and more robust than individual-event DTW because:
    - Event ordering within a timestamp doesn't matter
    - Gaps (missing time periods) are immediately obvious
    - Typical sequences have ~200-500 groups vs ~5000-10000 individual events
    
    After sequence comparison, timing analysis compares the time deltas
    of matched groups to detect timing drift between runs.
    
    Args:
        events_a: Events from first run
        events_b: Events from second run
        device_id: Device identifier
        run_a_label: Label for first run (e.g., run number or "input")
        run_b_label: Label for second run
        start_time_a: Start time for run A (for time delta calculation)
        start_time_b: Start time for run B
        event_ids: Optional list of event IDs to include in comparison
        auto_align: If True, automatically find best alignment offset
        trim_edges_minutes: (Deprecated, use settle_minutes) Trim this many
            minutes from start and end of each sequence before DTW
        settle_minutes: Ignore the first N minutes when computing match
            percentage and reporting divergences. Data is NOT removed — DTW
            still sees the full aligned sequence for best alignment, but the
            settling period is excluded from the results.
    
    Returns:
        ComparisonResult with DTW distances, divergence windows, and timing stats
    """
    # Prepare events (filter, sort, compute time_deltas)
    df_a = prepare_events_for_comparison(events_a, start_time_a, event_ids=event_ids)
    df_b = prepare_events_for_comparison(events_b, start_time_b, event_ids=event_ids)
    
    # Auto-alignment: temporal cross-correlation to find the best time shift,
    # then apply a small positional trim if needed.
    alignment_offset = 0
    trim_seconds_a = 0.0
    trim_seconds_b = 0.0
    temporal_shift = 0.0
    
    if auto_align and not df_a.empty and not df_b.empty:
        # Step 1: Find the true temporal shift via cross-correlation.
        # This avoids the cyclic false-match problem of positional alignment.
        temporal_shift = find_temporal_offset(df_a, df_b)
        if abs(temporal_shift) > 0.05:
            df_b = df_b.copy()
            df_b['time_delta'] = df_b['time_delta'] + temporal_shift
            df_b = df_b[df_b['time_delta'] >= 0].reset_index(drop=True)

        # Step 2: Positional alignment to skip leading gaps (e.g. sparse
        # startup periods) and fine-tune residual offsets.  max_offset=50
        # is safe here because temporal cross-correlation already resolved
        # cyclic ambiguity, and the per-group penalty (0.15 %) prevents
        # the positional pass from drifting onto a wrong cycle.
        alignment_offset, pre_align_match = find_alignment_offset(
            df_a, df_b, max_offset=50, align_seconds=360.0
        )
        raw_groups_a = _group_events_by_timestamp(df_a)
        raw_groups_b = _group_events_by_timestamp(df_b)
        
        if alignment_offset > 0 and alignment_offset < len(raw_groups_b):
            trim_seconds_b = raw_groups_b[alignment_offset][0]
            df_b = df_b[df_b['time_delta'] >= trim_seconds_b].reset_index(drop=True)
            if not df_b.empty:
                new_start = df_b['timestamp'].min()
                df_b['time_delta'] = (df_b['timestamp'] - new_start).dt.total_seconds()
        elif alignment_offset < 0 and -alignment_offset < len(raw_groups_a):
            trim_seconds_a = raw_groups_a[-alignment_offset][0]
            df_a = df_a[df_a['time_delta'] >= trim_seconds_a].reset_index(drop=True)
            if not df_a.empty:
                new_start = df_a['timestamp'].min()
                df_a['time_delta'] = (df_a['timestamp'] - new_start).dt.total_seconds()

    if trim_edges_minutes > 0 and not df_a.empty and not df_b.empty:
        trim_seconds = trim_edges_minutes * 60.0

        max_a = float(df_a['time_delta'].max())
        max_b = float(df_b['time_delta'].max())

        if max_a > (2 * trim_seconds):
            df_a = df_a[
                (df_a['time_delta'] >= trim_seconds)
                & (df_a['time_delta'] <= (max_a - trim_seconds))
            ].reset_index(drop=True)
            if not df_a.empty:
                new_start = df_a['timestamp'].min()
                df_a['time_delta'] = (df_a['timestamp'] - new_start).dt.total_seconds()
            trim_seconds_a += trim_seconds

        if max_b > (2 * trim_seconds):
            df_b = df_b[
                (df_b['time_delta'] >= trim_seconds)
                & (df_b['time_delta'] <= (max_b - trim_seconds))
            ].reset_index(drop=True)
            if not df_b.empty:
                new_start = df_b['timestamp'].min()
                df_b['time_delta'] = (df_b['timestamp'] - new_start).dt.total_seconds()
            trim_seconds_b += trim_seconds
    
    if df_a.empty or df_b.empty:
        return ComparisonResult(
            device_id=device_id,
            run_a=run_a_label,
            run_b=run_b_label,
            sequence_dtw=DTWResult(
                distance=float('inf'),
                normalized_distance=float('inf'),
                warping_path=[],
                sequence_length_a=0,
                sequence_length_b=0
            ),
            timing_dtw=DTWResult(
                distance=float('inf'),
                normalized_distance=float('inf'),
                warping_path=[],
                sequence_length_a=0,
                sequence_length_b=0
            ),
            divergence_windows=[],
            match_percentage=0.0,
            chunk_scores=[],
            temporal_shift_seconds=0.0,
        )
    
    # ===== GROUP-LEVEL DTW =====
    # Group events by timestamp into sets of (event_id, parameter) tuples.
    # Each group represents "what happened at this moment in time".
    groups_a = _group_events_by_timestamp(df_a)
    groups_b = _group_events_by_timestamp(df_b)
    
    # ===== ROLLING CHUNKED DTW =====
    # Use rolling windows (45 min window, 40 min step, 60 s edge clip) for
    # the official match score.  Much faster than full-series DTW and provides
    # per-chunk diagnostics (sparkline data).
    chunk_scores = _rolling_chunk_dtw(
        df_a, df_b,
        window_sec=2700.0,   # 45 minutes
        step_sec=2400.0,     # 40 minutes (5 min overlap)
        clip_sec=60.0,       # 60 s clipped from each edge
    )

    if chunk_scores:
        match_percentage = sum(c.match_percentage for c in chunk_scores) / len(chunk_scores)
    else:
        # Fallback to full-series DTW for very short data
        sequence_dtw_fallback, match_percentage, dist_matrix_fallback = _dtw_with_jaccard(groups_a, groups_b)

    # ===== FULL-SERIES DTW FOR DIVERGENCE DETECTION =====
    # We still need the warping path for divergence detection and timing stats.
    # For datasets that fit, run full DTW; for very large ones, skip divergence
    # detection (the per-chunk sparkline provides the diagnostic equivalent).
    full_dtw_cells = len(groups_a) * len(groups_b)
    MAX_DTW_CELLS = 50_000_000  # ~400 MB, ~35s — acceptable

    if full_dtw_cells <= MAX_DTW_CELLS:
        sequence_dtw, _full_match, dist_matrix = _dtw_with_jaccard(groups_a, groups_b)
    else:
        # Too large for full DTW — synthesize a minimal DTWResult.
        # Divergence windows will be derived from chunk_scores instead.
        sequence_dtw = DTWResult(
            distance=0.0,
            normalized_distance=0.0,
            warping_path=[],
            sequence_length_a=len(groups_a),
            sequence_length_b=len(groups_b),
        )
        dist_matrix = np.array([])
    
    # ===== SETTLE PERIOD ADJUSTMENT =====
    # If settle_minutes is set, recompute match_percentage excluding the
    # settling period from the chunk scores.
    settle_seconds = settle_minutes * 60.0
    if settle_seconds > 0 and chunk_scores:
        # Keep only chunks whose center is past the settle period
        settled_chunks = [c for c in chunk_scores if c.center_seconds >= settle_seconds]
        if settled_chunks:
            match_percentage = sum(c.match_percentage for c in settled_chunks) / len(settled_chunks)
    elif settle_seconds > 0 and sequence_dtw.warping_path:
        # Find the overlap window: both A and B must be past settle and
        # within the shorter sequence's duration
        max_time_a = groups_a[-1][0] if groups_a else 0.0
        max_time_b = groups_b[-1][0] if groups_b else 0.0
        overlap_end = min(max_time_a, max_time_b)

        settled_matches = 0
        settled_total = 0
        for i, j in sequence_dtw.warping_path:
            t_a = groups_a[i][0] if i < len(groups_a) else 0.0
            t_b = groups_b[j][0] if j < len(groups_b) else 0.0
            if (t_a >= settle_seconds and t_b >= settle_seconds
                    and t_a <= overlap_end and t_b <= overlap_end):
                settled_total += 1
                if dist_matrix[i, j] == 0.0:
                    settled_matches += 1
        if settled_total > 0:
            match_percentage = settled_matches / settled_total * 100
    
    # ===== DIVERGENCE DETECTION =====
    _use_chunk_divergences = not sequence_dtw.warping_path

    if not _use_chunk_divergences:
        # Full DTW available — use warping path for precise divergences
        grace = max(30.0, settle_seconds)  # At least 30s, or settle period
        divergences = _find_group_divergences(
            sequence_dtw.warping_path,
            dist_matrix,
            groups_a,
            groups_b,
            trim_seconds_a=trim_seconds_a,
            trim_seconds_b=trim_seconds_b,
            alignment_grace_seconds=grace,
        )
        # Filter out tail divergences past the overlap window
        if groups_a and groups_b:
            max_time_a = groups_a[-1][0] if groups_a else 0.0
            max_time_b = groups_b[-1][0] if groups_b else 0.0
            overlap_end = min(max_time_a, max_time_b)
            tail_margin = 5.0
            divergences = [
                d for d in divergences
                if (d.end_time_delta_a < (overlap_end - tail_margin)
                    and d.end_time_delta_b < (overlap_end - tail_margin))
            ]
    else:
        # Full DTW was skipped — run local DTW on each failing chunk to
        # find precise divergence points (same quality as full-DTW method).
        _pass = 95.0
        divergences = []
        for cs in chunk_scores:
            if cs.match_percentage < _pass:
                half_w = cs.window_seconds / 2.0
                c_start = max(0.0, cs.center_seconds - half_w)
                c_end = cs.center_seconds + half_w
                # Extract local groups for this chunk
                local_a = df_a[(df_a['time_delta'] >= c_start) & (df_a['time_delta'] < c_end)].copy()
                local_b = df_b[(df_b['time_delta'] >= c_start) & (df_b['time_delta'] < c_end)].copy()
                if local_a.empty or local_b.empty:
                    continue
                # Shift to chunk-local time
                local_a['time_delta'] = local_a['time_delta'] - c_start
                local_b['time_delta'] = local_b['time_delta'] - c_start
                lg_a = _group_events_by_timestamp(local_a)
                lg_b = _group_events_by_timestamp(local_b)
                if len(lg_a) < 2 or len(lg_b) < 2:
                    continue
                local_dtw, _, local_dist = _dtw_with_jaccard(lg_a, lg_b)
                local_divs = _find_group_divergences(
                    local_dtw.warping_path,
                    local_dist,
                    lg_a,
                    lg_b,
                    trim_seconds_a=c_start + trim_seconds_a,
                    trim_seconds_b=c_start + trim_seconds_b,
                    alignment_grace_seconds=0.0,
                )
                # Shift divergence times back to global coordinates
                for d in local_divs:
                    d.start_time_delta_a += c_start
                    d.end_time_delta_a += c_start
                    d.start_time_delta_b += c_start
                    d.end_time_delta_b += c_start
                divergences.extend(local_divs)
    
    # ===== TIMING ANALYSIS =====
    # For matched groups, compare how much the timing differs
    timing_stats = _analyze_timing(
        sequence_dtw.warping_path,
        dist_matrix,
        groups_a,
        groups_b,
    )
    
    # ===== TIMING DTW =====
    # DTW on the time-delta sequences (for backward compatibility).
    # Skip for very large datasets (same size threshold as sequence DTW).
    time_a = np.array([t for t, _ in groups_a], dtype=float)
    time_b = np.array([t for t, _ in groups_b], dtype=float)
    if full_dtw_cells <= MAX_DTW_CELLS:
        max_time = max(time_a.max() if len(time_a) > 0 else 0,
                       time_b.max() if len(time_b) > 0 else 0)
        if max_time > 0:
            time_a_norm = time_a / max_time
            time_b_norm = time_b / max_time
        else:
            time_a_norm = time_a
            time_b_norm = time_b
        timing_dtw = compute_dtw(time_a_norm, time_b_norm, categorical=False)
    else:
        timing_dtw = DTWResult(
            distance=0.0,
            normalized_distance=0.0,
            warping_path=[],
            sequence_length_a=len(groups_a),
            sequence_length_b=len(groups_b),
        )
    
    return ComparisonResult(
        device_id=device_id,
        run_a=run_a_label,
        run_b=run_b_label,
        sequence_dtw=sequence_dtw,
        timing_dtw=timing_dtw,
        divergence_windows=divergences,
        match_percentage=match_percentage,
        alignment_offset=alignment_offset,
        timing_stats=timing_stats,
        alignment_trim_seconds_a=trim_seconds_a,
        alignment_trim_seconds_b=trim_seconds_b,
        chunk_scores=chunk_scores,
        temporal_shift_seconds=temporal_shift,
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
        print(f"  Match Percentage:          {result.match_percentage:.1f}%")
        print(f"  Groups in A:               {result.sequence_dtw.sequence_length_a}")
        print(f"  Groups in B:               {result.sequence_dtw.sequence_length_b}")
        print(f"  Alignment offset:          {result.alignment_offset} groups")
        if result.alignment_trim_seconds_a > 0:
            print(f"    Trimmed {result.alignment_trim_seconds_a:.1f}s from A start")
        if result.alignment_trim_seconds_b > 0:
            print(f"    Trimmed {result.alignment_trim_seconds_b:.1f}s from B start")
        print(f"  Divergence Windows:        {len(result.divergence_windows)}")
        print(f"{'='*60}")
        
        if result.divergence_windows:
            print("\nDivergence Windows:")
            for i, div in enumerate(result.divergence_windows, 1):
                if div.description:
                    print(f"  {i}. {div.description}")
                else:
                    print(f"  {i}. A: {div.original_start_seconds_a:.1f}s - "
                          f"{div.original_end_seconds_a:.1f}s from start")
                    print(f"     B: {div.original_start_seconds_b:.1f}s - "
                          f"{div.original_end_seconds_b:.1f}s from start")
        
        if result.timing_stats:
            ts = result.timing_stats
            residual = ts.get('alignment_residual', 0.0)
            print(f"\nTiming Analysis ({ts['n_matched_groups']} matched groups):")
            if abs(residual) > 0.05:
                print(f"  Alignment residual: {residual:+.3f}s (constant offset, subtracted below)")
            print(f"  Timing jitter std:  {ts['std_diff']:.3f}s")
            print(f"  Max jitter:         {ts['max_abs_diff']:.3f}s")
            print(f"  95th percentile:    {ts['p95_abs_diff']:.3f}s")
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
        try:
            con.execute(f"ATTACH '{path}' AS source_db (TYPE SQLITE)")

            # DuckDB exposes sqlite_master unqualified for attached SQLite databases.
            tables = con.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table'
            """).fetchall()
            table_names = [t[0].lower() for t in tables]

            if 'event' in table_names:
                # MAXTIME format
                return con.execute("""
                    SELECT
                        TO_TIMESTAMP(Timestamp + (Tick / 10))::TIMESTAMP AS timestamp,
                        EventTypeID AS event_id,
                        Parameter AS parameter
                    FROM source_db.Event
                    ORDER BY timestamp
                """).df()
            if 'events' in table_names:
                return con.execute("""
                    SELECT * FROM source_db.events ORDER BY timestamp
                """).df()

            raise ValueError(
                f"No 'Event' or 'events' table found in {path}. Found tables: {[t[0] for t in tables]}"
            )
        finally:
            con.close()
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
    
    # Clip timeline to the raw data time range.
    # The atspm processor can extrapolate signal states well beyond the actual
    # data window (e.g. back-filling an entire 15-min bin), which breaks the
    # cross-correlation alignment and chart windowing downstream.
    if not timeline.empty and 'TimeStamp' in df.columns:
        data_start = df['TimeStamp'].min()
        data_end = df['TimeStamp'].max()
        timeline = timeline[
            (timeline['EndTime'] >= data_start) & (timeline['StartTime'] <= data_end)
        ].copy()
        # Clamp start/end times so bars don't extend beyond the raw data range
        timeline.loc[timeline['StartTime'] < data_start, 'StartTime'] = data_start
        timeline.loc[timeline['EndTime'] > data_end, 'EndTime'] = data_end
    
    return timeline


# =============================================================================
# Per-Phase Difference Breakdown
# =============================================================================

def generate_phase_difference_summary(
    timeline_a: pd.DataFrame,
    timeline_b: pd.DataFrame,
    time_offset_b: float = 0.0,
    tolerance_seconds: float = 3.0,
) -> List[Dict]:
    """
    Compare two timelines per-phase/overlap and identify which are different.

    For each unique (EventClass, EventValue) in the signal states, computes:
    - Count of state periods in A and B
    - Total duration in A and B
    - Delta in count and duration

    Args:
        timeline_a: Timeline DataFrame (StartTime, EndTime, EventClass, EventValue)
        timeline_b: Timeline DataFrame
        time_offset_b: Offset applied to B (not used for stats, just for reference)
        tolerance_seconds: Ignore differences smaller than this in total duration

    Returns:
        List of dicts, one per (phase_label, state), sorted by largest |duration_delta|.
        Only includes entries where there IS a meaningful difference.
    """
    signal_classes = {
        'Green', 'Yellow', 'Red',
        'Overlap Green', 'Overlap Trail Green', 'Overlap Yellow', 'Overlap Red',
    }

    def _compute_stats(tl: pd.DataFrame) -> Dict:
        filtered = tl[tl['EventClass'].isin(signal_classes)].copy()
        if 'Duration' not in filtered.columns or filtered['Duration'].isna().all():
            filtered['Duration'] = (
                filtered['EndTime'] - filtered['StartTime']
            ).dt.total_seconds()
        stats = {}
        for (ec, ev), grp in filtered.groupby(['EventClass', 'EventValue']):
            ev_int = int(float(ev)) if pd.notna(ev) else 0
            key = (ec, ev_int)
            stats[key] = {
                'count': len(grp),
                'total_seconds': float(grp['Duration'].sum()),
                'avg_seconds': float(grp['Duration'].mean()) if len(grp) > 0 else 0.0,
            }
        return stats

    stats_a = _compute_stats(timeline_a)
    stats_b = _compute_stats(timeline_b)

    all_keys = sorted(set(stats_a.keys()) | set(stats_b.keys()))

    results = []
    for ec, ev in all_keys:
        sa = stats_a.get((ec, ev), {'count': 0, 'total_seconds': 0.0, 'avg_seconds': 0.0})
        sb = stats_b.get((ec, ev), {'count': 0, 'total_seconds': 0.0, 'avg_seconds': 0.0})
        count_delta = sb['count'] - sa['count']
        dur_delta = sb['total_seconds'] - sa['total_seconds']

        if abs(dur_delta) <= tolerance_seconds and count_delta == 0:
            continue  # No meaningful difference

        # Build a human-readable label
        if ec in ('Green', 'Yellow', 'Red'):
            label = f"Ph {ev}"
        elif 'Overlap' in ec:
            label = f"Ovlp {ev}"
        else:
            label = f"{ec} {ev}"

        # Simplify the state name
        state = ec.replace('Overlap ', '')

        avg_delta = sb['avg_seconds'] - sa['avg_seconds']

        results.append({
            'label': label,
            'state': state,
            'event_class': ec,
            'event_value': ev,
            'count_a': sa['count'],
            'count_b': sb['count'],
            'count_delta': count_delta,
            'duration_a': sa['avg_seconds'],
            'duration_b': sb['avg_seconds'],
            'duration_delta': avg_delta,
            'total_duration_a': sa['total_seconds'],
            'total_duration_b': sb['total_seconds'],
            'total_duration_delta': dur_delta,
        })

    # Sort: prioritze cases where a count is zero (completely missing phase/state),
    # then sort by absolute total duration delta (largest first).
    results.sort(key=lambda r: (r['count_a'] == 0 or r['count_b'] == 0, abs(r['total_duration_delta'])), reverse=True)
    return results


def format_phase_differences(diffs: List[Dict], label_a: str = "Original", label_b: str = "Replay", max_rows: int = 10) -> str:
    """Format phase difference summary as a readable string."""
    if not diffs:
        return "  All phases/overlaps match within tolerance."

    shown = diffs[:max_rows]
    lines = []
    for d in shown:
        parts = [f"  {d['label']:>8s} {d['state']:<14s}"]

        # Count
        if d['count_delta'] != 0:
            sign = '+' if d['count_delta'] > 0 else ''
            parts.append(f"count: {d['count_a']}->{d['count_b']} ({sign}{d['count_delta']})")
        else:
            parts.append(f"count: {d['count_a']}")

        # Duration
        sign = '+' if d['duration_delta'] > 0 else ''
        parts.append(f"dur: {d['duration_a']:.1f}s->{d['duration_b']:.1f}s ({sign}{d['duration_delta']:.1f}s)")

        lines.append("  ".join(parts))

    if len(diffs) > max_rows:
        lines.append(f"  ... and {len(diffs) - max_rows} more")

    return "\n".join(lines)


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
    dpi: int = 150,
    align_by_time_delta: bool = True,
    time_offset_b: float = 0.0
) -> Optional[Any]:
    """
    Create a Gantt chart comparing two timelines using matplotlib (reliable PNG export).
    
    This function aligns two timelines by their relative time from start, NOT by absolute
    timestamp. This is critical because the two runs typically happen at different times
    but should be compared starting from their respective beginnings.
    
    Args:
        timeline_a: Timeline DataFrame from first source (requires StartTime, EndTime, EventClass, EventValue)
        timeline_b: Timeline DataFrame from second source  
        label_a: Label for first timeline (default "Original")
        label_b: Label for second timeline (default "Replay")
        title: Chart title
        divergence_start: Timestamp in timeline_a where divergence was detected (for centering view)
        divergence_end: Optional end time of divergence window
        output_path: Path to save the plot (PNG, PDF, SVG supported)
        window_minutes: Time window to show around divergence (default 5 min)
        event_classes: Optional list of event classes to include
        dpi: Resolution for PNG output (default 150)
        align_by_time_delta: If True, align timelines by relative time from their respective
                            starts. If False, use absolute timestamps (only works if both
                            timelines have the same absolute time range).
        time_offset_b: Time offset in seconds to apply to timeline B. Positive values
                      shift B's events to the right (later in time). Use this when the
                      raw events started at different points in the signal cycle.
    
    Returns:
        matplotlib Figure object, or None if matplotlib not available
    """
    # Default event classes for signal visualization
    if event_classes is None:
        event_classes = [
            'Green', 'Yellow', 'Red',
            'Overlap Green', 'Overlap Trail Green', 'Overlap Yellow', 'Overlap Red',
            'Preempt', 'Ped Service',
            'TSP Service', 'TSP Call'
        ]
    
    # Get the start times BEFORE filtering by event_classes.
    # This ensures the time reference matches what compute_timeline_offset() used.
    # If we computed start_a after filtering to e.g. ['Green','Yellow','Red'],
    # the first Green might start later than the first overall event, creating
    # a drift between the offset and the rendering.
    start_a = timeline_a['StartTime'].min() if not timeline_a.empty else None
    start_b = timeline_b['StartTime'].min() if not timeline_b.empty else None
    
    if start_a is None and start_b is None:
        warnings.warn("No events found in either timeline")
        return None
    
    # Filter timelines to relevant event classes
    df_a = timeline_a[timeline_a['EventClass'].isin(event_classes)].copy()
    df_b = timeline_b[timeline_b['EventClass'].isin(event_classes)].copy()
    
    if df_a.empty and df_b.empty:
        warnings.warn("No matching events found in either timeline")
        return None
    
    # Ensure EventValue is numeric
    if not df_a.empty:
        df_a['EventValue'] = pd.to_numeric(df_a['EventValue'], errors='coerce')
    if not df_b.empty:
        df_b['EventValue'] = pd.to_numeric(df_b['EventValue'], errors='coerce')
    
    # Calculate time deltas from respective starts
    if align_by_time_delta:
        # Each timeline is relative to its own start time
        if not df_a.empty:
            df_a['TimeDelta'] = (df_a['StartTime'] - start_a).dt.total_seconds()
            df_a['EndDelta'] = (df_a['EndTime'] - start_a).dt.total_seconds()
        if not df_b.empty:
            # Apply time offset to timeline B (shifts B's events in time relative to A)
            df_b['TimeDelta'] = (df_b['StartTime'] - start_b).dt.total_seconds() + time_offset_b
            df_b['EndDelta'] = (df_b['EndTime'] - start_b).dt.total_seconds() + time_offset_b
        
        # Convert divergence_start to time delta (it's a timestamp from timeline_a)
        if divergence_start is not None and start_a is not None:
            divergence_delta = (divergence_start - start_a).total_seconds()
        else:
            divergence_delta = None
        
        if divergence_end is not None and start_a is not None:
            divergence_end_delta = (divergence_end - start_a).total_seconds()
        else:
            divergence_end_delta = divergence_delta
    else:
        # Use absolute timestamps - both timelines must have same time range
        common_start = min(t for t in [start_a, start_b] if t is not None)
        if not df_a.empty:
            df_a['TimeDelta'] = (df_a['StartTime'] - common_start).dt.total_seconds()
            df_a['EndDelta'] = (df_a['EndTime'] - common_start).dt.total_seconds()
        if not df_b.empty:
            df_b['TimeDelta'] = (df_b['StartTime'] - common_start).dt.total_seconds() + time_offset_b
            df_b['EndDelta'] = (df_b['EndTime'] - common_start).dt.total_seconds()
        
        if divergence_start is not None:
            divergence_delta = (divergence_start - common_start).total_seconds()
        else:
            divergence_delta = None
        
        if divergence_end is not None:
            divergence_end_delta = (divergence_end - common_start).total_seconds()
        else:
            divergence_end_delta = divergence_delta
    
    # Determine the time window to display
    max_time_a = df_a['EndDelta'].max() if not df_a.empty else 0
    max_time_b = df_b['EndDelta'].max() if not df_b.empty else 0
    total_duration_seconds = max(max_time_a, max_time_b)
    
    MAX_WINDOW_SECONDS = 15.0 * 60  # Hard cap at 15 minutes
    window_seconds = min(window_minutes * 60, MAX_WINDOW_SECONDS)
    
    if divergence_delta is not None:
        div_end = divergence_end_delta if divergence_end_delta is not None else divergence_delta
        div_duration = div_end - divergence_delta

        if div_duration < 10.0 * 60:
            # Short divergence: center the window on it, but clamp to data bounds
            div_center = (divergence_delta + div_end) / 2.0
            window_start_sec = div_center - window_seconds / 2.0
            window_end_sec = div_center + window_seconds / 2.0
            # Clamp to data bounds
            if window_start_sec < 0:
                window_end_sec -= window_start_sec  # shift right
                window_start_sec = 0
            if window_end_sec > total_duration_seconds:
                window_start_sec -= (window_end_sec - total_duration_seconds)
                window_end_sec = total_duration_seconds
                window_start_sec = max(0, window_start_sec)
        else:
            # Long divergence: start 5 min before, show 10 min after start
            window_start_sec = max(0, divergence_delta - 5.0 * 60)
            window_end_sec = window_start_sec + MAX_WINDOW_SECONDS
    else:
        # Show from start, limited by window
        window_start_sec = 0
        window_end_sec = min(total_duration_seconds, window_seconds)
    
    # Filter to window
    if not df_a.empty:
        df_a = df_a[(df_a['EndDelta'] >= window_start_sec) & (df_a['TimeDelta'] <= window_end_sec)].copy()
    if not df_b.empty:
        df_b = df_b[(df_b['EndDelta'] >= window_start_sec) & (df_b['TimeDelta'] <= window_end_sec)].copy()
    
    # Keep absolute time deltas (from aligned start) for x-axis positioning
    if not df_a.empty:
        df_a['RelStart'] = df_a['TimeDelta']
        df_a['RelEnd'] = df_a['EndDelta']
        df_a['Duration'] = df_a['RelEnd'] - df_a['RelStart']
        df_a['Source'] = label_a
    
    if not df_b.empty:
        df_b['RelStart'] = df_b['TimeDelta']
        df_b['RelEnd'] = df_b['EndDelta']
        df_b['Duration'] = df_b['RelEnd'] - df_b['RelStart']
        df_b['Source'] = label_b
    
    # Combine dataframes
    dfs_to_concat = [df for df in [df_a, df_b] if not df.empty]
    if not dfs_to_concat:
        warnings.warn("No events in the time window")
        return None
    
    combined_df = pd.concat(dfs_to_concat, ignore_index=True)
    
    if combined_df.empty:
        warnings.warn("No events in the time window")
        return None
    
    # Create row labels
    def create_base_label(event_class, event_value):
        val = int(float(event_value)) if pd.notna(event_value) else 0
        if event_class in ['Green', 'Yellow', 'Red']:
            return f"Ph {val}"
        elif event_class in ['Overlap Green', 'Overlap Trail Green', 'Overlap Yellow', 'Overlap Red']:
            return f"Ovlp {val}"
        else:
            return f"{event_class} {val}"
    
    combined_df['BaseLabel'] = [create_base_label(ec, ev) for ec, ev in 
                                 zip(combined_df['EventClass'], combined_df['EventValue'])]
    combined_df['SortKey'] = [_get_sort_key(ec, int(float(ev)) if pd.notna(ev) else 0) for ec, ev in 
                              zip(combined_df['EventClass'], combined_df['EventValue'])]
    combined_df['RowLabel'] = combined_df['BaseLabel'] + " (" + combined_df['Source'] + ")"
    combined_df['Color'] = [_get_event_color(ec, int(float(ev)) if pd.notna(ev) else 0) for ec, ev in 
                            zip(combined_df['EventClass'], combined_df['EventValue'])]
    
    # Build row order (interleaved: label_a above label_b for each phase)
    # Only include rows that actually have data in at least one timeline
    # Dedup on BaseLabel only - different EventClasses (Green/Yellow/Red) share
    # the same BaseLabel (e.g., "Ph 2") but have different SortKeys. We want
    # ONE row per phase per source, not one row per phase per EventClass.
    base_sort = combined_df.groupby('BaseLabel')['SortKey'].min().reset_index()
    base_sort = base_sort.sort_values('SortKey')
    
    row_labels = []
    seen_labels = set()
    for _, row in base_sort.iterrows():
        base = row['BaseLabel']
        if base in seen_labels:
            continue
        seen_labels.add(base)
        # Always add BOTH rows for each phase so the visual comparison is easy.
        # If one timeline has no data for this phase, its row will be empty.
        row_labels.append(f"{base} ({label_a})")
        row_labels.append(f"{base} ({label_b})")
    
    # Map row labels to y positions
    row_to_y = {label: i for i, label in enumerate(row_labels)}
    
    # Create figure
    fig_height = max(5.0, 0.34 * len(row_labels))
    fig, ax = plt.subplots(figsize=(18, fig_height))
    
    # Plot bars using broken_barh
    bar_height = 0.72
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
    
    # Add divergence marker if specified
    if divergence_delta is not None:
        div_x_start = divergence_delta
        div_x_end = divergence_end_delta if divergence_end_delta is not None else div_x_start
        
        if div_x_start <= window_end_sec and div_x_end >= window_start_sec:
            # Clamp to visible window
            vis_start = max(window_start_sec, div_x_start)
            vis_end = min(window_end_sec, div_x_end)
            
            if vis_end > vis_start + 1.0:
                # Wide enough to show as a shaded region
                ax.axvspan(vis_start, vis_end, alpha=0.15, color='red', label='Divergence')
                ax.axvline(x=vis_start, color='red', linestyle='--', linewidth=1.5)
                ax.axvline(x=vis_end, color='red', linestyle='--', linewidth=1.5)
            else:
                # Narrow divergence - just show a single line
                ax.axvline(x=vis_start, color='red', linestyle='--', linewidth=2, label='Divergence')
    
    # Configure axes
    ax.set_yticks(range(len(row_labels)))
    ax.set_yticklabels(row_labels)
    ax.set_xlabel('Time from analysis start', fontsize=13, fontweight='semibold', labelpad=10)
    ax.set_title(title, fontsize=17, fontweight='bold', pad=14)
    ax.set_xlim(window_start_sec, window_end_sec)
    ax.invert_yaxis()  # Put first row at top
    ax.grid(axis='x', alpha=0.3)
    ax.tick_params(axis='x', labelsize=12)
    ax.tick_params(axis='y', labelsize=11)

    for label in ax.get_yticklabels():
        label.set_fontweight('medium')

    # Format x-axis as HH:MM at clean minute intervals
    import matplotlib.ticker as mticker
    def _fmt_hhmm(x, _pos=None):
        h, rem = divmod(int(x), 3600)
        m = rem // 60
        return f'{h:02d}:{m:02d}'
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(_fmt_hhmm))
    # Set ticks at clean minute intervals
    vis_span = window_end_sec - window_start_sec
    if vis_span <= 5 * 60:
        tick_step = 60       # 1 min
    elif vis_span <= 15 * 60:
        tick_step = 5 * 60   # 5 min
    elif vis_span <= 60 * 60:
        tick_step = 10 * 60  # 10 min
    else:
        tick_step = 30 * 60  # 30 min
    first_tick = (int(window_start_sec) // tick_step + 1) * tick_step
    ticks = list(range(first_tick, int(window_end_sec) + 1, tick_step))
    if ticks:
        ax.set_xticks(ticks)
    
    plt.tight_layout()
    
    # Save if output path specified
    if output_path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        # Force a large bounding box to ensure the chart isn't shrunk
        fig.savefig(str(output_path), dpi=dpi, bbox_inches=None)
        print(f"Saved comparison chart (fixed scale) to: {output_path}")
    
    return fig


def create_multi_divergence_plots(
    timeline_a: pd.DataFrame,
    timeline_b: pd.DataFrame,
    comparison_result: ComparisonResult,
    output_dir: Union[str, Path],
    label_a: str = "Original",
    label_b: str = "Replay",
    max_plots: int = 5,
    window_minutes: float = 10.0,
    dpi: int = 150,
    time_offset_b: float = 0.0,
) -> List[str]:
    """Create up to max_plots divergence-focused Gantt charts.

    Returns list of generated file paths.
    """
    if timeline_a.empty or timeline_b.empty:
        return []

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    base_start_a = timeline_a['StartTime'].min()
    divergences = comparison_result.divergence_windows or []
    if not divergences:
        return []

    selected = sorted(
        divergences,
        key=lambda d: d.original_start_seconds_a,
    )[:max_plots]

    paths: List[str] = []
    for idx, div in enumerate(selected, start=1):
        divergence_start = base_start_a + timedelta(seconds=div.original_start_seconds_a)
        divergence_end = base_start_a + timedelta(seconds=div.original_end_seconds_a)
        output_path = output_dir / f"{comparison_result.device_id}_divergence_{idx}.png"

        fig = create_comparison_gantt_matplotlib(
            timeline_a=timeline_a,
            timeline_b=timeline_b,
            label_a=label_a,
            label_b=label_b,
            title=f"{comparison_result.device_id} Divergence {idx}",
            divergence_start=divergence_start,
            divergence_end=divergence_end,
            output_path=output_path,
            window_minutes=window_minutes,
            dpi=dpi,
            align_by_time_delta=True,
            time_offset_b=time_offset_b,
        )
        if fig is not None:
            plt.close(fig)
            paths.append(str(output_path))

    return paths


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
        print(f"  Timestamp groups in A: {result.sequence_dtw.sequence_length_a}")
        print(f"  Timestamp groups in B: {result.sequence_dtw.sequence_length_b}")
        print(f"  Alignment: trimmed {result.alignment_offset} groups "
              f"({result.alignment_trim_seconds_a:.1f}s from A, "
              f"{result.alignment_trim_seconds_b:.1f}s from B)")
        print(f"  Divergences: {len(result.divergence_windows)}")
        
        if result.divergence_windows:
            for i, div in enumerate(result.divergence_windows, 1):
                if div.description:
                    print(f"    {i}. {div.description}")
                else:
                    print(f"    {i}. A: {div.original_start_seconds_a:.1f}s - "
                          f"{div.original_end_seconds_a:.1f}s")
        
        if result.timing_stats:
            ts = result.timing_stats
            residual = ts.get('alignment_residual', 0.0)
            residual_str = f" (residual={residual:+.1f}s)" if abs(residual) > 0.05 else ""
            print(f"  Timing: jitter_std={ts['std_diff']:.3f}s, "
                  f"max={ts['max_abs_diff']:.3f}s, "
                  f"p95={ts['p95_abs_diff']:.3f}s{residual_str}")
        
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
                # Compute timeline alignment using cross-correlation
                time_offset = compute_timeline_offset(timeline_a, timeline_b)
                
                # Generate output filename
                if output_name is None:
                    output_name = f"comparison_{label_a}_vs_{label_b}".replace(' ', '_')
                
                # Default to .png if not specified
                if not output_name.lower().endswith(('.html', '.png', '.jpeg', '.jpg', '.webp', '.pdf', '.svg')):
                    output_name += ".png"
                    
                output_path = Path(output_dir) / output_name
                
                print(f"Creating Gantt chart with {len(timeline_a)} events (A) and {len(timeline_b)} events (B)...")
                print(f"Timeline alignment offset: {time_offset:.1f}s")
                
                # Use matplotlib for all outputs (reliable export, no kaleido issues)
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
                    window_minutes=window_minutes,
                    align_by_time_delta=True,
                    time_offset_b=time_offset
                )
                plt.close(fig)  # Clean up
                
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
    # Retry for DuckDB file-lock issues on network shares
    last_err = None
    for _attempt in range(1, 6):
        try:
            con = duckdb.connect(db_path)
            break
        except Exception as e:
            err_msg = str(e).lower()
            if "being used by another process" in err_msg or "ioexception" in err_msg:
                last_err = e
                import time as _time
                _time.sleep(2.0 * _attempt)
            else:
                raise
    else:
        raise last_err  # type: ignore[misc]
    
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
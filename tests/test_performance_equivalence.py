"""The vectorized analysis helpers give exactly the results of the plain loops they replaced."""

import asyncio
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

import signal_replay as sr
from signal_replay import comparison as cmp
from signal_replay import replay as replay_mod
from signal_replay import validation as val


def _reference_dtw(dist):
    """The original loop: cumulative cost, then backtracking (diagonal, up, left on ties)."""
    n, m = dist.shape
    cum = np.full((n + 1, m + 1), np.inf)
    cum[0, 0] = 0.0
    for i in range(1, n + 1):
        for j in range(1, m + 1):
            cum[i, j] = dist[i - 1, j - 1] + min(cum[i - 1, j], cum[i, j - 1], cum[i - 1, j - 1])
    path = []
    i, j = n, m
    while i > 0 or j > 0:
        path.append((i - 1, j - 1))
        if i == 0:
            j -= 1
        elif j == 0:
            i -= 1
        else:
            _, i, j = min(
                [(cum[i - 1, j - 1], i - 1, j - 1), (cum[i - 1, j], i - 1, j), (cum[i, j - 1], i, j - 1)],
                key=lambda x: x[0],
            )
    path.reverse()
    return float(cum[n, m]), path


def _reference_jaccard(groups_a, groups_b):
    dist = np.ones((len(groups_a), len(groups_b)))
    for i, (_, a) in enumerate(groups_a):
        for j, (_, b) in enumerate(groups_b):
            if a == b:
                dist[i, j] = 0.0
            else:
                union = len(a | b)
                dist[i, j] = 1.0 - len(a & b) / union if union > 0 else 1.0
    return dist


def _groups(rng, n, alphabet, allow_empty):
    out = []
    for k in range(n):
        size = int(rng.integers(0 if allow_empty else 1, 4))
        items = frozenset((int(rng.integers(1, alphabet + 1)), int(rng.integers(1, 3))) for _ in range(size))
        out.append((float(k), items))
    return out


@pytest.mark.parametrize("n,m", [(1, 1), (1, 6), (6, 1), (2, 3), (17, 13), (60, 75)])
@pytest.mark.parametrize("alphabet", [1, 2, 6, 25])
@pytest.mark.parametrize("allow_empty", [False, True])
def test_jaccard_dtw_matches_reference_loops(n, m, alphabet, allow_empty):
    rng = np.random.default_rng(n * 1000 + m * 10 + alphabet + int(allow_empty))
    ga, gb = _groups(rng, n, alphabet, allow_empty), _groups(rng, m, alphabet, allow_empty)

    ref_dist = _reference_jaccard(ga, gb)
    ref_distance, ref_path = _reference_dtw(ref_dist)
    result, match_pct, dist = cmp._dtw_with_jaccard(ga, gb)

    assert np.array_equal(dist, ref_dist)
    assert result.distance == ref_distance
    assert [tuple(p) for p in result.warping_path] == ref_path
    assert match_pct == sum(1 for i, j in ref_path if ref_dist[i, j] == 0.0) / len(ref_path) * 100


@pytest.mark.parametrize("n,m", [(1, 1), (1, 5), (5, 1), (30, 41)])
def test_categorical_and_timing_dtw_match_reference(n, m):
    rng = np.random.default_rng(n * 100 + m)
    seq_a = rng.integers(0, 3, n).astype(float)
    seq_b = rng.integers(0, 3, m).astype(float)
    ref_distance, ref_path = _reference_dtw((seq_a[:, None] != seq_b[None, :]).astype(float))
    result = cmp.compute_dtw(seq_a, seq_b, categorical=True)
    assert result.distance == ref_distance
    assert [tuple(p) for p in result.warping_path] == ref_path

    times_a, times_b = rng.random(n), rng.random(m)
    fast = cmp.compute_dtw(times_a, times_b, categorical=False)
    slow_distance = cmp.dtw.distance(times_a, times_b, use_c=False)
    slow_path = cmp.dtw.warping_path(times_a, times_b, use_c=False)
    assert fast.distance == slow_distance
    assert [tuple(p) for p in fast.warping_path] == [tuple(p) for p in slow_path]


def _reference_window_stats(rows, window_start, window_end):
    count, active = 0, 0.0
    for row in rows.itertuples(index=False):
        start, end = pd.Timestamp(row.StartTime), pd.Timestamp(row.EndTime)
        overlap_start, overlap_end = max(start, window_start), min(end, window_end)
        if overlap_end <= overlap_start:
            continue
        count += 1
        active += (overlap_end - overlap_start).total_seconds()
    return count, active, active / count if count else 0.0


def test_window_activity_stats_matches_reference_loop():
    rng = np.random.default_rng(7)
    t0 = pd.Timestamp("2026-01-05 08:00:00")
    starts = t0 + pd.to_timedelta(np.sort(rng.uniform(0, 3600, 400)), unit="s")
    ends = starts + pd.to_timedelta(rng.uniform(-2, 40, 400), unit="s")   # some zero/negative spans
    rows = pd.DataFrame({"StartTime": starts, "EndTime": ends})
    as_strings = rows.astype(str)
    for k in range(60):
        ws = t0 + pd.Timedelta(seconds=float(rng.uniform(-100, 3600)))
        we = ws + pd.Timedelta(seconds=float(rng.uniform(1, 600)))
        expected = _reference_window_stats(rows, ws, we)
        assert val._window_activity_stats(rows, window_start=ws, window_end=we) == expected
        assert val._window_activity_stats(as_strings, window_start=ws, window_end=we) == expected


def test_interruptible_sleep_does_not_accumulate_chunk_oversleep(monkeypatch):
    # Simulate a coarse timer: every asyncio.sleep oversleeps by 30 ms. The old
    # chunked sleep added that per 0.25 s chunk (1.5 s -> about 1.68 s).
    real_sleep = asyncio.sleep

    async def coarse_sleep(delay, *args, **kwargs):
        await real_sleep(delay + 0.03)

    config = sr.SignalConfig(device_id="dev", ip="127.0.0.1", udp_port=1025)
    t0 = datetime(2026, 1, 5, 8)
    config.events = pd.DataFrame(
        {"timestamp": [t0, t0 + timedelta(seconds=1)], "event_id": [82, 81], "parameter": [2, 2],
         "device_id": ["dev", "dev"]}
    )
    replay = sr.SignalReplay(config)
    monkeypatch.setattr(replay_mod.asyncio, "sleep", coarse_sleep)

    async def timed():
        start = time.monotonic()
        finished = await replay._sleep_interruptibly(1.5)
        return finished, time.monotonic() - start

    finished, elapsed = asyncio.run(timed())
    assert finished is True
    assert 1.5 <= elapsed < 1.5 + 0.03 + 0.08

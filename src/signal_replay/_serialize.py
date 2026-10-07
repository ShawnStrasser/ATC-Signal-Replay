"""JSON-safe conversion helpers shared by the result types.

``to_jsonable`` turns dataclasses, pandas/numpy scalars, timestamps and
enums into plain JSON values: timestamps become ISO-8601 strings, ``inf``
and ``NaN`` become None, tuples and sets become lists. The output always
passes ``json.dumps(..., allow_nan=False)``.
"""

from __future__ import annotations

import dataclasses
import math
from datetime import date, datetime, time, timedelta
from enum import Enum
from pathlib import PurePath
from typing import Any, Iterable, Mapping, Optional

import numpy as np
import pandas as pd


def to_jsonable(value: Any, *, drop_keys: Iterable[str] = ()) -> Any:
    """Return ``value`` converted to JSON-safe Python types.

    Args:
        value: Anything the result types hold.
        drop_keys: Field or key names left out at every level (for example
            ``warping_path``).
    """
    drop = frozenset(drop_keys)
    return _convert(value, drop)


def _convert(value: Any, drop: frozenset) -> Any:
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, Enum):
        return _convert(value.value, drop)
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        number = float(value)
        return number if math.isfinite(number) else None
    if value is pd.NaT:
        return None
    if isinstance(value, pd.Timestamp):
        return None if pd.isna(value) else value.isoformat()
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, np.datetime64):
        ts = pd.Timestamp(value)
        return None if pd.isna(ts) else ts.isoformat()
    if isinstance(value, (timedelta, pd.Timedelta)):
        return None if pd.isna(value) else value.total_seconds()
    if isinstance(value, PurePath):
        return str(value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        to_dict = getattr(value, "to_dict", None)
        if callable(to_dict) and not drop:
            return to_dict()
        return {
            f.name: _convert(getattr(value, f.name), drop)
            for f in dataclasses.fields(value)
            if f.name not in drop and not f.name.startswith("_")
        }
    if isinstance(value, Mapping):
        return {str(k): _convert(v, drop) for k, v in value.items() if str(k) not in drop}
    if isinstance(value, (list, tuple, set, frozenset)):
        items = sorted(value, key=str) if isinstance(value, (set, frozenset)) else value
        return [_convert(v, drop) for v in items]
    if isinstance(value, np.ndarray):
        return [_convert(v, drop) for v in value.tolist()]
    if isinstance(value, pd.DataFrame):
        return [_convert(row, drop) for row in value.to_dict("records")]
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return str(value)


def parse_datetime(value: Any) -> Optional[datetime]:
    """ISO-8601 string (or datetime / Timestamp) back to a naive-or-aware datetime."""
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return None if pd.isna(value) else value.to_pydatetime()
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        if not value:
            return None
        return pd.Timestamp(value).to_pydatetime()
    raise TypeError(f"Cannot parse a timestamp from {type(value).__name__}")


def float_or_none(value: Any) -> Optional[float]:
    if value is None:
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def known_fields(cls: Any, data: Mapping[str, Any]) -> dict:
    """The items of ``data`` that are init fields of dataclass ``cls``."""
    names = {f.name for f in dataclasses.fields(cls) if f.init}
    return {k: v for k, v in data.items() if k in names}

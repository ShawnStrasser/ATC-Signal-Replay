"""
Working folder for one run, owned by the calling application.

When :class:`~signal_replay.ATCSimulation` or
:class:`~signal_replay.BatchRunner` gets ``work_dir``, every file the
package writes goes under that folder with a fixed name, and nothing is
derived from the process's current directory:

=====================  ==================================================
File                   Written by
=====================  ==================================================
``replay.duckdb``      ATCSimulation: working database (see
                       :class:`~signal_replay.DatabaseManager`)
``collected.db``       BatchRunner: shared working database
``checkpoint.json``    BatchRunner.run: batch resume state
``plots/``             Comparison plots
``run.log``            Log file, only when enabled
``manifest.json``      Both: ``run_uuid``, ``package_version``,
                       ``schema_version``, ``kind``, timestamps, ``files``
=====================  ==================================================

The application records ``work_dir`` and ``run_uuid`` in its own
database, copies the results it wants (see
:func:`~signal_replay.results_to_frames`) and may then delete the folder:
the package keeps no file open once ``run()`` (or ``BatchRunner.close()``)
returns.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Union

from .collector import SCHEMA_VERSION

MANIFEST_NAME = "manifest.json"
REPLAY_DB_NAME = "replay.duckdb"
BATCH_DB_NAME = "collected.db"
CHECKPOINT_NAME = "checkpoint.json"
RUN_LOG_NAME = "run.log"
PLOTS_DIR_NAME = "plots"


def resolve_work_dir(work_dir: Union[str, os.PathLike]) -> Path:
    """Absolute path of ``work_dir`` (created if missing).

    ``os.path.abspath`` is used instead of ``Path.resolve`` so a mapped
    Windows drive is not rewritten to its UNC share.
    """
    path = Path(os.path.abspath(os.fspath(work_dir)))
    path.mkdir(parents=True, exist_ok=True)
    return path


def list_files(work_dir: Path) -> list:
    """Files under ``work_dir`` (relative, forward slashes, sorted), manifest excluded."""
    files = []
    for path in sorted(work_dir.rglob("*")):
        if path.is_file() and path.name != MANIFEST_NAME and not path.name.endswith(".wal"):
            files.append(path.relative_to(work_dir).as_posix())
    return files


def write_manifest(
    work_dir: Union[str, os.PathLike],
    *,
    run_uuid: str,
    kind: str,
    extra: Optional[Dict[str, Any]] = None,
) -> Path:
    """Write (or refresh) ``manifest.json`` in ``work_dir`` and return its path.

    Keeps ``created_at`` from an existing manifest of the same ``run_uuid``.
    """
    from . import __version__ as package_version

    folder = Path(work_dir)
    path = folder / MANIFEST_NAME
    created_at = datetime.now().isoformat(timespec="seconds")
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                previous = json.load(f)
            if previous.get("run_uuid") == run_uuid and previous.get("created_at"):
                created_at = previous["created_at"]
        except (OSError, ValueError):
            pass
    manifest: Dict[str, Any] = {
        "run_uuid": run_uuid,
        "kind": kind,
        "package_version": package_version,
        "schema_version": SCHEMA_VERSION,
        "created_at": created_at,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "files": list_files(folder),
    }
    if extra:
        manifest.update(extra)
    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, default=str, allow_nan=False)
    os.replace(tmp, path)
    return path


def read_manifest(work_dir: Union[str, os.PathLike]) -> Dict[str, Any]:
    """Contents of ``manifest.json`` in ``work_dir``."""
    with open(Path(work_dir) / MANIFEST_NAME, "r", encoding="utf-8") as f:
        return json.load(f)

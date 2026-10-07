"""
Logging helpers for signal_replay.

The package never configures logging on import. Every module logs through
``logging.getLogger(__name__)`` under the ``signal_replay`` logger, which only
has a ``NullHandler`` attached. Applications route the output wherever they
like with the standard ``logging`` module. CLI scripts and notebooks can call
:func:`enable_console_logging` once to get the familiar console output back.
"""

import contextvars
import functools
import itertools
import logging
import sys
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Callable, Dict, FrozenSet, Iterator, Optional, Tuple, Union

PACKAGE_LOGGER_NAME = "signal_replay"
DEFAULT_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"

_CONSOLE_TAG = "_signal_replay_console"
_FILE_TAG = "_signal_replay_file"


def debug_level(enabled: bool) -> int:
    """Level for messages that the legacy ``debug`` flags used to print.

    With the flag set they are logged at INFO so they show up with
    :func:`enable_console_logging` defaults; otherwise they are logged at DEBUG.
    """
    return logging.INFO if enabled else logging.DEBUG


def _package_logger() -> logging.Logger:
    return logging.getLogger(PACKAGE_LOGGER_NAME)


def enable_console_logging(
    level: int = logging.INFO,
    fmt: Optional[str] = None,
    stream: Optional[IO[str]] = None,
) -> logging.Handler:
    """Send signal_replay log records to a console stream.

    Idempotent: calling it again replaces the handler added by a previous call
    instead of stacking a second one. While active, the ``signal_replay``
    logger does not propagate to the root logger, so records are not printed
    twice when the application also configures the root logger.

    Args:
        level: Level for the ``signal_replay`` logger and the handler.
        fmt: Format string. Defaults to ``DEFAULT_FORMAT``.
        stream: Stream to write to. Defaults to ``sys.stderr``.

    Returns:
        The handler that was attached.
    """
    pkg_logger = _package_logger()
    _remove_tagged(pkg_logger, _CONSOLE_TAG)

    handler = logging.StreamHandler(stream if stream is not None else sys.stderr)
    handler.setFormatter(logging.Formatter(fmt or DEFAULT_FORMAT))
    handler.setLevel(level)
    setattr(handler, _CONSOLE_TAG, True)

    pkg_logger.addHandler(handler)
    pkg_logger.setLevel(level)
    pkg_logger.propagate = False
    return handler


def disable_console_logging() -> None:
    """Remove the handler added by :func:`enable_console_logging`.

    Restores propagation to the root logger and resets the package logger
    level so the application's logging configuration applies again.
    """
    pkg_logger = _package_logger()
    if _remove_tagged(pkg_logger, _CONSOLE_TAG):
        pkg_logger.propagate = True
        pkg_logger.setLevel(logging.NOTSET)


def _remove_tagged(target: logging.Logger, tag: str) -> bool:
    removed = False
    for handler in list(target.handlers):
        if getattr(handler, tag, False):
            target.removeHandler(handler)
            handler.close()
            removed = True
    return removed


# ---------------------------------------------------------------------------
# Run-scoped file logs
# ---------------------------------------------------------------------------
#
# Several runs may write their own run.log at the same time (one per worker
# thread in a host application). Each log_to_file scope gets an id that is
# stored in a context variable; package code copies the context into the
# threads it starts (see run_in_log_context), so a record is written only to
# the files of the scopes it was logged under.
#
# The package logger level may have to be lowered for a scope to see INFO
# records. That is done once for all active scopes (reference counted under
# a lock) and propagation to the application's handlers is replaced by a
# forwarder that only passes records at or above the level the application
# would have seen anyway.

_ACTIVE_FILE_SCOPES: "contextvars.ContextVar[FrozenSet[int]]" = contextvars.ContextVar(
    "signal_replay_log_scopes", default=frozenset()
)
_scope_lock = threading.Lock()
_scope_ids = itertools.count(1)
_scope_levels: Dict[int, int] = {}
_saved_state: Optional[Tuple[int, bool]] = None  # (level, propagate) before the first scope
_forwarder: Optional[logging.Handler] = None


def run_in_log_context(target: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap ``target`` so it runs in a copy of the caller's context.

    Use it for every thread or executor task the package starts, so records
    logged there reach the run.log of the run that started them.
    """
    context = contextvars.copy_context()

    @functools.wraps(target)
    def _runner(*args: Any, **kwargs: Any) -> Any:
        return context.run(target, *args, **kwargs)

    return _runner


class _ScopeFilter(logging.Filter):
    """Accept records logged under one log_to_file scope."""

    def __init__(self, scope_id: int):
        super().__init__()
        self.scope_id = scope_id

    def filter(self, record: logging.LogRecord) -> bool:
        scopes = _ACTIVE_FILE_SCOPES.get()
        if self.scope_id in scopes:
            return True
        # A record from a thread that did not inherit any scope can only be
        # attributed when a single scope is active.
        if not scopes:
            with _scope_lock:
                return len(_scope_levels) == 1 and self.scope_id in _scope_levels
        return False


class _ParentForwarder(logging.Handler):
    """Stand-in for propagation while the package logger level is lowered.

    Passes a record to the parent loggers' handlers only when it is at or
    above the level the package logger had before any scope lowered it, so
    the application sees exactly what it would have seen without the scope.
    """

    def __init__(self, pkg_logger: logging.Logger, saved_level: int):
        super().__init__(logging.NOTSET)
        self.pkg_logger = pkg_logger
        self.saved_level = saved_level

    def _threshold(self) -> int:
        if self.saved_level != logging.NOTSET:
            return self.saved_level
        parent = self.pkg_logger.parent
        return parent.getEffectiveLevel() if parent is not None else logging.WARNING

    def emit(self, record: logging.LogRecord) -> None:
        if record.levelno < self._threshold():
            return
        current = self.pkg_logger.parent
        while current is not None:
            for handler in current.handlers:
                if record.levelno >= handler.level:
                    handler.handle(record)
            if not current.propagate:
                break
            current = current.parent


def _apply_scope_levels(pkg_logger: logging.Logger) -> None:
    """Set the package logger level for the active scopes (lock held)."""
    global _saved_state, _forwarder
    if not _scope_levels:
        if _saved_state is not None:
            level, propagate = _saved_state
            if _forwarder is not None:
                pkg_logger.removeHandler(_forwarder)
                _forwarder = None
            pkg_logger.setLevel(level)
            pkg_logger.propagate = propagate
            _saved_state = None
        return
    if _saved_state is None:
        _saved_state = (pkg_logger.level, pkg_logger.propagate)
    saved_level, saved_propagate = _saved_state
    needed = min(_scope_levels.values())
    pkg_logger.setLevel(saved_level)
    if pkg_logger.getEffectiveLevel() <= needed:
        if _forwarder is not None:
            pkg_logger.removeHandler(_forwarder)
            _forwarder = None
        pkg_logger.propagate = saved_propagate
        return
    pkg_logger.setLevel(needed)
    if saved_propagate:
        pkg_logger.propagate = False
        if _forwarder is None:
            _forwarder = _ParentForwarder(pkg_logger, saved_level)
            pkg_logger.addHandler(_forwarder)


@contextmanager
def log_to_file(
    path: Union[str, Path],
    level: int = logging.INFO,
    fmt: Optional[str] = None,
    mode: str = "a",
) -> Iterator[logging.Handler]:
    """Copy signal_replay log records to a file for the duration of a block.

    The handler is always removed and closed on exit, so the file is not left
    locked on Windows. Only records logged by the code inside the block (and
    by the threads the package starts for it) are written, so concurrent
    runs in different threads each get their own log. If the
    ``signal_replay`` logger would drop records below ``level``, it is
    lowered while any such block is active, without letting the extra
    records reach the application's own handlers.

    Example:
        >>> with log_to_file("run.log"):
        ...     runner.run()
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    pkg_logger = _package_logger()

    handler = logging.FileHandler(path, mode=mode, encoding="utf-8")
    handler.setFormatter(logging.Formatter(fmt or DEFAULT_FORMAT))
    handler.setLevel(level)
    setattr(handler, _FILE_TAG, True)

    scope_id = next(_scope_ids)
    handler.addFilter(_ScopeFilter(scope_id))
    token = _ACTIVE_FILE_SCOPES.set(_ACTIVE_FILE_SCOPES.get() | {scope_id})
    with _scope_lock:
        _scope_levels[scope_id] = level
        _apply_scope_levels(pkg_logger)
    pkg_logger.addHandler(handler)
    try:
        yield handler
    finally:
        pkg_logger.removeHandler(handler)
        handler.close()
        with _scope_lock:
            _scope_levels.pop(scope_id, None)
            _apply_scope_levels(pkg_logger)
        try:
            _ACTIVE_FILE_SCOPES.reset(token)
        except ValueError:  # exited in a different context than entered
            _ACTIVE_FILE_SCOPES.set(_ACTIVE_FILE_SCOPES.get() - {scope_id})

"""Threads whose completion survives an interrupted join (internal)."""

import threading
from typing import Optional


class TrackedThread(threading.Thread):
    """A thread that records its own completion in an Event.

    On Python 3.12 and earlier, a KeyboardInterrupt that lands inside
    ``Thread.join()`` can leave the thread marked as stopped while it is still
    running, so ``is_alive()`` returns False too early. This class answers
    ``is_alive()`` and ``join()`` from an Event set when ``run()`` returns,
    which an interrupt cannot corrupt.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.finished = threading.Event()
        self._began = False

    def start(self) -> None:
        self._began = True
        super().start()

    def run(self) -> None:
        try:
            super().run()
        finally:
            self.finished.set()

    def is_alive(self) -> bool:
        return self._began and not self.finished.is_set()

    def join(self, timeout: Optional[float] = None) -> None:
        if not self._began:
            raise RuntimeError("cannot join thread before it is started")
        self.finished.wait(timeout)

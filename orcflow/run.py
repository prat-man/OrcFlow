import time
from copy import deepcopy

from orcflow.result import Result


class Run(Result):
    """Represent a running flow and its runtime state."""

    def __init__(self, result, runtime):
        super().__init__(result.future)
        self._runtime = runtime

        self.started = time.perf_counter()
        self.finished = None
        self.future.add_done_callback(self._finish)

    def _finish(self, future):
        self.finished = time.perf_counter()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.shutdown()

    def shutdown(self):
        """Shut down the runtime for this run."""
        self._runtime.shutdown()

    def tree(self):
        """Return a detached snapshot of the current execution tree."""
        with self._runtime.scheduler.lock:
            return deepcopy(self._runtime.root)

    def capacity(self):
        """Return the current worker and tag capacity."""
        return self._runtime.scheduler.capacity()

    def counts(self):
        """Return the current execution counts by status."""
        return self._runtime.scheduler.counts()

    def elapsed(self):
        """Return the elapsed run time in seconds."""
        end = self.finished if self.finished is not None else time.perf_counter()
        return end - self.started

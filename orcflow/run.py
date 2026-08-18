from copy import deepcopy

from orcflow.result import Result


class Run(Result):
    """Represent a running flow and its runtime state."""

    def __init__(self, result, runtime):
        super().__init__(result.future)
        self._runtime = runtime

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.shutdown()

    def shutdown(self):
        """Shut down the runtime for this run."""
        self._runtime.shutdown()

    def tree(self):
        """Return a detached snapshot of the current execution tree."""
        return deepcopy(self._runtime.root)

    def capacity(self):
        """Return the current worker and tag capacity."""
        return self._runtime.scheduler.capacity()

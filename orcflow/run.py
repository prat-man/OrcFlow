from psutil import Process
import time

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
        """Return a snapshot of the current execution tree."""
        return self._runtime.get_root()

    def workers(self):
        """Return a snapshot of pool worker status."""
        workers = self._runtime.get_workers()
        processes = {worker.pid: Process(worker.pid) for worker in workers}

        # Prime all CPU measurements.
        for process in processes.values():
            process.cpu_percent(interval=None)

        # Use one shared sampling interval for all workers.
        time.sleep(0.1)

        for worker in workers:
            process = processes[worker.pid]

            with process.oneshot():
                worker.cpu = process.cpu_percent(interval=None)
                worker.memory = process.memory_info().rss
                worker.threads = process.num_threads()

        return workers

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

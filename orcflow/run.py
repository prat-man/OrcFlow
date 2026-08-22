import atexit
import math
import time

from orcflow import utils
from orcflow.handle import Handle


class Run(Handle):
    """Represent a running flow and its runtime state."""

    def __init__(self, handle, runtime):
        super().__init__(handle._future, handle.cancel)
        self._runtime = runtime

        self.started = time.perf_counter()
        self.finished = None
        self._future.add_done_callback(self._finish)
        atexit.register(self.shutdown)
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.shutdown()

    def shutdown(self):
        """Shut down the runtime for this run."""
        self._runtime.shutdown()
        atexit.unregister(self.shutdown)

    def _finish(self, future):
        self.finished = time.perf_counter()

    def tree(self):
        """Return a snapshot of the current execution tree."""
        return self._runtime.get_root()

    def workers(self):
        """Return a snapshot of pool worker status."""
        return self._runtime.get_workers()

    def capacity(self):
        """Return the current worker and tag capacity."""
        return self._runtime.capacity()

    def counts(self):
        """Return the current execution counts by status."""
        return self._runtime.counts()

    def elapsed(self):
        """Return the elapsed run time in seconds."""
        end = self.finished if self.finished is not None else time.perf_counter()
        return end - self.started

    def status(self):
        """Return the current status of the run."""
        return self._runtime.get_root().status

    def print(self):
        """Print detailed information about the current run."""
        counts = self.counts()
        capacity = self.capacity()
        workers = self.workers()

        pid_width = 8
        node_width = 20
        progress_width = 10
        cpu_num_width = 8
        cpu_percent_width = 8
        memory_width = 12
        threads_width = 10

        table_indent = "  "

        header = (
            f"{table_indent}"
            f"{'PID':<{pid_width}}"
            f"{'NODE':<{node_width}}"
            f"{'PROGRESS':>{progress_width}}"
            f"{'CPU #':>{cpu_num_width}}"
            f"{'CPU %':>{cpu_percent_width}}"
            f"{'MEMORY':>{memory_width}}"
            f"{'THREADS':>{threads_width}}"
        )

        content_width = len(header)
        inner_width = content_width + 2

        top = f"┌{'─' * inner_width}┐"
        middle = f"├{'─' * inner_width}┤"
        bottom = f"└{'─' * inner_width}┘"

        def row(text=""):
            print(f"│ {text:<{content_width}} │")

        print(top)
        row("ORCFLOW".center(content_width))
        print(middle)

        row("Run")
        row(f"  {'Runtime':<10}{self._runtime.id}")
        row(f"  {'Elapsed':<10}{utils.format_time(self.elapsed())}")
        row(f"  {'Done':<10}{self.done()}")
        row()

        row("Tasks")
        row(f"  {'Queued':<12}{counts.queued}")
        row(f"  {'Running':<12}{counts.running}")
        row(f"  {'Finished':<12}{counts.finished}")
        row(f"  {'Failed':<12}{counts.failed}")
        row(f"  {'Cancelled':<12}{counts.cancelled}")
        row()

        row("Capacity")
        row(f"  {'Workers':<12}{capacity.workers.used} / {capacity.workers.total} used")

        if capacity.tags:
            row("  Tags")

            for tag, value in capacity.tags.items():
                row(f"    {tag:<10}{value.used} / {value.limit} used")

        row()
        row("Workers")
        if workers:
            row(header)

            for worker in workers:
                node = "-" if worker.node is None else worker.node.name
                progress = "-" if worker.node is None or worker.node.progress is None else f"{math.floor(worker.node.progress * 100)}%"

                cpu_num = worker.cpu_num()
                cpu_num = "-" if cpu_num is None else str(cpu_num)

                cpu_percent = f"{worker.cpu_percent():.1f}%"
                memory = f"{worker.memory() / 1024**2:.1f} MB"
                threads = str(worker.threads())

                row(
                    f"{table_indent}"
                    f"{worker.pid:<{pid_width}}"
                    f"{node:<{node_width}}"
                    f"{progress:>{progress_width}}"
                    f"{cpu_num:>{cpu_num_width}}"
                    f"{cpu_percent:>{cpu_percent_width}}"
                    f"{memory:>{memory_width}}"
                    f"{threads:>{threads_width}}"
                )
        else:
            row("  No active workers")

        print(bottom)

import time
from contextlib import redirect_stdout
from io import StringIO

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

    def print(self):
        """Print the current run status."""
        counts = self.counts()
        capacity = self.capacity()
        workers = self.workers()

        pid_width = 8
        node_width = 20
        cpu_num_width = 8
        cpu_percent_width = 8
        memory_width = 12
        threads_width = 10

        table_indent = "  "

        header = (
            f"{table_indent}"
            f"{'PID':<{pid_width}}"
            f"{'NODE':<{node_width}}"
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
        row(f"  {'Elapsed':<10}{self.elapsed():.2f}s")
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
        row(header)

        for worker in workers:
            node = "-" if worker.node is None else worker.node.name

            cpu_num = worker.cpu_num()
            cpu_num = "-" if cpu_num is None else str(cpu_num)

            cpu_percent = f"{worker.cpu_percent():.1f}%"
            memory = f"{worker.memory() / 1024**2:.1f} MB"
            threads = str(worker.threads())

            row(
                f"{table_indent}"
                f"{worker.pid:<{pid_width}}"
                f"{node:<{node_width}}"
                f"{cpu_num:>{cpu_num_width}}"
                f"{cpu_percent:>{cpu_percent_width}}"
                f"{memory:>{memory_width}}"
                f"{threads:>{threads_width}}"
            )

        print(bottom)

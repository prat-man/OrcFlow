from copy import deepcopy
import math
from multiprocessing import Manager, get_context

from bunch_py3 import Bunch
from coolname import generate_slug
from pebble import ProcessPool

from orcflow import utils
from orcflow.scheduler import Scheduler
from orcflow.types import Status


class Runtime:
    """Own the process pool and execution state for one or more flow runs."""

    def __init__(self, n_workers=None, concurrency=None, initializer=None, initargs=(), verbose=False):
        self.id = generate_slug(2)
        self.verbose = verbose
        self.concurrency = concurrency or {}

        for tag, limit in self.concurrency.items():
            if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
                raise ValueError(f"Concurrency limit for {tag!r} must be a positive integer")

        self.manager = Manager()
        self.n_workers = n_workers
        self.nodes = {}
        self.registry = {}
        self.scheduler = Scheduler(self)
        self._shutdown = False

        # The initializer runs once when each process-pool worker starts.
        self.ctx = get_context("spawn")
        self.pool = ProcessPool(max_workers=n_workers, initializer=initializer, initargs=initargs, context=self.ctx)

        self.scheduler.start()

        if self.verbose:
            utils.log(Status.ORCHESTRATE, None, self)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.shutdown()

    def submit(self, fn, *args, node=None, parent=None, name=None, tag=None, timeout=None, **kwargs):
        """Submit work through this runtime's scheduler."""
        return self.scheduler.submit(fn, *args, node=node, parent=parent, name=name, tag=tag, timeout=timeout, **kwargs)

    def shutdown(self):
        """Finish outstanding work and release runtime resources."""
        if self._shutdown:
            return

        self._shutdown = True

        self.scheduler.wait_idle()
        self.pool.close()
        self.pool.join()
        self.scheduler.stop()
        self.manager.shutdown()

        if self.verbose:
            utils.log(Status.SHUTDOWN, None, self)

    def workers(self):
        """Return a snapshot of the current pool workers."""
        with self.scheduler.lock:
            return [deepcopy(worker) for worker in self.registry.values()]

    def capacity(self):
        """Return the current worker and tag capacity."""
        with self.scheduler.lock:
            # Running nodes correspond to occupied process-pool workers.
            workers_used = sum(node.status is Status.RUNNING for node in self.nodes.values())

            tags = Bunch()

            # Tag capacity is tracked separately from physical worker capacity.
            for tag, limit in self.concurrency.items():
                used = self.scheduler.running.get(tag, 0)

                tags[tag] = Bunch(
                    limit=limit,
                    used=used,
                    free=limit - used,
                )

            workers_total = self.n_workers
            workers_free = None if workers_total is None else workers_total - workers_used

            return Bunch(
                workers=Bunch(
                    total=workers_total,
                    used=workers_used,
                    free=workers_free,
                ),
                tags=tags,
            )

    def counts(self):
        """Return the current execution counts by status."""
        with self.scheduler.lock:
            # Count execution nodes by their current status.
            counts = Bunch(
                queued=0,
                running=0,
                finished=0,
                failed=0,
                cancelled=0,
            )

            for node in self.nodes.values():
                if node.status is Status.QUEUED:
                    counts.queued += 1
                elif node.status is Status.RUNNING:
                    counts.running += 1
                elif node.status is Status.FINISHED:
                    counts.finished += 1
                elif node.status is Status.FAILED:
                    counts.failed += 1
                elif node.status is Status.CANCELLED:
                    counts.cancelled += 1

            return counts

    def print(self):
        """Print detailed information about the current runtime."""
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

        row("Runtime")
        row(f"  {'ID':<12}{self.id}")
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
                progress = (
                    "-"
                    if worker.node is None or worker.node.progress is None
                    else f"{math.floor(worker.node.progress * 100)}%"
                )

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

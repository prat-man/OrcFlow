import math
import os
from multiprocessing import Manager, get_context
from numbers import Real
import time

from bunch_py3 import Bunch
from coolname import generate_slug
from pebble import ProcessPool
from psutil import NoSuchProcess

from orcflow import utils
from orcflow.monitoring import NodeInfo, WorkerInfo
from orcflow.scheduler import Scheduler
from orcflow.types import Status


class Runtime:
    """Own the process pool and execution state for one or more flow runs."""

    def __init__(self, n_workers=None, concurrency=None, initializer=None, initargs=(), verbose=False):
        if n_workers is None:
            n_workers = os.cpu_count() or 1
        if not isinstance(n_workers, int) or isinstance(n_workers, bool) or n_workers < 1:
            raise ValueError("n_workers must be a positive integer")

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

    def submit(self, fn, args, kwargs, *, node=None, parent=None, name=None, tag=None, timeout=None):
        """Keep function arguments separate from scheduling options."""
        return self.scheduler.submit(fn, args, kwargs, node=node, parent=parent, name=name, tag=tag, timeout=timeout)

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

    def workers(self, *, interval=0.2):
        """Return immutable worker snapshots after one shared CPU sampling wait.

        interval must be a finite positive number of seconds. CPU usage
        is measured over the actual elapsed interval, independently of previous
        calls. Workers that leave the runtime during sampling are omitted;
        workers that start during sampling are included on the next call.
        """
        if (
            isinstance(interval, bool)
            or not isinstance(interval, Real)
            or not math.isfinite(interval)
            or interval <= 0
        ):
            raise ValueError("interval must be a finite positive number of seconds")

        with self.scheduler.lock:
            workers = list(self.registry.values())

        starts = []
        for worker in workers:
            try:
                started = time.perf_counter()
                starts.append((worker, worker.cpu_time(), started))
            except NoSuchProcess:
                continue

        if not starts:
            return []

        # Sample every process over the same wait without holding scheduler state.
        time.sleep(interval)

        snapshots = []
        for worker, cpu_start, started in starts:
            try:
                cpu_end = worker.cpu_time()
                elapsed = time.perf_counter() - started
                timestamp = time.time()
                cpu_num = worker.cpu_num()
                memory = worker.memory()
                threads = worker.threads()
            except NoSuchProcess:
                continue

            with self.scheduler.lock:
                current = self.registry.get(worker.pid)
                if current is None or current._process != worker._process:
                    continue
                node = current.node
                node_info = None if node is None else NodeInfo(
                    id=node.id,
                    name=node.name,
                    type=node.type,
                    status=node.status,
                    progress=node.progress,
                )

            cpu_percent = (
                max(0.0, cpu_end - cpu_start) / elapsed * 100.0
                if elapsed > 0 else 0.0
            )
            snapshots.append(WorkerInfo(
                pid=worker.pid,
                node=node_info,
                cpu_num=cpu_num,
                cpu_percent=cpu_percent,
                memory=memory,
                threads=threads,
                interval=elapsed,
                timestamp=timestamp,
            ))

        return snapshots

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
            workers_free = workers_total - workers_used

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

    def print(self, *, interval=0.2):
        """Print detailed information about the current runtime."""
        workers = self.workers(interval=interval)
        counts = self.counts()
        capacity = self.capacity()

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

                cpu_num = worker.cpu_num
                cpu_num = "-" if cpu_num is None else str(cpu_num)

                cpu_percent = f"{worker.cpu_percent:.1f}%"
                memory = f"{worker.memory / 1024**2:.1f} MB"
                threads = str(worker.threads)

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

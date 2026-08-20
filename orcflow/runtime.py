from copy import deepcopy
from multiprocessing import Manager

from bunch_py3 import Bunch
from coolname import generate_slug
from pebble import ProcessPool

from orcflow import utils
from orcflow.scheduler import Scheduler
from orcflow.types import Status


class Runtime:
    """Own the process pool and execution state for one flow run."""

    def __init__(self, n_workers=None, concurrency=None, initializer=None, initargs=(), verbose=False):
        self.id = generate_slug(2)
        self.verbose = verbose
        self.concurrency = concurrency or {}

        for tag, limit in self.concurrency.items():
            if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
                raise ValueError(f"Concurrency limit for {tag!r} must be a positive integer")

        self.root = None
        self.manager = Manager()
        self.n_workers = n_workers
        self.nodes = {}
        self.workers = {}
        self.scheduler = Scheduler(self)

        # The initializer runs once when each process-pool worker starts.
        self.pool = ProcessPool(max_workers=n_workers, initializer=initializer, initargs=initargs)

        self.scheduler.start()

        if self.verbose:
            utils.log(Status.ORCHESTRATE, None, self)

    def run(self, fn, *args, node=None, parent=None, name=None, tag=None, **kwargs):
        """Submit work through this runtime's scheduler."""
        return self.scheduler.submit(fn, *args, node=node, parent=parent, name=name, tag=tag, **kwargs)

    def shutdown(self):
        """Finish outstanding work and release runtime resources."""
        self.pool.close()
        self.pool.join()
        self.scheduler.stop()
        self.manager.shutdown()

        if self.verbose:
            utils.log(Status.SHUTDOWN, None, self)

    def get_workers(self):
        """Return a snapshot of the current pool workers."""
        with self.scheduler.lock:
            return [deepcopy(worker) for worker in self.workers.values()]

    def get_root(self):
        """Return a snapshot of the execution root node."""
        with self.scheduler.lock:
            return deepcopy(self.root)

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

            return Bunch(
                workers=Bunch(
                    total=self.n_workers,
                    used=workers_used,
                    free=self.n_workers - workers_used,
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

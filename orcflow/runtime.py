import uuid
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager

from orcflow import utils
from orcflow.scheduler import Scheduler
from orcflow.types import Status
from orcflow.worker import Worker


class Runtime:
    """Own the process pool and execution state for one flow run."""

    def __init__(self, workers=None, concurrency=None, verbose=False):
        self.id = uuid.uuid4().hex[:8]
        self.verbose = verbose
        self.concurrency = concurrency or {}

        for tag, limit in self.concurrency.items():
            if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
                raise ValueError(f"Concurrency limit for {tag!r} must be a positive integer")

        self.root = None
        self.manager = Manager()
        self.pool = ProcessPoolExecutor(max_workers=workers)
        self.nodes = {}
        self.scheduler = Scheduler(self)

        # Every process receives its own copy of this lightweight worker interface.
        self.worker = Worker(self.id, self.verbose, self.scheduler.requests)
        self.scheduler.worker = self.worker
        self.scheduler.start()

        if self.verbose:
            utils.log(Status.ORCHESTRATE, None, self)

    def run(self, fn, *args, node=None, parent=None, name=None, tag=None, **kwargs):
        """Submit work through this runtime's scheduler."""
        return self.scheduler.submit(fn, *args, node=node, parent=parent, name=name, tag=tag, **kwargs)

    def shutdown(self):
        """Finish outstanding work and release runtime resources."""
        self.pool.shutdown()
        self.scheduler.stop()
        self.manager.shutdown()

        if self.verbose:
            utils.log(Status.SHUTDOWN, None, self)

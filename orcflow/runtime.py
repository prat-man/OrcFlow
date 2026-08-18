import uuid
from copy import deepcopy
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager

from bunch_py3 import Bunch

from orcflow import utils
from orcflow.scheduler import Scheduler
from orcflow.types import Status
from orcflow.worker import Worker


class Runtime:
    """Own the process pool and execution state for one flow run."""

    def __init__(self, workers=None, concurrency=None, initializer=None, initargs=(), verbose=False):
        self.id = uuid.uuid4().hex[:8]
        self.verbose = verbose
        self.concurrency = concurrency or {}

        for tag, limit in self.concurrency.items():
            if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
                raise ValueError(f"Concurrency limit for {tag!r} must be a positive integer")

        self.root = None
        self.manager = Manager()
        self.workers = workers
        self.nodes = {}
        self.processes = {}
        self.scheduler = Scheduler(self)

        # The initializer runs once when each process-pool worker starts.
        self.pool = ProcessPoolExecutor(max_workers=workers, initializer=initializer, initargs=initargs)

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

    def get_workers(self):
        """Return a snapshot of pool workers and their current nodes."""
        with self.scheduler.lock:
            workers = []

            for pid, node_id in self.processes.items():
                worker = Bunch()
                workers.append(worker)

                worker.pid = pid

                if node_id is not None:
                    node = self.nodes[node_id]

                    worker.node = Bunch()
                    worker.node.id = node.id
                    worker.node.name = node.name
                    worker.node.type = node.type
                    worker.node.status = node.status

                else:
                    worker.node = None

            return workers

    def get_root(self):
        """Return a snapshot of the execution root node."""
        with self.scheduler.lock:
            return deepcopy(self.root)

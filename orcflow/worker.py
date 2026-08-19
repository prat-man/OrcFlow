from copy import deepcopy
import importlib
import os
import sys
from multiprocessing import Pipe
from psutil import Process

from orcflow import utils
from orcflow.result import Result, WorkerFuture
from orcflow.types import Request, Status
from orcflow.task import Task, bind_tasks


class Worker:
    """Represent a pool worker process."""

    def __init__(self, pid):
        self.pid = pid
        self.node = None

        self._process = Process(self.pid)
        self.cpu_percent()

    def __deepcopy__(self, memo):
        worker = Worker(self.pid)
        memo[id(self)] = worker
        worker.node = deepcopy(self.node, memo)
        return worker

    def cpu_num(self):
        """Return the current logical CPU number, if available."""
        if hasattr(self._process, "cpu_num"):
            return self._process.cpu_num()
        return None

    def cpu_percent(self, interval=None):
        """Return the current CPU usage percentage."""
        return self._process.cpu_percent(interval=interval)

    def memory(self):
        """Return the current resident memory usage in bytes."""
        return self._process.memory_info().rss

    def threads(self):
        """Return the current number of OS threads."""
        return self._process.num_threads()


class Client:
    """Provide worker processes with access to the parent scheduler."""

    def __init__(self, id, verbose, requests):
        self.id = id
        self.verbose = verbose
        self.requests = requests

    def run(self, fn, *args, parent=None, name=None, tag=None, **kwargs):
        """Ask the parent scheduler to run a nested task."""
        reference = fn.__module__, fn.__qualname__

        # Each nested task gets a private one-way reply pipe.
        receiver, sender = Pipe(duplex=False)
        self.requests.put((Request.SUBMIT, reference, args, kwargs, parent, name, tag, sender))
        return Result(WorkerFuture(receiver, sender))

    def set_running(self, node_id):
        """Tell the scheduler that this node has started running."""
        self.requests.put((Status.RUNNING, node_id, os.getpid()))


def resolve(reference):
    """Resolve an importable function reference inside a worker process."""
    module_name, qualname = reference

    # Spawned workers load the original script as __mp_main__.
    if module_name == "__main__" and "__mp_main__" in sys.modules:
        module = sys.modules["__mp_main__"]
    else:
        module = importlib.import_module(module_name)

    value = module

    for part in qualname.split("."):
        if part == "<locals>":
            raise TypeError("Flows and tasks must be defined at module scope when using ProcessPoolExecutor")
        value = getattr(value, part)

    # Decorators replace the module-level function with its OrcFlow wrapper.
    if isinstance(value, Task):
        return value.fn

    if hasattr(value, "fn"):
        return value.fn

    return value


def execute(reference, args, kwargs, client, node_id, name):
    """Resolve and execute one flow or task inside a worker process."""
    fn = resolve(reference)
    fn = bind_tasks(fn, client, node_id)

    client.set_running(node_id)

    if client.verbose:
        utils.log(Status.RUNNING, name, client)

    try:
        value = fn(*args, **kwargs)
    except Exception:
        if client.verbose:
            utils.log(Status.FAILED, name, client)
        raise

    if client.verbose:
        utils.log(Status.FINISHED, name, client)

    return value

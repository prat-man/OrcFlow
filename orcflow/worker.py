import importlib
import sys
import uuid
from multiprocessing import Pipe

from orcflow import utils
from orcflow.result import Result, WorkerFuture
from orcflow.types import Request, Status
from orcflow.task import Task, bind_tasks


class Worker:
    """Provide worker processes with access to the parent scheduler."""

    def __init__(self, id, verbose, requests):
        self.id = id
        self.verbose = verbose
        self.requests = requests

    def run(self, fn, *args, parent=None, name=None, tag=None, **kwargs):
        """Ask the parent scheduler to run a nested task."""
        node_id = uuid.uuid4().hex
        reference = fn.__module__, fn.__qualname__

        # Each nested task gets a private one-way reply pipe.
        receiver, sender = Pipe(duplex=False)
        self.requests.put((Request.SUBMIT, node_id, reference, args, kwargs, parent, name, tag, sender))
        return Result(WorkerFuture(receiver, sender))

    def set_running(self, node_id):
        """Tell the scheduler that this node has started running."""
        self.requests.put((Status.RUNNING, node_id))


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


def execute(reference, args, kwargs, worker, node_id, name):
    """Resolve and execute one flow or task inside a worker process."""
    fn = resolve(reference)
    fn = bind_tasks(fn, worker, node_id)

    worker.set_running(node_id)

    if worker.verbose:
        utils.log(Status.RUNNING, name, worker)

    try:
        value = fn(*args, **kwargs)
    except Exception:
        if worker.verbose:
            utils.log(Status.FAILED, name, worker)
        raise

    if worker.verbose:
        utils.log(Status.FINISHED, name, worker)

    return value

from copy import deepcopy
import os
from multiprocessing import Pipe
import uuid

from psutil import Process

from orcflow import utils
from orcflow.handle import Handle, ResultProxy
from orcflow.serialization import deserialize_call, serialize_call
from orcflow.types import Request, Status
from orcflow.task import bind_tasks


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

    def run(self, fn, *args, parent=None, name=None, tag=None, timeout=None, **kwargs):
        """Ask the parent scheduler to run a nested task."""
        payload = serialize_call(fn, args, kwargs)

        # Generate a new node ID.
        node_id = uuid.uuid4().hex

        # Each nested task gets a private one-way reply pipe.
        receiver, sender = Pipe(duplex=False)
        self.requests.put((Request.SUBMIT, payload, node_id, parent, name, tag, timeout, sender))
        return Handle(ResultProxy(receiver), lambda: self.cancel(node_id))

    def set_running(self, node_id):
        """Tell the scheduler that this node has started running."""
        self.requests.put((Status.RUNNING, node_id, os.getpid()))

    def cancel(self, node_id):
        """Ask the parent scheduler to cancel a nested task."""
        self.requests.put((Request.CANCEL, node_id))

    def progress(self, node_id, value):
        """Report progress for a running node."""
        if value is not None and not 0 <= value <= 1:
            raise ValueError("Progress must be None or between 0 and 1.")
        self.requests.put((Request.PROGRESS, node_id, value))



def execute(payload, client, node_id, name):
    """Deserialize and execute one flow or task inside a worker process."""
    fn, args, kwargs = deserialize_call(payload)
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

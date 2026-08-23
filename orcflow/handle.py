from concurrent.futures import CancelledError
from multiprocessing.connection import wait
from threading import Lock

from orcflow.types import Status


class Handle:
    """Expose a small, common interface for task and flow results."""

    def __init__(self, proxy, cancel):
        self._proxy = proxy
        self._cancel = cancel

    def result(self):
        """Wait for and return the result."""
        return self._proxy.result()

    def done(self):
        """Return whether the result is ready."""
        return self._proxy.done()

    def cancel(self):
        """Request cancellation of the underlying work."""
        self._cancel()

    def exception(self):
        """Return the exception raised by the work, if any."""
        return self._proxy.exception()


class GroupHandle:
    """Represent a group of results as one result."""

    def __init__(self, handles):
        self._handles = handles

    def results(self):
        """Wait for and return all results in submission order, failing fast on error."""
        proxies = [handle._proxy for handle in self._handles]
        pending = set(proxies)

        values = [None] * len(proxies)
        index = {proxy: i for i, proxy in enumerate(proxies)}

        while pending:
            ready = wait(pending)

            for proxy in ready:
                pending.remove(proxy)
                values[index[proxy]] = proxy.result()

        return values

    def done(self):
        """Return whether every result is ready."""
        return all(handle.done() for handle in self._handles)

    def cancel(self):
        """Request cancellation of all underlying work."""
        for handle in self._handles:
            handle.cancel()

    def exception(self):
        """Wait for a failure and return its exception, ignoring cancellations, or None if none failed."""
        proxies = [handle._proxy for handle in self._handles]
        pending = set(proxies)

        while pending:
            ready = wait(pending)

            for proxy in ready:
                pending.remove(proxy)

                try:
                    exception = proxy.exception()
                except CancelledError:
                    continue

                if exception is not None:
                    return exception

        return None

    def exceptions(self):
        """Wait for all results to complete and return the exceptions raised by any that failed, ignoring cancellations."""
        exceptions = []

        for handle in self._handles:
            try:
                exception = handle.exception()
            except CancelledError:
                continue

            if exception is not None:
                exceptions.append(exception)

        return exceptions


class Run(Handle):
    """Represent a flow execution."""

    def __init__(self, handle, root):
        super().__init__(handle._proxy, handle.cancel)
        self._root = root

    def tree(self):
        """Return a snapshot of the current execution tree."""
        return self._root

    def elapsed(self):
        """Return the elapsed run time in seconds."""
        return self._root.elapsed()

    def status(self):
        """Return the current execution status."""
        return self._root.status


class ResultProxy:
    """Receive a scheduler-owned execution result through a reply channel."""

    def __init__(self, receiver):
        self.receiver = receiver
        self._resolved = False
        self._status = None
        self._value = None
        self._lock = Lock()

    def result(self):
        """Wait for and return the execution result."""
        status, value = self._wait()

        if status is Status.FAILED:
            raise value
        if status is Status.CANCELLED:
            raise CancelledError()

        return value

    def done(self):
        """Return whether a reply has arrived."""
        with self._lock:
            return self._resolved or self.receiver.poll()

    def exception(self):
        """Wait for and return the execution exception, if any."""
        status, value = self._wait()

        if status is Status.FAILED:
            return value
        if status is Status.CANCELLED:
            raise CancelledError()

        return None

    def _wait(self):
        # Only one thread may consume the reply; later callers use the cached value.
        with self._lock:
            if not self._resolved:
                try:
                    self._status, self._value = self.receiver.recv()
                    self._resolved = True
                finally:
                    self.receiver.close()

        return self._status, self._value

    def fileno(self):
        """Return the underlying reply connection handle."""
        return self.receiver.fileno()

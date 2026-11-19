from concurrent.futures import CancelledError
from copy import deepcopy
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
        self._results_resolved = False
        self._results_value = None
        self._results_error = None
        self._exception_resolved = False
        self._exception_value = None
        self._lock = Lock()

    def results(self):
        """Wait for and return all results in submission order, failing fast on error."""
        with self._lock:
            if not self._results_resolved:
                try:
                    self._results_value = self._collect_results()
                except Exception as exc:
                    self._results_error = exc
                self._results_resolved = True

            if self._results_error is not None:
                raise self._results_error

            return list(self._results_value)

    def _collect_results(self):
        proxies = [handle._proxy for handle in self._handles]
        pending = {proxy for proxy in proxies if not proxy.done()}

        while pending:
            for proxy in wait(pending):
                pending.remove(proxy)
                proxy.result()

        return [handle.result() for handle in self._handles]

    def done(self):
        """Return whether every result is ready."""
        return all(handle.done() for handle in self._handles)

    def cancel(self):
        """Request cancellation of all underlying work."""
        for handle in self._handles:
            handle.cancel()

    def exception(self):
        """Wait for a failure and return its exception, ignoring cancellations, or None if none failed."""
        with self._lock:
            if not self._exception_resolved:
                self._exception_value = self._collect_exception()
                self._exception_resolved = True

            return self._exception_value

    def _collect_exception(self):
        proxies = [handle._proxy for handle in self._handles]
        pending = set()

        for proxy in proxies:
            if not proxy.done():
                pending.add(proxy)
                continue

            try:
                exception = proxy.exception()
            except CancelledError:
                continue

            if exception is not None:
                return exception

        while pending:
            for proxy in wait(pending):
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
        with self._lock:
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

    def __init__(self, handle, runtime, root):
        super().__init__(handle._proxy, handle.cancel)
        self._runtime = runtime
        self._root = root

    def tree(self):
        """Return a synchronized snapshot of this run's execution tree."""
        with self._runtime.scheduler.lock:
            return deepcopy(self._root)

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
        self._error = None
        self._lock = Lock()
        # Polling and closing share a short lock, separate from blocking reads.
        self._state_lock = Lock()

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
        with self._state_lock:
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
                except Exception as exc:
                    self._error = exc
                    raise
                finally:
                    with self._state_lock:
                        self._resolved = True
                        self.receiver.close()

            if self._error is not None:
                raise self._error

        return self._status, self._value

    def fileno(self):
        """Return the underlying reply connection handle."""
        return self.receiver.fileno()

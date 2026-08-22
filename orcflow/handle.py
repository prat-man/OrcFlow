from concurrent.futures import CancelledError, Future
from multiprocessing.connection import wait

from orcflow.types import Status


class Handle:
    """Expose a small, common interface for task and flow results."""

    def __init__(self, future, cancel):
        self._future = future
        self._cancel = cancel

    def result(self):
        """Wait for and return the result."""
        return self._future.result()

    def done(self):
        """Return whether the result is ready."""
        return self._future.done()

    def cancel(self):
        """Request cancellation of the underlying work."""
        self._cancel()

    def exception(self):
        """Return the exception raised by the work, if any."""
        return self._future.exception()


class GroupHandle:
    """Represent a group of results as one result."""

    def __init__(self, handles):
        self._handles = handles

    def results(self):
        """Wait for and return all results in submission order, failing fast on error."""
        futures = [handle._future for handle in self._handles]
        pending = set(futures)

        values = [None] * len(futures)
        index = {future: i for i, future in enumerate(futures)}

        while pending:
            ready = wait(pending)

            for future in ready:
                pending.remove(future)
                values[index[future]] = future.result()

        return values

    def done(self):
        """Return whether every result is ready."""
        return all(handle.done() for handle in self._handles)

    def cancel(self):
        """Request cancellation of all underlying work."""
        for handle in self._handles:
            handle.cancel()

    def exception(self):
        """Wait for a failure and return its exception, or None if all results succeed."""
        futures = [handle._future for handle in self._handles]
        pending = set(futures)

        while pending:
            ready = wait(pending)

            for future in ready:
                pending.remove(future)
                exception = future.exception()

                if exception is not None:
                    return exception

        return None

    def exceptions(self):
        """Wait for all results to complete and return all exceptions they raised."""
        return [
            exception
            for handle in self._handles
            if (exception := handle.exception()) is not None
        ]


class WorkerFuture(Future):
    """Wait for a nested task result sent back to this worker."""

    def __init__(self, receiver, sender):
        self.receiver = receiver
        self.sender = sender
        self._resolved = False
        self._status = None
        self._value = None

    def result(self):
        """Wait for and return the nested task result."""
        status, value = self._wait()

        if status is Status.FAILED:
            raise value
        if status is Status.CANCELLED:
            raise CancelledError()

        return value

    def done(self):
        """Return whether a reply has arrived."""
        return self._resolved or self.receiver.poll()

    def cancel(self):
        """Nested worker results cannot currently be cancelled."""
        return False

    def exception(self):
        """Wait for and return the nested task exception, if any."""
        status, value = self._wait()

        if status is Status.FAILED:
            return value
        if status is Status.CANCELLED:
            raise CancelledError()

        return None

    def _wait(self):
        # The pipe blocks here until the scheduler sends this task's reply.
        if not self._resolved:
            self._status, self._value = self.receiver.recv()
            self.receiver.close()
            self.sender.close()
            self._resolved = True

        return self._status, self._value

    def fileno(self):
        """Return the underlying reply connection handle."""
        return self.receiver.fileno()

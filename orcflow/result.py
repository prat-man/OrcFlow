from concurrent.futures import CancelledError

from orcflow.types import Status


class Result:
    """Expose a small, common interface for task and flow results."""

    def __init__(self, future):
        self.future = future

    def result(self):
        """Wait for and return the result."""
        return self.future.result()

    def done(self):
        """Return whether the result is ready."""
        return self.future.done()

    def cancel(self):
        """Try to cancel the underlying work."""
        return self.future.cancel()

    def exception(self):
        """Return the exception raised by the work, if any."""
        return self.future.exception()


class WorkerFuture:
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

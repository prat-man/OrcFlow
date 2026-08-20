import threading
from functools import partial

from pebble import ProcessFuture

from orcflow import utils
from orcflow.node import NodeType
from orcflow.types import Request, Status
from orcflow.result import Result
from orcflow.worker import Client, Worker, execute


class Scheduler:
    """Accept work, schedule it, and track its execution state."""

    def __init__(self, runtime):
        self.runtime = runtime
        self.requests = runtime.manager.Queue()
        self.futures = {}
        self.pending = []
        self.running = {}
        self.client = Client(runtime.id, runtime.verbose, self.requests)
        self.lock = threading.RLock()

        # Worker requests are handled without blocking the main thread.
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        """Start listening for worker requests."""
        self.thread.start()

    def stop(self):
        """Stop the scheduler thread cleanly."""
        self.requests.put((Request.STOP,))
        self.thread.join()

    def submit(self, fn, *args, node=None, parent=None, name=None, tag=None, **kwargs):
        """Accept a flow or task for execution."""
        if node is None:
            node = self.runtime.nodes[parent].add(name, NodeType.TASK)

        # Workers resolve the function from its module and qualified name.
        reference = fn.__module__, fn.__qualname__

        # This future exists even while the work is waiting in the OrcFlow queue.
        future = ProcessFuture()

        self._queue(reference, args, kwargs, node, tag, future)
        return Result(future)

    def _queue(self, reference, args, kwargs, node, tag=None, future=None, reply=None):
        """Queue work until its concurrency limit allows it to run."""
        with self.lock:
            self.runtime.nodes[node.id] = node
            node.status = Status.QUEUED

            if self.runtime.verbose:
                utils.log(Status.QUEUED, node.name, self.runtime)

            work = reference, args, kwargs, node, tag, future, reply

            if self._can_schedule(tag):
                self._schedule(*work)
            else:
                self.pending.append(work)

    def _can_schedule(self, tag):
        """Return whether a tag currently has capacity."""
        if tag is None or tag not in self.runtime.concurrency:
            return True

        return self.running.get(tag, 0) < self.runtime.concurrency[tag]

    def _schedule(self, reference, args, kwargs, node, tag=None, result=None, reply=None):
        """Put queued work onto the process pool."""
        if tag in self.runtime.concurrency:
            self.running[tag] = self.running.get(tag, 0) + 1

        try:
            future = self.runtime.pool.schedule(execute, args=(reference, args, kwargs, self.client, node.id, node.name))
        except Exception as exc:
            print(exc)
            node.status = Status.FAILED
            self._release(tag)

            if result is not None:
                result.set_exception(exc)

            if reply is not None:
                reply.send((Status.FAILED, exc))
                reply.close()

            self._schedule_pending()
            return

        self.futures[node.id] = future

        # Keep this task's tag and result channels attached to its future.
        future.add_done_callback(partial(self._complete, node.id, tag, result, reply))

    def _complete(self, node_id, tag, result, reply, future):
        """Record completion and release the task's concurrency slot."""
        with self.lock:
            self.futures.pop(node_id, None)
            node = self.runtime.nodes[node_id]

            for worker in self.runtime.workers.values():
                if worker.node is node:
                    worker.node = None
                    break

            try:
                if future.cancelled():
                    node.status = Status.CANCELLED

                    if result is not None:
                        result.cancel()

                    if reply is not None:
                        try:
                            reply.send((Status.CANCELLED, None))
                        except (BrokenPipeError, EOFError, OSError):
                            pass

                    return

                exc = future.exception()

                if exc is None:
                    value = future.result()
                    node.status = Status.FINISHED

                    if result is not None:
                        result.set_result(value)

                    if reply is not None:
                        try:
                            reply.send((Status.FINISHED, value))
                        except (BrokenPipeError, EOFError, OSError):
                            pass

                else:
                    node.status = Status.FAILED

                    if result is not None:
                        result.set_exception(exc)

                    if reply is not None:
                        try:
                            reply.send((Status.FAILED, exc))
                        except (BrokenPipeError, EOFError, OSError):
                            pass
                        except Exception:
                            try:
                                reply.send((Status.FAILED, RuntimeError(str(exc))))
                            except (BrokenPipeError, EOFError, OSError):
                                pass

            finally:
                self._cancel_children(node)

                if reply is not None:
                    reply.close()

                self._release(tag)
                self._schedule_pending()

    def _cancel(self, node):
        """Cancel a node and its unfinished descendants."""
        if node.status in {
            Status.FINISHED,
            Status.FAILED,
            Status.CANCELLED,
        }:
            return

        self._cancel_children(node)

        future = self.futures.get(node.id)

        if future is not None:
            future.cancel()

    def _cancel_children(self, node):
        """Cancel all unfinished children."""
        for child in node.children:
            if child.status not in {
                Status.FINISHED,
                Status.FAILED,
                Status.CANCELLED,
            }:
                self._cancel(child)

    def _release(self, tag):
        """Release one running slot for a limited tag."""
        if tag in self.runtime.concurrency:
            self.running[tag] -= 1

    def _schedule_pending(self):
        """Schedule queued work whose tag now has capacity."""
        for work in list(self.pending):
            tag = work[4]

            if not self._can_schedule(tag):
                continue

            self.pending.remove(work)
            self._schedule(*work)

    def _run(self):
        """Handle messages sent back by worker processes."""
        while True:
            # Block here until a worker or shutdown sends a message.
            message = self.requests.get()
            kind = message[0]

            if kind == Request.STOP:
                return

            if kind is Status.RUNNING:
                _, node_id, pid = message

                with self.lock:
                    node = self.runtime.nodes[node_id]
                    worker = self.runtime.workers.setdefault(pid, Worker(pid))

                    if node.status is Status.QUEUED:
                        node.status = Status.RUNNING
                        worker.node = node

                continue

            if kind == Request.SUBMIT:
                _, reference, args, kwargs, parent_id, name, tag, reply = message

                # The real tree lives in the parent process.
                parent = self.runtime.nodes[parent_id]
                node = parent.add(name, NodeType.TASK)

                self._queue(reference, args, kwargs, node, tag, reply=reply)

from concurrent.futures import Future
from multiprocessing import Pipe
from queue import Queue

from orcflow.handle import ResultProxy
from orcflow.node import Node, NodeType
from orcflow.scheduler import Scheduler
from orcflow.serialization import serialize_call
from orcflow.types import Status


class FakeManager:
    def Queue(self):
        return Queue()


class FakePool:
    def __init__(self):
        self.calls = []
        self.futures = []

    def schedule(self, fn, args=(), timeout=None):
        future = Future()
        self.calls.append((fn, args, timeout))
        self.futures.append(future)
        return future


class FakeRuntime:
    def __init__(self, concurrency=None):
        self.id = "test-runtime"
        self.verbose = False
        self.manager = FakeManager()
        self.concurrency = concurrency or {}
        self.nodes = {}
        self.workers = {}
        self.pool = FakePool()


def make_scheduler(concurrency=None):
    runtime = FakeRuntime(concurrency)
    scheduler = Scheduler(runtime)
    runtime.scheduler = scheduler
    return scheduler, runtime


def dummy():
    return None


def queue_work(scheduler, runtime, name, tag=None):
    node = Node(name, NodeType.TASK)
    receiver, sender = Pipe(duplex=False)
    proxy = ResultProxy(receiver)
    scheduler._queue(serialize_call(dummy, (), {}), node, sender, tag)
    return node, proxy


def test_unlimited_work_schedules_immediately():
    scheduler, runtime = make_scheduler()
    node, _ = queue_work(scheduler, runtime, "a")

    assert node.id in scheduler.futures
    assert scheduler.pending == []
    assert len(runtime.pool.calls) == 1


def test_tag_limit_keeps_extra_work_pending():
    scheduler, runtime = make_scheduler({"cpu": 1})
    first, _ = queue_work(scheduler, runtime, "first", "cpu")
    second, _ = queue_work(scheduler, runtime, "second", "cpu")

    assert first.id in scheduler.futures
    assert second.id not in scheduler.futures
    assert [work[1] for work in scheduler.pending] == [second]
    assert scheduler.running["cpu"] == 1


def test_pending_work_runs_when_tag_is_released():
    scheduler, runtime = make_scheduler({"cpu": 1})
    first, first_proxy = queue_work(scheduler, runtime, "first", "cpu")
    second, _ = queue_work(scheduler, runtime, "second", "cpu")

    runtime.pool.futures[0].set_result("done")

    assert first_proxy.result() == "done"
    assert first.status is Status.FINISHED
    assert second.id in scheduler.futures
    assert scheduler.pending == []
    assert scheduler.running["cpu"] == 1


def test_pending_cancellation_removes_work_without_consuming_tag():
    scheduler, runtime = make_scheduler({"cpu": 1})
    first, _ = queue_work(scheduler, runtime, "first", "cpu")
    second, second_proxy = queue_work(scheduler, runtime, "second", "cpu")

    scheduler._cancel(second)

    assert second_proxy.done()
    assert second.status is Status.CANCELLED
    assert second.finished is not None
    assert second.started is None
    assert second.id not in scheduler.futures
    assert all(work[1] is not second for work in scheduler.pending)
    assert scheduler.running["cpu"] == 1
    assert first.id in scheduler.futures


def test_cancelling_pending_work_allows_next_pending_item_to_advance():
    scheduler, runtime = make_scheduler({"cpu": 1})
    _, _ = queue_work(scheduler, runtime, "first", "cpu")
    second, _ = queue_work(scheduler, runtime, "second", "cpu")
    third, _ = queue_work(scheduler, runtime, "third", "cpu")

    scheduler._cancel(second)
    runtime.pool.futures[0].set_result("done")

    assert second.status is Status.CANCELLED
    assert third.id in scheduler.futures
    assert scheduler.pending == []


def test_running_cancellation_delegates_to_process_future():
    scheduler, runtime = make_scheduler()
    node, proxy = queue_work(scheduler, runtime, "a")
    future = scheduler.futures[node.id]

    scheduler._cancel(node)

    assert future.cancelled()
    assert proxy.done()
    assert node.status is Status.CANCELLED
    assert node.finished is not None


def test_cancellation_propagates_to_unfinished_descendants():
    scheduler, runtime = make_scheduler({"cpu": 1})
    parent = Node("parent", NodeType.TASK)
    runtime.nodes[parent.id] = parent
    parent.status = Status.RUNNING

    child1 = parent.add("child1", NodeType.TASK)
    child2 = parent.add("child2", NodeType.TASK)
    receiver1, sender1 = Pipe(duplex=False)
    receiver2, sender2 = Pipe(duplex=False)
    scheduler._queue(serialize_call(dummy, (), {}), child1, sender1, "cpu")
    scheduler._queue(serialize_call(dummy, (), {}), child2, sender2, "cpu")

    scheduler._cancel(parent)

    assert child1.status is Status.CANCELLED
    assert child2.status is Status.CANCELLED
    receiver1.close()
    receiver2.close()


def test_finished_node_cancellation_is_noop():
    scheduler, runtime = make_scheduler()
    node = Node("done", NodeType.TASK)
    node.status = Status.FINISHED
    runtime.nodes[node.id] = node

    scheduler._cancel(node)

    assert node.status is Status.FINISHED


def test_oldest_eligible_work_schedules_without_head_of_line_blocking():
    scheduler, runtime = make_scheduler({"blocked": 1, "free": 1})

    blocker, _ = queue_work(scheduler, runtime, "blocker", "blocked")
    blocked, _ = queue_work(scheduler, runtime, "blocked", "blocked")
    free, _ = queue_work(scheduler, runtime, "free", "free")

    assert blocker.id in scheduler.futures
    assert blocked.id not in scheduler.futures
    assert free.id in scheduler.futures
    assert [work[1] for work in scheduler.pending] == [blocked]


def test_completion_releases_tag_exactly_once():
    scheduler, runtime = make_scheduler({"cpu": 1})
    node, proxy = queue_work(scheduler, runtime, "a", "cpu")
    future = scheduler.futures[node.id]

    assert scheduler.running["cpu"] == 1
    future.set_result(1)

    assert proxy.result() == 1
    assert node.status is Status.FINISHED
    assert node.finished is not None
    assert scheduler.running["cpu"] == 0


def test_cancel_is_idempotent_for_pending_work():
    scheduler, runtime = make_scheduler({"cpu": 1})
    queue_work(scheduler, runtime, "blocker", "cpu")
    target, _ = queue_work(scheduler, runtime, "target", "cpu")

    scheduler._cancel(target)
    scheduler._cancel(target)

    assert target.status is Status.CANCELLED
    assert all(work[1] is not target for work in scheduler.pending)


def test_cancel_is_idempotent_for_running_work():
    scheduler, runtime = make_scheduler()
    node, _ = queue_work(scheduler, runtime, "a")

    scheduler._cancel(node)
    scheduler._cancel(node)

    assert node.status is Status.CANCELLED

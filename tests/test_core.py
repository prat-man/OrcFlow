from concurrent.futures import CancelledError, Future
from multiprocessing import Pipe

import pytest

from orcflow.flow import Flow, flow
from orcflow.handle import GroupHandle, Handle, WorkerFuture
from orcflow.node import Node, NodeType
from orcflow.task import BoundTask, Task, progress, task
from orcflow.types import Status
from orcflow.utils import format_time


def test_task_decorator_creates_task():
    @task
    def add(x, y=2):
        return x + y

    assert isinstance(add, Task)
    assert add.name == "add"


def test_task_direct_call_is_rejected():
    @task
    def work():
        pass

    with pytest.raises(TypeError, match=r"cannot be called directly"):
        work()


def test_task_name_formatting_and_defaults():
    @task(name="add({x}, {y})")
    def add(x, y=2):
        pass

    assert add.get_name(3) == "add(3, 2)"


def test_task_callable_name():
    @task(name=lambda x: f"item-{x}")
    def work(x):
        pass

    assert work.get_name(7) == "item-7"


def test_task_with_options_returns_copy():
    @task(name="original", tag="cpu", timeout=5)
    def work():
        pass

    changed = work.with_options(name="changed", timeout=10)

    assert changed is not work
    assert changed.fn is work.fn
    assert changed.name == "changed"
    assert changed.tag == "cpu"
    assert changed.timeout == 10
    assert work.name == "original"
    assert work.timeout == 5


def test_flow_decorator_and_name_formatting():
    @flow(name="flow({x})")
    def pipeline(x=4):
        return x

    assert isinstance(pipeline, Flow)
    assert pipeline.get_name() == "flow(4)"


def test_flow_with_options_returns_copy():
    @flow(name="original", n_workers=2, concurrency={"cpu": 1}, timeout=5)
    def pipeline():
        pass

    changed = pipeline.with_options(name="changed", n_workers=4)

    assert changed is not pipeline
    assert changed.fn is pipeline.fn
    assert changed.name == "changed"
    assert changed.n_workers == 4
    assert changed.concurrency == {"cpu": 1}
    assert changed.timeout == 5


def test_progress_outside_task_is_rejected():
    with pytest.raises(RuntimeError, match=r"only be called from an OrcFlow task"):
        progress(0.5)


def test_node_add_preserves_supplied_id():
    root = Node("root", NodeType.FLOW)
    child = root.add("child", NodeType.TASK, "child-id")

    assert child.id == "child-id"
    assert root.children == [child]


def test_node_progress_is_floored(capsys):
    node = Node("work", NodeType.TASK)
    node.status = Status.RUNNING
    node.progress = 0.999

    node.print()
    output = capsys.readouterr().out

    assert "99%" in output
    assert "100%" not in output


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (0.9, "0s"),
        (59.9, "59s"),
        (60, "1m 00s"),
        (125, "2m 05s"),
        (3600, "1h 00m"),
        (7380, "2h 03m"),
        (86400, "1d 00h"),
        (183600, "2d 03h"),
    ],
)
def test_format_time(seconds, expected):
    assert format_time(seconds) == expected


def test_handle_delegates_observation_and_cancel():
    future = Future()
    cancelled = []
    handle = Handle(future, lambda: cancelled.append(True))

    assert not handle.done()
    handle.cancel()
    assert cancelled == [True]

    future.set_result(42)
    assert handle.done()
    assert handle.result() == 42
    assert handle.exception() is None


def test_handle_propagates_exception():
    future = Future()
    future.set_exception(ValueError("boom"))
    handle = Handle(future, lambda: None)

    with pytest.raises(ValueError, match="boom"):
        handle.result()
    assert isinstance(handle.exception(), ValueError)


def make_worker_future(status, value):
    receiver, sender = Pipe(duplex=False)
    future = WorkerFuture(receiver, sender)
    sender.send((status, value))
    return future


def test_worker_future_finished():
    future = make_worker_future(Status.FINISHED, 12)
    assert future.done()
    assert future.result() == 12
    assert future.exception() is None


def test_worker_future_failed():
    future = make_worker_future(Status.FAILED, ValueError("bad"))
    assert isinstance(future.exception(), ValueError)


def test_worker_future_cancelled():
    future = make_worker_future(Status.CANCELLED, None)
    with pytest.raises(CancelledError):
        future.result()


def test_group_handle_done_and_cancel():
    cancelled = []
    futures = [Future(), Future()]
    handles = [Handle(future, lambda i=i: cancelled.append(i)) for i, future in enumerate(futures)]
    group = GroupHandle(handles)

    assert not group.done()
    group.cancel()
    assert cancelled == [0, 1]

    futures[0].set_result(1)
    futures[1].set_result(2)
    assert group.done()

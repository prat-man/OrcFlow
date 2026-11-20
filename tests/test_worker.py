from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from threading import RLock
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from orcflow import NodeInfo, WorkerInfo
from orcflow.node import Node, NodeType
from orcflow.runtime import Runtime
from orcflow.types import Status
from orcflow.worker import Worker
from psutil import NoSuchProcess


class Clock:
    def __init__(self):
        self.now = 10.0
        self.waits = []
        self.extra_wait = 0.0
        self.on_sleep = lambda: None

    def perf_counter(self):
        return self.now

    def time(self):
        return 1_700_000_000.0 + self.now

    def sleep(self, duration):
        self.waits.append(duration)
        self.now += duration + self.extra_wait
        self.on_sleep()


@pytest.fixture
def sampling_runtime(monkeypatch):
    clock = Clock()
    monkeypatch.setattr("orcflow.runtime.time", clock)
    runtime = Runtime.__new__(Runtime)
    runtime.id = "test-runtime"
    runtime.n_workers = 10
    runtime.concurrency = {}
    runtime.nodes = {}
    runtime.registry = {}
    runtime.scheduler = SimpleNamespace(lock=RLock(), running={})
    return runtime, clock


def add_worker(runtime, monkeypatch, pid=123, cpu_times=(10.0, 10.2)):
    process = Mock()
    process.cpu_times.side_effect = [
        SimpleNamespace(user=cpu, system=1.0) for cpu in cpu_times
    ]
    process.cpu_num.return_value = 2
    process.memory_info.return_value = SimpleNamespace(rss=64 * 1024**2)
    process.num_threads.return_value = 4
    monkeypatch.setattr("orcflow.worker.Process", Mock(return_value=process))
    worker = Worker(pid)
    worker.node = Node("task", NodeType.TASK)
    worker.node.status = Status.RUNNING
    worker.node.progress = 0.25
    runtime.registry[pid] = worker
    runtime.nodes[worker.node.id] = worker.node
    return worker, process


def test_workers_share_one_sampling_wait_and_use_actual_elapsed_time(sampling_runtime, monkeypatch):
    runtime, clock = sampling_runtime
    clock.extra_wait = 0.05
    for pid in range(1, 11):
        add_worker(runtime, monkeypatch, pid, cpu_times=(10.0, 10.5))

    snapshots = runtime.workers(interval=0.2)

    assert len(snapshots) == 10
    assert clock.waits == [0.2]
    for info in snapshots:
        assert isinstance(info, WorkerInfo)
        assert info.cpu_percent == pytest.approx(200.0)
        assert info.interval == pytest.approx(0.25)
        assert info.sampled_at == clock.time()
        assert info.memory == 64 * 1024**2
        assert info.threads == 4
        assert info.cpu_num == 2


def test_worker_info_and_node_summary_are_immutable(sampling_runtime, monkeypatch):
    runtime, _ = sampling_runtime
    worker, process = add_worker(runtime, monkeypatch)
    worker.node.add("child", NodeType.TASK)
    info = runtime.workers()[0]

    assert isinstance(info.node, NodeInfo)
    assert info.node.id == worker.node.id
    assert info.node.type is NodeType.TASK
    assert info.node.status is Status.RUNNING
    assert not hasattr(info, "_process")
    assert not hasattr(info.node, "children")
    worker.node.progress = 0.75
    worker.node.name = "changed"
    worker.node.status = Status.FINISHED
    process.memory_info.return_value.rss = 0
    assert info.node.progress == 0.25
    assert info.node.name == "task"
    assert info.node.status is Status.RUNNING
    assert info.memory == 64 * 1024**2
    with pytest.raises(FrozenInstanceError):
        info.cpu_percent = 0
    with pytest.raises(FrozenInstanceError):
        info.node.name = "changed"


def test_cpu_sampling_is_independent_of_previous_requests(sampling_runtime, monkeypatch):
    runtime, clock = sampling_runtime
    _, process = add_worker(runtime, monkeypatch, cpu_times=(0, 0.2, 600, 600.2))
    first = runtime.workers()[0]
    clock.now += 600
    second = runtime.workers()[0]

    assert first.cpu_percent == pytest.approx(100)
    assert second.cpu_percent == pytest.approx(100)
    assert clock.waits == [0.2, 0.2]
    # Sampling uses counters, not cpu_percent()'s mutable last-call baseline.
    assert process.cpu_percent.call_count == 1  # Internal Worker initialization.


def test_sampling_releases_scheduler_lock_and_captures_node_at_end(sampling_runtime, monkeypatch):
    runtime, clock = sampling_runtime
    worker, _ = add_worker(runtime, monkeypatch)
    replacement = Node("next task", NodeType.TASK)
    replacement.status = Status.RUNNING

    def scheduler_update():
        acquired = runtime.scheduler.lock.acquire(blocking=False)
        if acquired:
            try:
                worker.node = replacement
            finally:
                runtime.scheduler.lock.release()
        return acquired

    with ThreadPoolExecutor(max_workers=1) as executor:
        def during_sampling():
            assert executor.submit(scheduler_update).result(timeout=5)
        clock.on_sleep = during_sampling
        info = runtime.workers()[0]

    assert info.node.id == replacement.id
    assert info.node.name == "next task"


@pytest.mark.parametrize("phase", ["start", "end", "metrics", "unregistered"])
def test_workers_that_disappear_during_sampling_are_omitted(sampling_runtime, monkeypatch, phase):
    runtime, clock = sampling_runtime
    worker, process = add_worker(runtime, monkeypatch)
    if phase == "start":
        process.cpu_times.side_effect = NoSuchProcess(worker.pid)
    elif phase == "end":
        process.cpu_times.side_effect = [
            SimpleNamespace(user=10, system=1), NoSuchProcess(worker.pid),
        ]
    elif phase == "metrics":
        process.memory_info.side_effect = NoSuchProcess(worker.pid)
    else:
        clock.on_sleep = runtime.registry.clear

    assert runtime.workers() == []
    assert clock.waits == ([] if phase == "start" else [0.2])


def test_worker_info_can_have_no_node_or_cpu_number(sampling_runtime, monkeypatch):
    runtime, _ = sampling_runtime
    worker, process = add_worker(runtime, monkeypatch)
    worker.node = None
    del process.cpu_num
    info = runtime.workers()[0]
    assert info.node is None
    assert info.cpu_num is None


@pytest.mark.parametrize("duration", [0, -1, None, True, False, "0.2", float("inf"), float("nan")])
def test_invalid_sampling_duration_is_rejected(sampling_runtime, duration):
    runtime, clock = sampling_runtime
    with pytest.raises(ValueError, match="finite positive"):
        runtime.workers(interval=duration)
    assert clock.waits == []


def test_no_workers_returns_immediately(sampling_runtime):
    runtime, clock = sampling_runtime
    assert runtime.workers() == []
    assert clock.waits == []


def test_runtime_print_uses_captured_metrics(sampling_runtime, monkeypatch, capsys):
    runtime, clock = sampling_runtime
    add_worker(runtime, monkeypatch, cpu_times=(10, 10.1))
    runtime.print(interval=0.1)
    output = capsys.readouterr().out
    assert "100.0%" in output
    assert "64.0 MB" in output
    assert "task" in output
    assert clock.waits == [0.1]

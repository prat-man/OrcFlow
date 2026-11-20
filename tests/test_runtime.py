import pytest

from orcflow import flow
from orcflow.runtime import Runtime


@flow
def answer():
    return 42


class FakePool:
    def __init__(self):
        self.closed = 0
        self.joined = 0

    def close(self):
        self.closed += 1

    def join(self):
        self.joined += 1


class FakeScheduler:
    def __init__(self):
        self.waited = 0
        self.stopped = 0

    def wait_idle(self):
        self.waited += 1

    def stop(self):
        self.stopped += 1


class FakeManager:
    def __init__(self):
        self.shutdowns = 0

    def shutdown(self):
        self.shutdowns += 1


@pytest.mark.parametrize(
    "limit",
    [0, -1, 1.5, "1", None, True, False],
)
def test_invalid_concurrency_limit_is_rejected(limit):
    # Validation happens before Manager()/ProcessPool() are created, so
    # this never spins up real processes and needs no cleanup.
    with pytest.raises(ValueError, match="must be a positive integer"):
        Runtime(concurrency={"cpu": limit})


def test_valid_concurrency_limit_is_accepted():
    runtime = Runtime(concurrency={"cpu": 1})
    try:
        assert runtime.concurrency == {"cpu": 1}
    finally:
        runtime.shutdown()


@pytest.mark.parametrize("cpu_count, expected", [(2, 2), (None, 1)])
def test_default_worker_count_can_execute_flow(monkeypatch, cpu_count, expected):
    monkeypatch.setattr("orcflow.runtime.os.cpu_count", lambda: cpu_count)

    with Runtime() as runtime:
        capacity = runtime.capacity()

        assert runtime.n_workers == expected
        assert capacity.workers.total == expected
        assert capacity.workers.used == 0
        assert capacity.workers.free == expected
        assert answer.bind(runtime).submit().result() == 42


@pytest.mark.parametrize("n_workers", [0, -1, True, False, 1.5, "2"])
def test_invalid_worker_count_is_rejected_before_creating_resources(monkeypatch, n_workers):
    def unexpected_manager():
        pytest.fail("Worker count must be validated before creating resources")

    monkeypatch.setattr("orcflow.runtime.Manager", unexpected_manager)

    with pytest.raises(ValueError, match="n_workers must be a positive integer"):
        Runtime(n_workers=n_workers)


def test_shutdown_is_idempotent_and_waits_before_closing_pool():
    runtime = Runtime.__new__(Runtime)
    runtime.verbose = False
    runtime.pool = FakePool()
    runtime.scheduler = FakeScheduler()
    runtime.manager = FakeManager()
    runtime._shutdown = False

    runtime.shutdown()
    runtime.shutdown()

    assert runtime.scheduler.waited == 1
    assert runtime.pool.closed == 1
    assert runtime.pool.joined == 1
    assert runtime.scheduler.stopped == 1
    assert runtime.manager.shutdowns == 1


def test_empty_concurrency_is_accepted():
    runtime = Runtime()
    try:
        assert runtime.concurrency == {}
    finally:
        runtime.shutdown()

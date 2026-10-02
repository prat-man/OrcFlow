import pytest

from orcflow.runtime import Runtime


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


def test_capacity_accepts_default_worker_count():
    runtime = Runtime()
    try:
        capacity = runtime.capacity()

        assert capacity.workers.total is None
        assert capacity.workers.used == 0
        assert capacity.workers.free is None
    finally:
        runtime.shutdown()


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

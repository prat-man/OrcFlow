import os
import time
from multiprocessing import Manager

import pytest

from orcflow.flow import flow
from orcflow.task import task
from orcflow.types import Status


@pytest.fixture
def manager():
    """A Manager for creating .list()/.dict() proxies visible across the
    worker processes a flow run spawns."""
    mgr = Manager()
    try:
        yield mgr
    finally:
        mgr.shutdown()


# --- timeout -----------------------------------------------------------

@task(name="slow_timeout", timeout=0.2)
def slow_timeout_task():
    time.sleep(2)
    return "should not get here"


@flow(name="timeout_flow")
def timeout_flow():
    return slow_timeout_task.submit().result()


def test_task_timeout_fails_the_task():
    with timeout_flow.with_options(n_workers=2)() as run:
        with pytest.raises(Exception):
            run.result()

        assert run.status() is Status.FAILED


# --- initializer / initargs --------------------------------------------

def record_worker_start(log):
    log.append(os.getpid())


@task(name="touch")
def touch(marker):
    marker.append(True)
    return "ok"


@flow(name="init_flow")
def init_flow(marker):
    handles = [touch.submit(marker=marker) for _ in range(4)]
    return [handle.result() for handle in handles]


def test_initializer_runs_in_worker_processes(manager):
    init_log = manager.list()
    marker = manager.list()

    with init_flow.with_options(
        n_workers=2,
        initializer=record_worker_start,
        initargs=(init_log,),
    )(marker) as run:
        run.result()

    # At least one worker started (and ran the initializer); never more
    # than the pool size, regardless of whether the pool spawns workers
    # eagerly or lazily.
    assert 1 <= len(init_log) <= 2
    assert len(marker) == 4


# --- capacity() / counts() under a live tag limit -----------------------

@task(name="cap_sleep[{index}]", tag="limited")
def cap_sleep(seconds, index):
    time.sleep(seconds)
    return index


@flow(name="capacity_flow")
def capacity_flow():
    handles = [cap_sleep.submit(seconds=0.3, index=i) for i in range(3)]
    return [handle.result() for handle in handles]


def test_capacity_and_counts_reflect_tag_limit():
    run = capacity_flow.with_options(n_workers=2, concurrency={"limited": 1})()
    try:
        deadline = time.monotonic() + 3
        used_one = False

        while time.monotonic() < deadline:
            if run.capacity().tags.limited.used == 1:
                used_one = True
                break
            time.sleep(0.02)

        assert used_one, "expected exactly one 'limited' slot in use at some point"

        capacity = run.capacity()
        assert capacity.tags.limited.limit == 1
        assert capacity.tags.limited.free == 0

        counts = run.counts()
        assert counts.running >= 1

        run.result()

        final_counts = run.counts()
        assert final_counts.finished == 4  # flow root + 3 tagged tasks
        assert final_counts.queued == 0
        assert final_counts.running == 0
        assert final_counts.failed == 0
        assert final_counts.cancelled == 0
    finally:
        run.shutdown()


# --- worker resource reporting ------------------------------------------

def test_workers_report_resource_usage():
    run = capacity_flow.with_options(n_workers=2, concurrency={"limited": 1})()
    try:
        def has_active_worker():
            return any(worker.node is not None for worker in run.workers())

        deadline = time.monotonic() + 3
        found = False

        while time.monotonic() < deadline:
            if has_active_worker():
                found = True
                break
            time.sleep(0.02)

        assert found, "expected at least one worker to be attached to a running node"

        active = [worker for worker in run.workers() if worker.node is not None]
        for worker in active:
            assert worker.pid > 0
            assert worker.memory() > 0
            assert worker.threads() >= 1
            assert worker.cpu_percent() >= 0

        run.result()
    finally:
        run.shutdown()


# --- GroupHandle partial failure, real processes -------------------------

@task(name="maybe_fail_real({value})")
def maybe_fail_real(value):
    if value % 2 == 0:
        raise ValueError(f"even value {value}")
    return value


@task(name="group_parent")
def group_parent(values):
    group = maybe_fail_real.map([{"value": value} for value in values])

    exception = group.exception()
    exceptions = group.exceptions()

    return {
        "exception_type": None if exception is None else type(exception).__name__,
        "num_exceptions": len(exceptions),
        "done": group.done(),
    }


@flow(name="group_failure_flow")
def group_failure_flow(values):
    return group_parent.submit(values=values).result()


def test_group_handle_partial_failure_reporting():
    with group_failure_flow.with_options(n_workers=4)([1, 2, 3, 4]) as run:
        info = run.result()

    assert info["exception_type"] == "ValueError"
    assert info["num_exceptions"] == 2
    assert info["done"] is True


# --- module-scope resolution guard ---------------------------------------

def test_locally_defined_flow_is_rejected():
    @flow(n_workers=1)
    def local_flow():
        return 1

    with pytest.raises(TypeError, match="must be defined at module scope"):
        with local_flow() as run:
            run.result()

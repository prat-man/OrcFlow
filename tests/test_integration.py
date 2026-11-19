import importlib
import os
import time
from multiprocessing import Manager

import pytest

from orcflow.flow import flow
from orcflow.runtime import Runtime
from orcflow.task import progress, task
from orcflow.types import Status


@pytest.fixture
def manager():
    mgr = Manager()
    try:
        yield mgr
    finally:
        mgr.shutdown()


@task(name="add({x}, {y})")
def add(x, y):
    return x + y


@task(name="fail")
def fail():
    raise ValueError("expected failure")


@task(name="progress_task")
def progress_task():
    progress(0.25)
    time.sleep(0.1)
    progress(0.75)
    return "done"


@task(name="slow({x})", tag="serial")
def slow(x):
    time.sleep(1.0)
    return x


@task(name="parent")
def nested_parent():
    handles = add.map([
        {"x": 1, "y": 2},
        {"x": 3, "y": 4},
        {"x": 5, "y": 6},
    ])
    return handles.results()


@task(name="cancel_parent")
def cancel_parent():
    handles = slow.map([{"x": 1}, {"x": 2}, {"x": 3}])
    time.sleep(0.2)
    handles.cancel()
    return "parent finished"


@flow(name="basic_flow")
def basic_flow():
    return add.submit(2, 3).result()


@flow(name="nested_flow")
def nested_flow():
    return nested_parent.submit().result()


@flow(name="failure_flow")
def failure_flow():
    return fail.submit().result()


@flow(name="progress_flow")
def progress_flow():
    return progress_task.submit().result()


@flow(name="cancel_flow")
def cancel_flow():
    return cancel_parent.submit().result()


def test_basic_flow_end_to_end():
    with Runtime(n_workers=2) as runtime:
        run = basic_flow.submit(runtime)
        assert run.result() == 5
        assert run.done()
        assert run.status() is Status.FINISHED


def test_nested_map_preserves_submission_order():
    with Runtime(n_workers=4) as runtime:
        run = nested_flow.submit(runtime)
        assert run.result() == [3, 7, 11]
        tree = run.tree()
        assert tree.status is Status.FINISHED
        assert tree.children[0].status is Status.FINISHED
        assert [child.status for child in tree.children[0].children] == [Status.FINISHED] * 3


def test_failure_propagates_to_flow():
    with Runtime(n_workers=2) as runtime:
        run = failure_flow.submit(runtime)
        with pytest.raises(ValueError, match="expected failure"):
            run.result()
        assert run.status() is Status.FAILED


def test_progress_reaches_tree():
    with Runtime(n_workers=2) as runtime:
        run = progress_flow.submit(runtime)

        deadline = time.monotonic() + 5
        observed = False

        while time.monotonic() < deadline and not run.done():
            tree = run.tree()
            if tree.children and tree.children[0].progress in {0.25, 0.75}:
                observed = True
                break
            time.sleep(0.02)

        assert run.result() == "done"
        assert observed


def test_group_cancellation_is_not_failure():
    with Runtime(n_workers=4, concurrency={"serial": 1}) as runtime:
        run = cancel_flow.submit(runtime)

        assert run.result() == "parent finished"

        tree = run.tree()
        parent = tree.children[0]

        assert tree.status is Status.FINISHED
        assert parent.status is Status.FINISHED
        assert len(parent.children) == 3
        assert all(child.status is Status.CANCELLED for child in parent.children)


@task(name="slow_timeout", timeout=0.2)
def slow_timeout_task():
    time.sleep(2)
    return "should not get here"


@flow(name="timeout_flow")
def timeout_flow():
    return slow_timeout_task.submit().result()


def test_task_timeout_fails_the_task():
    with Runtime(n_workers=2) as runtime:
        run = timeout_flow.submit(runtime)

        with pytest.raises(Exception):
            run.result()

        assert run.status() is Status.FAILED


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

    with Runtime(
        n_workers=2,
        initializer=record_worker_start,
        initargs=(init_log,),
    ) as runtime:
        run = init_flow.submit(runtime, marker)
        run.result()

    assert 1 <= len(init_log) <= 2
    assert len(marker) == 4


@task(name="cap_sleep[{index}]", tag="limited")
def cap_sleep(seconds, index):
    time.sleep(seconds)
    return index


@flow(name="capacity_flow")
def capacity_flow():
    handles = [cap_sleep.submit(seconds=0.3, index=i) for i in range(3)]
    return [handle.result() for handle in handles]


def test_capacity_and_counts_reflect_tag_limit():
    with Runtime(n_workers=2, concurrency={"limited": 1}) as runtime:
        run = capacity_flow.submit(runtime)

        deadline = time.monotonic() + 3
        used_one = False

        while time.monotonic() < deadline:
            if runtime.capacity().tags.limited.used == 1:
                used_one = True
                break
            time.sleep(0.02)

        assert used_one

        capacity = runtime.capacity()
        assert capacity.tags.limited.limit == 1
        assert capacity.tags.limited.free == 0

        counts = runtime.counts()
        assert counts.running >= 1

        run.result()

        final_counts = runtime.counts()
        assert final_counts.finished == 4
        assert final_counts.queued == 0
        assert final_counts.running == 0
        assert final_counts.failed == 0
        assert final_counts.cancelled == 0


def test_workers_report_resource_usage():
    with Runtime(n_workers=2, concurrency={"limited": 1}) as runtime:
        run = capacity_flow.submit(runtime)

        def has_active_worker():
            return any(worker.node is not None for worker in runtime.workers())

        deadline = time.monotonic() + 3
        found = False

        while time.monotonic() < deadline:
            if has_active_worker():
                found = True
                break
            time.sleep(0.02)

        assert found

        active = [worker for worker in runtime.workers() if worker.node is not None]

        for worker in active:
            assert worker.pid > 0
            assert worker.memory() > 0
            assert worker.threads() >= 1
            assert worker.cpu_percent() >= 0

        run.result()


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
    with Runtime(n_workers=4) as runtime:
        run = group_failure_flow.submit(runtime, [1, 2, 3, 4])
        info = run.result()

    assert info["exception_type"] == "ValueError"
    assert info["num_exceptions"] == 2
    assert info["done"] is True


def make_closure_flow(n):
    @task
    def local_child(x):
        return x + n

    @flow
    def local_flow():
        return local_child.submit(10).result()

    return local_flow


def test_local_flow_and_task_closures_are_supported():
    with Runtime(n_workers=3) as runtime:
        run = make_closure_flow(5).submit(runtime)
        assert run.result() == 15
        assert run.status() is Status.FINISHED


@task(name="timed_child")
def timed_child():
    time.sleep(0.1)
    return "done"


@flow(name="timed_flow")
def timed_flow():
    return timed_child.submit().result()


def test_run_elapsed_comes_from_root_node_timing():
    with Runtime(n_workers=2) as runtime:
        run = timed_flow.submit(runtime)

        assert run.result() == "done"

        tree = run.tree()

        assert tree.started is not None
        assert tree.finished is not None
        assert tree.finished >= tree.started
        assert run.elapsed() == pytest.approx(tree.elapsed(), abs=0.05)

        assert tree.children[0].started is not None
        assert tree.children[0].finished is not None
        assert tree.children[0].elapsed() > 0


def submit_from_helper():
    progress(0.5)
    return add.with_options(name="helper_add").submit(2, 3).result()


@task
def helper_parent():
    return submit_from_helper()


@flow
def helper_flow():
    return helper_parent.submit().result()


def test_helpers_submit_under_current_task_and_report_its_progress():
    with Runtime(n_workers=3) as runtime:
        run = helper_flow.submit(runtime)
        assert run.result() == 5
        parent = run.tree().children[0]
        assert parent.name == "helper_parent"
        assert parent.progress == 0.5
        assert [child.name for child in parent.children] == ["helper_add"]
        assert parent.children[0].status is Status.FINISHED


@flow
def module_access_flow():
    tasks = importlib.import_module(__name__)
    tasks.progress(0.25)
    return tasks.add.map([{"x": 1, "y": 2}, {"x": 3, "y": 4}]).results()


def test_module_qualified_tasks_and_progress_are_supported():
    with Runtime(n_workers=2) as runtime:
        run = module_access_flow.submit(runtime)
        assert run.result() == [3, 7]
        tree = run.tree()
        assert tree.progress == 0.25
        assert [child.name for child in tree.children] == ["add(1, 2)", "add(3, 4)"]


@flow
def dynamically_defined_task_flow(offset):
    @task
    def local_child(x):
        return x + offset

    return local_child.submit(10).result()


def test_task_defined_during_execution_is_supported():
    with Runtime(n_workers=2) as runtime:
        run = dynamically_defined_task_flow.submit(runtime, 5)
        assert run.result() == 15
        assert run.tree().children[0].name == "local_child"

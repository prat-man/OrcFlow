import time

import pytest

from orcflow.flow import flow
from orcflow.task import progress, task
from orcflow.types import Status


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
    with basic_flow.with_options(n_workers=2)() as run:
        assert run.result() == 5
        assert run.done()
        assert run.status() is Status.FINISHED


def test_nested_map_preserves_submission_order():
    with nested_flow.with_options(n_workers=4)() as run:
        assert run.result() == [3, 7, 11]
        tree = run.tree()
        assert tree.status is Status.FINISHED
        assert tree.children[0].status is Status.FINISHED
        assert [child.status for child in tree.children[0].children] == [Status.FINISHED] * 3


def test_failure_propagates_to_flow():
    with failure_flow.with_options(n_workers=2)() as run:
        with pytest.raises(ValueError, match="expected failure"):
            run.result()
        assert run.status() is Status.FAILED


def test_progress_reaches_tree():
    with progress_flow.with_options(n_workers=2)() as run:
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
    with cancel_flow.with_options(n_workers=4, concurrency={"serial": 1})() as run:
        assert run.result() == "parent finished"
        tree = run.tree()
        parent = tree.children[0]

        assert tree.status is Status.FINISHED
        assert parent.status is Status.FINISHED
        assert len(parent.children) == 3
        assert all(child.status is Status.CANCELLED for child in parent.children)

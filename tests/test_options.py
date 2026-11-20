import pytest

from orcflow import flow, task
from orcflow.runtime import Runtime
from orcflow.task import Task


def work(value=42):
    return value


@pytest.fixture(params=[task, flow], ids=["task", "flow"])
def configured(request):
    options = {"name": "custom", "timeout": 30}
    if request.param is task:
        options["tag"] = "io"
    return request.param(work, **options)


def test_omitted_options_are_preserved(configured):
    updated = configured.with_options()
    assert updated is not configured
    assert updated.fn is configured.fn
    assert updated.name == "custom"
    assert updated.timeout == 30
    if isinstance(updated, Task):
        assert updated.tag == "io"


def test_changing_one_option_preserves_others(configured):
    updated = configured.with_options(timeout=60)
    assert updated.timeout == 60
    assert configured.timeout == 30
    assert updated.name == "custom"
    if isinstance(updated, Task):
        assert updated.tag == "io"


def test_none_removes_timeout(configured):
    updated = configured.with_options(timeout=None)
    assert updated.timeout is None
    assert updated.name == "custom"
    assert configured.timeout == 30
    assert updated.with_options().timeout is None
    if isinstance(updated, Task):
        assert updated.tag == "io"


def test_none_restores_default_name(configured):
    updated = configured.with_options(name=None)
    assert updated.name == "work"
    assert updated.timeout == 30
    assert configured.name == "custom"


def test_options_can_be_reset_together(configured):
    options = {"name": None, "timeout": None}
    if isinstance(configured, Task):
        options["tag"] = None
    updated = configured.with_options(**options)
    assert updated.name == "work"
    assert updated.timeout is None
    assert configured.name == "custom"
    assert configured.timeout == 30
    if isinstance(updated, Task):
        assert updated.tag is None
        assert configured.tag == "io"


def test_none_removes_tag_without_changing_other_options():
    configured = task(work, name="custom", tag="io", timeout=30)
    updated = configured.with_options(tag=None)
    assert updated.tag is None
    assert updated.with_options().tag is None
    assert updated.name == "custom"
    assert updated.timeout == 30
    assert configured.tag == "io"


def test_clearing_flow_options_preserves_runtime_binding():
    configured = flow(work, name="custom", timeout=30)
    with Runtime(n_workers=1) as runtime:
        bound = configured.bind(runtime)
        updated = bound.with_options(name=None, timeout=None)
        run = updated.submit(value=7)
        assert run.result() == 7
        assert run.tree().name == "work"
        assert bound.name == "custom"
        assert bound.timeout == 30


@task(name="custom", tag="io", timeout=30)
def echo(value):
    return value


@flow
def configure_task_in_worker():
    preserved = echo.with_options()
    cleared = preserved.with_options(name=None, tag=None, timeout=None)
    return (
        (preserved.name, preserved.tag, preserved.timeout),
        (cleared.name, cleared.tag, cleared.timeout),
        cleared.submit(value=7).result(),
    )


def test_options_can_be_preserved_and_cleared_inside_worker():
    with Runtime(n_workers=2) as runtime:
        run = configure_task_in_worker.bind(runtime).submit()
        assert run.result() == (("custom", "io", 30), ("echo", None, None), 7)
        assert run.tree().children[0].name == "echo"

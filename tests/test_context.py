from queue import Queue
from unittest.mock import ANY, Mock

import pytest

from orcflow.serialization import serialize_call
from orcflow.task import progress, task
from orcflow.types import Request, Status
from orcflow.worker import Client, execute


@task
def outside_task():
    return 1


@pytest.mark.parametrize("operation", [
    lambda: outside_task.submit(),
    lambda: outside_task.map([{}]),
    lambda: progress(0.5),
])
def test_operations_outside_execution_raise_clear_error(operation):
    with pytest.raises(RuntimeError, match="running OrcFlow flow or task"):
        operation()


@pytest.mark.parametrize("error", [None, ValueError, KeyboardInterrupt])
def test_execute_restores_context_after_success_or_failure(error):
    client = Client("test", False, Queue())

    def body():
        progress(0.5)
        if error is not None:
            raise error("expected")
        return 42

    payload = serialize_call(body, (), {})
    if error is None:
        assert execute(payload, client, "inner", "body") == 42
    else:
        with pytest.raises(error, match="expected"):
            execute(payload, client, "inner", "body")

    assert client.requests.get()[0:2] == (Status.RUNNING, "inner")
    assert client.requests.get() == (Request.PROGRESS, "inner", 0.5)
    with pytest.raises(RuntimeError, match="running OrcFlow flow or task"):
        progress(0.25)

    # A reused worker must use the new node, regardless of the previous outcome.
    next_payload = serialize_call(lambda: progress(1.0), (), {})
    execute(next_payload, client, "next", "next")
    assert client.requests.get()[0:2] == (Status.RUNNING, "next")
    assert client.requests.get() == (Request.PROGRESS, "next", 1.0)
    with pytest.raises(RuntimeError, match="running OrcFlow flow or task"):
        outside_task.submit()


def test_reused_worker_routes_helper_calls_to_current_client_and_parent():
    def helper():
        outside_task.submit()
        progress(0.5)

    def body():
        helper()

    payload = serialize_call(body, (), {})
    first_client = Mock(spec=Client, verbose=False)
    second_client = Mock(spec=Client, verbose=False)

    # Reuse the same worker entry point and payload for two separate executions.
    execute(payload, first_client, "first", "body")
    execute(payload, second_client, "second", "body")

    for client, parent in [(first_client, "first"), (second_client, "second")]:
        client.submit.assert_called_once_with(
            ANY,
            parent=parent,
            name="outside_task",
            tag=None,
            timeout=None,
        )
        client.progress.assert_called_once_with(parent, 0.5)

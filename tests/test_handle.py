from concurrent.futures import CancelledError
from multiprocessing import Pipe

import pytest

from orcflow.handle import GroupHandle, Handle, ResultProxy
from orcflow.types import Status


def make_handle(status, value=None, cancel=None):
    receiver, sender = Pipe(duplex=False)
    sender.send((status, value))
    sender.close()
    return Handle(ResultProxy(receiver), cancel or (lambda: None))


def test_result_proxy_returns_finished_value():
    handle = make_handle(Status.FINISHED, 42)
    assert handle.done()
    assert handle.result() == 42
    assert handle.result() == 42
    assert handle.exception() is None


def test_result_proxy_raises_failure():
    handle = make_handle(Status.FAILED, ValueError("boom"))
    with pytest.raises(ValueError, match="boom"):
        handle.result()


def test_result_proxy_reports_exception():
    handle = make_handle(Status.FAILED, ValueError("boom"))
    exc = handle.exception()
    assert isinstance(exc, ValueError)
    assert str(exc) == "boom"


def test_result_proxy_raises_cancelled_error():
    handle = make_handle(Status.CANCELLED)
    with pytest.raises(CancelledError):
        handle.result()


def test_handle_cancel_uses_control_callback():
    calls = []
    handle = make_handle(Status.FINISHED, 1, cancel=lambda: calls.append(True))
    handle.cancel()
    assert calls == [True]


def test_group_results_preserve_submission_order():
    group = GroupHandle([
        make_handle(Status.FINISHED, 3),
        make_handle(Status.FINISHED, 1),
        make_handle(Status.FINISHED, 2),
    ])
    assert group.results() == [3, 1, 2]
    assert group.done()


def test_group_cancel_requests_every_member():
    calls = []
    handles = [make_handle(Status.FINISHED, i, cancel=lambda i=i: calls.append(i)) for i in range(3)]
    GroupHandle(handles).cancel()
    assert calls == [0, 1, 2]

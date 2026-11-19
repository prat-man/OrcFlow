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


def test_result_proxy_exception_raises_cancelled_error():
    handle = make_handle(Status.CANCELLED)
    with pytest.raises(CancelledError):
        handle.exception()


def test_result_proxy_is_not_done_before_reply():
    receiver, sender = Pipe(duplex=False)
    proxy = ResultProxy(receiver)

    assert not proxy.done()

    sender.send((Status.FINISHED, 42))
    sender.close()

    assert proxy.done()
    assert proxy.result() == 42


def test_result_proxy_repeats_receive_error():
    receiver, sender = Pipe(duplex=False)
    sender.close()
    proxy = ResultProxy(receiver)

    with pytest.raises(EOFError):
        proxy.result()

    with pytest.raises(EOFError):
        proxy.result()


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
    assert group.results() == [3, 1, 2]


def test_group_exception_is_repeatable():
    group = GroupHandle([
        make_handle(Status.FINISHED, 1),
        make_handle(Status.FAILED, ValueError("boom")),
    ])

    first = group.exception()
    second = group.exception()

    assert isinstance(first, ValueError)
    assert first is second


@pytest.mark.parametrize("status", [Status.FINISHED, Status.CANCELLED])
def test_group_exception_when_result_arrives_after_readiness_check(monkeypatch, status):
    receiver, sender = Pipe(duplex=False)
    proxy = ResultProxy(receiver)
    group = GroupHandle([Handle(proxy, lambda: None)])
    original_poll = receiver.poll

    def poll_then_deliver_result(timeout=0.0):
        ready = original_poll(timeout)
        if not ready:
            # Force completion just after the readiness check, without sleeps.
            sender.send((status, 42 if status is Status.FINISHED else None))
            sender.close()
        return ready

    try:
        monkeypatch.setattr(receiver, "poll", poll_then_deliver_result)
        assert group.exception() is None
        assert group.exception() is None
        if status is Status.FINISHED:
            assert group.results() == [42]
        else:
            with pytest.raises(CancelledError):
                group.results()
    finally:
        receiver.close()
        sender.close()


def test_group_exception_uses_results_already_consumed():
    group = GroupHandle([
        make_handle(Status.FINISHED, 42),
        make_handle(Status.CANCELLED),
    ])

    assert group.exceptions() == []
    assert group.exception() is None


def test_group_results_repeats_failure():
    group = GroupHandle([
        make_handle(Status.FINISHED, 1),
        make_handle(Status.FAILED, ValueError("boom")),
    ])

    with pytest.raises(ValueError, match="boom"):
        group.results()

    with pytest.raises(ValueError, match="boom"):
        group.results()


def test_group_cancel_requests_every_member():
    calls = []
    handles = [make_handle(Status.FINISHED, i, cancel=lambda i=i: calls.append(i)) for i in range(3)]
    GroupHandle(handles).cancel()
    assert calls == [0, 1, 2]

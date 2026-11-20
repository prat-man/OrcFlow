from concurrent.futures import CancelledError, ThreadPoolExecutor
from multiprocessing import Pipe
from threading import Event

import pytest

from orcflow import handle as handle_module
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


@pytest.mark.parametrize("method", ["result", "exception"])
def test_done_does_not_wait_for_concurrent_result_reader(monkeypatch, method):
    receiver, sender = Pipe(duplex=False)
    handle = Handle(ResultProxy(receiver), lambda: None)
    receiving = Event()
    original_recv = receiver.recv

    def recv():
        receiving.set()
        return original_recv()

    monkeypatch.setattr(receiver, "recv", recv)
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            reader = executor.submit(getattr(handle, method))
            try:
                assert receiving.wait(5), "Reader did not reach recv()"
                # No result is sent until this readiness check has returned.
                assert executor.submit(handle.done).result(timeout=5) is False
            finally:
                sender.send((Status.FINISHED, 42))
                sender.close()

            assert reader.result(timeout=5) == (42 if method == "result" else None)

        assert handle.done()
        assert handle.result() == 42
    finally:
        receiver.close()
        sender.close()


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


@pytest.mark.parametrize("ready_first", [False, True])
@pytest.mark.parametrize("status", [Status.FAILED, Status.CANCELLED])
def test_group_results_reports_ready_error_without_waiting(status, ready_first):
    receiver, sender = Pipe(duplex=False)
    unfinished = Handle(ResultProxy(receiver), lambda: None)
    ready = make_handle(status, ValueError("boom") if status is Status.FAILED else None)
    group = GroupHandle([ready, unfinished] if ready_first else [unfinished, ready])
    expected = ValueError if status is Status.FAILED else CancelledError

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            result = executor.submit(group.results)
            try:
                with pytest.raises(expected):
                    result.result(timeout=2)
                assert not unfinished.done()
                # Repeated calls must still raise without waiting for siblings.
                with pytest.raises(expected):
                    group.results()
            finally:
                # Release the unfinished result if the regression returns.
                sender.send((Status.FINISHED, 42))
                sender.close()
    finally:
        receiver.close()
        sender.close()


def test_group_cancel_requests_every_member():
    calls = []
    handles = [make_handle(Status.FINISHED, i, cancel=lambda i=i: calls.append(i)) for i in range(3)]
    GroupHandle(handles).cancel()
    assert calls == [0, 1, 2]


@pytest.mark.parametrize("method", ["results", "exception"])
def test_group_exceptions_does_not_close_pipe_during_another_group_wait(monkeypatch, method):
    receiver, sender = Pipe(duplex=False)
    group = GroupHandle([Handle(ResultProxy(receiver), lambda: None)])
    waiting, resume_wait, exceptions_finished = Event(), Event(), Event()

    original_wait = handle_module.wait

    def delayed_wait(proxies):
        waiting.set()
        assert resume_wait.wait(5), "Group wait was not released"
        return original_wait(proxies)

    def collect_exceptions():
        try:
            return group.exceptions()
        finally:
            exceptions_finished.set()

    monkeypatch.setattr(handle_module, "wait", delayed_wait)
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(getattr(group, method))
            try:
                assert waiting.wait(5), "First group call did not reach wait()"
                second = executor.submit(collect_exceptions)
                sender.send((Status.FINISHED, 42))
                sender.close()
                # Give exceptions() a chance to consume the result before
                # the first call resumes. It must wait for the group lock.
                exceptions_finished.wait(1)
            finally:
                if not sender.closed:
                    sender.send((Status.FINISHED, 42))
                    sender.close()
                resume_wait.set()

            assert first.result(timeout=5) == ([42] if method == "results" else None)
            assert second.result(timeout=5) == []
    finally:
        receiver.close()
        sender.close()


def test_group_done_and_cancel_work_while_exceptions_waits(monkeypatch):
    receiver, sender = Pipe(duplex=False)
    receiving = Event()
    original_recv = receiver.recv

    def recv():
        receiving.set()
        return original_recv()

    def cancel():
        sender.send((Status.CANCELLED, None))
        sender.close()

    monkeypatch.setattr(receiver, "recv", recv)
    group = GroupHandle([Handle(ResultProxy(receiver), cancel)])
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            reader = executor.submit(group.exceptions)
            try:
                assert receiving.wait(5), "exceptions() did not reach recv()"
                assert executor.submit(group.done).result(timeout=5) is False
                executor.submit(group.cancel).result(timeout=5)
            finally:
                if not sender.closed:
                    cancel()
            assert reader.result(timeout=5) == []
        assert group.done()
    finally:
        receiver.close()
        sender.close()

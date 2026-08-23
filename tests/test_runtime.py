import pytest

from orcflow.runtime import Runtime


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


def test_empty_concurrency_is_accepted():
    runtime = Runtime()
    try:
        assert runtime.concurrency == {}
    finally:
        runtime.shutdown()

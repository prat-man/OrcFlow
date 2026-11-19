"""Internal context for the flow or task currently executing in a worker."""

from contextlib import contextmanager
from contextvars import ContextVar


_execution = ContextVar("orcflow_execution", default=None)


def current_execution():
    """Return the current scheduler client and parent node ID."""
    context = _execution.get()
    if context is None:
        raise RuntimeError(
            "Task submission and progress require a running OrcFlow flow or task."
        )
    return context


@contextmanager
def execution_context(client, node_id):
    """Scope execution state to one call, restoring it even after failure."""
    token = _execution.set((client, node_id))
    try:
        yield
    finally:
        _execution.reset(token)

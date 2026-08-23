from inspect import signature

from orcflow.node import Node, NodeType
from orcflow.handle import Run


class Flow:
    """Wrap a function as an OrcFlow flow."""

    def __init__(self, fn, name=None, timeout=None):
        self.fn = fn
        self.name = name or fn.__name__
        self.timeout = timeout

    def with_options(self, *, name=None, timeout=None):
        """Return a copy of this flow with updated options."""
        return Flow(
            self.fn,
            name=self.name if name is None else name,
            timeout=self.timeout if timeout is None else timeout,
        )

    def get_name(self, *args, **kwargs):
        """Resolve the display name for this flow call."""
        if callable(self.name):
            return self.name(*args, **kwargs)

        parameters = signature(self.fn).bind(*args, **kwargs)
        parameters.apply_defaults()

        return self.name.format(**parameters.arguments)

    def submit(self, runtime, *args, **kwargs):
        """Start a new run of the flow."""
        root = Node(
            name=self.get_name(*args, **kwargs),
            type=NodeType.FLOW,
        )

        handle = runtime.submit(
            self.fn,
            *args,
            node=root,
            timeout=self.timeout,
            **kwargs,
        )

        return Run(handle, root)


def flow(fn=None, *, name=None, timeout=None):
    """Decorate a function as a flow."""

    def decorate(fn):
        return Flow(
            fn,
            name=name,
            timeout=timeout,
        )

    if fn is None:
        return decorate

    return decorate(fn)

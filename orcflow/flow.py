from inspect import signature

from orcflow.node import Node, NodeType
from orcflow.handle import Run


class Flow:
    """Wrap a function as an OrcFlow flow."""

    def __init__(self, fn, name=None, timeout=None):
        self.fn = fn
        self.name = name or fn.__name__
        self.timeout = timeout
        self._runtime = None

    def with_options(self, *, name=None, timeout=None):
        """Return a copy of this flow with updated options."""
        configured = Flow(
            self.fn,
            name=self.name if name is None else name,
            timeout=self.timeout if timeout is None else timeout,
        )
        configured._runtime = self._runtime
        return configured

    def bind(self, runtime):
        """Return a copy bound to a runtime, leaving this flow unchanged.

        Flow-function arguments are supplied to submit(), not to bind().
        """
        bound = self.with_options()
        bound._runtime = runtime
        return bound

    def get_name(self, /, *args, **kwargs):
        """Resolve the display name for this flow call."""
        if callable(self.name):
            return self.name(*args, **kwargs)

        parameters = signature(self.fn).bind(*args, **kwargs)
        parameters.apply_defaults()

        return self.name.format(**parameters.arguments)

    def submit(self, /, *args, **kwargs):
        """Start a run on the bound runtime using only function arguments."""
        runtime = self._runtime
        if runtime is None:
            raise RuntimeError(
                f"Flow {self.name!r} is not bound to a Runtime. "
                "Call .bind(runtime) before submitting."
            )

        root = Node(
            name=self.get_name(*args, **kwargs),
            type=NodeType.FLOW,
        )

        handle = runtime.submit(
            self.fn,
            args,
            kwargs,
            node=root,
            timeout=self.timeout,
        )

        return Run(handle, runtime, root)

    def __call__(self, *args, **kwargs):
        raise TypeError(
            f"Flow {self.name!r} cannot be called directly. "
            "Use .bind(runtime).submit(...) instead."
        )


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

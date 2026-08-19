from inspect import signature

from orcflow.run import Run
from orcflow.runtime import Runtime
from orcflow.node import Node, NodeType


class Flow:
    """Wrap a function as an OrcFlow flow."""

    def __init__(self, fn, name=None, n_workers=None, concurrency=None, initializer=None, initargs=(), verbose=False):
        self.fn = fn
        self.name = name or fn.__name__
        self.n_workers = n_workers
        self.concurrency = concurrency or {}
        self.initializer = initializer
        self.initargs = initargs
        self.verbose = verbose

    def with_options(self, *, name=None, n_workers=None, concurrency=None, initializer=None, initargs=None, verbose=None):
        """Return a copy of this flow with updated options."""
        return Flow(
            self.fn,
            name=self.name if name is None else name,
            n_workers=self.n_workers if n_workers is None else n_workers,
            concurrency=self.concurrency if concurrency is None else concurrency,
            initializer=self.initializer if initializer is None else initializer,
            initargs=self.initargs if initargs is None else initargs,
            verbose=self.verbose if verbose is None else verbose,
        )

    def get_name(self, *args, **kwargs):
        """Resolve the display name for this flow call."""
        if callable(self.name):
            return self.name(*args, **kwargs)

        parameters = signature(self.fn).bind(*args, **kwargs)
        parameters.apply_defaults()

        return self.name.format(**parameters.arguments)

    def __call__(self, *args, **kwargs):
        """Start a new run of the flow."""
        runtime = Runtime(
            n_workers=self.n_workers,
            concurrency=self.concurrency,
            initializer=self.initializer,
            initargs=self.initargs,
            verbose=self.verbose,
        )
        name = self.get_name(*args, **kwargs)
        runtime.root = Node(name, NodeType.FLOW)

        result = runtime.run(self.fn, *args, node=runtime.root, **kwargs)
        return Run(result=result, runtime=runtime)


def flow(fn=None, *, name=None, n_workers=None, concurrency=None, initializer=None, initargs=(), verbose=False):
    """Decorate a function as a flow."""
    def decorate(fn):
        return Flow(
            fn,
            name=name,
            n_workers=n_workers,
            concurrency=concurrency,
            initializer=initializer,
            initargs=initargs,
            verbose=verbose,
        )

    if fn is None:
        return decorate

    return decorate(fn)

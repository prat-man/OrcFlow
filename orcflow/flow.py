from inspect import signature

from orcflow.run import Run
from orcflow.runtime import Runtime
from orcflow.node import Node, NodeType


class Flow:
    """Wrap a function as an OrcFlow flow."""

    def __init__(self, fn, name=None, workers=None, concurrency=None, verbose=False):
        self.fn = fn
        self.name = name or fn.__name__
        self.workers = workers
        self.concurrency = concurrency or {}
        self.verbose = verbose

    def with_options(self, *, name=None, workers=None, concurrency=None, verbose=None):
        return Flow(
            self.fn,
            name=self.name if name is None else name,
            workers=self.workers if workers is None else workers,
            concurrency=self.concurrency if concurrency is None else concurrency,
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
        runtime = Runtime(workers=self.workers, concurrency=self.concurrency, verbose=self.verbose)
        name = self.get_name(*args, **kwargs)
        runtime.root = Node(name, NodeType.FLOW)

        result = runtime.run(self.fn, *args, node=runtime.root, **kwargs)
        return Run(result=result, runtime=runtime)


def flow(fn=None, *, name=None, workers=None, concurrency=None, verbose=False):
    """Decorate a function as a flow."""
    def decorate(fn):
        return Flow(fn, name=name, workers=workers, concurrency=concurrency, verbose=verbose)

    if fn is None:
        return decorate

    return decorate(fn)

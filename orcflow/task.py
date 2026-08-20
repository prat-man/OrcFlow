from inspect import signature
from types import FunctionType

from orcflow.result import CompositeResult


class Task:
    """Wrap a reusable OrcFlow task function."""

    def __init__(self, fn, name=None, tag=None, timeout=None):
        self.fn = fn
        self.name = name or fn.__name__
        self.tag = tag
        self.timeout = timeout

    def with_options(self, *, name=None, tag=None, timeout=None):
        """Return a copy of this task with updated options."""
        return Task(
            self.fn,
            name=self.name if name is None else name,
            tag=self.tag if tag is None else tag,
            timeout=self.timeout if timeout is None else timeout,
        )

    def get_name(self, *args, **kwargs):
        """Resolve the display name for this task call."""
        if callable(self.name):
            return self.name(*args, **kwargs)

        parameters = signature(self.fn).bind(*args, **kwargs)
        parameters.apply_defaults()

        return self.name.format(**parameters.arguments)

    def bind(self, runtime, parent):
        """Bind this task to a specific runtime and parent node."""
        return BoundTask(self, runtime, parent)

    def __call__(self, *args, **kwargs):
        raise TypeError(
            f"Task {self.name!r} cannot be called directly. "
            f"Use {self.name}.submit(...)."
        )


class BoundTask:
    """A task bound to the runtime of the function currently executing."""

    def __init__(self, task, runtime, parent):
        self.task = task
        self.runtime = runtime
        self.parent = parent

    def with_options(self, *, name=None, tag=None, timeout=None):
        """Return a copy of this bound task with updated options."""
        task = self.task.with_options(name=name, tag=tag, timeout=timeout)
        return BoundTask(task, self.runtime, self.parent)

    def submit(self, *args, **kwargs):
        """Submit this task under its bound parent node."""
        name = self.task.get_name(*args, **kwargs)

        return self.runtime.run(self.task.fn, *args, parent=self.parent, name=name, tag=self.task.tag, timeout=self.task.timeout, **kwargs)

    def map(self, arguments):
        """Submit this task once for each argument mapping."""
        results = [self.submit(**values) for values in arguments]
        return CompositeResult(results)

    def __call__(self, *args, **kwargs):
        raise TypeError(
            f"Task {self.task.name!r} cannot be called directly. "
            f"Use {self.task.name}.submit(...)."
        )


def task(fn=None, *, name=None, tag=None, timeout=None):
    """Decorate a function as a task."""
    def decorate(fn):
        return Task(fn, name=name, tag=tag, timeout=timeout)

    if fn is None:
        return decorate

    return decorate(fn)


def bind_tasks(fn, runtime, parent):
    """Bind task globals to the runtime handling this execution."""
    globals_ = fn.__globals__.copy()

    # Replace task definitions with runtime-bound task objects for this call only.
    for name, value in globals_.items():
        if isinstance(value, Task):
            globals_[name] = value.bind(runtime, parent)

    return FunctionType(
        fn.__code__,
        globals_,
        fn.__name__,
        fn.__defaults__,
        fn.__closure__,
    )

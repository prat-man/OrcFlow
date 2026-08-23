from functools import partial
from inspect import signature
from types import FunctionType

from orcflow.handle import GroupHandle


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
        return self.runtime.submit(self.task.fn, *args, parent=self.parent, name=name, tag=self.task.tag, timeout=self.task.timeout, **kwargs)

    def map(self, arguments):
        """Submit this task once for each argument mapping."""
        handles = [self.submit(**values) for values in arguments]
        return GroupHandle(handles)

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


def _make_cell(value):
    """Create a closure cell containing a value."""
    return (lambda: value).__closure__[0]


def _bind_value(value, runtime, parent):
    """Bind OrcFlow objects captured by a function to this execution."""
    if isinstance(value, Task):
        return value.bind(runtime, parent)
    if value is progress:
        return partial(runtime.progress, parent)
    return value


def bind_tasks(fn, runtime, parent):
    """Bind task globals and closure values to the current execution."""
    globals_ = {
        name: _bind_value(value, runtime, parent)
        for name, value in fn.__globals__.items()
    }

    closure = None
    if fn.__closure__ is not None:
        closure = tuple(
            _make_cell(_bind_value(cell.cell_contents, runtime, parent))
            for cell in fn.__closure__
        )

    bound = FunctionType(
        fn.__code__,
        globals_,
        fn.__name__,
        fn.__defaults__,
        closure,
    )
    bound.__kwdefaults__ = fn.__kwdefaults__
    bound.__annotations__ = fn.__annotations__.copy()
    bound.__dict__.update(fn.__dict__)
    bound.__doc__ = fn.__doc__
    bound.__module__ = fn.__module__
    bound.__qualname__ = fn.__qualname__
    return bound


def progress(value):
    """Report progress for the currently executing task."""
    raise RuntimeError("progress() can only be called from an OrcFlow task.")

from inspect import signature

from orcflow.constants import _UNSET
from orcflow.context import current_execution
from orcflow.handle import GroupHandle


class Task:
    """Wrap a reusable OrcFlow task function."""

    def __init__(self, fn, name=None, tag=None, timeout=None):
        self.fn = fn
        self.name = name or fn.__name__
        self.tag = tag
        self.timeout = timeout

    def with_options(self, *, name=_UNSET, tag=_UNSET, timeout=_UNSET):
        """Return a copy, preserving omitted options and resetting explicit None."""
        return Task(
            self.fn,
            name=self.name if name is _UNSET else name,
            tag=self.tag if tag is _UNSET else tag,
            timeout=self.timeout if timeout is _UNSET else timeout,
        )

    def get_name(self, /, *args, **kwargs):
        """Resolve the display name for this task call."""
        if callable(self.name):
            return self.name(*args, **kwargs)

        parameters = signature(self.fn).bind(*args, **kwargs)
        parameters.apply_defaults()

        return self.name.format(**parameters.arguments)

    def submit(self, /, *args, **kwargs):
        """Submit this task under the currently executing flow or task."""
        client, parent = current_execution()
        name = self.get_name(*args, **kwargs)
        return client.submit(
            self.fn,
            args,
            kwargs,
            parent=parent,
            name=name,
            tag=self.tag,
            timeout=self.timeout,
        )

    def map(self, arguments):
        """Submit this task once for each argument mapping."""
        handles = [self.submit(**values) for values in arguments]
        return GroupHandle(handles)

    def __call__(self, *args, **kwargs):
        raise TypeError(
            f"Task {self.name!r} cannot be called directly. "
            "Use .submit(...) instead."
        )


def task(fn=None, *, name=None, tag=None, timeout=None):
    """Decorate a function as a task."""
    def decorate(fn):
        return Task(fn, name=name, tag=tag, timeout=timeout)

    if fn is None:
        return decorate

    return decorate(fn)


def progress(value):
    """Report progress for the currently executing flow or task."""
    client, parent = current_execution()
    client.progress(parent, value)

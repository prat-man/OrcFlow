import os

from orcflow.constants import ORCFLOW_PREFIX, COLOR_BLUE, COLOR_GREEN, COLOR_RED, COLOR_RESET, COLOR_YELLOW, COLOR_MAGENTA
from orcflow.types import Status


def color_status(status):
    """Return a colored display string for a status."""
    colors = {
        Status.ORCHESTRATE: COLOR_MAGENTA,
        Status.SHUTDOWN: COLOR_MAGENTA,
        Status.QUEUED: COLOR_YELLOW,
        Status.RUNNING: COLOR_BLUE,
        Status.FINISHED: COLOR_GREEN,
        Status.FAILED: COLOR_RED,
        Status.CANCELLED: COLOR_RED,
    }

    color = colors.get(status, "")
    return f"{color}{status.value}{COLOR_RESET}"


def log(status, name=None, runtime=None):
    """Print one OrcFlow lifecycle message."""
    message = f"{ORCFLOW_PREFIX} {color_status(status)}"

    if name is not None:
        message += f" {name}"

    if runtime is not None:
        message += f" runtime={runtime.id}"

    message += f" pid={os.getpid()}"

    print(message)

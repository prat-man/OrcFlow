from enum import Enum


class Status(Enum):
    """Lifecycle states used by OrcFlow."""

    ORCHESTRATE = "orchestrate"
    QUEUED = "queued"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SHUTDOWN = "shutdown"


class Request(Enum):
    """Requests sent to the scheduler."""

    STOP = "stop"
    SUBMIT = "submit"
    CANCEL = "cancel"
    PROGRESS = "progress"

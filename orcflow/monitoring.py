"""Immutable values returned by runtime monitoring."""

from dataclasses import dataclass

from orcflow.node import NodeType
from orcflow.types import Status


@dataclass(frozen=True, slots=True)
class NodeInfo:
    """A node's execution summary, without its mutable tree."""

    id: str
    name: str
    type: NodeType
    status: Status | None
    progress: float | None


@dataclass(frozen=True, slots=True)
class WorkerInfo:
    """Captured worker metrics; memory is in bytes and times are in seconds.

    CPU usage covers interval and may exceed 100% for multithreaded
    processes. sampled_at is the Unix timestamp at the end of CPU sampling.
    """

    pid: int
    node: NodeInfo | None
    cpu_num: int | None
    cpu_percent: float
    memory: int
    threads: int
    interval: float
    sampled_at: float

import uuid
from enum import Enum

from orcflow import utils


class NodeType(Enum):
    FLOW = "flow"
    TASK = "task"


class Node:
    """Represent one flow or task execution in the run tree."""

    def __init__(self, name, type):
        self.id = uuid.uuid4().hex
        self.name = name
        self.type = type
        self.status = None
        self.children = []

    def add(self, name, type):
        """Add and return a child node."""
        child = Node(name, type)
        self.children.append(child)
        return child

    def _format(self):
        return f"{self.name} ({self.type.value}, {utils.color_status(self.status)})"

    def _print_children(self, prefix="  ", depth=None):
        if depth == 0:
            return

        for i, child in enumerate(self.children):
            is_last = i == len(self.children) - 1
            connector = "└── " if is_last else "├── "

            print(prefix + connector + child._format())

            child_prefix = prefix + ("    " if is_last else "│   ")
            child._print_children(child_prefix, None if depth is None else depth - 1)

    def print(self, depth=None):
        print(f"  {self._format()}")
        self._print_children(depth=depth)

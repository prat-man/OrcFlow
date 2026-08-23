import time

from orcflow.node import Node, NodeType


def test_elapsed_is_zero_before_node_starts():
    node = Node("task", NodeType.TASK)
    assert node.elapsed() == 0.0


def test_elapsed_uses_finished_time_when_terminal():
    node = Node("task", NodeType.TASK)
    node.started = 10.0
    node.finished = 12.5
    assert node.elapsed() == 2.5


def test_elapsed_advances_while_running():
    node = Node("task", NodeType.TASK)
    node.started = time.perf_counter()
    time.sleep(0.01)
    assert node.elapsed() > 0

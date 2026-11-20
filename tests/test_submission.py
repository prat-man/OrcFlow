import pytest

from orcflow import flow, task
from orcflow.runtime import Runtime


@pytest.fixture
def function_arguments():
    return {
        "name": "Ada",
        "tag": "user tag",
        "timeout": "user timeout",
        "parent": "user parent",
        "node": "user node",
        "fn": "user function",
        "args": [1, 2],
        "kwargs": {"x": 3},
        "runtime": "user runtime",
        "self": "user self",
    }


@task(name="echo({name})", tag="limited", timeout=10)
def echo_arguments(name, tag, timeout, parent, node, fn, args, kwargs, runtime, self):
    return {
        "name": name,
        "tag": tag,
        "timeout": timeout,
        "parent": parent,
        "node": node,
        "fn": fn,
        "args": args,
        "kwargs": kwargs,
        "runtime": runtime,
        "self": self,
    }


echo_flow = flow(echo_arguments.fn, name="flow({name})", timeout=20)


@flow
def submit_and_map_arguments(values):
    submitted = echo_arguments.submit(**values).result()
    mapped = echo_arguments.with_options(name="mapped({name})").map([
        values, {**values, "name": "Grace"},
    ]).results()
    return submitted, mapped


def test_task_submit_and_map_separate_function_arguments_from_options(function_arguments):
    with Runtime(n_workers=2, concurrency={"limited": 1}) as runtime:
        run = submit_and_map_arguments.bind(runtime).submit(function_arguments)
        assert run.result() == (
            function_arguments,
            [function_arguments, {**function_arguments, "name": "Grace"}],
        )
        assert [node.name for node in run.tree().children] == [
            "echo(Ada)", "mapped(Ada)", "mapped(Grace)",
        ]


def test_flow_separates_function_arguments_from_options(function_arguments):
    with Runtime(n_workers=1) as runtime:
        run = echo_flow.bind(runtime).submit(**function_arguments)
        assert run.result() == function_arguments
        assert run.tree().name == "flow(Ada)"


@flow(name="greeting({name})")
def greeting_flow(name):
    return f"Hello, {name}!"


@pytest.mark.parametrize("runtime_as_keyword", [False, True])
def test_flow_accepts_positional_or_keyword_runtime_binding(runtime_as_keyword):
    with Runtime(n_workers=1) as runtime:
        if runtime_as_keyword:
            bound = greeting_flow.bind(runtime=runtime)
        else:
            bound = greeting_flow.bind(runtime)
        run = bound.submit(name="Ada")
        assert run.result() == "Hello, Ada!"
        assert run.tree().name == "greeting(Ada)"


def test_flow_requires_a_runtime():
    with pytest.raises(RuntimeError, match="bind"):
        greeting_flow.submit(name="Ada")


def test_binding_leaves_original_unbound_and_can_be_reused():
    with Runtime(n_workers=1) as runtime:
        bound = greeting_flow.bind(runtime)
        assert bound is not greeting_flow
        with pytest.raises(RuntimeError, match="bind"):
            greeting_flow.submit(name="Ada")

        first = bound.submit(name="Ada")
        second = bound.submit(name="Grace")
        assert first.result() == "Hello, Ada!"
        assert second.result() == "Hello, Grace!"
        assert first.tree().id != second.tree().id
        assert first.tree().name == "greeting(Ada)"
        assert second.tree().name == "greeting(Grace)"


def test_bound_flow_with_options_can_submit_without_arguments():
    @flow
    def answer():
        return 42

    with Runtime(n_workers=1) as runtime:
        bound_flow = answer.bind(runtime)
        assert bound_flow.with_options(timeout=10).submit().result() == 42
        assert bound_flow.timeout is None


@pytest.mark.parametrize("options_first", [False, True])
def test_options_and_runtime_binding_work_in_either_order(options_first):
    with Runtime(n_workers=1) as runtime:
        if options_first:
            bound = greeting_flow.with_options(
                name="custom({name})", timeout=5,
            ).bind(runtime)
        else:
            original_binding = greeting_flow.bind(runtime)
            bound = original_binding.with_options(name="custom({name})", timeout=5)
            assert original_binding.name == "greeting({name})"
            assert original_binding.timeout is None

        assert greeting_flow.name == "greeting({name})"
        assert greeting_flow.timeout is None
        assert bound.timeout == 5
        run = bound.submit(name="Ada")
        assert run.result() == "Hello, Ada!"
        assert run.tree().name == "custom(Ada)"


def test_rebinding_does_not_change_existing_binding():
    with Runtime(n_workers=1) as first_runtime, Runtime(n_workers=1) as second_runtime:
        first_binding = greeting_flow.bind(first_runtime)
        second_binding = first_binding.bind(second_runtime)
        assert first_binding.submit(name="Ada").result() == "Hello, Ada!"
        assert second_binding.submit(name="Grace").result() == "Hello, Grace!"
        assert first_runtime.counts().finished == 1
        assert second_runtime.counts().finished == 1


def test_direct_flow_call_explains_binding_and_submission():
    with pytest.raises(TypeError, match="bind"):
        greeting_flow("Ada")


@flow
def submit_positional_argument(values):
    name, *rest = values.values()
    return echo_arguments.submit(name, *rest).result()


def test_positional_task_arguments_keep_their_order(function_arguments):
    with Runtime(n_workers=2) as runtime:
        run = submit_positional_argument.bind(runtime).submit(function_arguments)
        assert run.result() == function_arguments

"""Unit tests for task and workflow decorators."""

from aegisflow.core.models import RetryPolicy
from aegisflow.dsl.task import task
from aegisflow.dsl.workflow import Workflow, workflow


def test_bare_task_decorator() -> None:
    @task
    def add(x: int, y: int) -> int:
        return x + y

    assert add.name == "add"
    assert add.task_id == "add"
    assert add.is_async is False
    assert add(2, 3) == 5

    definition = add.to_definition()
    assert definition.name == "add"
    assert definition.retry_policy.max_retries == 3


def test_task_with_options() -> None:
    @task(retries=5, initial_interval=2.0, backoff=3.0, timeout=10.0)
    async def fetch_data(url: str) -> str:
        return f"content from {url}"

    assert fetch_data.name == "fetch_data"
    assert fetch_data.is_async is True
    assert fetch_data.timeout == 10.0
    assert fetch_data.retry_policy.max_retries == 5
    assert fetch_data.retry_policy.initial_interval_seconds == 2.0


def test_dependency_chaining_after() -> None:
    @task
    def step_a() -> str:
        return "a"

    @task
    def step_b() -> str:
        return "b"

    step_b.after(step_a)
    assert "step_a" in step_b.dependencies


def test_workflow_decorator_and_compilation() -> None:
    @task
    def step_1() -> str:
        return "1"

    @task
    def step_2() -> str:
        return "2"

    step_2.after(step_1)

    wf = Workflow(name="SimplePipeline", tasks=[step_1, step_2])
    dag = wf.compile()
    order = dag.topological_sort()
    assert order == ["step_1", "step_2"]

    # Also test @workflow decorator syntax
    @workflow(tasks=[step_1, step_2])
    def my_pipeline() -> None:
        pass

    assert isinstance(my_pipeline, Workflow)
    assert my_pipeline.compile().topological_sort() == ["step_1", "step_2"]


def test_retry_policy_delay_calculation() -> None:
    policy = RetryPolicy(
        max_retries=3,
        initial_interval_seconds=1.0,
        backoff_coefficient=2.0,
        max_interval_seconds=10.0,
        jitter=False,
    )
    # Attempt 1: 1.0 * (2^0) = 1.0
    assert policy.calculate_delay(1) == 1.0
    # Attempt 2: 1.0 * (2^1) = 2.0
    assert policy.calculate_delay(2) == 2.0
    # Attempt 3: 1.0 * (2^2) = 4.0
    assert policy.calculate_delay(3) == 4.0

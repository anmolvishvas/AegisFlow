"""OpenTelemetry instrumentation, distributed tracing, and metrics for AegisFlow."""

import contextlib
from collections.abc import Iterator
from typing import Any

try:
    from opentelemetry import trace
    from opentelemetry.trace import StatusCode

    HAS_OPENTELEMETRY = True
except ImportError:
    HAS_OPENTELEMETRY = False
    trace = None  # type: ignore[assignment]
    StatusCode = None  # type: ignore[assignment, misc]


class AegisTelemetry:
    """Provides vendor-neutral distributed tracing and metrics propagation."""

    def __init__(self, service_name: str = "aegisflow") -> None:
        self.service_name = service_name
        self._tracer: Any = None
        if HAS_OPENTELEMETRY and trace is not None:
            self._tracer = trace.get_tracer(service_name)

    @property
    def is_enabled(self) -> bool:
        """Return True if OpenTelemetry is installed and tracer is initialized."""
        return self._tracer is not None

    @contextlib.contextmanager
    def trace_workflow(
        self,
        workflow_id: str,
        workflow_name: str,
        attributes: dict[str, Any] | None = None,
    ) -> Iterator[Any]:
        """Create a tracing span for an entire workflow run."""
        if not self.is_enabled or self._tracer is None:
            yield None
            return

        span_name = f"workflow:{workflow_name}"
        with self._tracer.start_as_current_span(span_name) as span:
            span.set_attribute("aegisflow.workflow_id", workflow_id)
            span.set_attribute("aegisflow.workflow_name", workflow_name)
            if attributes:
                for k, v in attributes.items():
                    span.set_attribute(f"aegisflow.workflow.{k}", str(v))
            try:
                yield span
                if StatusCode is not None:
                    span.set_status(StatusCode.OK)
            except Exception as exc:
                if StatusCode is not None:
                    span.set_status(StatusCode.ERROR, str(exc))
                span.record_exception(exc)
                raise

    @contextlib.contextmanager
    def trace_task(
        self,
        task_id: str,
        workflow_id: str,
        attributes: dict[str, Any] | None = None,
    ) -> Iterator[Any]:
        """Create a tracing span for a single task execution."""
        if not self.is_enabled or self._tracer is None:
            yield None
            return

        span_name = f"task:{task_id}"
        with self._tracer.start_as_current_span(span_name) as span:
            span.set_attribute("aegisflow.task_id", task_id)
            span.set_attribute("aegisflow.workflow_id", workflow_id)
            if attributes:
                for k, v in attributes.items():
                    span.set_attribute(f"aegisflow.task.{k}", str(v))
            try:
                yield span
                if StatusCode is not None:
                    span.set_status(StatusCode.OK)
            except Exception as exc:
                if StatusCode is not None:
                    span.set_status(StatusCode.ERROR, str(exc))
                span.record_exception(exc)
                raise


# Global default telemetry provider
telemetry = AegisTelemetry()

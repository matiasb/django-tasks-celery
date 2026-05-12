from collections.abc import Iterable
from typing import Any, TypeVar

from celery import Task as CeleryTask
from celery import current_app as celery_app
from celery import shared_task
from celery.app import default_app
from celery.result import AsyncResult
from celery.states import FAILURE, PENDING, RECEIVED, RETRY, REVOKED, STARTED, SUCCESS
from django.apps import apps
from django.core import checks
from django.utils import timezone
from django_tasks.backends.base import BaseTaskBackend
from django_tasks.base import (
    DEFAULT_TASK_PRIORITY,
    DEFAULT_TASK_QUEUE_NAME,
    TASK_MAX_PRIORITY,
    TASK_MIN_PRIORITY,
    TaskError,
    TaskResult,
    TaskResultStatus,
)
from django_tasks.base import (
    Task as BaseTask,
)
from django_tasks.exceptions import TaskResultDoesNotExist
from django_tasks.signals import task_enqueued
from django_tasks.utils import get_random_id
from typing_extensions import ParamSpec

from .compat import TASK_CLASSES

if not default_app:
    from django_tasks_celery.app import app as celery_app

    celery_app.set_default()


T = TypeVar("T")
P = ParamSpec("P")


CELERY_MIN_PRIORITY = 0
CELERY_MAX_PRIORITY = 9

CELERY_STATUS_TO_RESULT_STATUS = {
    PENDING: TaskResultStatus.READY,
    RECEIVED: TaskResultStatus.READY,
    STARTED: TaskResultStatus.RUNNING,
    RETRY: TaskResultStatus.RUNNING,
    SUCCESS: TaskResultStatus.SUCCESSFUL,
    FAILURE: TaskResultStatus.FAILED,
    REVOKED: TaskResultStatus.FAILED,
}

DJANGO_TASKS_PRIORITY_HEADER = "django_tasks_priority"


def _map_priority(value: int) -> int:
    """Map django-tasks priority range to Celery's 0-9 range."""
    scaled_value = (value + abs(TASK_MIN_PRIORITY)) / (
        (TASK_MAX_PRIORITY - TASK_MIN_PRIORITY)
        / (CELERY_MAX_PRIORITY - CELERY_MIN_PRIORITY)
    )
    mapped_value = int(scaled_value)

    return max(CELERY_MIN_PRIORITY, min(mapped_value, CELERY_MAX_PRIORITY))


def _unmap_priority(value: int) -> int:
    """Map Celery's 0-9 priority range back to django-tasks."""
    scaled_value = TASK_MIN_PRIORITY + (
        value * (TASK_MAX_PRIORITY - TASK_MIN_PRIORITY)
    ) / (CELERY_MAX_PRIORITY - CELERY_MIN_PRIORITY)
    mapped_value = int(round(scaled_value))

    return max(TASK_MIN_PRIORITY, min(mapped_value, TASK_MAX_PRIORITY))


class Task(BaseTask[P, T]):
    """Celery proxy to the task in the current celery app task registry."""

    def __post_init__(self) -> None:
        import functools

        if self.takes_context:

            @functools.wraps(self.func)
            def wrapper(celery_task_self: CeleryTask, *args: Any, **kwargs: Any) -> Any:
                from django_tasks.base import TaskContext

                request = celery_task_self.request
                hostname = request.hostname or "unknown"
                headers = request.headers or {}
                celery_priority = (
                    request.delivery_info.get("priority", DEFAULT_TASK_PRIORITY)
                    if request.delivery_info
                    else DEFAULT_TASK_PRIORITY
                )
                priority = headers.get(
                    DJANGO_TASKS_PRIORITY_HEADER, _unmap_priority(celery_priority)
                )

                # We build a synthetic TaskResult on the fly from the worker context
                # without needing the result backend
                task_result = TaskResult[
                    T
                ](
                    task=self.using(
                        priority=priority,
                        queue_name=request.delivery_info.get(
                            "routing_key", DEFAULT_TASK_QUEUE_NAME
                        )
                        if request.delivery_info
                        else DEFAULT_TASK_QUEUE_NAME,
                        backend=self.backend,
                        run_after=request.eta,
                    ),
                    id=request.id,
                    status=TaskResultStatus.RUNNING,
                    enqueued_at=None,
                    started_at=None,  # Cannot determine start time precisely without additional context
                    last_attempted_at=None,
                    finished_at=None,
                    args=list(args),
                    kwargs=kwargs,
                    backend=self.backend,
                    errors=[],
                    worker_ids=[hostname],
                )

                # Synthesize attempt from retries + 1 (first run is retries=0)
                # TaskContext internally counts len(worker_ids) for attempts, so let's adjust array size
                for _ in range(request.retries):
                    task_result.worker_ids.append(hostname)

                context = TaskContext(task_result=task_result)
                args = (context, *args)
                return self.call(*args, **kwargs)

            # register task with Celery app
            # https://docs.celeryq.dev/en/stable/django/first-steps-with-django.html#using-the-shared-task-decorator
            shared_task(name=self.module_path, bind=True)(wrapper)
        else:
            # register task with Celery app
            shared_task(name=self.module_path)(self.call)

        return super().__post_init__()


class CeleryBackend(BaseTaskBackend):
    task_class = Task
    supports_defer = True
    supports_async_task = True
    supports_priority = True
    supports_get_result = True

    def enqueue(
        self,
        task: Task[P, T],  # type: ignore[override]
        args: P.args,  # type:ignore[valid-type]
        kwargs: P.kwargs,  # type:ignore[valid-type]
    ) -> TaskResult[T]:
        self.validate_task(task)

        task_id = get_random_id()

        send_task_kwargs: dict[str, Any] = {
            "task_id": task_id,
            "eta": task.run_after,
            "priority": _map_priority(task.priority),
            "headers": {DJANGO_TASKS_PRIORITY_HEADER: task.priority},
        }
        if task.queue_name:
            send_task_kwargs["queue"] = task.queue_name

        celery_app.send_task(
            task.module_path,
            args=args,
            kwargs=kwargs,
            **send_task_kwargs,
        )

        task_result = TaskResult[T](
            task=task,
            id=task_id,
            status=TaskResultStatus.READY,
            enqueued_at=timezone.now(),
            started_at=None,
            last_attempted_at=None,
            finished_at=None,
            args=args,
            kwargs=kwargs,
            backend=self.alias,
            errors=[],
            worker_ids=[],
        )

        task_enqueued.send(type(self), task_result=task_result)

        return task_result

    def get_result(self, result_id: str) -> TaskResult:
        if celery_app.conf.result_backend is None:
            raise ValueError("Celery result backend is not configured")

        async_result = AsyncResult(result_id)
        state = async_result.state
        status = CELERY_STATUS_TO_RESULT_STATUS.get(state, TaskResultStatus.READY)

        errors: list[TaskError] = []
        if state == FAILURE and async_result.result is not None:
            exc = async_result.result
            from django_tasks.utils import get_exception_traceback, get_module_path

            errors.append(
                TaskError(
                    exception_class_path=get_module_path(type(exc)),
                    traceback=get_exception_traceback(exc),
                )
            )

        return_value = None
        if state == SUCCESS:
            return_value = async_result.result

        date_done = async_result.date_done

        completed = state in (SUCCESS, FAILURE, REVOKED)

        task_result: TaskResult = TaskResult(
            task=self._get_task_from_result(async_result),
            id=result_id,
            status=status,
            enqueued_at=None,
            started_at=None,
            last_attempted_at=date_done if completed else None,
            finished_at=date_done if completed else None,
            args=async_result.args or [],
            kwargs=async_result.kwargs or {},
            backend=self.alias,
            errors=errors,
            worker_ids=[],
        )

        if return_value is not None:
            object.__setattr__(task_result, "_return_value", return_value)

        return task_result

    def _get_task_from_result(self, async_result: AsyncResult) -> Task:
        from django.utils.module_loading import import_string

        if not celery_app.conf.result_extended:
            # we cannot reverse the task without the result extended information
            # which include the task name
            raise ValueError(
                "You need to set CELERY_RESULT_EXTENDED=True in your settings"
            )

        task_name = async_result.name
        if task_name is None:
            raise TaskResultDoesNotExist(async_result.id)

        task = import_string(task_name)

        if not isinstance(task, TASK_CLASSES):
            from django.core.exceptions import SuspiciousOperation

            raise SuspiciousOperation(
                f"Task {async_result.id} does not point to a Task ({task_name})"
            )

        return task.using(backend=self.alias)  # type:ignore[return-value]

    def check(self, **kwargs: Any) -> Iterable[checks.CheckMessage]:
        yield from super().check(**kwargs)

        backend_name = self.__class__.__name__

        if not apps.is_installed("django_tasks_celery"):
            yield checks.Error(
                f"{backend_name} configured as django_tasks backend, but django_tasks_celery app not installed",
                hint="Insert 'django_tasks_celery' in INSTALLED_APPS",
            )

        # Check that a result backend is configured
        result_backend = celery_app.conf.result_backend
        if not result_backend or result_backend == "disabled":
            yield checks.Warning(
                f"{backend_name} requires a Celery result backend for get_result() support",
                hint="Configure CELERY_RESULT_BACKEND in your settings",
            )
        elif not celery_app.conf.result_extended:
            yield checks.Warning(
                f"{backend_name} requires CELERY_RESULT_EXTENDED=True for get_result() support",
                hint="Set CELERY_RESULT_EXTENDED = True in your settings",
            )

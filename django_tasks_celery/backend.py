from collections.abc import Iterable
from typing import Any, TypeVar

from celery import current_app as celery_app
from celery.result import AsyncResult
from celery.states import FAILURE, PENDING, REVOKED, STARTED, SUCCESS
from django.apps import apps
from django.core import checks
from django.utils import timezone
from django_tasks.backends.base import BaseTaskBackend
from django_tasks.base import (
    TASK_MAX_PRIORITY,
    TASK_MIN_PRIORITY,
    Task,
    TaskError,
    TaskResult,
    TaskResultStatus,
)
from django_tasks.exceptions import TaskResultDoesNotExist
from django_tasks.signals import task_enqueued
from django_tasks.utils import get_random_id
from typing_extensions import ParamSpec

from .compat import TASK_CLASSES

T = TypeVar("T")
P = ParamSpec("P")


CELERY_MIN_PRIORITY = 0
CELERY_MAX_PRIORITY = 9

CELERY_STATUS_TO_RESULT_STATUS = {
    PENDING: TaskResultStatus.READY,
    STARTED: TaskResultStatus.RUNNING,
    SUCCESS: TaskResultStatus.SUCCESSFUL,
    FAILURE: TaskResultStatus.FAILED,
    REVOKED: TaskResultStatus.FAILED,
}


def _map_priority(value: int) -> int:
    """Map django-tasks priority range to Celery's 0-9 range."""
    scaled_value = (value + abs(TASK_MIN_PRIORITY)) / (
        (TASK_MAX_PRIORITY - TASK_MIN_PRIORITY)
        / (CELERY_MAX_PRIORITY - CELERY_MIN_PRIORITY)
    )
    mapped_value = int(scaled_value)

    return max(CELERY_MIN_PRIORITY, min(mapped_value, CELERY_MAX_PRIORITY))


class CeleryBackend(BaseTaskBackend):
    supports_defer = True
    supports_async_task = True
    supports_priority = True
    supports_get_result = True

    def enqueue(
        self,
        task: Task[P, T],
        args: P.args,  # type:ignore[valid-type]
        kwargs: P.kwargs,  # type:ignore[valid-type]
    ) -> TaskResult[T]:
        self.validate_task(task)

        task_id = get_random_id()

        send_task_kwargs: dict[str, Any] = {
            "task_id": task_id,
            "eta": task.run_after,
            "priority": _map_priority(task.priority),
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

        task_result: TaskResult = TaskResult(
            task=self._get_task_from_result(async_result),
            id=result_id,
            status=status,
            enqueued_at=None,
            started_at=date_done if state in (SUCCESS, FAILURE) else None,
            last_attempted_at=date_done if state in (SUCCESS, FAILURE) else None,
            finished_at=date_done if state in (SUCCESS, FAILURE) else None,
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

        task_name = async_result.name
        if task_name is None:
            raise TaskResultDoesNotExist(async_result.id)

        task = import_string(task_name)

        if not isinstance(task, TASK_CLASSES):
            from django.core.exceptions import SuspiciousOperation

            raise SuspiciousOperation(
                f"Task {async_result.id} does not point to a Task ({task_name})"
            )

        return task.using(backend=self.alias)  # type:ignore[no-any-return]

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

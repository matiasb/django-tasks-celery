import functools
from collections.abc import Iterable
from datetime import datetime
from typing import Any, TypeVar

from celery import Task as CeleryTask
from celery import current_app as celery_app
from celery import shared_task
from celery.app import default_app
from celery.backends.base import KeyValueStoreBackend
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
from django_tasks.signals import task_enqueued, task_finished, task_started
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
STARTED_AT_KEY_PREFIX = b"django-tasks-celery-started-at:"


def _result_backend_enabled() -> bool:
    backend = celery_app.conf.result_backend
    return bool(backend) and backend != "disabled"


def _supports_started_at_persistence() -> bool:
    """`started_at` is persisted via a side-channel key in the result backend.

    Requires a key-value-style backend (Redis, memcached, cache, filesystem,
    MongoDB). Database and RPC backends don't expose set/get and so don't
    persist `started_at`.
    """
    return _result_backend_enabled() and isinstance(
        celery_app.backend, KeyValueStoreBackend
    )


def _started_at_key(task_id: str) -> bytes:
    return STARTED_AT_KEY_PREFIX + task_id.encode()


def _store_started_at(task_id: str, started_at: datetime) -> None:
    if not _supports_started_at_persistence():
        return
    celery_app.backend.set(_started_at_key(task_id), started_at.isoformat().encode())


def _read_started_at(task_id: str) -> datetime | None:
    if not _supports_started_at_persistence():
        return None
    raw = celery_app.backend.get(_started_at_key(task_id))
    if not raw:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode()
    return datetime.fromisoformat(raw)


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

    def _build_task_result(
        self,
        request: Any,
        args: tuple,
        kwargs: dict,
    ) -> "TaskResult[T]":
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

        task_result = TaskResult[T](
            task=self.using(
                priority=priority,
                queue_name=(
                    request.delivery_info.get("routing_key", DEFAULT_TASK_QUEUE_NAME)
                    if request.delivery_info
                    else DEFAULT_TASK_QUEUE_NAME
                ),
                backend=self.backend,
                run_after=request.eta,
            ),
            id=request.id,
            status=TaskResultStatus.RUNNING,
            enqueued_at=None,
            started_at=None,
            last_attempted_at=None,
            finished_at=None,
            args=list(args),
            kwargs=kwargs,
            backend=self.backend,
            errors=[],
            worker_ids=[hostname],
        )

        for _ in range(request.retries):
            task_result.worker_ids.append(hostname)

        return task_result

    def __post_init__(self) -> None:
        # Idempotent registration: every `task.using(...)` call constructs a
        # new Task instance via dataclasses.replace, which re-runs
        # __post_init__. Without this guard we'd re-register the Celery task
        # (and rebuild the wrapper closure) on every enqueue path that uses
        # .using(). `func` and `takes_context` don't change across .using()
        # calls, so the first-registered wrapper is correct.
        if self.module_path in celery_app.tasks:
            return super().__post_init__()

        @functools.wraps(self.func)
        def wrapper(celery_task_self: CeleryTask, *args: Any, **kwargs: Any) -> Any:
            from django_tasks.base import TaskContext

            started_at = timezone.now()
            task_result = self._build_task_result(
                celery_task_self.request, args, kwargs
            )
            object.__setattr__(task_result, "started_at", started_at)
            object.__setattr__(task_result, "last_attempted_at", started_at)
            backend_cls = type(self.get_backend())

            # Mark STARTED so get_result() can surface RUNNING status
            # without requiring CELERY_TASK_TRACK_STARTED=True, and persist
            # started_at in a side-channel key so it survives the result
            # meta being overwritten when Celery stores the return value /
            # exception on completion.
            if _result_backend_enabled():
                celery_task_self.update_state(state=STARTED)
                _store_started_at(celery_task_self.request.id, started_at)

            task_started.send(backend_cls, task_result=task_result)
            try:
                # `self.call()` handles both sync and async funcs (coroutines
                # are wrapped via async_to_sync), so we don't need separate
                # branches or asyncio.run() here.
                if self.takes_context:
                    return_value = self.call(
                        TaskContext(task_result=task_result),  # type: ignore[arg-type]
                        *args,
                        **kwargs,
                    )
                else:
                    return_value = self.call(*args, **kwargs)
            except Exception:
                object.__setattr__(task_result, "status", TaskResultStatus.FAILED)
                task_finished.send(backend_cls, task_result=task_result)
                raise
            object.__setattr__(task_result, "status", TaskResultStatus.SUCCESSFUL)
            task_finished.send(backend_cls, task_result=task_result)
            return return_value

        shared_task(name=self.module_path, bind=True)(wrapper)

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

        # Pre-populate the result backend with the task name so get_result()
        # can reconstruct the Task object before the worker has run.
        # Must happen before send_task() to avoid a race where the worker
        # stores SUCCESS before we store PENDING.
        if (
            celery_app.conf.result_backend
            and celery_app.conf.result_backend != "disabled"
            and celery_app.conf.result_extended
        ):
            from types import SimpleNamespace

            celery_app.backend.store_result(
                task_id,
                None,
                "PENDING",
                request=SimpleNamespace(
                    task=task.module_path,
                    args=list(args),
                    kwargs=kwargs,
                    hostname=None,
                    retries=0,
                    delivery_info={},
                ),
            )

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
            from django_tasks.utils import get_module_path

            exc = async_result.result
            errors.append(
                TaskError(
                    exception_class_path=get_module_path(type(exc)),
                    # Use the worker's serialized traceback string rather
                    # than re-formatting the deserialized exception (whose
                    # __traceback__ is lost in transit).
                    traceback=async_result.traceback or "",
                )
            )

        return_value = None
        if state == SUCCESS:
            return_value = async_result.result

        date_done = async_result.date_done

        completed = state in (SUCCESS, FAILURE, REVOKED)

        started_at = _read_started_at(result_id)

        # Populate worker_ids from result_extended; repeat per attempt so
        # `attempts` (== len(worker_ids)) reflects retries.
        worker_ids: list[str] = []
        if async_result.worker:
            retries = async_result.retries or 0
            worker_ids = [async_result.worker] * (retries + 1)

        task_result: TaskResult = TaskResult(
            task=self._get_task_from_result(async_result),
            id=result_id,
            status=status,
            enqueued_at=None,
            started_at=started_at,
            last_attempted_at=started_at or (date_done if completed else None),
            finished_at=date_done if completed else None,
            args=async_result.args or [],
            kwargs=async_result.kwargs or {},
            backend=self.alias,
            errors=errors,
            worker_ids=worker_ids,
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

        broker_url = celery_app.conf.broker_url or ""
        if broker_url and not broker_url.startswith(("amqp://", "amqps://")):
            yield checks.Warning(
                f"{backend_name} priority support requires an AMQP-compatible broker",
                hint=(
                    "Priority queues are reliably supported only with RabbitMQ (amqp://). "
                    "Other brokers may not respect task priority."
                ),
            )

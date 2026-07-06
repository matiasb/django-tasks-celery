from __future__ import annotations

import functools
import json
from collections.abc import Iterable
from datetime import datetime
from typing import Any, Generic, TypeVar

from celery import Task as CeleryTask
from celery import current_app as celery_app
from celery import shared_task
from celery.backends.base import KeyValueStoreBackend
from celery.result import AsyncResult
from celery.states import FAILURE, PENDING, RECEIVED, RETRY, REVOKED, STARTED, SUCCESS
from django.apps import apps
from django.core import checks
from django.core.exceptions import ImproperlyConfigured, SuspiciousOperation
from django.utils import timezone
from typing_extensions import ParamSpec

from .compat import (
    DEFAULT_TASK_PRIORITY,
    DEFAULT_TASK_QUEUE_NAME,
    TASK_CLASSES,
    TASK_MAX_PRIORITY,
    TASK_MIN_PRIORITY,
    BaseTaskBackend,
    TaskError,
    TaskResult,
    TaskResultDoesNotExist,
    TaskResultStatus,
    get_exception_traceback,
    get_module_path,
    get_random_id,
    normalize_json,
    task_enqueued,
    task_finished,
    task_started,
)
from .compat import (
    Task as BaseTask,
)

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
STARTED_AT_KEY_PREFIX = b"django-tasks-started-at:"
ENQUEUE_INFO_KEY_PREFIX = b"django-tasks-enqueue:"

# Prefix used when registering Django Tasks in Celery's task registry, so
# they don't collide with unrelated @shared_task registrations that may
# share the same dotted path (e.g., a user-defined `tasks.send_email` as
# both a plain Celery task and a Django Task).
CELERY_TASK_NAME_PREFIX = "django_tasks:"


def _to_celery_name(module_path: str) -> str:
    return f"{CELERY_TASK_NAME_PREFIX}{module_path}"


def _to_module_path(task_name: str) -> str:
    """Strip the django-tasks-fennel namespace prefix from a Celery task
    name to recover the importable module path. Leaves unprefixed names
    alone so the side-channel (which stores raw module paths) round-trips."""
    if task_name.startswith(CELERY_TASK_NAME_PREFIX):
        return task_name[len(CELERY_TASK_NAME_PREFIX) :]
    return task_name


def _result_backend_enabled() -> bool:
    backend = celery_app.conf.result_backend
    return bool(backend) and backend != "disabled"


def _supports_side_channel() -> bool:
    """Side-channel keys live in the Celery result backend.

    They require a key-value-style backend (Redis, memcached, cache,
    filesystem, MongoDB). Database (db+://) and RPC (rpc://) backends
    don't expose set/get, so side-channel data is unavailable there.
    """
    return _result_backend_enabled() and isinstance(
        celery_app.backend, KeyValueStoreBackend
    )


def _started_at_key(task_id: str) -> bytes:
    return STARTED_AT_KEY_PREFIX + task_id.encode()


def _enqueue_info_key(task_id: str) -> bytes:
    return ENQUEUE_INFO_KEY_PREFIX + task_id.encode()


def _store_started_at(task_id: str, started_at: datetime) -> None:
    if not _supports_side_channel():
        return
    celery_app.backend.set(_started_at_key(task_id), started_at.isoformat().encode())


def _read_started_at(task_id: str) -> datetime | None:
    if not _supports_side_channel():
        return None
    raw = celery_app.backend.get(_started_at_key(task_id))
    if not raw:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode()
    return datetime.fromisoformat(raw)


def _store_enqueue_info(
    task_id: str,
    task_module_path: str,
    args: Any,
    kwargs: Any,
    enqueued_at: datetime,
) -> None:
    """Persist task name, args, kwargs, and enqueued_at on enqueue.

    Lets get_result() reconstruct the Task before the worker has stored a
    result (and even without CELERY_RESULT_EXTENDED), and surface
    enqueued_at on TaskResult — Celery's result backend doesn't store it.
    """
    if not _supports_side_channel():
        return
    payload = json.dumps(
        {
            "name": task_module_path,
            "args": normalize_json(list(args)),
            "kwargs": normalize_json(dict(kwargs)),
            "enqueued_at": enqueued_at.isoformat(),
        }
    ).encode()
    celery_app.backend.set(_enqueue_info_key(task_id), payload)


def _read_enqueue_info(task_id: str) -> dict[str, Any] | None:
    if not _supports_side_channel():
        return None
    raw = celery_app.backend.get(_enqueue_info_key(task_id))
    if not raw:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode()
    return json.loads(raw)  # type: ignore[no-any-return]


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


class Task(BaseTask, Generic[P, T]):
    """Celery proxy to the task in the current celery app task registry.

    ``BaseTask`` is generic on the standalone ``django-tasks`` package but not
    on Django 6.0's built-in ``django.tasks``, so we mix in ``Generic[P, T]``
    directly rather than subscripting the base (which fails at runtime on the
    built-in framework).
    """

    def _build_task_result(
        self,
        request: Any,
        args: tuple,
        kwargs: dict,
    ) -> TaskResult[T]:
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

        # On the worker side Celery delivers `eta` as an ISO-8601 string, but
        # django-tasks expects an aware datetime for run_after (validate_task
        # calls timezone.is_aware on it). Parse it back; leave None as-is.
        run_after = request.eta
        if isinstance(run_after, str):
            run_after = datetime.fromisoformat(run_after)

        task_result: TaskResult[T] = TaskResult(
            task=self.using(
                priority=priority,
                queue_name=(
                    request.delivery_info.get("routing_key", DEFAULT_TASK_QUEUE_NAME)
                    if request.delivery_info
                    else DEFAULT_TASK_QUEUE_NAME
                ),
                backend=self.backend,
                run_after=run_after,
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
        celery_name = _to_celery_name(self.module_path)
        if celery_name in celery_app.tasks:
            return super().__post_init__()

        @functools.wraps(self.func)
        def wrapper(celery_task_self: CeleryTask, *args: Any, **kwargs: Any) -> Any:
            from .compat import TaskContext

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
                        TaskContext(task_result=task_result),
                        *args,
                        **kwargs,
                    )
                else:
                    return_value = self.call(*args, **kwargs)
            except Exception as exc:
                task_result.errors.append(
                    TaskError(
                        exception_class_path=get_module_path(type(exc)),
                        traceback=get_exception_traceback(exc),
                    )
                )
                object.__setattr__(task_result, "status", TaskResultStatus.FAILED)
                task_finished.send(backend_cls, task_result=task_result)
                raise
            object.__setattr__(task_result, "status", TaskResultStatus.SUCCESSFUL)
            task_finished.send(backend_cls, task_result=task_result)
            return return_value

        shared_task(name=celery_name, bind=True)(wrapper)

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
        enqueued_at = timezone.now()

        # Persist enqueue info in a side-channel key so get_result() can
        # reconstruct the Task and surface enqueued_at before the worker
        # has stored a result — and even when CELERY_RESULT_EXTENDED is
        # False. Must happen before send_task() to avoid a race where the
        # worker writes its own result first.
        _store_enqueue_info(task_id, task.module_path, args, kwargs, enqueued_at)

        send_task_kwargs: dict[str, Any] = {
            "task_id": task_id,
            "eta": task.run_after,
            "priority": _map_priority(task.priority),
            "headers": {DJANGO_TASKS_PRIORITY_HEADER: task.priority},
        }
        if task.queue_name:
            send_task_kwargs["queue"] = task.queue_name

        celery_app.send_task(
            _to_celery_name(task.module_path),
            args=args,
            kwargs=kwargs,
            **send_task_kwargs,
        )

        task_result: TaskResult[T] = TaskResult(
            task=task,
            id=task_id,
            status=TaskResultStatus.READY,
            enqueued_at=enqueued_at,
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
        if not _result_backend_enabled():
            raise ImproperlyConfigured(
                "Celery result backend is not configured; "
                "set CELERY_RESULT_BACKEND to enable get_result()."
            )

        async_result = AsyncResult(result_id)
        state = async_result.state
        status = CELERY_STATUS_TO_RESULT_STATUS.get(state, TaskResultStatus.READY)

        enqueue_info = _read_enqueue_info(result_id) or {}

        # Prefer Celery's view (populated once the worker has stored the
        # result with CELERY_RESULT_EXTENDED=True) and fall back to the
        # side-channel info we wrote on enqueue. This covers the pre-worker
        # window as well as result_extended=False setups.
        task_name = async_result.name or enqueue_info.get("name")
        if task_name is None:
            raise TaskResultDoesNotExist(result_id)

        errors: list[TaskError] = []
        if state == FAILURE and async_result.result is not None:
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

        enqueued_at = None
        if "enqueued_at" in enqueue_info:
            enqueued_at = datetime.fromisoformat(enqueue_info["enqueued_at"])

        # Populate worker_ids from result_extended; repeat per attempt so
        # `attempts` (== len(worker_ids)) reflects retries.
        worker_ids: list[str] = []
        if async_result.worker:
            retries = async_result.retries or 0
            worker_ids = [async_result.worker] * (retries + 1)

        task_result: TaskResult = TaskResult(
            task=self._resolve_task(task_name, result_id),
            id=result_id,
            status=status,
            enqueued_at=enqueued_at,
            started_at=started_at,
            last_attempted_at=started_at or (date_done if completed else None),
            finished_at=date_done if completed else None,
            args=async_result.args or enqueue_info.get("args") or [],
            kwargs=async_result.kwargs or enqueue_info.get("kwargs") or {},
            backend=self.alias,
            errors=errors,
            worker_ids=worker_ids,
        )

        if return_value is not None:
            object.__setattr__(task_result, "_return_value", return_value)

        return task_result

    def _resolve_task(self, task_name: str, result_id: str) -> Task:
        from django.utils.module_loading import import_string

        # task_name may come from async_result.name (prefixed by us at
        # registration) or from the side-channel (raw module path).
        module_path = _to_module_path(task_name)
        task = import_string(module_path)

        if not isinstance(task, TASK_CLASSES):
            raise SuspiciousOperation(
                f"Task {result_id} does not point to a Task ({task_name})"
            )

        return task.using(backend=self.alias)  # type:ignore[return-value]

    def check(self, **kwargs: Any) -> Iterable[checks.CheckMessage]:
        yield from super().check(**kwargs)

        backend_name = self.__class__.__name__

        if not apps.is_installed("django_tasks_fennel"):
            yield checks.Error(
                f"{backend_name} configured as django_tasks backend, but django_tasks_fennel app not installed",
                hint="Insert 'django_tasks_fennel' in INSTALLED_APPS",
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
                f"{backend_name} recommends CELERY_RESULT_EXTENDED=True for full TaskResult fidelity",
                hint=(
                    "Without CELERY_RESULT_EXTENDED, worker_ids and attempts "
                    "on completed tasks will be empty. Task name, args, and "
                    "kwargs still work via the side-channel on key-value "
                    "result backends."
                ),
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

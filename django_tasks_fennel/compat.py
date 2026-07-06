"""Framework compatibility layer.

This backend works against either Django's built-in ``django.tasks`` framework
(Django 6.0+) or the standalone ``django-tasks`` package (Django 5.2). On 6.0+
the built-in framework is preferred, so the standalone ``django-tasks``
dependency is only required on 5.2 (installed via the ``django-tasks`` extra:
``pip install django-tasks-fennel[django-tasks]``).

Every framework import in this package goes through this module, so the switch
between the two implementations lives in exactly one place.
"""

from traceback import format_exception
from typing import TYPE_CHECKING, Any

from django import VERSION as _DJANGO_VERSION
from django.utils.crypto import get_random_string

# Django 6.0 ships the framework in core as ``django.tasks``; earlier versions
# rely on the standalone ``django-tasks`` package. Switch on the version rather
# than import availability so the built-in framework wins when both happen to be
# installed on 6.0.
USE_CORE_TASKS = _DJANGO_VERSION >= (6, 0)

if TYPE_CHECKING:
    # Type-check against the standalone ``django-tasks`` package, which ships
    # inline types; Django's built-in ``django.tasks`` isn't covered by
    # django-stubs yet, so type-checking the core imports below would collapse
    # everything to ``Any``. The runtime branches pick the real framework.
    from django_tasks import (
        TaskResultStatus,
        default_task_backend,
        task,
        task_backends,
    )
    from django_tasks.backends.base import BaseTaskBackend
    from django_tasks.base import (
        DEFAULT_TASK_PRIORITY,
        DEFAULT_TASK_QUEUE_NAME,
        TASK_MAX_PRIORITY,
        TASK_MIN_PRIORITY,
        Task,
        TaskContext,
        TaskError,
        TaskResult,
    )
    from django_tasks.exceptions import InvalidTaskError, TaskResultDoesNotExist
    from django_tasks.signals import task_enqueued, task_finished, task_started
    from django_tasks.utils import normalize_json
elif USE_CORE_TASKS:
    from django.tasks import (
        TaskResultStatus,
        default_task_backend,
        task,
        task_backends,
    )
    from django.tasks.backends.base import BaseTaskBackend
    from django.tasks.base import (
        DEFAULT_TASK_PRIORITY,
        DEFAULT_TASK_QUEUE_NAME,
        TASK_MAX_PRIORITY,
        TASK_MIN_PRIORITY,
        Task,
        TaskContext,
        TaskError,
        TaskResult,
    )
    from django.tasks.exceptions import (
        InvalidTask as InvalidTaskError,
    )
    from django.tasks.exceptions import (
        TaskResultDoesNotExist,
    )
    from django.tasks.signals import task_enqueued, task_finished, task_started
    from django.utils.json import normalize_json
else:
    from importlib.util import find_spec

    if find_spec("django_tasks") is None:
        raise ImportError(
            "django-tasks-fennel requires the standalone 'django-tasks' package "
            "on Django < 6.0 (Django 6.0+ ships the built-in django.tasks "
            "framework instead). Install it with: "
            "pip install 'django-tasks-fennel[django-tasks]'"
        )

    from django_tasks import (
        TaskResultStatus,
        default_task_backend,
        task,
        task_backends,
    )
    from django_tasks.backends.base import BaseTaskBackend
    from django_tasks.base import (
        DEFAULT_TASK_PRIORITY,
        DEFAULT_TASK_QUEUE_NAME,
        TASK_MAX_PRIORITY,
        TASK_MIN_PRIORITY,
        Task,
        TaskContext,
        TaskError,
        TaskResult,
    )
    from django_tasks.exceptions import InvalidTaskError, TaskResultDoesNotExist
    from django_tasks.signals import task_enqueued, task_finished, task_started
    from django_tasks.utils import normalize_json


# The three helpers below live in ``django_tasks.utils`` in the standalone
# package but were not carried into core ``django.tasks`` (``normalize_json``
# was, at ``django.utils.json``, and is imported above). They're small and
# stable, so vendor them here to keep a single implementation across both
# frameworks.
def get_module_path(val: Any) -> str:
    return f"{val.__module__}.{val.__qualname__}"


def get_exception_traceback(exc: BaseException) -> str:
    return "".join(format_exception(exc))


def get_random_id() -> str:
    # 32 chars: same length the standalone package uses; Celery accepts any
    # string task id.
    return get_random_string(32)


# Recognize a Task from either framework in isinstance checks: the active one
# always, plus the other when it's importable (both installed on 6.0).
def _other_framework_task() -> Any:
    import importlib

    other = "django_tasks.base" if USE_CORE_TASKS else "django.tasks.base"
    try:
        return importlib.import_module(other).Task
    except ImportError:
        return None


_OTHER_TASK = _other_framework_task()
TASK_CLASSES = (Task,) if _OTHER_TASK is None else (Task, _OTHER_TASK)


__all__ = [
    "BaseTaskBackend",
    "DEFAULT_TASK_PRIORITY",
    "DEFAULT_TASK_QUEUE_NAME",
    "TASK_CLASSES",
    "TASK_MAX_PRIORITY",
    "TASK_MIN_PRIORITY",
    "InvalidTaskError",
    "Task",
    "TaskContext",
    "TaskError",
    "TaskResult",
    "TaskResultDoesNotExist",
    "TaskResultStatus",
    "USE_CORE_TASKS",
    "default_task_backend",
    "get_exception_traceback",
    "get_module_path",
    "get_random_id",
    "normalize_json",
    "task",
    "task_backends",
    "task_enqueued",
    "task_finished",
    "task_started",
]

try:
    from django.tasks.base import Task as DjangoTask
except ImportError:
    # `unused-ignore` keeps this valid on both Django 5.2 (where the import
    # fails and the ignore is needed) and 6.0 (where it succeeds and the
    # ignore would otherwise be flagged as unused under warn_unused_ignores).
    DjangoTask = None  # type: ignore[assignment, misc, unused-ignore]

from django_tasks.base import Task

__all__ = ["TASK_CLASSES"]

TASK_CLASSES = (Task, DjangoTask) if DjangoTask is not None else (Task,)

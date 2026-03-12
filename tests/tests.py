import uuid
from typing import cast
from unittest.mock import patch

from celery.contrib.testing.worker import start_worker
from celery.result import AsyncResult
from django import VERSION
from django.core.exceptions import SuspiciousOperation
from django.test import SimpleTestCase, override_settings
from django_tasks import TaskResultStatus, default_task_backend, task_backends
from django_tasks.base import Task
from django_tasks.exceptions import InvalidTaskError, TaskResultDoesNotExist

from django_tasks_celery import compat
from django_tasks_celery.app import app
from django_tasks_celery.backend import CeleryBackend, _map_priority
from tests import tasks as test_tasks


class CeleryBackendTestCase(SimpleTestCase):
    def test_using_correct_backend(self) -> None:
        self.assertEqual(default_task_backend, task_backends["default"])
        self.assertIsInstance(task_backends["default"], CeleryBackend)
        self.assertEqual(default_task_backend.alias, "default")
        self.assertEqual(default_task_backend.options, {})

    def test_enqueue_task(self) -> None:
        for task in [test_tasks.noop_task, test_tasks.noop_task_async]:
            with self.subTest(task):
                result = cast(Task, task).enqueue(1, two=3)

                self.assertEqual(result.status, TaskResultStatus.READY)
                self.assertFalse(result.is_finished)
                self.assertIsNone(result.started_at)
                self.assertIsNone(result.last_attempted_at)
                self.assertIsNone(result.finished_at)
                with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
                    result.return_value  # noqa:B018
                self.assertEqual(result.task, task)
                self.assertEqual(result.args, [1])
                self.assertEqual(result.kwargs, {"two": 3})
                self.assertEqual(result.attempts, 0)

    async def test_enqueue_task_async(self) -> None:
        for task in [test_tasks.noop_task, test_tasks.noop_task_async]:
            with self.subTest(task):
                result = await cast(Task, task).aenqueue()

                self.assertEqual(result.status, TaskResultStatus.READY)
                self.assertFalse(result.is_finished)
                self.assertIsNone(result.started_at)
                self.assertIsNone(result.last_attempted_at)
                self.assertIsNone(result.finished_at)
                with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
                    result.return_value  # noqa:B018
                self.assertEqual(result.task, task)
                self.assertEqual(result.args, [])
                self.assertEqual(result.kwargs, {})
                self.assertEqual(result.attempts, 0)

    def test_enqueue_logs(self) -> None:
        with self.assertLogs("django_tasks", level="DEBUG") as captured_logs:
            result = test_tasks.noop_task.enqueue()

        self.assertEqual(len(captured_logs.output), 1)
        self.assertIn("enqueued", captured_logs.output[0])
        self.assertIn(result.id, captured_logs.output[0])

    def test_using_additional_params(self) -> None:
        from datetime import timedelta

        from django.utils import timezone

        run_after = timezone.now() + timedelta(hours=10)
        with patch(
            "django_tasks_celery.backend.celery_app.send_task"
        ) as mock_send_task:
            result = test_tasks.noop_task.using(
                run_after=run_after, priority=75, queue_name="queue-1"
            ).enqueue()

        self.assertEqual(result.status, TaskResultStatus.READY)
        mock_send_task.assert_called_once_with(
            test_tasks.noop_task.module_path,
            args=(),
            kwargs={},
            task_id=result.id,
            eta=run_after,
            priority=7,
            queue="queue-1",
        )

    def test_priority_mapping(self) -> None:
        for priority, expected in [(-100, 0), (-50, 2), (0, 4), (75, 7), (100, 9)]:
            with self.subTest(priority=priority):
                self.assertEqual(_map_priority(priority), expected)

    def test_check(self) -> None:
        errors = list(default_task_backend.check())

        # May have a warning about result backend, but no errors
        actual_errors = [e for e in errors if e.level >= 40]  # ERROR level
        self.assertEqual(len(actual_errors), 0, actual_errors)

    @override_settings(INSTALLED_APPS=[])
    def test_celery_backend_app_missing(self) -> None:
        errors = list(default_task_backend.check())

        error_messages = [e for e in errors if e.level >= 40]
        self.assertEqual(len(error_messages), 1)
        self.assertIn("django_tasks_celery", error_messages[0].hint)  # type:ignore[arg-type]

    def test_queue_isolation(self) -> None:
        with patch(
            "django_tasks_celery.backend.celery_app.send_task"
        ) as mock_send_task:
            test_tasks.noop_task.enqueue()
            default_call_kwargs = mock_send_task.call_args

            test_tasks.noop_task.using(queue_name="queue-1").enqueue()
            queue1_call_kwargs = mock_send_task.call_args

        self.assertEqual(default_call_kwargs.kwargs["queue"], "default")
        self.assertEqual(queue1_call_kwargs.kwargs["queue"], "queue-1")

    def test_validate_on_enqueue(self) -> None:
        with override_settings(
            TASKS={
                "default": {
                    "BACKEND": "django_tasks_celery.CeleryBackend",
                    "QUEUES": ["unknown_queue"],
                }
            }
        ):
            task_with_custom_queue_name = test_tasks.noop_task.using(
                queue_name="unknown_queue"
            )

        with self.assertRaisesMessage(
            InvalidTaskError, "Queue 'unknown_queue' is not valid for backend"
        ):
            task_with_custom_queue_name.enqueue()

    async def test_validate_on_aenqueue(self) -> None:
        with override_settings(
            TASKS={
                "default": {
                    "BACKEND": "django_tasks_celery.CeleryBackend",
                    "QUEUES": ["unknown_queue"],
                }
            }
        ):
            task_with_custom_queue_name = test_tasks.noop_task.using(
                queue_name="unknown_queue"
            )

        with self.assertRaisesMessage(
            InvalidTaskError, "Queue 'unknown_queue' is not valid for backend"
        ):
            await task_with_custom_queue_name.aenqueue()

    def test_get_result(self) -> None:
        from django.utils import timezone

        result = default_task_backend.enqueue(test_tasks.noop_task, [], {})

        with patch("django_tasks_celery.backend.AsyncResult") as mock_async_result_cls:
            mock_async_result = mock_async_result_cls.return_value
            mock_async_result.name = test_tasks.noop_task.module_path
            mock_async_result.state = "SUCCESS"
            mock_async_result.result = None
            mock_async_result.date_done = timezone.now()
            mock_async_result.args = []
            mock_async_result.kwargs = {}

            new_result = default_task_backend.get_result(result.id)

        self.assertEqual(result.id, new_result.id)
        self.assertEqual(new_result.status, TaskResultStatus.SUCCESSFUL)

    async def test_get_result_async(self) -> None:
        from django.utils import timezone

        result = await default_task_backend.aenqueue(test_tasks.noop_task, [], {})

        with patch("django_tasks_celery.backend.AsyncResult") as mock_async_result_cls:
            mock_async_result = mock_async_result_cls.return_value
            mock_async_result.name = test_tasks.noop_task.module_path
            mock_async_result.state = "SUCCESS"
            mock_async_result.result = None
            mock_async_result.date_done = timezone.now()
            mock_async_result.args = []
            mock_async_result.kwargs = {}

            new_result = await default_task_backend.aget_result(result.id)

        self.assertEqual(result.id, new_result.id)
        self.assertEqual(new_result.status, TaskResultStatus.SUCCESSFUL)

    def test_get_missing_result(self) -> None:
        with self.assertRaises((TaskResultDoesNotExist, SuspiciousOperation)):
            default_task_backend.get_result(str(uuid.uuid4()))

    async def test_async_get_missing_result(self) -> None:
        with self.assertRaises((TaskResultDoesNotExist, SuspiciousOperation)):
            await default_task_backend.aget_result(str(uuid.uuid4()))

    def test_get_result_missing_extend_setting(self) -> None:
        with override_settings(CELERY_RESULT_EXTENDED=False):
            with self.assertRaises(ValueError):
                default_task_backend.get_result(str(uuid.uuid4()))

    def test_invalid_uuid(self) -> None:
        with self.assertRaises((TaskResultDoesNotExist, SuspiciousOperation)):
            default_task_backend.get_result("123")

    async def test_async_invalid_uuid(self) -> None:
        with self.assertRaises((TaskResultDoesNotExist, SuspiciousOperation)):
            await default_task_backend.aget_result("123")

    def test_send_task_called_with_module_path(self) -> None:
        """Verify that enqueue uses send_task with the task's module_path."""
        with patch(
            "django_tasks_celery.backend.celery_app.send_task"
        ) as mock_send_task:
            result = test_tasks.noop_task.enqueue()

        mock_send_task.assert_called_once()
        call_args = mock_send_task.call_args
        self.assertEqual(call_args.args[0], test_tasks.noop_task.module_path)
        self.assertEqual(call_args.kwargs["task_id"], result.id)

    @override_settings(
        TASKS={
            "default": {
                "BACKEND": "django_tasks_celery.CeleryBackend",
                "QUEUES": [],
            }
        }
    )
    def test_empty_queues_setting(self) -> None:
        """Empty QUEUES should use the default queue."""
        self.assertEqual(default_task_backend.queues, set())

    @override_settings(CELERY_RESULT_BACKEND="disabled")
    def test_result_backend_warning(self) -> None:
        """Check that a warning is issued when no result backend is configured."""
        from django_tasks_celery.backend import celery_app

        celery_app.config_from_object("django.conf:settings", namespace="CELERY")
        try:
            errors = list(default_task_backend.check())
        finally:
            # reload original settings
            celery_app.config_from_object("django.conf:settings", namespace="CELERY")

        warnings = [e for e in errors if e.level < 40]
        self.assertTrue(
            any("result backend" in str(w.msg).lower() for w in warnings),
            f"Expected a result backend warning, got: {warnings}",
        )

    def test_takes_context_injected(self) -> None:
        with start_worker(app, perform_ping_check=False):
            # test_context(attempt: int) internally asserts that:
            # assert isinstance(context, TaskContext)
            # assert context.attempt == attempt
            result = test_tasks.test_context.enqueue(1)

            # Wait for it to finish processing using underlying Celery AsyncResult
            celery_async_result = AsyncResult(result.id, app=app)
            celery_async_result.get(timeout=2)  # block until done

            result.refresh()
            self.assertEqual(result.status, TaskResultStatus.SUCCESSFUL)

    def test_takes_context_get_id(self) -> None:
        with start_worker(app, perform_ping_check=False):
            result = test_tasks.get_task_id.enqueue()

            celery_async_result = AsyncResult(result.id, app=app)
            celery_async_result.get(timeout=2)  # block until done

            result.refresh()
            self.assertEqual(result.status, TaskResultStatus.SUCCESSFUL)

            # The task returns `context.task_result.id`. We check if it matches the enqueued result ID.
            self.assertEqual(result.return_value, result.id)


class CompatTestCase(SimpleTestCase):
    def test_compat_has_django_task(self) -> None:
        self.assertIn(Task, compat.TASK_CLASSES)

        if VERSION >= (6, 0):
            from django.tasks.base import Task as DjangoTask

            self.assertIn(DjangoTask, compat.TASK_CLASSES)

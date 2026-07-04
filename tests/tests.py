import logging
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
from django_tasks_celery.backend import CeleryBackend, _map_priority, _unmap_priority
from tests import tasks as test_tasks

logger = logging.getLogger(__name__)


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
            headers={"django_tasks_priority": 75},
            queue="queue-1",
        )

    def test_priority_mapping(self) -> None:
        for priority, expected in [(-100, 0), (-50, 2), (0, 4), (75, 7), (100, 9)]:
            with self.subTest(priority=priority):
                self.assertEqual(_map_priority(priority), expected)

    def test_priority_unmapping(self) -> None:
        for priority, expected in [(0, -100), (2, -56), (4, -11), (7, 56), (9, 100)]:
            with self.subTest(priority=priority):
                self.assertEqual(_unmap_priority(priority), expected)

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
        self.assertIsNone(new_result.started_at)
        self.assertIsNotNone(new_result.last_attempted_at)
        self.assertEqual(new_result.last_attempted_at, new_result.finished_at)

    def test_get_result_retry_is_running(self) -> None:
        from django.utils import timezone

        result = default_task_backend.enqueue(test_tasks.noop_task, [], {})

        with patch("django_tasks_celery.backend.AsyncResult") as mock_async_result_cls:
            mock_async_result = mock_async_result_cls.return_value
            mock_async_result.name = test_tasks.noop_task.module_path
            mock_async_result.state = "RETRY"
            mock_async_result.result = Exception("retrying")
            mock_async_result.date_done = timezone.now()
            mock_async_result.args = []
            mock_async_result.kwargs = {}

            new_result = default_task_backend.get_result(result.id)

        self.assertEqual(new_result.status, TaskResultStatus.RUNNING)
        self.assertIsNone(new_result.started_at)
        self.assertIsNone(new_result.last_attempted_at)
        self.assertIsNone(new_result.finished_at)

    def test_get_result_received_is_ready(self) -> None:
        from django.utils import timezone

        result = default_task_backend.enqueue(test_tasks.noop_task, [], {})

        with patch("django_tasks_celery.backend.AsyncResult") as mock_async_result_cls:
            mock_async_result = mock_async_result_cls.return_value
            mock_async_result.name = test_tasks.noop_task.module_path
            mock_async_result.state = "RECEIVED"
            mock_async_result.result = None
            mock_async_result.date_done = timezone.now()
            mock_async_result.args = []
            mock_async_result.kwargs = {}

            new_result = default_task_backend.get_result(result.id)

        self.assertEqual(new_result.status, TaskResultStatus.READY)
        self.assertIsNone(new_result.started_at)
        self.assertIsNone(new_result.last_attempted_at)
        self.assertIsNone(new_result.finished_at)

    def test_get_result_revoked_is_failed(self) -> None:
        from django.utils import timezone

        result = default_task_backend.enqueue(test_tasks.noop_task, [], {})

        with patch("django_tasks_celery.backend.AsyncResult") as mock_async_result_cls:
            mock_async_result = mock_async_result_cls.return_value
            mock_async_result.name = test_tasks.noop_task.module_path
            mock_async_result.state = "REVOKED"
            mock_async_result.result = None
            mock_async_result.date_done = timezone.now()
            mock_async_result.args = []
            mock_async_result.kwargs = {}

            new_result = default_task_backend.get_result(result.id)

        self.assertEqual(new_result.status, TaskResultStatus.FAILED)
        self.assertIsNone(new_result.started_at)
        self.assertIsNotNone(new_result.last_attempted_at)
        self.assertEqual(new_result.last_attempted_at, new_result.finished_at)

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

    def test_using_does_not_reregister_celery_task(self) -> None:
        """`task.using(...)` should not re-register a Celery task each call."""
        registered = app.tasks[test_tasks.noop_task.module_path]

        test_tasks.noop_task.using(priority=50)
        test_tasks.noop_task.using(queue_name="queue-1")
        test_tasks.noop_task.using(priority=10, queue_name="queue-1")

        self.assertIs(app.tasks[test_tasks.noop_task.module_path], registered)

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

    @override_settings(CELERY_RESULT_EXTENDED=False)
    def test_result_extended_warning(self) -> None:
        from django_tasks_celery.backend import celery_app

        celery_app.config_from_object("django.conf:settings", namespace="CELERY")
        try:
            errors = list(default_task_backend.check())
        finally:
            celery_app.config_from_object("django.conf:settings", namespace="CELERY")

        warnings = [e for e in errors if e.level < 40]
        self.assertTrue(
            any("result_extended" in str(w.msg).lower() for w in warnings),
            f"Expected a result_extended warning, got: {warnings}",
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

    def test_async_task_runs_in_worker(self) -> None:
        with start_worker(app, perform_ping_check=False):
            result = test_tasks.async_task_returns_42.enqueue()

            celery_async_result = AsyncResult(result.id, app=app)
            celery_async_result.get(timeout=2)

            result.refresh()
            self.assertEqual(result.status, TaskResultStatus.SUCCESSFUL)
            self.assertEqual(result.return_value, 42)

    def test_takes_context_preserves_priority(self) -> None:
        with start_worker(app, perform_ping_check=False):
            result = test_tasks.get_task_priority.using(priority=75).enqueue()

            celery_async_result = AsyncResult(result.id, app=app)
            celery_async_result.get(timeout=2)

            result.refresh()
            self.assertEqual(result.status, TaskResultStatus.SUCCESSFUL)
            self.assertEqual(result.return_value, 75)

    def test_broker_priority_warning(self) -> None:
        errors = list(default_task_backend.check())
        warnings = [e for e in errors if e.level < 40]
        self.assertTrue(
            any("priority" in str(w.msg).lower() for w in warnings),
            f"Expected a priority warning for non-AMQP broker, got: {warnings}",
        )

    @override_settings(CELERY_BROKER_URL="amqp://localhost")
    def test_no_priority_warning_with_amqp_broker(self) -> None:
        from django_tasks_celery.backend import celery_app

        celery_app.config_from_object("django.conf:settings", namespace="CELERY")
        try:
            errors = list(default_task_backend.check())
        finally:
            celery_app.config_from_object("django.conf:settings", namespace="CELERY")

        warnings = [e for e in errors if e.level < 40]
        self.assertFalse(
            any("priority" in str(w.msg).lower() for w in warnings),
            f"Expected no priority warning for AMQP broker, got: {warnings}",
        )

    def test_get_result_pending_returns_ready(self) -> None:
        result = default_task_backend.enqueue(test_tasks.noop_task, [], {})

        with patch("django_tasks_celery.backend.AsyncResult") as mock_async_result_cls:
            mock_async_result = mock_async_result_cls.return_value
            mock_async_result.id = result.id
            # enqueue() pre-stores the task name in the result backend, so name
            # is populated even in PENDING state
            mock_async_result.name = test_tasks.noop_task.module_path
            mock_async_result.state = "PENDING"
            mock_async_result.result = None
            mock_async_result.date_done = None
            mock_async_result.args = []
            mock_async_result.kwargs = {}

            pending_result = default_task_backend.get_result(result.id)

        self.assertEqual(pending_result.id, result.id)
        self.assertEqual(pending_result.status, TaskResultStatus.READY)
        self.assertEqual(pending_result.task, test_tasks.noop_task)

    def test_task_started_signal_fired(self) -> None:
        from django_tasks.signals import task_started

        received: list = []

        def handler(sender, task_result, **kw):  # type: ignore[no-untyped-def]
            received.append(task_result)

        task_started.connect(handler)
        try:
            with start_worker(app, perform_ping_check=False):
                result = test_tasks.noop_task.enqueue()
                AsyncResult(result.id, app=app).get(timeout=2)
        finally:
            task_started.disconnect(handler)

        matching = [r for r in received if r.id == result.id]
        self.assertEqual(len(matching), 1)

    def test_task_finished_signal_fired_on_success(self) -> None:
        from django_tasks.signals import task_finished

        received: list = []

        def handler(sender, task_result, **kw):  # type: ignore[no-untyped-def]
            received.append(task_result)

        task_finished.connect(handler)
        try:
            with start_worker(app, perform_ping_check=False):
                result = test_tasks.noop_task.enqueue()
                AsyncResult(result.id, app=app).get(timeout=2)
        finally:
            task_finished.disconnect(handler)

        matching = [r for r in received if r.id == result.id]
        self.assertEqual(len(matching), 1)
        self.assertEqual(matching[0].status, TaskResultStatus.SUCCESSFUL)

    def test_task_finished_signal_fired_on_failure(self) -> None:
        from django_tasks.signals import task_finished

        received: list = []

        def handler(sender, task_result, **kw):  # type: ignore[no-untyped-def]
            received.append(task_result)

        task_finished.connect(handler)
        try:
            with start_worker(app, perform_ping_check=False):
                result = test_tasks.failing_task_value_error.enqueue()
                try:
                    AsyncResult(result.id, app=app).get(timeout=2)
                except Exception:
                    # Expected failure
                    logger.exception("Task failed")
                    pass
        finally:
            task_finished.disconnect(handler)

        matching = [r for r in received if r.id == result.id]
        self.assertEqual(len(matching), 1)
        self.assertEqual(matching[0].status, TaskResultStatus.FAILED)


class CompatTestCase(SimpleTestCase):
    def test_compat_has_django_task(self) -> None:
        self.assertIn(Task, compat.TASK_CLASSES)

        if VERSION >= (6, 0):
            from django.tasks.base import Task as DjangoTask

            self.assertIn(DjangoTask, compat.TASK_CLASSES)

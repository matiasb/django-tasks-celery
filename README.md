# Django Tasks Fennel

A [Django Tasks](https://docs.djangoproject.com/en/stable/topics/tasks/) backend which uses Celery as its underlying queue.

> **Django version support.** This backend works on Django 5.2 and 6.0+, and
> adapts to whichever Tasks framework your Django version provides:
>
> - **Django 6.0+** — uses the built-in
>   [`django.tasks`](https://docs.djangoproject.com/en/stable/topics/tasks/)
>   framework. No extra dependency is needed; define tasks with
>   `from django.tasks import task`.
> - **Django 5.2** — uses the standalone
>   [`django-tasks`](https://pypi.org/project/django-tasks/) package. Install it
>   via the `django-tasks` extra (see below) and define tasks with
>   `from django_tasks import task`.

## Installation

On Django 6.0 and later:

```
python -m pip install django-tasks-fennel
```

On Django 5.2, also pull in the standalone `django-tasks` package via the extra:

```
python -m pip install "django-tasks-fennel[django-tasks]"
```

First, add `django_tasks_fennel` to your `INSTALLED_APPS`:

```python
INSTALLED_APPS = [
    # ...
    "django_tasks_fennel",
]
```

Then, configure it as your `TASKS` backend:

```python
TASKS = {
    "default": {
        "BACKEND": "django_tasks_fennel.CeleryBackend",
        "QUEUES": ["default"]
    }
}
```

You also need to configure Celery in your Django project (broker, result backend, etc.):

```python
CELERY_BROKER_URL = "redis://localhost:6379/0"
CELERY_RESULT_BACKEND = "redis://localhost:6379/0"
```

You can review the [Celery documentation](https://docs.celeryq.dev/en/stable/userguide/configuration.html#configuration) for more information on how to configure Celery.

Note that all Celery configuration options must be specified in uppercase instead of lowercase, and start with `CELERY_`, so for example the `broker_url` setting becomes `CELERY_BROKER_URL`.

## Usage

The Celery-based backend acts as an interface between [Django's Tasks framework](https://docs.djangoproject.com/en/stable/topics/tasks/) and Celery, allowing tasks to be defined and enqueued using the Tasks API, but sent to a Celery broker and executed by Celery workers.

### Quickstart

Define a task with the `task` decorator. On Django 6.0+ import it from the
built-in `django.tasks`; on Django 5.2 import it from the standalone
`django_tasks` package instead (see the **Django version support** note above):

```python
# my_app/tasks.py
from django.tasks import task  # Django 5.2: from django_tasks import task

@task()
def send_welcome_email(user_id: int) -> None:
    ...
```

Enqueue it from a view or anywhere in your Django code:

```python
result = send_welcome_email.enqueue(user_id=42)
```

`result` is a `TaskResult`. When the worker has picked the task up and finished it, refresh and inspect:

```python
result.refresh()

if result.is_finished:
    print(result.status)         # TaskResultStatus.SUCCESSFUL or .FAILED
    print(result.started_at, result.finished_at)
    if result.errors:
        print(result.errors[0].exception_class, result.errors[0].traceback)
```

Pass per-call overrides through `using()`:

```python
from datetime import timedelta
from django.utils import timezone

send_welcome_email.using(
    queue_name="emails",
    priority=50,
    run_after=timezone.now() + timedelta(minutes=5),
).enqueue(user_id=42)
```

### Backend Capabilities

| Feature | Supported | How |
| --- | :---: | --- |
| `supports_defer` (`run_after`) | yes | Celery `eta` |
| `supports_async_task` (coroutines) | yes | wrapped via `async_to_sync` |
| `supports_priority` (`-100`..`100`) | yes | mapped to Celery's `0`..`9`; **requires AMQP broker** (RabbitMQ) for reliable ordering |
| `supports_get_result` / `refresh()` | yes | requires a Celery result backend |

This backend bridges Django's Tasks framework to Celery; it doesn't expose Celery-specific primitives. If you need **chains, groups, chords, or periodic tasks (beat)**, keep using plain `@shared_task` for those — both can coexist in the same project. Django Task names are namespaced under `django_tasks:` in Celery's registry (see [Task Names in Celery](#task-names-in-celery)), so there's no collision.

### Celery App

A Celery app is included at `django_tasks_fennel.app`. It reads configuration from your Django settings with the `CELERY_` prefix and auto-discovers tasks. You can use it directly, or [configure your own Celery app](https://docs.celeryq.dev/en/main/django/first-steps-with-django.html#using-celery-with-django) as you normally would.

### Running Workers

Start a Celery worker as usual:

```shell
DJANGO_SETTINGS_MODULE=<your_project.settings> celery -A django_tasks_fennel.app worker -l INFO
```

### Task Names in Celery

Django Tasks are registered in Celery's task registry under a namespaced name to avoid collisions with unrelated `@shared_task` registrations that may share the same dotted path. The wire format is:

```
django_tasks:<module_path>
```

For example, a `@task()`-decorated `my_app.tasks.send_email` is registered (and routed) as `django_tasks:my_app.tasks.send_email`. You'll see this name in `celery inspect registered`, worker logs, and any external monitoring (Flower, etc.). This is the name to use in Celery routing rules (`task_routes`) if you need per-task overrides.

### Result Backend

A [Celery result backend](https://docs.celeryq.dev/en/main/userguide/configuration.html#conf-result-backend) is **required** for `get_result()` and `refresh()` to work; without one, a warning is raised during Django's system checks. Setting [`CELERY_RESULT_EXTENDED=True`](https://docs.celeryq.dev/en/main/userguide/configuration.html#result-extended) is recommended so `worker_ids` and `attempts` are populated on completed results (`args` and `kwargs` don't need it — they come from the side-channel on key-value backends).

#### `TaskResult` field availability

Which `TaskResult` fields are populated depends on how Celery is configured. Below, KV-backend means a key-value-style Celery result backend (Redis, memcached, `cache+...://`, filesystem, MongoDB); DB/RPC means `db+...://` or `rpc://`.

| Field | Required configuration | Notes |
| --- | --- | --- |
| `status` (`RUNNING` during execution) | result backend | The wrapper explicitly writes `STARTED` from the worker, so works without `CELERY_TASK_TRACK_STARTED`. |
| `finished_at`, `last_attempted_at` (completed) | result backend | From Celery's `date_done`. |
| `errors[*].traceback` | result backend | The worker's serialized traceback string (`AsyncResult.traceback`). |
| `errors[*].exception_class` | result backend | The original exception class round-trips through Celery's serializer. |
| `worker_ids`, `attempts` | result backend + `CELERY_RESULT_EXTENDED=True` | Populated from `AsyncResult.worker` and `retries`. |
| `started_at` | KV-backend | Persisted via a side-channel key so it survives Celery overwriting the meta with the return value on completion. `None` on DB/RPC backends. |
| `enqueued_at`, pre-worker `task` / `args` / `kwargs` | KV-backend | Written by the same side-channel on `enqueue()`. Also lets `get_result()` reconstruct the Task even with `CELERY_RESULT_EXTENDED=False`. On DB/RPC backends `enqueued_at` is `None` and `get_result()` only works after the worker has stored the result (with `CELERY_RESULT_EXTENDED=True`). |

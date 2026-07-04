# Django Tasks Celery

A [Django Tasks](https://docs.djangoproject.com/en/stable/topics/tasks/) backend which uses Celery as its underlying queue.

## Installation

```
python -m pip install django-tasks-celery
```

First, add `django_tasks_celery` to your `INSTALLED_APPS`:

```python
INSTALLED_APPS = [
    # ...
    "django_tasks_celery",
]
```

Then, configure it as your `TASKS` backend:

```python
TASKS = {
    "default": {
        "BACKEND": "django_tasks_celery.CeleryBackend",
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

The Celery-based backend acts as an interface between [Django's tasks interface](https://docs.djangoproject.com/en/stable/topics/tasks/) and Celery, allowing tasks to be defined and enqueued using `django_tasks`, but sent to a Celery broker and executed by Celery workers.

### Celery App

A Celery app is included at `django_tasks_celery.app`. It reads configuration from your Django settings with the `CELERY_` prefix and auto-discovers tasks. You can use it directly, or [configure your own Celery app](https://docs.celeryq.dev/en/main/django/first-steps-with-django.html#using-celery-with-django) as you normally would.

### Running Workers

Start a Celery worker as usual:

```shell
DJANGO_SETTINGS_MODULE=<your_project.settings> celery -A django_tasks_celery.app worker -l INFO
```

### Priorities

Task priorities are mapped from the Django Tasks range (`-100` to `100`) to Celery's range (`0` to `9`) using a linear scale. This requires a broker that supports priority queues (e.g., RabbitMQ).

### Result Backend

A [Celery result backend](https://docs.celeryq.dev/en/main/userguide/configuration.html#conf-result-backend) is **required** for `get_result()` and `refresh()` to work. If no result backend is configured, a warning will be raised during Django's system checks. Also, you will need to set [`CELERY_RESULT_EXTENDED=True`](https://docs.celeryq.dev/en/main/userguide/configuration.html#result-extended) so the backend can populate `args`, `kwargs`, `worker_ids`, and `attempts` on `TaskResult`.

#### `TaskResult` field availability

The Django Tasks `TaskResult` exposes several fields that depend on what Celery's result backend can store:

- **`status`** while a task is running: the backend explicitly marks the task as `STARTED` (mapped to `RUNNING`) from inside the worker, so this works regardless of the [`CELERY_TASK_TRACK_STARTED`](https://docs.celeryq.dev/en/main/userguide/configuration.html#task-track-started) setting.
- **`finished_at`** and **`last_attempted_at`** (for completed tasks): come from Celery's `date_done`. Always available with a configured result backend.
- **`errors[*].traceback`**: uses the worker's serialized traceback string (`AsyncResult.traceback`).
- **`worker_ids`** and **`attempts`**: require `CELERY_RESULT_EXTENDED=True` so the result meta carries `worker` and `retries`.
- **`started_at`**: persisted via a side-channel key in the result backend so it survives Celery overwriting the meta with the return value on completion. This requires a **key-value-style result backend** — Redis, memcached, cache (`cache+...://`), filesystem, and MongoDB are supported. With the database (`db+...://`) or RPC (`rpc://`) backends, `started_at` will remain `None`.
- **`enqueued_at`** and pre-worker **`task` / `args` / `kwargs`**: persisted via the same side-channel on `enqueue()`. This is what lets `get_result()` reconstruct the Task before the worker has stored anything — and what lets task reconstruction work even when `CELERY_RESULT_EXTENDED=False`. Same KV-backend requirement as `started_at`; on database/RPC backends, `enqueued_at` will be `None` and `get_result()` will only work after the worker has stored the result (with `CELERY_RESULT_EXTENDED=True`).

### Deferred Tasks

The backend supports `run_after` for scheduling tasks to execute at a future time, using Celery's `eta` parameter.

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

A Celery app is included at `django_tasks_celery.app`. It reads configuration from your Django settings with the `CELERY_` prefix and auto-discovers tasks. You can use it directly, or configure your own Celery app as you normally would.

### Running Workers

Start a Celery worker as usual:

```shell
celery -A django_tasks_celery.app worker --loglevel=info
```

### Priorities

Task priorities are mapped from the Django Tasks range (`-100` to `100`) to Celery's range (`0` to `9`) using a linear scale. This requires a broker that supports priority queues (e.g., RabbitMQ).

### Result Backend

A Celery result backend is **required** for `get_result()` and `refresh()` to work. If no result backend is configured, a warning will be raised during Django's system checks.

### Deferred Tasks

The backend supports `run_after` for scheduling tasks to execute at a future time, using Celery's `eta` parameter.

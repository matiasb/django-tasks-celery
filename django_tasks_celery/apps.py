from django.apps import AppConfig


class DjangoTasksCeleryConfig(AppConfig):
    name = "django_tasks_celery"

    def ready(self) -> None:
        # If no Celery app has been configured by the time Django apps are
        # ready (e.g., the project doesn't have its own `myproject/celery.py`
        # imported via `myproject/__init__.py`), fall back to the bundled
        # `django_tasks_celery.app`. We do this in ready() rather than at
        # backend module import time so importing the backend doesn't
        # mutate global Celery state, and so user-defined Celery apps
        # imported during Django startup take precedence automatically.
        from celery.app import default_app

        if default_app is None:
            # Importing the module creates the bundled Celery app, which
            # registers itself as current+default via its constructor.
            from django_tasks_celery import app  # noqa: F401

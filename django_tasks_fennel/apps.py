from django.apps import AppConfig


class DjangoTasksFennelConfig(AppConfig):
    name = "django_tasks_fennel"

    def ready(self) -> None:
        # If the project hasn't configured its own Celery app by the time
        # Django is ready (e.g. no `myproject/celery.py` imported via
        # `myproject/__init__.py`), install the bundled `django_tasks_fennel.app`
        # so enqueue() works out of the box.
        #
        # We key off `celery._state._tls.current_app` — the app that was
        # explicitly made "current" — rather than celery's `default_app`. The
        # latter is unreliable here: celery lazily creates a trivial, broker-less
        # fallback app the first time `current_app` is resolved (which can happen
        # during startup before this runs), leaving `default_app` non-None even
        # when no real app was configured. `_tls.current_app` stays None until a
        # Celery app is actually created, so it distinguishes "the project has
        # its own app" from "nothing configured".
        #
        # Running in ready() (not at import time) means a user-provided app,
        # imported during Django startup, is already current and takes
        # precedence — we leave it untouched.
        from celery import _state

        if _state._tls.current_app is None:
            from django_tasks_fennel.app import app

            # Importing app.py makes it the current app (set_as_current); also
            # set it as the default so `current_app` resolves to it in request
            # and worker threads, where the thread-local current app is unset.
            app.set_default()

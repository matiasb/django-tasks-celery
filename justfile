# Recipes
@default:
  just --list

test *ARGS:
    python -m manage check
    python -m coverage run --source=django_tasks_fennel -m manage test --shuffle --noinput {{ ARGS }}
    python -m coverage report
    python -m coverage html

format:
    python -m ruff check django_tasks_fennel tests --fix
    python -m ruff format django_tasks_fennel tests

lint:
    python -m ruff check django_tasks_fennel tests
    python -m ruff format django_tasks_fennel tests --check
    python -m mypy django_tasks_fennel tests

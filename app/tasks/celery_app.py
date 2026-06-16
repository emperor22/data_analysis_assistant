from celery import Celery, signals


from app.crud.models import (
    base_engine_sync,
)

from app.core.config import Config

import sentry_sdk
from sentry_sdk.integrations.celery import CeleryIntegration


app = Celery(
    "tasks",
    backend=Config.REDIS_URL,
    broker=Config.REDIS_URL,
    include=[
        "app.tasks.prompt_tasks",
        "app.tasks.processing_tasks",
        "app.tasks.maintenance_tasks",
        "app.tasks.email_tasks",
    ],
)
app.conf.task_routes = {
    "get_prompt_result_task": {"queue": "io_tasks_queue"},
    "get_additional_analyses_prompt_result_task": {"queue": "io_tasks_queue"},
    "data_processing_task": {"queue": "cpu_tasks_queue"},
    "send_email_task": {"queue": "io_tasks_queue"},
}

# app.conf.beat_schedule = {
#     'update_last_accessed_at_db': {'task': 'tasks.update_last_accessed_at_task','schedule': crontab(hour=1)},
#     'cleanup_unused_datasets': {'task': 'tasks.cleanup_unused_datasets_task', 'schedule': crontab(day_of_month=1)}
# }


@signals.celeryd_init.connect
def init_sentry(**_kwargs):
    sentry_sdk.init(
        dsn=Config.SENTRY_DSN,
        send_default_pii=True,
        integrations=[CeleryIntegration(monitor_beat_tasks=False)],
        enable_logs=True,
    )


class DatabaseTask(app.Task):
    def get_engine(self):
        return base_engine_sync

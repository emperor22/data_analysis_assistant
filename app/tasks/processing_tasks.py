import time


from app.services.analysis import DataAnalysisProcessor


from app.crud import (
    TaskRunTableOperation,
    PromptTableOperation,
)

from app.schemas import (
    DataTasks,
    TaskStatus,
    RunInfo,
    TaskProcessingRunType,
)

from app.tasks.celery_app import DatabaseTask

from app.logger import logger
from app.config import Config

from celery import shared_task

import psycopg


retry_for_exceptions_data_processing_task = [
    psycopg.OperationalError,
    psycopg.DatabaseError,
]

STARTING_STATUS_RUNTYPE_MAPPING = {
    TaskProcessingRunType.first_run_after_request.value: TaskStatus.doing_initial_tasks_run.value,
    TaskProcessingRunType.modified_tasks_execution.value: TaskStatus.doing_customized_tasks_run.value,
    TaskProcessingRunType.additional_analyses_request.value: TaskStatus.doing_additional_tasks_run.value,
    TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value: TaskStatus.doing_customized_tasks_run_with_new_dataset.value,
}

FINISHED_STATUS_RUNTYPE_MAPPING = {
    TaskProcessingRunType.first_run_after_request.value: TaskStatus.initial_tasks_run_finished.value,
    TaskProcessingRunType.modified_tasks_execution.value: TaskStatus.customized_tasks_run_finished.value,
    TaskProcessingRunType.additional_analyses_request.value: TaskStatus.additional_tasks_run_finished.value,
    TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value: TaskStatus.customized_tasks_run_with_new_dataset_finished.value,
}


@shared_task(
    bind=True,
    base=DatabaseTask,
    name="data_processing_task",
    acks_late=True,
    ignore_result=True,
    time_limit=20,
    max_retries=3,
    autoretry_for=retry_for_exceptions_data_processing_task,
)
def data_processing_task(self, data_tasks_dict, run_info, run_type):
    run_info = RunInfo(**run_info)

    request_id = run_info.request_id
    user_id = run_info.user_id

    logger.info(
        f"task execution request processed: run type {run_type}, request_id {request_id}, user_id {user_id}"
    )

    start_time = time.perf_counter()

    engine = self.get_engine()

    with engine.connect() as conn:
        prompt_table_ops = PromptTableOperation(conn_sync=conn)
        task_run_table_ops = TaskRunTableOperation(conn_sync=conn)

        prompt_table_ops.change_request_status_sync(
            request_id=request_id, status=STARTING_STATUS_RUNTYPE_MAPPING[run_type]
        )

        data_tasks = DataTasks.model_validate(
            data_tasks_dict,
            context={
                "run_type": run_type,
                "request_id": request_id,
                "is_from_data_tasks": True,
            },
        )

        if (
            run_type == TaskProcessingRunType.first_run_after_request.value
            and not task_run_table_ops.request_id_exists(request_id)
        ):
            task_run_table_ops.add_task_result_sync(
                request_id=request_id, user_id=user_id
            )

        processor = DataAnalysisProcessor(
            data_tasks=data_tasks,
            run_info=run_info,
            task_run_table_ops=task_run_table_ops,
            run_type=run_type,
        )
        processor.process_all_tasks()

        prompt_table_ops.change_request_status_sync(
            request_id=request_id, status=FINISHED_STATUS_RUNTYPE_MAPPING[run_type]
        )

        process_time_ms = round((time.perf_counter() - start_time) * 1000, 2)

        logger.info(
            f"task execution request finished in {process_time_ms} ms: run type {run_type}, request_id {request_id}, user_id {user_id}"
        )

        if process_time_ms > Config.THRES_SLOW_TASK_EXECUTION_PROCESS_TIME_MS:
            logger.warning(
                f"slow task execution request processing time ({process_time_ms} ms): run_type {run_type}, request_id {request_id}, user_id {user_id}"
            )

        return

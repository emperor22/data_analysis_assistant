import json
import time


from app.services.llm import (
    cleanup_agg_col_names,
)
from app.core.exceptions import (
    RetryableRateLimitException,
)

from app.core.auth import decrypt_api_key

from app.crud.queries import (
    PromptTableOperation,
    BlacklistedDatasetsTableOperation,
)

from app.schemas.enums import (
    TaskStatus,
)

from app.tasks.celery_app import DatabaseTask

from app.tasks.prompt_tasks_helpers import (
    get_elapsed_time_ms,
    check_if_dataset_is_not_blacklisted,
    get_llm_response,
    write_debug_prompt_and_response_if_requested,
    validate_initial_prompt_resp_part_one,
    validate_initial_prompt_resp_part_two,
    build_part_two_prompt,
    build_initial_data_tasks,
    build_additional_analysis_prompt,
    validate_additional_analyses_prompt_res,
    build_additional_analyses_data_task,
    log_slow_prompt_tasks,
)


from app.core.logger import logger
from app.core.config import Config

from celery import shared_task

from pydantic import ValidationError
from requests.exceptions import RequestException

from psycopg import OperationalError


retry_for_exceptions_get_prompt_task = [
    ValidationError,
    RequestException,
    OperationalError,
    RetryableRateLimitException,
]


@shared_task(
    bind=True,
    base=DatabaseTask,
    name="get_prompt_result_task",
    acks_late=True,
    time_limit=200,
    max_retries=Config.MAX_RETRIES_GET_PROMPT_RESULT_TASK,
    rate_limit="15/m",
    retry_backoff=3,
    retry_backoff_max=60,
    autoretry_for=retry_for_exceptions_get_prompt_task,
)
def get_prompt_result_task(
    self,
    model,
    provider,
    api_key,
    prompt_pt_1,
    task_count,
    dataset_id,
    request_id,
    user_id,
    dataset_cols,
    debug_prompt_and_res=False,
    mock_pt1_resp_file=None,
    mock_pt2_resp_file=None,
):

    logger.info(
        f"initial task request processed: request_id {request_id}, user_id {user_id}"
    )

    start_time = time.perf_counter()

    api_key = decrypt_api_key(api_key)

    engine = self.get_engine()

    with engine.connect() as conn:
        prompt_table_ops = PromptTableOperation(conn_sync=conn)
        blacklist_table_ops = BlacklistedDatasetsTableOperation(conn_sync=conn)

        prompt_table_ops.change_request_status_sync(
            request_id=request_id,
            status=TaskStatus.waiting_for_initial_request_prompt.value,
        )

        check_if_dataset_is_not_blacklisted(
            blacklist_table_ops=blacklist_table_ops, dataset_id=dataset_id
        )

        # getting result for prompt part 1
        resp_pt_1 = get_llm_response(
            prompt=prompt_pt_1,
            model=model,
            provider=provider,
            api_key=api_key,
            user_id=user_id,
            request_id=request_id,
            prompt_table_ops=prompt_table_ops,
            mock_file=mock_pt1_resp_file,
            mock_part=1,
        )

        write_debug_prompt_and_response_if_requested(
            enabled=debug_prompt_and_res,
            prompt=prompt_pt_1,
            response=resp_pt_1,
            part=1,
            request_id=request_id,
        )

        resp_pt_1 = validate_initial_prompt_resp_part_one(
            response=resp_pt_1,
            dataset_cols=dataset_cols,
            request_id=request_id,
            user_id=user_id,
            dataset_id=dataset_id,
            blacklist_table_ops=blacklist_table_ops,
            prompt_table_ops=prompt_table_ops,
        )

        prompt_pt_2 = build_part_two_prompt(
            part_one_result=resp_pt_1, task_count=task_count
        )

        resp_pt_2 = get_llm_response(
            prompt=prompt_pt_2,
            model=model,
            provider=provider,
            api_key=api_key,
            user_id=user_id,
            request_id=request_id,
            prompt_table_ops=prompt_table_ops,
            mock_file=mock_pt2_resp_file,
            mock_part=2,
        )

        write_debug_prompt_and_response_if_requested(
            enabled=debug_prompt_and_res,
            prompt=prompt_pt_2,
            response=resp_pt_2,
            part=2,
            request_id=request_id,
        )

        resp_pt_2 = validate_initial_prompt_resp_part_two(
            response=resp_pt_2,
            request_id=request_id,
            user_id=user_id,
            dataset_id=dataset_id,
            blacklist_table_ops=blacklist_table_ops,
            prompt_table_ops=prompt_table_ops,
        )

        resp_pt_2 = cleanup_agg_col_names(resp_pt_2=resp_pt_2, resp_pt_1=resp_pt_1)
        result = {**resp_pt_1, **resp_pt_2}

        prompt_table_ops.insert_prompt_result_sync(
            request_id=request_id, prompt_result=json.dumps(result)
        )
        prompt_table_ops.change_request_status_sync(
            request_id=request_id,
            status=TaskStatus.initial_request_prompt_received.value,
        )

        data_tasks = build_initial_data_tasks(
            prompt_result=result,
            dataset_cols=dataset_cols,
            request_id=request_id,
        )

        blacklist_table_ops.reset_failed_attempt_count(dataset_id)

        process_time_ms = get_elapsed_time_ms(start_time)

        logger.info(
            f"initial task request finished in {process_time_ms} ms: request_id {request_id}, user_id {user_id}"
        )

        log_slow_prompt_tasks(
            process_time_ms=process_time_ms,
            request_id=request_id,
            user_id=user_id,
            task="initial analysis",
        )

        return data_tasks.model_dump()


retry_for_exceptions_addt_analyses_request = [
    RequestException,
    OperationalError,
    RetryableRateLimitException,
]


@shared_task(
    bind=True,
    base=DatabaseTask,
    name="get_additional_analyses_prompt_result_task",
    acks_late=True,
    time_limit=200,
    max_retries=Config.MAX_RETRIES_ADDT_ANALYSES_REQ_TASK,
    rate_limit="15/m",
    retry_backoff=3,
    retry_backoff_max=60,
    autoretry_for=retry_for_exceptions_addt_analyses_request,
)
def get_additional_analyses_prompt_result_task(
    self,
    model,
    provider,
    api_key,
    new_tasks_prompt,
    request_id,
    user_id,
    debug_prompt_and_res=False,
    mock_addt_request_resp_file=None,
):

    logger.info(
        f"additional analyses task request processed: request_id {request_id}, user_id {user_id}"
    )

    start_time = time.perf_counter()

    api_key = decrypt_api_key(api_key)

    engine = self.get_engine()

    with engine.connect() as conn:
        prompt_table_ops = PromptTableOperation(conn_sync=conn)

        prompt_table_ops.change_request_status_sync(
            request_id=request_id,
            status=TaskStatus.waiting_for_additional_analysis_prompt_result.value,
        )

        prompt = build_additional_analysis_prompt(
            new_tasks_prompt=new_tasks_prompt,
            request_id=request_id,
            user_id=user_id,
            prompt_table_ops=prompt_table_ops,
            mock_file=mock_addt_request_resp_file,
        )

        resp = get_llm_response(
            prompt=prompt,
            model=model,
            provider=provider,
            api_key=api_key,
            user_id=user_id,
            request_id=request_id,
            prompt_table_ops=prompt_table_ops,
            mock_file=mock_addt_request_resp_file,
            mock_part="addt_analyses",
        )

        write_debug_prompt_and_response_if_requested(
            enabled=debug_prompt_and_res,
            prompt=prompt,
            response=resp,
            part="addt_analyses",
            request_id=request_id,
        )

        resp = validate_additional_analyses_prompt_res(
            resp=resp,
            user_id=user_id,
            request_id=request_id,
            prompt_table_ops=prompt_table_ops,
        )

        prompt_table_ops.insert_additional_analyses_prompt_result_sync(
            request_id=request_id, additional_analyses_prompt_result=json.dumps(resp)
        )

        prompt_table_ops.change_request_status_sync(
            request_id=request_id,
            status=TaskStatus.additional_analysis_prompt_result_received.value,
        )

        data_tasks = build_additional_analyses_data_task(
            resp=resp, request_id=request_id
        )

        process_time_ms = get_elapsed_time_ms(start_time)

        logger.info(
            f"additional analyses task request finished in {process_time_ms} ms: request_id {request_id}, user_id {user_id}"
        )

        log_slow_prompt_tasks(
            process_time_ms=process_time_ms,
            request_id=request_id,
            user_id=user_id,
            task="additional analyses",
        )

        return data_tasks.model_dump()

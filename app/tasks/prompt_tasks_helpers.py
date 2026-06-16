
import time


from app.services.llm import (
    insert_prompt_context,
    resp_loader,
    mock_resp_loader,
    write_prompt_and_res,
)
from app.exceptions import (
    BlacklistedDatasetException,
    RateLimitedException,
    ModelNotFoundException,
)

from app.crud import (
    PromptTableOperation,
    BlacklistedDatasetsTableOperation,
)

from app.schemas import (
    DataTasks,
    DatasetAnalysisModelPartOne,
    DatasetAnalysisModelPartTwo,
    TaskProcessingRunType,
    TaskStatus
)


from app.tasks.exception_handlers import (
    handle_model_not_found_exception, 
    handle_rate_limit_exception_prompt_task, 
    handle_validation_error_prompt_task
    )

from app.logger import logger
from app.config import Config


from pydantic import ValidationError


from datetime import datetime

import json


def get_elapsed_time_ms(start_time):
    return round((time.perf_counter() - start_time) * 1000, 2)

def check_if_dataset_is_not_blacklisted(blacklist_table_ops: BlacklistedDatasetsTableOperation, 
                                        dataset_id):
    dataset_blacklisted = blacklist_table_ops.check_if_blacklisted(dataset_id)
    if dataset_blacklisted is not None and dataset_blacklisted:
        raise BlacklistedDatasetException
    
def get_llm_response(prompt, model, provider, api_key, user_id, request_id, prompt_table_ops: PromptTableOperation, 
                        mock_file=None, mock_part=None):
    if mock_file:
        return mock_resp_loader(mock_file, pt=mock_part)

    try:
        return resp_loader(prompt, model, provider, api_key)

    except RateLimitedException:
        handle_rate_limit_exception_prompt_task(
            user_id=user_id,
            request_id=request_id,
            prompt_table_ops=prompt_table_ops,
        )

    except ModelNotFoundException:
        handle_model_not_found_exception(
            request_id=request_id,
            prompt_table_ops=prompt_table_ops,
        )
        
def write_debug_prompt_and_response_if_requested(enabled, prompt, response, part, request_id,):
    if not enabled:
        return

    write_prompt_and_res(
        prompt=prompt,
        res=response,
        part=part,
        request_id=request_id,
        dir_=Config.DEBUG_PROMPT_AND_RES_SAVE_DIR,
    )
    
def validate_initial_prompt_resp_part_one( response, dataset_cols, request_id, user_id, dataset_id, 
                                        blacklist_table_ops: BlacklistedDatasetsTableOperation, 
                                        prompt_table_ops: PromptTableOperation):
    try:
        validated = DatasetAnalysisModelPartOne.model_validate(
            response,
            context={
                "run_type": TaskProcessingRunType.first_run_after_request.value,
                "required_cols": dataset_cols,
                "request_id": request_id,
            },
        )
        return validated.model_dump()

    except ValidationError as error:
        handle_validation_error_prompt_task(
            error=error,
            user_id=user_id,
            request_id=request_id,
            resp=response,
            dataset_id=dataset_id,
            blacklist_table_ops=blacklist_table_ops,
            prompt_table_ops=prompt_table_ops,
        )
        
def build_part_two_prompt(part_one_result, task_count):
    return insert_prompt_context(
        prompt_file=Config.PT2_PROMPT_TEMPLATE,
        context={
            "context_json": part_one_result,
            "task_count": task_count,
            "current_time": datetime.now().strftime("%H:%M:%S"),
        },
    )
    
def validate_initial_prompt_resp_part_two(response, request_id, user_id, dataset_id,
                                    blacklist_table_ops: BlacklistedDatasetsTableOperation,
                                    prompt_table_ops: PromptTableOperation):
    try:
        validated = DatasetAnalysisModelPartTwo.model_validate(
            response,
            context={
                "run_type": TaskProcessingRunType.first_run_after_request.value,
                "request_id": request_id,
            },
        )
        return validated.model_dump()

    except ValidationError as error:
        handle_validation_error_prompt_task(
            error=error,
            user_id=user_id,
            request_id=request_id,
            resp=response,
            dataset_id=dataset_id,
            blacklist_table_ops=blacklist_table_ops,
            prompt_table_ops=prompt_table_ops,
        )
        

def build_initial_data_tasks(prompt_result, dataset_cols, request_id):
    data_tasks_fields = DataTasks.model_fields.keys()
    data_tasks_dct = {
        key: value
        for key, value in prompt_result.items()
        if key in data_tasks_fields
    }

    return DataTasks.model_validate(
        data_tasks_dct,
        context={
            "run_type": TaskProcessingRunType.first_run_after_request.value,
            "is_from_data_tasks": True,
            "required_cols": dataset_cols,
            "request_id": request_id,
        },
    )
    
ADDITIONAL_ANALYSIS_CONTEXT_FIELDS = ["columns", "common_column_cleaning_or_transformation", "common_column_combination"]

def build_additional_analysis_prompt(new_tasks_prompt, request_id, user_id, mock_file,
                                        prompt_table_ops: PromptTableOperation):
    # no need to build prompt when mock response is provided
    if mock_file:
        return None
    
    initial_prompt_result = prompt_table_ops.get_prompt_result_sync(
        request_id=request_id,
        user_id=user_id,
    )

    initial_prompt_result = json.loads(initial_prompt_result["prompt_result"])

    context_json = {
        field: initial_prompt_result[field] for field in ADDITIONAL_ANALYSIS_CONTEXT_FIELDS
    }

    return insert_prompt_context(
        prompt_file=Config.ADDT_REQ_PROMPT_TEMPLATE,
        context={
            "context_json": json.dumps(context_json),
            "new_tasks_prompt": new_tasks_prompt,
            "current_time": datetime.now().strftime("%H:%M:%S"),
        },
    )
    
def validate_additional_analyses_prompt_res(resp, user_id, request_id, prompt_table_ops: PromptTableOperation):
    try:
        resp = DatasetAnalysisModelPartTwo.model_validate(
            resp,
            context={
                "run_type": TaskProcessingRunType.additional_analyses_request.value,
                "request_id": request_id,
            },
        )
    except ValidationError:
        logger.exception(
            f"failed additional analyses response validation: request_id {request_id}, user_id {user_id}, resp -> {resp}"
        )
        prompt_table_ops.change_request_status_sync(
            request_id=request_id,
            status=TaskStatus.additional_analysis_invalid_resp.value,
        )
        raise

    return resp.model_dump()

def build_additional_analyses_data_task(resp, request_id):
    data_tasks_dct = {"common_tasks": resp["common_tasks"]}
    return DataTasks.model_validate(
        data_tasks_dct,
        context={
            "run_type": TaskProcessingRunType.additional_analyses_request.value,
            "request_id": request_id,
            "is_from_data_tasks": True,
        },
    )

def log_slow_prompt_tasks(process_time_ms, request_id, user_id, task):
    if process_time_ms > Config.THRES_SLOW_INITIAL_REQUEST_PROCESS_TIME_MS:
        logger.warning(
            f"slow {task} request processing time ({process_time_ms} ms): request_id {request_id}, user_id {user_id}"
        )


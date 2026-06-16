from fastapi import APIRouter, Depends, Form, Request, UploadFile

from app.auth import get_current_user
from app.config import Config
from app.services.infra import limiter

from app.crud import (
    PromptTableOperation,
    UserTableOperation,
    TaskRunTableOperation,
    UserCustomizedTasksTableOperation,
    get_prompt_table_ops,
    get_task_run_table_ops,
    get_user_table_ops,
    get_user_customized_tasks_table_ops,
)

from app.schemas import (
    UploadDatasetSchema,
    ExecuteAnalysesSchema,
    AdditionalAnalysesRequestSchema,
    TaskProcessingRunType
)

from app.services.infra import check_if_task_is_valid
from app.services.analyses_service import (
    parse_form_json,
    build_data_tasks_context,
    create_initial_analysis_run,
    execute_existing_analysis_run,
    execute_analysis_run_with_new_dataset,
    create_additional_analysis_run,
)

router = APIRouter()

@router.post("/initial_analysis")
@limiter.limit(Config.RATE_LIMIT_TASK_ENDPOINTS)
async def upload_dataset(
    request: Request,
    file: UploadFile,
    upload_dataset_data: str = Form(...),
    current_user=Depends(get_current_user),
    prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops),
    user_cust_tasks_table_ops: UserCustomizedTasksTableOperation = Depends(
        get_user_customized_tasks_table_ops
    ),
    user_table_ops: UserTableOperation = Depends(get_user_table_ops),
):
    data = parse_form_json(upload_dataset_data, UploadDatasetSchema)

    await create_initial_analysis_run(
        file=file,
        upload_dataset_data=data,
        current_user=current_user,
        prompt_table_ops=prompt_table_ops,
        user_cust_tasks_table_ops=user_cust_tasks_table_ops,
        user_table_ops=user_table_ops,
    )

    return {"detail": "request task executed"}


@router.post("/execute_analyses/{request_id}")
@limiter.limit(Config.RATE_LIMIT_TASK_ENDPOINTS)
@check_if_task_is_valid
async def execute_analyses(
    request: Request,
    request_id: str,
    execute_analyses_data: str = Form(...),
    current_user=Depends(get_current_user),
    prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops),
):
    run_type = TaskProcessingRunType.modified_tasks_execution.value

    data_tasks_context = build_data_tasks_context(
        request_id=request_id,
        run_type=run_type,
    )

    data = parse_form_json(
        execute_analyses_data,
        ExecuteAnalysesSchema,
        context=data_tasks_context,
    )

    await execute_existing_analysis_run(
        request_id=request_id,
        execute_analyses_data=data,
        current_user=current_user,
        prompt_table_ops=prompt_table_ops,
    )

    return {"detail": "analysis task executed"}


@router.post("/execute_analyses_with_new_dataset/{request_id}")
@limiter.limit(Config.RATE_LIMIT_TASK_ENDPOINTS)
@check_if_task_is_valid
async def execute_analyses_with_new_dataset(
    request: Request,
    request_id: str,
    file: UploadFile,
    execute_analyses_data: str = Form(...),
    current_user=Depends(get_current_user),
    task_run_table_ops: TaskRunTableOperation = Depends(get_task_run_table_ops),
    prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops),
):
    run_type = TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value

    data_tasks_context = build_data_tasks_context(
        request_id=request_id,
        run_type=run_type,
    )

    data = parse_form_json(
        execute_analyses_data,
        ExecuteAnalysesSchema,
        context=data_tasks_context,
    )

    await execute_analysis_run_with_new_dataset(
        request_id=request_id,
        file=file,
        execute_analyses_data=data,
        current_user=current_user,
        prompt_table_ops=prompt_table_ops,
        task_run_table_ops=task_run_table_ops,
    )

    return {"detail": "analysis task executed"}


@router.post("/make_additional_analyses_request/{request_id}")
@limiter.limit(Config.RATE_LIMIT_TASK_ENDPOINTS)
@check_if_task_is_valid
async def make_additional_analyses_request(
    request: Request,
    request_id: str,
    additional_analyses_request_data: AdditionalAnalysesRequestSchema,
    current_user=Depends(get_current_user),
    prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops),
    user_table_ops: UserTableOperation = Depends(get_user_table_ops),
):
    await create_additional_analysis_run(
        request_id=request_id,
        additional_analyses_request_data=additional_analyses_request_data,
        current_user=current_user,
        prompt_table_ops=prompt_table_ops,
        user_table_ops=user_table_ops,
    )

    return {"detail": "additional analyses request executed"}
from fastapi import HTTPException, Depends, Request, APIRouter

from fastapi.responses import FileResponse

from app.services.infra import (
    get_task_plot_results,
    update_last_accessed_at_when_called,
    check_if_task_is_valid,
    limiter,
)

from app.services.llm import check_if_api_key_valid

from app.crud.queries import (
    PromptTableOperation,
    TaskRunTableOperation,
    UserCustomizedTasksTableOperation,
    UserTableOperation,
)

from app.crud.dependencies import (
    get_prompt_table_ops,
    get_task_run_table_ops,
    get_user_customized_tasks_table_ops,
    get_redis_client,
    get_user_table_ops,
)

from app.core.auth import get_current_user, get_admin, encrypt_api_key
from app.schemas.routes import (
    UserCustomizedTasksSchema,
    SetupAPIKeySchema,
)
from app.schemas.enums import TaskProcessingRunType

from app.core.logger import logger

from app.core.config import Config

import json

import os


router = APIRouter()


@router.get("/get_original_tasks_by_id/{request_id}")
@limiter.limit(Config.RATE_LIMIT_GET_ENDPOINTS)
@update_last_accessed_at_when_called
@check_if_task_is_valid
async def get_original_tasks_by_id(
    request: Request,
    request_id: str,
    current_user=Depends(get_current_user),
    prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops),
    task_run_table_ops: TaskRunTableOperation = Depends(get_task_run_table_ops),
    redis_client=Depends(get_redis_client),
):
    user_id = current_user.user_id

    res = await task_run_table_ops.get_original_tasks_by_id(user_id, request_id)

    if not res:
        raise HTTPException(
            status_code=404, detail="cannot find the requested original tasks"
        )

    plot_result = get_task_plot_results(
        request_id, run_type=TaskProcessingRunType.first_run_after_request.value
    )

    return {"res": res, "plot_result": plot_result}


@router.get("/get_modified_tasks_by_id/{request_id}")
@limiter.limit(Config.RATE_LIMIT_GET_ENDPOINTS)
@update_last_accessed_at_when_called
@check_if_task_is_valid
async def get_modified_tasks_by_id(
    request: Request,
    request_id: str,
    current_user=Depends(get_current_user),
    prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops),
    task_run_table_ops: TaskRunTableOperation = Depends(get_task_run_table_ops),
    redis_client=Depends(get_redis_client),
):
    user_id = current_user.user_id

    res = await task_run_table_ops.get_modified_tasks_by_id(user_id, request_id)

    if not res:
        raise HTTPException(
            status_code=404, detail="cannot find the requested modified tasks"
        )

    plot_result = get_task_plot_results(
        request_id, run_type=TaskProcessingRunType.modified_tasks_execution.value
    )

    return {"res": res, "plot_result": plot_result}


@router.get("/get_col_info_by_id/{request_id}")
@limiter.limit(Config.RATE_LIMIT_GET_ENDPOINTS)
@update_last_accessed_at_when_called
@check_if_task_is_valid
async def get_col_info_by_id(
    request: Request,
    request_id: str,
    current_user=Depends(get_current_user),
    prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops),
    task_run_table_ops: TaskRunTableOperation = Depends(get_task_run_table_ops),
    redis_client=Depends(get_redis_client),
):
    user_id = current_user.user_id

    res = await task_run_table_ops.get_columns_info_by_id(user_id, request_id)

    if not res:
        raise HTTPException(
            status_code=404, detail="cannot find the requested columns info"
        )

    return res


@router.get("/get_dataset_snippet_by_id/{request_id}")
@limiter.limit(Config.RATE_LIMIT_GET_ENDPOINTS)
@update_last_accessed_at_when_called
@check_if_task_is_valid
async def get_dataset_snippet_by_id(
    request: Request,
    request_id: str,
    current_user=Depends(get_current_user),
    prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops),
    task_run_table_ops: TaskRunTableOperation = Depends(get_task_run_table_ops),
    redis_client=Depends(get_redis_client),
):
    user_id = current_user.user_id

    res = await task_run_table_ops.get_dataset_snippet_by_id(user_id, request_id)

    if not res:
        raise HTTPException(
            status_code=404, detail="cannot find the requested dataset snippet"
        )

    return res


@router.get("/get_request_ids")
@limiter.limit(Config.RATE_LIMIT_GET_ENDPOINTS)
async def get_request_ids(
    request: Request,
    current_user=Depends(get_current_user),
    prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops),
):
    user_id = current_user.user_id
    res = await prompt_table_ops.get_request_ids_by_user(user_id)

    if not res:
        raise HTTPException(status_code=404, detail="cannot find any request ids")

    return {"request_ids": res}


@router.get("/get_prompt_result_req_id/{request_id}")
@limiter.limit(Config.RATE_LIMIT_GET_ENDPOINTS)
async def get_prompt_result_req_id(
    request: Request,
    request_id: str,
    prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops),
    get_admin=Depends(get_admin),
):
    res = await prompt_table_ops.get_prompt_result(request_id)
    res = res["prompt_result"]
    return {"res": json.loads(res)}


@router.post("/manage_user_cust_tasks")
@limiter.limit(Config.RATE_LIMIT_GET_ENDPOINTS)
async def manage_user_customized_tasks(
    request: Request,
    user_cust_tasks_schema: UserCustomizedTasksSchema,
    current_user=Depends(get_current_user),
    user_cust_tasks_table_ops: UserCustomizedTasksTableOperation = Depends(
        get_user_customized_tasks_table_ops
    ),
):
    user_id = current_user.user_id

    request_id = user_cust_tasks_schema.request_id
    operation = user_cust_tasks_schema.operation
    slot = user_cust_tasks_schema.slot
    tasks = user_cust_tasks_schema.tasks

    customized_tasks_key = "customized_tasks"

    if operation == "fetch":
        res = await user_cust_tasks_table_ops.fetch_customized_tasks(
            user_id, request_id, slot
        )
        return {"res": res}

    elif operation == "check_if_empty":
        res = await user_cust_tasks_table_ops.check_if_customized_tasks_empty(
            user_id, request_id
        )
        return {"res": res}

    elif operation == "delete":
        await user_cust_tasks_table_ops.delete_customized_tasks(
            user_id, request_id, slot
        )
        return {"detail": "delete customized tasks operation successful"}

    elif operation == "update":
        if customized_tasks_key not in tasks:
            raise HTTPException(
                status_code=400, detail="tasks cant be empty for update operation"
            )
        tasks = json.dumps(tasks)
        await user_cust_tasks_table_ops.update_customized_tasks(
            user_id, request_id, slot, tasks
        )
        return {"detail": "update customized tasks operation successful"}


@router.get("/download_excel_result/{task_type}/{request_id}/{task_id}")
@limiter.limit(Config.RATE_LIMIT_GET_ENDPOINTS)
@check_if_task_is_valid
async def download_excel_result(
    request: Request,
    task_type: str,
    request_id: str,
    task_id: int,
    current_user=Depends(get_current_user),
    prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops),
):
    if task_type not in ["original_tasks", "customized_tasks"]:
        raise HTTPException(status_code=400, detail="not a valid task_type category")

    # task_id = secure_filename(str(task_id))
    # request_id = secure_filename(request_id)

    file_path = (
        f"{Config.DATASET_SAVE_PATH}/{request_id}/{task_type}/artifacts/{task_id}.xlsx"
    )

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="the requested file does not exist")

    name = os.path.basename(file_path)

    return FileResponse(file_path, media_type="application/octet-stream", filename=name)


@router.post("/setup_api_key")
@limiter.limit(Config.RATE_LIMIT_TASK_ENDPOINTS)
async def setup_api_key(
    request: Request,
    api_key_data: SetupAPIKeySchema,
    current_user=Depends(get_current_user),
    user_table_ops: UserTableOperation = Depends(get_user_table_ops),
):
    provider = api_key_data.provider
    key = api_key_data.key
    user_id = current_user.user_id

    # delete key if no key is specified
    if len(key) == 0:
        await user_table_ops.delete_api_key(user_id, provider)
        return {"detail": "api key deleted"}

    check_result = await check_if_api_key_valid(key, provider)

    if check_result == "INVALID_KEY":
        raise HTTPException(status_code=400, detail="invalid api key")

    if check_result == "TRY_LATER":
        raise HTTPException(
            status_code=400, detail="cant determine validity now. try again later."
        )

    encrypted_key = encrypt_api_key(key).decode()

    await user_table_ops.add_api_key(
        user_id=user_id, key=encrypted_key, provider=provider
    )

    logger.info(f"user added api key: user_id {user_id}")

    return {"detail": "api key setup successful"}

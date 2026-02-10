from fastapi import (
    FastAPI,
    UploadFile,
    HTTPException,
    Depends,
    Form,
    Request,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from starlette.concurrency import run_in_threadpool

from app.services.dataset import (
    CsvReader,
    save_dataset_req_id,
    get_request_id_saved_dataset_dir,
    get_column_names_csv,
    get_row_count_csv,
)
from app.services.llm import (
    DatasetProcessorForPtOnePrompt,
    check_if_api_key_valid_cerebras,
    check_if_api_key_valid_google,
    get_api_key,
)
from app.services.infra import (
    get_background_tasks,
    get_task_plot_results,
    LogRequestMiddleware,
    update_last_accessed_at_when_called,
    check_if_task_is_valid,
    get_email_data_and_attachment,
    get_col_transform_and_combination,
)
from app.services.utils import (
    split_and_validate_new_prompt,
)
from app.exceptions import InvalidDatasetException, FileReadException


from app.tasks import (
    get_prompt_result_task,
    data_processing_task,
    get_additional_analyses_prompt_result,
    send_email_task,
)
from app.crud import (
    PromptTableOperation,
    UserTableOperation,
    TaskRunTableOperation,
    get_prompt_table_ops,
    get_task_run_table_ops,
    get_user_table_ops,
    get_user_customized_tasks_table_ops,
    UserCustomizedTasksTableOperation,
    get_redis_client,
)
from app.auth import (
    create_access_token,
    get_current_user,
    generate_random_otp,
    verify_otp,
    get_admin,
    encrypt_api_key,
)
from app.schemas import (
    UserRegisterSchema,
    ExecuteAnalysesSchema,
    AdditionalAnalysesRequestSchema,
    RunInfo,
    UploadDatasetSchema,
    UserCustomizedTasksSchema,
    GetOTPSchema,
    LoginSchema,
    TaskProcessingRunType,
    DataTasks,
    SetupAPIKeySchema,
    JoinDatasetSchema,
)
from dataclasses import asdict


from app.data_transform_utils import clean_column_name, join_df_duckdb

from app.config import Config


from app.logger import logger

from celery import chain

import json

from sqlalchemy.exc import IntegrityError

from ast import literal_eval

from datetime import datetime, timezone, timedelta

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

import os


from pydantic import ValidationError

import shutil


# init_sentry()


limiter = Limiter(key_func=get_remote_address)
app = FastAPI()


app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(LogRequestMiddleware)

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)


@app.get("/")
@limiter.limit(Config.RATE_LIMIT_GET_ENDPOINTS)
async def read_root(request: Request):
    return {"Hello": "World"}


@app.get("/health")
@limiter.limit(Config.RATE_LIMIT_GET_ENDPOINTS)
async def health_check(request: Request):
    return {"detail": "app is running"}


@app.post("/delete_task/{request_id}")
@limiter.limit(Config.RATE_LIMIT_TASK_ENDPOINTS)
@check_if_task_is_valid
async def delete_task(
    request: Request,
    request_id: str,
    current_user=Depends(get_current_user),
    prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops),
    user_cust_tasks_table_ops: UserCustomizedTasksTableOperation = Depends(
        get_user_customized_tasks_table_ops
    ),
    task_run_table_ops: TaskRunTableOperation = Depends(get_task_run_table_ops),
):
    user_id = current_user.user_id

    await user_cust_tasks_table_ops.delete_task(user_id=user_id, request_id=request_id)
    await task_run_table_ops.delete_task(user_id=user_id, request_id=request_id)
    await prompt_table_ops.delete_task(user_id=user_id, request_id=request_id)

    data_dir = f"{Config.DATASET_SAVE_PATH}/{request_id}"

    if not os.path.exists(data_dir):
        raise HTTPException(
            status_code=400, detail="cannot find directory for the task deletion"
        )

    shutil.rmtree(data_dir)

    logger.info(f"task deleted: request_id {request_id}, user_id {user_id}")

    return {"detail": f"task {request_id} has been deleted"}


@app.post("/upload_dataset")
@limiter.limit(Config.RATE_LIMIT_TASK_ENDPOINTS)
async def upload(
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

    try:
        upload_dataset_data: UploadDatasetSchema = (
            UploadDatasetSchema.model_validate_json(upload_dataset_data)
        )
    except ValidationError:
        raise HTTPException(status_code=422, detail="invalid parameters")

    run_name = upload_dataset_data.run_name
    model = upload_dataset_data.model
    provider = upload_dataset_data.provider
    analysis_task_count = upload_dataset_data.analysis_task_count
    send_result_to_email = upload_dataset_data.send_result_to_email

    user_id = current_user.user_id
    user_email = current_user.email

    api_key = await get_api_key(
        user_table_ops=user_table_ops, user_id=user_id, provider=provider
    )

    try:
        file_reader = CsvReader(upload_file=file)
        file_data = await run_in_threadpool(file_reader.get_dataframe_dict)
    except FileReadException:
        raise HTTPException(status_code=400, detail="invalid file format")
    except InvalidDatasetException:
        raise HTTPException(
            status_code=400, detail="dataset is too big or does not have header"
        )

    dataset_dataframe = file_data["dataframe"]
    dataset_filename = file_data["filename"]
    dataset_columns_str = file_data["columns_str"]
    dataset_granularity_map = file_data["granularity_map"]
    dataset_id = file_data["dataset_id"]

    run_type = TaskProcessingRunType.first_run_after_request.value

    data_processor = DatasetProcessorForPtOnePrompt(
        dataframe=dataset_dataframe,
        filename=dataset_filename,
        prompt_template_file=Config.PT1_PROMPT_TEMPLATE,
        granularity_data=dataset_granularity_map,
    )

    prompt_pt_1 = await run_in_threadpool(data_processor.create_prompt)

    request_id = await prompt_table_ops.add_task(
        user_id=user_id,
        prompt_version=Config.DEFAULT_PROMPT_VERSION,
        filename=dataset_filename,
        dataset_cols=dataset_columns_str,
        model=model,
        run_name=run_name,
    )

    parquet_file = await run_in_threadpool(
        save_dataset_req_id,
        request_id=request_id,
        dataframe=dataset_dataframe,
        run_type=run_type,
    )

    run_info = RunInfo(
        request_id=request_id,
        user_id=user_id,
        parquet_file=parquet_file,
        filename=dataset_filename,
        send_result_to_email=send_result_to_email,
        email=user_email,
        run_name=run_name,
    )
    run_info = asdict(run_info)

    tasks = [
        get_prompt_result_task.s(
            model=model,
            provider=provider,
            api_key=api_key,
            prompt_pt_1=prompt_pt_1,
            task_count=analysis_task_count,
            dataset_id=dataset_id,
            request_id=request_id,
            user_id=user_id,
            dataset_cols=literal_eval(dataset_columns_str),
            debug_prompt_and_res=True,
            # mock_pt1_resp_file='resp_jsons/res_pt1_test.json', mock_pt2_resp_file='resp_jsons/res_pt2_test.json'
        ),
        data_processing_task.s(
            run_info=run_info,
            run_type=TaskProcessingRunType.first_run_after_request.value,
        ),
    ]

    if send_result_to_email:
        email_data = get_email_data_and_attachment(
            request_id, run_type, dataset_filename, run_name
        )
        subject = email_data["subject"]
        body = email_data["body"]
        attachment = email_data["attachment"]

        tasks.append(
            send_email_task.si(
                subject=subject,
                receiver=user_email,
                body=body,
                attachment_path=attachment,
            )
        )

    chain(*tasks).apply_async()

    await user_cust_tasks_table_ops.add_request_id_to_table(user_id, request_id)

    logger.info(
        f"initial task request added: request_id {request_id}, user_id {user_id}"
    )

    return {"detail": "request task executed"}


@app.post("/execute_analyses/{request_id}")
@limiter.limit(Config.RATE_LIMIT_TASK_ENDPOINTS)
@check_if_task_is_valid
async def execute_analyses(
    request: Request,
    request_id: str,
    execute_analyses_data: str = Form(...),
    current_user=Depends(get_current_user),
    prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops),
):
    user_id = current_user.user_id
    user_email = current_user.email

    run_type = TaskProcessingRunType.modified_tasks_execution.value

    data_tasks_context = {
        "run_type": run_type,
        "request_id": request_id,
        "is_from_data_tasks": True,
    }

    try:
        execute_analyses_data = ExecuteAnalysesSchema.model_validate_json(
            execute_analyses_data, context=data_tasks_context
        )
    except ValidationError:
        raise HTTPException(status_code=422, detail="invalid parameters")

    send_result_to_email = execute_analyses_data.send_result_to_email

    parquet_file = get_request_id_saved_dataset_dir(request_id, run_type)

    dataset_filename = await prompt_table_ops.get_dataset_filename(request_id, user_id)
    run_name = await prompt_table_ops.get_run_name(request_id, user_id)

    run_info = RunInfo(
        request_id=request_id,
        user_id=user_id,
        parquet_file=parquet_file,
        filename=dataset_filename,
        send_result_to_email=send_result_to_email,
        email=user_email,
        run_name=run_name,
    )
    run_info = asdict(run_info)

    data_tasks = execute_analyses_data.model_dump()
    data_tasks = DataTasks.model_validate(
        data_tasks, context=data_tasks_context
    ).model_dump()

    tasks = [
        data_processing_task.s(
            data_tasks_dict=data_tasks, run_info=run_info, run_type=run_type
        )
    ]

    if send_result_to_email:
        email_data = get_email_data_and_attachment(
            request_id, run_type, dataset_filename, run_name
        )
        subject = email_data["subject"]
        body = email_data["body"]
        attachment = email_data["attachment"]

        tasks.append(
            send_email_task.si(
                subject=subject,
                receiver=user_email,
                body=body,
                attachment_path=attachment,
            )
        )

    chain(*tasks).apply_async()

    logger.info(
        f"modified task execution request added: request_id {request_id}, user_id {user_id}"
    )

    return {"detail": "analysis task executed"}


@app.post("/execute_analyses_with_new_dataset/{request_id}")
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
    user_id = current_user.user_id
    user_email = current_user.email
    run_type = TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value

    data_tasks_context = {
        "run_type": run_type,
        "request_id": request_id,
        "is_from_data_tasks": True,
    }

    try:
        execute_analyses_data = ExecuteAnalysesSchema.model_validate_json(
            execute_analyses_data, context=data_tasks_context
        )
    except ValidationError:
        raise HTTPException(status_code=422, detail="invalid parameters")

    send_result_to_email = execute_analyses_data.send_result_to_email
    parquet_file = f"{Config.DATASET_SAVE_PATH}/{request_id}/new_dataset.parquet"

    file_reader = CsvReader(upload_file=file)
    file_data = await run_in_threadpool(file_reader.get_dataframe_dict)
    dataset_dataframe = file_data["dataframe"]
    dataset_columns_str = file_data["columns_str"]
    dataset_filename = file_data["filename"]

    original_columns_str = await prompt_table_ops.get_dataset_columns_by_id(
        request_id=request_id, user_id=user_id
    )

    run_name = await prompt_table_ops.get_run_name(request_id, user_id)

    if dataset_columns_str != original_columns_str:
        raise HTTPException(
            status_code=403,
            detail="this dataset does not have the columns from the original dataset",
        )

    parquet_file = await run_in_threadpool(
        save_dataset_req_id,
        request_id=request_id,
        dataframe=dataset_dataframe,
        run_type=run_type,
    )

    run_info = RunInfo(
        request_id=request_id,
        user_id=user_id,
        parquet_file=parquet_file,
        filename=dataset_filename,
        send_result_to_email=send_result_to_email,
        email=user_email,
        run_name=run_name,
    )
    run_info = asdict(run_info)

    data_tasks = execute_analyses_data.model_dump()

    col_transforms, col_combinations = await get_col_transform_and_combination(
        user_id, request_id, task_run_table_ops
    )
    data_tasks["common_column_cleaning_or_transformation"] = col_transforms
    data_tasks["common_column_combination"] = col_combinations

    data_tasks = DataTasks.model_validate(
        data_tasks, context=data_tasks_context
    ).model_dump()

    tasks = [
        data_processing_task.s(
            data_tasks_dict=data_tasks, run_info=run_info, run_type=run_type
        )
    ]

    if send_result_to_email:
        email_data = get_email_data_and_attachment(
            request_id, run_type, dataset_filename, run_name
        )
        subject = email_data["subject"]
        body = email_data["body"]
        attachment = email_data["attachment"]

        tasks.append(
            send_email_task.si(
                subject=subject,
                receiver=user_email,
                body=body,
                attachment_path=attachment,
            )
        )

    chain(*tasks).apply_async()

    logger.info(
        f"modified task execution request with new dataset added: request_id {request_id}, user_id {user_id}"
    )

    return {"detail": "analysis task executed"}


@app.post("/make_additional_analyses_request/{request_id}")
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

    user_id = current_user.user_id
    user_email = current_user.email
    model = additional_analyses_request_data.model
    provider = additional_analyses_request_data.provider
    new_tasks_prompt = additional_analyses_request_data.new_tasks_prompt
    send_result_to_email = additional_analyses_request_data.send_result_to_email

    run_type = TaskProcessingRunType.additional_analyses_request.value

    api_key = await get_api_key(
        user_table_ops=user_table_ops, user_id=user_id, provider=provider
    )

    new_tasks_prompt = split_and_validate_new_prompt(new_tasks_prompt)

    if not new_tasks_prompt:
        raise HTTPException(
            status_code=403,
            detail="the new tasks request prompt does not meet the requirements",
        )

    additional_analyses_prompt_res = (
        await prompt_table_ops.get_additional_analyses_prompt_result(
            request_id, user_id
        )
    )

    if (
        additional_analyses_prompt_res is not None
    ):  # if user already run additional analyses on this req_id previously
        raise HTTPException(
            status_code=400,
            detail="can only execute one additional analyses request for one dataset",
        )

    parquet_file = get_request_id_saved_dataset_dir(request_id, run_type)

    dataset_filename = await prompt_table_ops.get_dataset_filename(request_id, user_id)
    run_name = await prompt_table_ops.get_run_name(request_id, user_id)

    run_info = RunInfo(
        request_id=request_id,
        user_id=user_id,
        parquet_file=parquet_file,
        filename=dataset_filename,
        send_result_to_email=send_result_to_email,
        email=user_email,
        run_name=run_name,
    )
    run_info = asdict(run_info)

    tasks = [
        get_additional_analyses_prompt_result.s(
            model=model,
            provider=provider,
            api_key=api_key,
            new_tasks_prompt=new_tasks_prompt,
            request_id=request_id,
            user_id=user_id,
        ),
        data_processing_task.s(
            run_info=run_info,
            run_type=TaskProcessingRunType.additional_analyses_request.value,
        ),
    ]

    if send_result_to_email:
        email_data = get_email_data_and_attachment(
            request_id, run_type, dataset_filename, run_name
        )
        subject = email_data["subject"]
        body = email_data["body"]
        attachment = email_data["attachment"]

        tasks.append(
            send_email_task.si(
                subject=subject,
                receiver=user_email,
                body=body,
                attachment_path=attachment,
            )
        )

    chain(*tasks).apply_async()

    logger.info(
        f"additional analyses request added: request_id {request_id}, user_id {user_id}"
    )

    return {"detail": "additional analyses request executed"}


@app.post("/join_dataset")
@limiter.limit(Config.RATE_LIMIT_TASK_ENDPOINTS)
async def join_dataset(
    request: Request,
    dataset_1: UploadFile,
    dataset_2: UploadFile,
    join_dataset_data: str = Form(...),
    current_user=Depends(get_current_user),
):
    try:
        join_dataset_data = JoinDatasetSchema.model_validate_json(join_dataset_data)
    except ValidationError:
        raise HTTPException(status_code=422, detail="invalid parameters")

    join_keys = join_dataset_data.join_keys
    join_method = join_dataset_data.join_method

    dataset_1_cols = get_column_names_csv(dataset_1)
    dataset_2_cols = get_column_names_csv(dataset_2)

    dataset_1_row_count = get_row_count_csv(dataset_1)
    dataset_2_row_count = get_row_count_csv(dataset_2)

    left_on_cols_in_dataset_1_cols = all([i[0] in dataset_1_cols for i in join_keys])
    right_on_cols_in_dataset_2_cols = all([i[1] in dataset_2_cols for i in join_keys])

    if not (left_on_cols_in_dataset_1_cols and right_on_cols_in_dataset_2_cols):
        raise HTTPException(
            status_code=400, detail="all join columns must exist in both datasets"
        )

    dataset_1_size_too_big = (
        dataset_1_row_count > Config.MAX_DATAFRAME_ROWS_JOIN_UTIL
        or len(dataset_1_cols) > Config.MAX_DATAFRAME_COLS_JOIN_UTIL
    )
    dataset_2_size_too_big = (
        dataset_2_row_count > Config.MAX_DATAFRAME_ROWS_JOIN_UTIL
        or len(dataset_2_cols) > Config.MAX_DATAFRAME_COLS_JOIN_UTIL
    )

    if dataset_1_size_too_big or dataset_2_size_too_big:
        raise HTTPException(
            status_code=400, detail="the datasets dont meet the size criteria"
        )

    dataset_1_reader = CsvReader(upload_file=dataset_1)
    dataset_1_data = await run_in_threadpool(dataset_1_reader.get_dataframe_dict)
    dataset_1_df = dataset_1_data["dataframe"]

    dataset_2_reader = CsvReader(upload_file=dataset_2)
    dataset_2_data = await run_in_threadpool(dataset_2_reader.get_dataframe_dict)
    dataset_2_df = dataset_2_data["dataframe"]

    join_keys_clean = [
        (clean_column_name(i), clean_column_name(j)) for i, j in join_keys
    ]

    joined_df = await run_in_threadpool(
        join_df_duckdb,
        df1=dataset_1_df,
        df2=dataset_2_df,
        join_keys=join_keys_clean,
        how=join_method,
    )
    joined_df_buffer = joined_df.to_csv(index=False, compression="gzip")

    content = joined_df_buffer.getvalue()
    headers = {"Content-Disposition": 'attachment; filename="result_dataset.csv.gz"'}

    return Response(content=content, headers=headers, media_type="application/gzip")


@app.get("/get_original_tasks_by_id/{request_id}")
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


@app.get("/get_modified_tasks_by_id/{request_id}")
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


@app.get("/get_col_info_by_id/{request_id}")
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


@app.get("/get_dataset_snippet_by_id/{request_id}")
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


@app.get("/get_request_ids")
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


@app.get("/get_prompt_result_req_id/{request_id}")
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


@app.post("/manage_user_cust_tasks")
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


@app.get("/download_excel_result/{task}/{request_id}/{task_id}")
@limiter.limit(Config.RATE_LIMIT_GET_ENDPOINTS)
@check_if_task_is_valid
async def download_excel_result(
    request: Request,
    task: str,
    request_id: str,
    task_id: int,
    current_user=Depends(get_current_user),
    prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops),
):
    if task not in ["original_tasks", "customized_tasks"]:
        raise HTTPException(status_code=400, detail="not a valid task category")

    # task_id = secure_filename(str(task_id))
    # request_id = secure_filename(request_id)

    file_path = (
        f"{Config.DATASET_SAVE_PATH}/{request_id}/{task}/artifacts/{task_id}.xlsx"
    )

    if not os.path.exists(file_path):
        raise HTTPException(status_code=400, detail="the requested file does not exist")

    name = os.path.basename(file_path)

    return FileResponse(file_path, media_type="application/octet-stream", filename=name)


################### USER ROUTES ###################


@app.post("/get_otp")
async def get_otp(
    get_otp_data: GetOTPSchema,
    background_tasks=Depends(get_background_tasks),
    user_table_ops: UserTableOperation = Depends(get_user_table_ops),
):
    username = get_otp_data.username

    user = await user_table_ops.get_user(username)

    if not user:
        logger.warning(f"user {username} does not exist in db and tried to log in")
        raise HTTPException(status_code=401, detail="Invalid credentials.")

    if user["last_otp_request"] is not None:
        if datetime.now(timezone.utc) < user["last_otp_request"] + timedelta(
            minutes=1
        ):  # if one minute has not passed since the last otp request
            raise HTTPException(
                status_code=429,
                detail="You need to wait one minute before requesting another OTP",
            )

    raw_otp, encrypted_otp = generate_random_otp()
    otp_expire = datetime.now(timezone.utc) + timedelta(
        minutes=Config.OTP_EXPIRE_MINUTES
    )

    await user_table_ops.update_otp(
        username=username, otp=encrypted_otp, otp_expire=otp_expire
    )

    receiver = user["email"]
    subject = "OTP for Data Analysis Assistant app"
    body = f"Your OTP is {raw_otp}"

    send_email_task.delay(subject=subject, receiver=receiver, body=body)

    return {"detail": "otp has been sent"}


@app.post("/login")
@limiter.limit(Config.RATE_LIMIT_LOGIN)
async def login(
    request: Request,
    login_data: LoginSchema,
    user_table_ops: UserTableOperation = Depends(get_user_table_ops),
):
    user = await user_table_ops.get_user(login_data.username)
    user_otp = user["otp"]
    username = user["username"]

    if not user or not verify_otp(login_data.otp, user_otp):
        logger.warning(f"user {login_data.username} failed to log in")
        raise HTTPException(status_code=401, detail="Incorrect username")

    if datetime.now(timezone.utc) > user["otp_expire"]:
        raise HTTPException(
            status_code=401, detail="Expired OTP. Please generate a new one."
        )

    access_token = create_access_token(
        data={"sub": username}, expire_minutes=Config.ACCESS_TOKEN_EXPIRE_MINUTES
    )

    logger.info(f"user {username} logged in")

    # generate new otp to invalidate previous otp
    _, encrypted_otp = generate_random_otp()
    otp_expire = datetime.now(timezone.utc) + timedelta(
        minutes=Config.OTP_EXPIRE_MINUTES
    )

    await user_table_ops.update_otp(
        username=username, otp=encrypted_otp, otp_expire=otp_expire
    )

    return {"access_token": access_token, "token_type": "bearer"}


@app.post("/register_user")
@limiter.limit(Config.RATE_LIMIT_REGISTER)
async def register_user(
    request: Request,
    user_register_data: UserRegisterSchema,
    user_table_ops: UserTableOperation = Depends(get_user_table_ops),
):
    try:
        username = user_register_data.username
        email = user_register_data.email
        first_name = user_register_data.first_name
        last_name = user_register_data.last_name

        await user_table_ops.create_user(
            username=username, email=email, first_name=first_name, last_name=last_name
        )

        logger.info(f"account {username} successfully created")

        return {"detail": f"account {username} successfully created"}

    except IntegrityError:
        logger.warning(
            f"{username}/{email} failed to register because of conflicting username/email."
        )
        raise HTTPException(
            status_code=409,
            detail=f"username {username} or email {email} already exists.",
        )


@app.post("/setup_api_key")
@limiter.limit(Config.RATE_LIMIT_REGISTER)
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

    validate_func_dct = {
        "google": check_if_api_key_valid_google,
        "cerebras": check_if_api_key_valid_cerebras,
    }
    check_if_api_key_valid = validate_func_dct[provider]

    check_result = await check_if_api_key_valid(key)

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

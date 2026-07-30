from ast import literal_eval
from dataclasses import asdict

from fastapi import HTTPException, UploadFile
from pydantic import ValidationError
from starlette.concurrency import run_in_threadpool
from celery import chain

from app.core.config import Config
from app.core.logger import logger

from app.core.exceptions import InvalidDatasetException, FileReadException

from app.schemas.routes import (
    UploadDatasetSchema,
    ExecuteAnalysesSchema,
    AdditionalAnalysesRequestSchema,
    RunInfo,
)

from app.schemas.llm_validation import DataTasks

from app.schemas.enums import (
    TaskProcessingRunType,
)

from app.services.dataset import (
    CsvReader,
    XlsxReader,
    save_dataset_req_id,
    get_request_id_saved_dataset_dir,
)

from app.services.llm import (
    DatasetProcessorForPtOnePrompt,
    get_api_key,
)

from app.services.infra import (
    get_email_data_and_attachment,
    get_col_transform_and_combination,
)

from app.services.utils import (
    dataset_columns_match,
    split_and_validate_new_prompt,
)

from app.tasks import (
    get_prompt_result_task,
    get_additional_analyses_prompt_result_task,
    data_processing_task,
    send_email_task,
)

from app.crud.queries import (
    PromptTableOperation,
    UserTableOperation,
    TaskRunTableOperation,
    UserCustomizedTasksTableOperation,
)


def parse_form_json(
    raw_data,
    schema_cls,
    context=None,
):
    try:
        if context is not None:
            return schema_cls.model_validate_json(raw_data, context=context)

        return schema_cls.model_validate_json(raw_data)

    except ValidationError:
        raise HTTPException(status_code=422, detail="invalid parameters")


def build_data_tasks_context(
    request_id,
    run_type,
):
    return {
        "run_type": run_type,
        "request_id": request_id,
        "is_from_data_tasks": True,
    }


def build_run_info(
    request_id,
    user_id,
    parquet_file,
    filename,
    send_result_to_email,
    email,
    run_name,
):
    return asdict(
        RunInfo(
            request_id=request_id,
            user_id=user_id,
            parquet_file=parquet_file,
            filename=filename,
            send_result_to_email=send_result_to_email,
            email=email,
            run_name=run_name,
        )
    )


async def read_uploaded_file(file: UploadFile):
    filename = file.filename

    if filename.endswith(".csv"):
        FileReader = CsvReader
    elif filename.endswith(".xlsx"):
        FileReader = XlsxReader

    try:
        file_reader = FileReader(upload_file=file)
        return await run_in_threadpool(file_reader.get_dataframe_dict)

    except FileReadException:
        raise HTTPException(
            status_code=400, detail="There is a problem with reading the file"
        )

    except InvalidDatasetException as e:
        raise HTTPException(
            status_code=400,
            detail=str(e),
        )


def append_email_task_if_requested(
    tasks,
    send_result_to_email,
    request_id,
    run_type,
    dataset_filename,
    run_name,
    receiver,
) -> None:
    if not send_result_to_email:
        return

    email_data = get_email_data_and_attachment(
        request_id,
        run_type,
        dataset_filename,
        run_name,
    )

    tasks.append(
        send_email_task.si(
            subject=email_data["subject"],
            receiver=receiver,
            body=email_data["body"],
            attachment_path=email_data["attachment"],
        )
    )


def dispatch_task_chain(tasks):
    chain(*tasks).apply_async()


async def create_initial_analysis_run(
    file: UploadFile,
    upload_dataset_data: UploadDatasetSchema,
    current_user,
    prompt_table_ops: PromptTableOperation,
    user_cust_tasks_table_ops: UserCustomizedTasksTableOperation,
    user_table_ops: UserTableOperation,
):
    user_id = current_user.user_id
    user_email = current_user.email

    run_name = upload_dataset_data.run_name
    model = upload_dataset_data.model
    provider = upload_dataset_data.provider
    analysis_task_count = upload_dataset_data.analysis_task_count
    send_result_to_email = upload_dataset_data.send_result_to_email

    run_type = TaskProcessingRunType.first_run_after_request.value

    api_key = await get_api_key(
        user_table_ops=user_table_ops,
        user_id=user_id,
        provider=provider,
    )

    file_data = await read_uploaded_file(file)

    dataset_dataframe = file_data["dataframe"]
    dataset_filename = file_data["filename"]
    dataset_columns_str = file_data["columns_str"]
    dataset_granularity_map = file_data["granularity_map"]
    dataset_id = file_data["dataset_id"]

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

    run_info = build_run_info(
        request_id=request_id,
        user_id=user_id,
        parquet_file=parquet_file,
        filename=dataset_filename,
        send_result_to_email=send_result_to_email,
        email=user_email,
        run_name=run_name,
    )

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
        ),
        data_processing_task.s(
            run_info=run_info,
            run_type=run_type,
        ),
    ]

    append_email_task_if_requested(
        tasks=tasks,
        send_result_to_email=send_result_to_email,
        request_id=request_id,
        run_type=run_type,
        dataset_filename=dataset_filename,
        run_name=run_name,
        receiver=user_email,
    )

    dispatch_task_chain(tasks)

    await user_cust_tasks_table_ops.add_request_id_to_table(user_id, request_id)

    logger.info(
        f"initial task request added: request_id {request_id}, user_id {user_id}"
    )


async def execute_existing_analysis_run(
    request_id: str,
    execute_analyses_data: ExecuteAnalysesSchema,
    current_user,
    prompt_table_ops: PromptTableOperation,
):
    user_id = current_user.user_id
    user_email = current_user.email

    run_type = TaskProcessingRunType.modified_tasks_execution.value

    data_tasks_context = build_data_tasks_context(
        request_id=request_id,
        run_type=run_type,
    )

    send_result_to_email = execute_analyses_data.send_result_to_email

    parquet_file = get_request_id_saved_dataset_dir(request_id, run_type)

    dataset_filename = await prompt_table_ops.get_dataset_filename(
        request_id,
        user_id,
    )
    run_name = await prompt_table_ops.get_run_name(
        request_id,
        user_id,
    )

    run_info = build_run_info(
        request_id=request_id,
        user_id=user_id,
        parquet_file=parquet_file,
        filename=dataset_filename,
        send_result_to_email=send_result_to_email,
        email=user_email,
        run_name=run_name,
    )

    data_tasks = execute_analyses_data.model_dump()

    data_tasks = DataTasks.model_validate(
        data_tasks,
        context=data_tasks_context,
    ).model_dump()

    tasks = [
        data_processing_task.s(
            data_tasks_dict=data_tasks,
            run_info=run_info,
            run_type=run_type,
        )
    ]

    append_email_task_if_requested(
        tasks=tasks,
        send_result_to_email=send_result_to_email,
        request_id=request_id,
        run_type=run_type,
        dataset_filename=dataset_filename,
        run_name=run_name,
        receiver=user_email,
    )

    dispatch_task_chain(tasks)

    logger.info(
        f"modified task execution request added: request_id {request_id}, user_id {user_id}",
    )


async def execute_analysis_run_with_new_dataset(
    request_id: str,
    file: UploadFile,
    execute_analyses_data: ExecuteAnalysesSchema,
    current_user,
    prompt_table_ops: PromptTableOperation,
    task_run_table_ops: TaskRunTableOperation,
):
    user_id = current_user.user_id
    user_email = current_user.email

    run_type = TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value

    data_tasks_context = build_data_tasks_context(
        request_id=request_id,
        run_type=run_type,
    )

    send_result_to_email = execute_analyses_data.send_result_to_email

    file_data = await read_uploaded_file(file)

    dataset_dataframe = file_data["dataframe"]
    dataset_columns_str = file_data["columns_str"]
    dataset_filename = file_data["filename"]

    original_columns_str = await prompt_table_ops.get_dataset_columns_by_id(
        request_id=request_id,
        user_id=user_id,
    )

    if not dataset_columns_match(dataset_columns_str, original_columns_str):
        raise HTTPException(
            status_code=403,
            detail="this dataset does not have the columns from the original dataset",
        )

    run_name = await prompt_table_ops.get_run_name(
        request_id,
        user_id,
    )

    parquet_file = await run_in_threadpool(
        save_dataset_req_id,
        request_id=request_id,
        dataframe=dataset_dataframe,
        run_type=run_type,
    )

    run_info = build_run_info(
        request_id=request_id,
        user_id=user_id,
        parquet_file=parquet_file,
        filename=dataset_filename,
        send_result_to_email=send_result_to_email,
        email=user_email,
        run_name=run_name,
    )

    data_tasks = execute_analyses_data.model_dump()

    col_transforms, col_combinations = await get_col_transform_and_combination(
        user_id,
        request_id,
        task_run_table_ops,
    )

    data_tasks["common_column_cleaning_or_transformation"] = col_transforms
    data_tasks["common_column_combination"] = col_combinations

    data_tasks = DataTasks.model_validate(
        data_tasks,
        context=data_tasks_context,
    ).model_dump()

    tasks = [
        data_processing_task.s(
            data_tasks_dict=data_tasks,
            run_info=run_info,
            run_type=run_type,
        )
    ]

    append_email_task_if_requested(
        tasks=tasks,
        send_result_to_email=send_result_to_email,
        request_id=request_id,
        run_type=run_type,
        dataset_filename=dataset_filename,
        run_name=run_name,
        receiver=user_email,
    )

    dispatch_task_chain(tasks)

    logger.info(
        f"modified task execution with new dataset added: request_id {request_id}, user_id {user_id}",
    )


async def create_additional_analysis_run(
    request_id: str,
    additional_analyses_request_data: AdditionalAnalysesRequestSchema,
    current_user,
    prompt_table_ops: PromptTableOperation,
    user_table_ops: UserTableOperation,
):
    user_id = current_user.user_id
    user_email = current_user.email

    model = additional_analyses_request_data.model
    provider = additional_analyses_request_data.provider
    new_tasks_prompt = additional_analyses_request_data.new_tasks_prompt
    send_result_to_email = additional_analyses_request_data.send_result_to_email

    run_type = TaskProcessingRunType.additional_analyses_request.value

    api_key = await get_api_key(
        user_table_ops=user_table_ops,
        user_id=user_id,
        provider=provider,
    )

    new_tasks_prompt = split_and_validate_new_prompt(new_tasks_prompt)

    if not new_tasks_prompt:
        raise HTTPException(
            status_code=403,
            detail="the new tasks request prompt does not meet the requirements",
        )

    existing_additional_prompt_result = (
        await prompt_table_ops.get_additional_analyses_prompt_result(
            request_id,
            user_id,
        )
    )

    if existing_additional_prompt_result is not None:
        raise HTTPException(
            status_code=400,
            detail="can only execute one additional analyses request for one dataset",
        )

    parquet_file = get_request_id_saved_dataset_dir(request_id, run_type)

    dataset_filename = await prompt_table_ops.get_dataset_filename(
        request_id,
        user_id,
    )
    run_name = await prompt_table_ops.get_run_name(
        request_id,
        user_id,
    )

    run_info = build_run_info(
        request_id=request_id,
        user_id=user_id,
        parquet_file=parquet_file,
        filename=dataset_filename,
        send_result_to_email=send_result_to_email,
        email=user_email,
        run_name=run_name,
    )

    tasks = [
        get_additional_analyses_prompt_result_task.s(
            model=model,
            provider=provider,
            api_key=api_key,
            new_tasks_prompt=new_tasks_prompt,
            request_id=request_id,
            user_id=user_id,
        ),
        data_processing_task.s(
            run_info=run_info,
            run_type=run_type,
        ),
    ]

    append_email_task_if_requested(
        tasks=tasks,
        send_result_to_email=send_result_to_email,
        request_id=request_id,
        run_type=run_type,
        dataset_filename=dataset_filename,
        run_name=run_name,
        receiver=user_email,
    )

    dispatch_task_chain(tasks)

    logger.info(
        f"additional analyses request added: request_id {request_id}, user_id {user_id}",
    )

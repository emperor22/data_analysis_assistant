from app.schemas import TaskStatus, TaskProcessingRunType
from app.services.analysis import get_attachment_zip_filename

from app.logger import logger
from app.config import Config



import json
import time
import smtplib
import ssl
import base64
import os
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from functools import wraps

from fastapi.exceptions import HTTPException

from fastapi import BackgroundTasks, Request

from starlette.middleware.base import BaseHTTPMiddleware

import sentry_sdk



async def is_task_invalid_or_still_processing(request_id, user_id, prompt_table_ops):
    req_status = await prompt_table_ops.get_request_status(
        request_id=request_id, user_id=user_id
    )
    exclude_status = (
        TaskStatus.waiting_for_initial_request_prompt.value,
        TaskStatus.initial_request_prompt_received.value,
        TaskStatus.doing_initial_tasks_run.value,
        TaskStatus.failed_because_blacklisted_dataset,
        TaskStatus.deleted_because_not_accessed_recently,
    )

    if not req_status or (req_status and req_status["status"] in exclude_status):
        return True
    return False


def check_if_task_is_valid(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):

        request_id = kwargs.get("request_id")

        if not request_id:
            raise HTTPException(
                status_code=500,
                detail="the check_if_task_is_valid decorator expects request id argument",
            )

        current_user = kwargs.get("current_user")
        user_id = current_user.user_id

        prompt_table_ops = kwargs.get("prompt_table_ops")

        if not prompt_table_ops:
            raise HTTPException(status_code=400, detail="no prompt_table_ops argument")

        if await is_task_invalid_or_still_processing(
            request_id, user_id, prompt_table_ops
        ):
            raise HTTPException(
                status_code=403,
                detail="this is an invalid task or you must run an initial analysis request first",
            )

        return await func(*args, **kwargs)

    return wrapper


def update_last_accessed_at_when_called(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        request_id = kwargs.get("request_id")
        redis_client = kwargs.get("redis_client")

        if not request_id or not redis_client:
            logger.warning(
                f'update_last_accessed_at decorator called on function "{func.__name__}" with no request_id or redis_client parameter'
            )
            return await func(*args, **kwargs)

        today = datetime.now().strftime("%Y-%m-%d")

        hashtable_last_accessed = Config.REDIS_LAST_ACCESSED_HASHTABLE_NAME
        cooldown_key = f"{request_id}:last_write:{today}"

        logger.debug(f"cooldown key {cooldown_key}")

        if redis_client.set(cooldown_key, "on_cooldown", ex=3600 * 24 * 2, nx=True):
            redis_client.hset(hashtable_last_accessed, request_id, today)

        return await func(*args, **kwargs)

    return wrapper


def get_background_tasks(background_tasks: BackgroundTasks):
    return background_tasks


def init_sentry():
    sentry_sdk.init(dsn=Config.SENTRY_DSN, send_default_pii=True, enable_logs=True)


class LogRequestMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.perf_counter()

        response = await call_next(request)

        end_time = time.perf_counter()
        process_time = end_time - start_time
        process_time_ms = round(process_time * 1000, 2)

        if response.status_code == 200:
            logger.debug(
                f"{request.method} {request.url} | Completed with status {response.status_code} in {process_time_ms} ms"
            )
        elif response.status_code >= 400:
            logger.warning(
                f"CLIENT ERROR 4XX | {request.method} {request.url} | Headers: {dict(request.headers)} | Completed with status {response.status_code} in {process_time_ms} ms"
            )
        elif response.status_code >= 500:
            logger.error(
                f"SERVER ERROR 5XX | {request.method} {request.url} | Headers: {dict(request.headers)} | Completed with status {response.status_code} in {process_time_ms} ms"
            )

        slow_routes_exclude = ["/upload_dataset"]
        if (
            process_time_ms > Config.THRES_SLOW_RESPONSE_TIME_MS
            and Config.WARN_FOR_SLOW_RESPONSE_TIME
            and not any(i in str(request.url) for i in slow_routes_exclude)
        ):
            logger.warning(
                f"SLOW RESPONSE TIME | {request.method} {request.url} | Headers: {dict(request.headers)} | Completed with status {response.status_code} in {process_time_ms} ms"
            )

        return response


async def get_col_transform_and_combination(user_id, request_id, task_run_table_ops):
    def remove_status_field_from_res(dct):
        lst = []

        for task in dct:
            task = {i: j for i, j in task.items() if i != "status"}
            lst.append(task)
        return lst

    original_col_transforms = (
        await task_run_table_ops.get_columns_transformations_by_id(
            user_id=user_id, request_id=request_id
        )
    )
    original_col_transforms = json.loads(
        original_col_transforms["column_transforms_status"]
    )["column_transforms"]
    original_col_transforms = remove_status_field_from_res(original_col_transforms)

    original_col_combinations = await task_run_table_ops.get_columns_combinations_by_id(
        user_id=user_id, request_id=request_id
    )
    original_col_combinations = json.loads(
        original_col_combinations["column_combinations_status"]
    )["column_combinations"]
    original_col_combinations = remove_status_field_from_res(original_col_combinations)

    return original_col_transforms, original_col_combinations


def send_email_sync(receiver, subject, body, attachment_path):
    msg = MIMEMultipart()
    msg["From"] = Config.EMAIL_USERNAME
    msg["To"] = receiver
    msg["Subject"] = subject

    msg.attach(MIMEText(body, "plain", "utf-8"))

    if attachment_path is not None:
        with open(attachment_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())

        encoders.encode_base64(part)

        filename = os.path.basename(attachment_path)
        part.add_header("Content-Disposition", f'attachment; filename="{filename}"')

        msg.attach(part)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(Config.EMAIL_USERNAME, Config.EMAIL_PASSWORD)
        server.send_message(msg)
    # except smtplib.SMTPException as e:


def get_email_data_and_attachment(request_id, run_type, dataset_name, run_name):
    subject = f"Output for your analysis tasks. (Run name: {run_name})"
    body = f'Here is the result of your "{run_type}" analysis tasks using the dataset {dataset_name}.'

    attachment = get_attachment_zip_filename(request_id, run_name, run_type)

    return {"subject": subject, "body": body, "attachment": attachment}


def get_task_plot_results(request_id, run_type):

    def get_image_byte_as_str(image_path):
        with open(image_path, "rb") as f:
            data = f.read()

        encoded = base64.b64encode(data).decode("utf-8")

        return encoded

    task_category_dct = {
        TaskProcessingRunType.first_run_after_request.value: "original_tasks",
        TaskProcessingRunType.modified_tasks_execution.value: "customized_tasks",
        TaskProcessingRunType.additional_analyses_request.value: "original_tasks",
        TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value: "customized_tasks",
    }

    save_dir = task_category_dct[run_type]

    path = f"{Config.DATASET_SAVE_PATH}/{request_id}/{save_dir}/artifacts"

    if not os.path.exists(path):
        return {}

    plot_files = [i for i in os.listdir(path) if i.endswith(".png")]
    plot_task_ids = [i.split(".")[0] for i in plot_files]

    image_data = [
        get_image_byte_as_str(f"{path}/{plot_file}") for plot_file in plot_files
    ]

    return {task_id: image for task_id, image in zip(plot_task_ids, image_data)}

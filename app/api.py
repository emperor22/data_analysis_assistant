from fastapi import FastAPI, UploadFile, HTTPException, status, Depends, Form, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer
from starlette.concurrency import run_in_threadpool
from pydantic import BaseModel


from app.services import (CsvReader, DatasetProcessorForPtOnePrompt, is_task_invalid_or_still_processing, get_background_tasks, 
                          split_and_validate_new_prompt, save_dataset_req_id, get_task_plot_results, send_email)
from app.tasks import get_prompt_result_task, data_processing_task, get_additional_analyses_prompt_result
from app.crud import (PromptTableOperation, UserTableOperation, TaskRunTableOperation, DATABASE_URL_SYNC, get_conn, 
                      get_prompt_table_ops, get_task_run_table_ops, get_user_table_ops, get_user_customized_tasks_table_ops, UserCustomizedTasksTableOperation)
from app.auth import create_access_token, get_current_user, generate_random_otp, verify_otp, ACCESS_TOKEN_EXPIRE_MINUTES
from app.schemas import UserRegisterSchema, DataTasks, ExecuteAnalysesSchema, AdditionalAnalysesRequestSchema, RunInfoSchema

from starlette.middleware.base import BaseHTTPMiddleware

from typing import Literal

from app.logger import logger

from celery import chain

import json

from sqlalchemy.exc import IntegrityError

from ast import literal_eval

import time

import sentry_sdk

from functools import wraps

from datetime import datetime, timezone, timedelta

from fastapi import FastAPI
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from apscheduler.schedulers.background import BackgroundScheduler
from contextlib import asynccontextmanager

import redis

import asyncio

import shutil











    

# sentry_sdk.init(
#     dsn="https://d3b5d645c124aaaf40eb877f11b8704a@o4510198724296704.ingest.us.sentry.io/4510198734520320",
#     send_default_pii=True,
#     enable_logs=True
# )

WARN_FOR_SLOW_RESPONSE_TIME = True
THRES_SLOW_RESPONSE_TIME_MS = 1000
DATASET_SAVE_PATH = 'app/datasets'
OTP_EXPIRE_MINUTES = 5

REDIS_HOST = '127.0.0.1'

REDIS_LAST_ACCESSED_HASHTABLE_NAME = 'req_id_last_accessed'

DEFAULT_PROMPT_VERSION = 3

PT1_PROMPT_TEMPLATE = 'app/prompts/split_prompt/prompt_part1.md'

RATE_LIMIT_GET_ENDPOINTS = '60/minute'
RATE_LIMIT_TASK_ENDPOINTS = '5/minute'
RATE_LIMIT_LOGIN = '5/5minute'
RATE_LIMIT_REGISTER = '2/hour'


class LogRequestMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.perf_counter()
        
        response = await call_next(request)
        
        end_time = time.perf_counter()
        process_time = end_time - start_time
        process_time_ms = round(process_time * 1000, 2)
        
        if response.status_code == 200:
            logger.debug(f"{request.method} {request.url} | Completed with status {response.status_code} in {process_time_ms} ms")
        elif response.status_code >= 400:
            logger.warning(f"CLIENT ERROR 4XX | {request.method} {request.url} | Headers: {dict(request.headers)} | Completed with status {response.status_code} in {process_time_ms} ms")
        elif response.status_code >= 500:
            logger.error(f"SERVER ERROR 5XX | {request.method} {request.url} | Headers: {dict(request.headers)} | Completed with status {response.status_code} in {process_time_ms} ms")
        
        slow_routes_exclude = ['/upload_dataset']
        if process_time_ms > THRES_SLOW_RESPONSE_TIME_MS and WARN_FOR_SLOW_RESPONSE_TIME and not any(i in str(request.url) for i in slow_routes_exclude):
            logger.warning(f'SLOW RESPONSE TIME | {request.method} {request.url} | Headers: {dict(request.headers)} | Completed with status {response.status_code} in {process_time_ms} ms')
        
        return response


def get_redis_client():
    conn_pool = redis.ConnectionPool(host=REDIS_HOST, port=6379, db=0)

    return redis.Redis(connection_pool=conn_pool)

# # runs every day at end of day
# def get_values_and_update_db(redis_client, db_engine):
#     temp_hashtable_name = f'{REDIS_LAST_ACCESSED_HASHTABLE_NAME}_temp'
#     redis_client.rename(REDIS_LAST_ACCESSED_HASHTABLE_NAME, temp_hashtable_name)
#     req_id_last_accessed_dct = redis_client.hgetall(temp_hashtable_name)

#     with db_engine.connect() as conn:
#         prompt_table_ops = PromptTableOperation(conn_sync=conn)
#         prompt_table_ops.update_last_accessed_column(req_id_last_accessed_dct)
    
#     redis_client.delete(temp_hashtable_name)

# # runs at first of each month
# def cleanup_unused_datasets(db_engine):
#     THRES_DELETE_UNUSED_DATASETS_DAYS = 30
    
#     with db_engine.connect() as conn:
#         prompt_table_ops = PromptTableOperation(conn_sync=conn)
#         res = prompt_table_ops.get_least_accessed_request_ids(THRES_DELETE_UNUSED_DATASETS_DAYS)
        
#     if res:
#         for req_id in res:
#             path_delete = f'{DATASET_SAVE_PATH}/{req_id}'
#             shutil.rmtree(path_delete)
#             logger.info(f'deleted {req_id} files on cleanup function')
    
    

def update_last_accessed_at_when_called(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        request_id = kwargs.get('request_id')
        redis_client = kwargs.get('redis_client')
        
        if not request_id or not redis_client:
            logger.warning(f'update_last_accessed_at decorator called on function "{func.__name__}" with no request_id or redis_client parameter')
            return await func(*args, **kwargs)
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        hashtable_last_accessed = REDIS_LAST_ACCESSED_HASHTABLE_NAME
        cooldown_key = f'{request_id}:last_write:{today}'
        
        logger.debug(f'cooldown key {cooldown_key}')
        
        if redis_client.set(cooldown_key, 'on_cooldown', ex=3600*24*2, nx=True):
            redis_client.hset(hashtable_last_accessed, request_id, today)
            
        return await func(*args, **kwargs)
    
    return wrapper


def do_task():
    print('doing task now..')

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = BackgroundScheduler()
    scheduler.add_job(do_task, 'interval', minutes=5)
    # scheduler.add_job(cleanup_unused_datasets,'cron',day='1',hour='0',minute='0',second='0', misfire_grace_time=2*24*3600) # run at 1st of each month
    scheduler.start()
    
    yield


limiter = Limiter(key_func=get_remote_address)
app = FastAPI(lifespan=lifespan)

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
    expose_headers=["*"]
)

@app.get('/')
@limiter.limit(RATE_LIMIT_GET_ENDPOINTS)
async def read_root(request: Request):
    return {'Hello': 'World'}

# to do:
# check last user request time from prompt_and_result db and put it in pending if still within time limit

@app.post('/upload_dataset')
@limiter.limit(RATE_LIMIT_TASK_ENDPOINTS)
async def upload(request: Request, file: UploadFile, model: str = Form(...), analysis_task_count: int = Form(...), send_result_to_email: bool = True,
                 current_user=Depends(get_current_user), prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops), 
                 user_cust_tasks_table_ops: UserCustomizedTasksTableOperation = Depends(get_user_customized_tasks_table_ops)):
    try:
        user_id = current_user.user_id
        user_email = current_user.email
        
        file_reader = CsvReader(upload_file=file)
        file_data = await run_in_threadpool(file_reader.get_dataframe_dict)
        dataset_dataframe = file_data['dataframe']
        dataset_filename = file_data['filename']
        dataset_columns_str = file_data['columns_str']
        dataset_granularity_map = file_data['granularity_map']
        dataset_id = file_data['dataset_id']
        
        data_processor = DatasetProcessorForPtOnePrompt(dataframe=dataset_dataframe, 
                                                    filename=dataset_filename, 
                                                    prompt_template_file=PT1_PROMPT_TEMPLATE, 
                                                    granularity_data=dataset_granularity_map)
        
        prompt_pt_1 = await run_in_threadpool(data_processor.create_prompt)
        
        request_id = await prompt_table_ops.add_task(user_id=user_id, prompt_version=DEFAULT_PROMPT_VERSION, filename=dataset_filename, 
                                                dataset_cols=dataset_columns_str, model=model)
        
        parquet_file = await run_in_threadpool(save_dataset_req_id, request_id=request_id, dataframe=dataset_dataframe, 
                                               save_type='original_dataset')
        
        run_info: RunInfoSchema = {'request_id': request_id, 'user_id': user_id, 'parquet_file': parquet_file, 
                                   'filename': dataset_filename, 'send_result_to_email': send_result_to_email, 'email': user_email}

        _ = chain(get_prompt_result_task.s(model=model, prompt_pt_1=prompt_pt_1, task_count=analysis_task_count, dataset_id=dataset_id,
                                        request_id=request_id, user_id=user_id, dataset_cols=literal_eval(dataset_columns_str)), 
                data_processing_task.s(run_info=run_info, run_type='first_run_after_request')
                ).apply_async()
        
        await user_cust_tasks_table_ops.add_request_id_to_table(user_id, request_id)
        
        logger.info(f'initial task request added: request_id {request_id}, user_id {user_id}')
        
        return {'detail': 'request task executed'}
        
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail=f"an error occurred")



@app.post('/execute_analyses')
@limiter.limit(RATE_LIMIT_TASK_ENDPOINTS)
async def execute_analyses(request: Request, execute_analyses_data: ExecuteAnalysesSchema, current_user=Depends(get_current_user), 
                           prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops)):
    user_id = current_user.user_id
    user_email = current_user.email
    request_id = execute_analyses_data.request_id
    send_result_to_email = execute_analyses_data.send_result_to_email
    parquet_file = f'app/datasets/{request_id}/original_dataset.parquet'
    
    dataset_filename = await prompt_table_ops.get_dataset_filename(request_id, user_id)

    task_still_in_initial_request_process = await is_task_invalid_or_still_processing(request_id=request_id, user_id=user_id, prompt_table_ops=prompt_table_ops)
    if task_still_in_initial_request_process:
        raise HTTPException(status_code=403, detail=f"this is an invalid task or you must run an initial analysis request first")
    
    run_info: RunInfoSchema = {'request_id': request_id, 'user_id': user_id, 'parquet_file': parquet_file, 
                                'filename': dataset_filename, 'send_result_to_email': send_result_to_email, 'email': user_email}
    
    data_tasks = execute_analyses_data.model_dump()
    del data_tasks['request_id']
    del data_tasks['send_result_to_email']
    
    data_processing_task.delay(data_tasks_dict=data_tasks, run_info=run_info, run_type='modified_tasks_execution')
    
    logger.info(f'modified task execution request added: request_id {request_id}, user_id {user_id}')
    
    return {'detail': 'analysis task executed'}

@app.post('/execute_analyses_with_new_dataset')
@limiter.limit(RATE_LIMIT_TASK_ENDPOINTS)
async def execute_analyses_with_new_dataset(request: Request, file: UploadFile, execute_analyses_data: str = Form(...), 
                                            current_user=Depends(get_current_user), task_run_table_ops: TaskRunTableOperation = Depends(get_task_run_table_ops), 
                                            prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops)):
    user_id = current_user.user_id
    user_email = current_user.email
    
    execute_analyses_data = ExecuteAnalysesSchema.model_validate_json(execute_analyses_data)
    
    request_id = execute_analyses_data.request_id
    send_result_to_email = execute_analyses_data.send_result_to_email
    parquet_file = f'app/datasets/{request_id}.parquet'

    task_still_in_initial_request_process = await is_task_invalid_or_still_processing(request_id=request_id, user_id=user_id, prompt_table_ops=prompt_table_ops)
    if task_still_in_initial_request_process:
        raise HTTPException(status_code=403, detail="this is an invalid task or you must run an initial analysis request first")
    
    file_reader = CsvReader(upload_file=file)
    file_data = await run_in_threadpool(file_reader.get_dataframe_dict)
    dataset_dataframe = file_data['dataframe']
    dataset_columns_str = file_data['columns_str']
    dataset_filename = file_data['filename']
    
    original_columns_str = await prompt_table_ops.get_dataset_columns_by_id(request_id=request_id, user_id=user_id)
    original_columns_str = original_columns_str
    
    if dataset_columns_str != original_columns_str:
        raise HTTPException(status_code=403, details='this dataset does not have the columns from the original dataset')
    
    parquet_file = await run_in_threadpool(save_dataset_req_id, request_id=request_id, dataframe=dataset_dataframe, 
                                           save_type='new_dataset')
    
    run_info: RunInfoSchema = {'request_id': request_id, 'user_id': user_id, 'parquet_file': parquet_file, 
                                'filename': dataset_filename, 'send_result_to_email': send_result_to_email, 'email': user_email}
    
    data_tasks = execute_analyses_data.model_dump()
    del data_tasks['request_id']
    del data_tasks['send_result_to_email']
    
    def remove_status_field_from_res(dct):
        lst = []
        
        for task in dct:
            task = {i: j for i, j in task.items() if i != 'status'}
            lst.append(task)
        return lst
    
    original_col_transforms = await task_run_table_ops.get_columns_transformations_by_id(user_id=user_id, request_id=request_id)
    original_col_transforms = json.loads(original_col_transforms['column_transforms_status'])['column_transforms']
    original_col_transforms = remove_status_field_from_res(original_col_transforms)
    
    original_col_combinations = await task_run_table_ops.get_columns_combinations_by_id(user_id=user_id, request_id=request_id)
    original_col_combinations = json.loads(original_col_combinations['column_combinations_status'])['column_combinations']
    original_col_combinations = remove_status_field_from_res(original_col_combinations)
    
    data_tasks['common_column_cleaning_or_transformation'] = original_col_transforms
    data_tasks['common_column_combination'] = original_col_combinations
    
    data_processing_task.delay(data_tasks_dict=data_tasks, run_info=run_info, run_type='modified_tasks_execution_with_new_dataset')
    
    logger.info(f'modified task execution request with new dataset added: request_id {request_id}, user_id {user_id}')
    
    return {'detail': 'analysis task executed'}  

@app.post('/make_additional_analyses_request')
@limiter.limit(RATE_LIMIT_TASK_ENDPOINTS)
async def make_additional_analyses_request(request: Request, additional_analyses_request_data: AdditionalAnalysesRequestSchema, 
                                           current_user=Depends(get_current_user), prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops)):
    
    user_id = current_user.user_id
    request_id = additional_analyses_request_data.request_id
    model = additional_analyses_request_data.model
    new_tasks_prompt = additional_analyses_request_data.new_tasks_prompt
    
    new_tasks_prompt = split_and_validate_new_prompt(new_tasks_prompt)
    
    if not new_tasks_prompt:
        raise HTTPException(status_code=403, detail=f"the new tasks request prompt does not meet the requirements")

    task_still_in_initial_request_process = await is_task_invalid_or_still_processing(request_id=request_id, user_id=user_id, prompt_table_ops=prompt_table_ops)
    if task_still_in_initial_request_process:
        raise HTTPException(status_code=403, detail=f"this is an invalid task or you must run an initial analysis request first")
    
    additional_analyses_prompt_res = await prompt_table_ops.get_additional_analyses_prompt_result(request_id, user_id)
    
    if additional_analyses_prompt_res is not None: # if user already run additional analyses on this req_id previously
        raise HTTPException(status_code=400, detail=f"can only execute one additional analyses request for one dataset")
    
    new_tasks_prompt = new_tasks_prompt.split('\n')

    parquet_file = f'app/datasets/{request_id}.parquet'
    
    run_info = {'request_id': request_id, 'user_id': user_id, 'parquet_file': parquet_file}
    
    _ = chain(get_additional_analyses_prompt_result.s(model=model, new_tasks_prompt=new_tasks_prompt, request_id=request_id, user_id=user_id),
              data_processing_task.s(run_info=run_info, run_type='additional_analyses_request')
              ).apply_async()
    
    logger.info(f'additional analyses request added: request_id {request_id}, user_id {user_id}')
    
    return {'detail': 'additional analyses request executed'}
    

@app.get('/get_redis_value/{key}')
async def get_redis_value(key: str, redis_client=Depends(get_redis_client)):
    res = redis_client.hget(REDIS_LAST_ACCESSED_HASHTABLE_NAME, '50cdaea5-2470-4b5f-a722-d82ef66d5c75')
    
    return {'res': res}
    
@app.get('/get_original_tasks_by_id/{request_id}')
@limiter.limit(RATE_LIMIT_GET_ENDPOINTS)
@update_last_accessed_at_when_called
async def get_original_tasks_by_id(request: Request, request_id: str, current_user=Depends(get_current_user), prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops), 
                                   task_run_table_ops: TaskRunTableOperation = Depends(get_task_run_table_ops)):
    user_id = current_user.user_id

    task_still_in_initial_request_process = await is_task_invalid_or_still_processing(request_id=request_id, user_id=user_id, prompt_table_ops=prompt_table_ops)
    if task_still_in_initial_request_process:
        raise HTTPException(status_code=403, detail=f"this is an invalid task or you must run an initial analysis request first")

    res = await task_run_table_ops.get_original_tasks_by_id(user_id, request_id)
    
    if not res:
        raise HTTPException(status_code=404, detail=f"cannot find the requested original tasks")
    
    plot_result = get_task_plot_results(request_id, run_type='first_run_after_request')

    return {'res': res, 'plot_result': plot_result}



@app.get('/get_modified_tasks_by_id/{request_id}')
@limiter.limit(RATE_LIMIT_GET_ENDPOINTS)
async def get_modified_tasks_by_id(request: Request, request_id: str, current_user=Depends(get_current_user), prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops), 
                                   task_run_table_ops: TaskRunTableOperation = Depends(get_task_run_table_ops)):
    user_id = current_user.user_id

    task_still_in_initial_request_process = await is_task_invalid_or_still_processing(request_id=request_id, user_id=user_id, prompt_table_ops=prompt_table_ops)
    if task_still_in_initial_request_process:
        raise HTTPException(status_code=403, detail=f"this is an invalid task or you must run an initial analysis request first")

    res = await task_run_table_ops.get_modified_tasks_by_id(user_id, request_id)
    
    if not res:
        raise HTTPException(status_code=404, detail=f"cannot find the requested modified tasks")
    
    plot_result = get_task_plot_results(request_id, run_type='modified_tasks_execution')

    return {'res': res, 'plot_result': plot_result}

@app.get('/get_col_info_by_id/{request_id}')
@limiter.limit(RATE_LIMIT_GET_ENDPOINTS)
async def get_col_info_by_id(request: Request, request_id: str, current_user=Depends(get_current_user), prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops), 
                             task_run_table_ops: TaskRunTableOperation = Depends(get_task_run_table_ops)):
    user_id = current_user.user_id

    task_still_in_initial_request_process = await is_task_invalid_or_still_processing(request_id=request_id, user_id=user_id, prompt_table_ops=prompt_table_ops)
    if task_still_in_initial_request_process:
        raise HTTPException(status_code=403, detail=f"this is an invalid task or you must run an initial analysis request first")

    res = await task_run_table_ops.get_columns_info_by_id(user_id, request_id)
    
    if not res:
        raise HTTPException(status_code=404, detail=f"cannot find the requested columns info")

    return res

@app.get('/get_dataset_snippet_by_id/{request_id}')
@limiter.limit(RATE_LIMIT_GET_ENDPOINTS)
async def get_dataset_snippet_by_id(request: Request, request_id: str, current_user=Depends(get_current_user), prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops), 
                                    task_run_table_ops: TaskRunTableOperation = Depends(get_task_run_table_ops)):
    user_id = current_user.user_id

    task_still_in_initial_request_process = await is_task_invalid_or_still_processing(request_id=request_id, user_id=user_id, prompt_table_ops=prompt_table_ops)
    if task_still_in_initial_request_process:
        raise HTTPException(status_code=403, detail=f"this is an invalid task or you must run an initial analysis request first")
    
    res = await task_run_table_ops.get_dataset_snippet_by_id(user_id, request_id)

    if not res:
        raise HTTPException(status_code=404, detail=f"cannot find the requested dataset snippet")
    
    return res



@app.get('/get_request_ids')
@limiter.limit(RATE_LIMIT_GET_ENDPOINTS)
async def get_request_ids(request: Request, current_user=Depends(get_current_user), prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops)):
    user_id = current_user.user_id
    res = await prompt_table_ops.get_request_ids_by_user(user_id)
    
    if not res:
        raise HTTPException(status_code=404, detail=f"not authenticated or cannot find any request ids")
    
    return {'request_ids': res}

class CommonUserCustomizedTasksSchema(BaseModel):
    request_id: str
    slot: int = Literal[1, 2, 3]
    tasks: dict = {}
    operation: Literal['fetch', 'delete', 'update', 'check_if_empty']

@app.post('/manage_user_cust_tasks')
async def manage_user_customized_tasks(user_cust_tasks_schema: CommonUserCustomizedTasksSchema, current_user=Depends(get_current_user), 
                                       user_cust_tasks_table_ops: UserCustomizedTasksTableOperation = Depends(get_user_customized_tasks_table_ops)):
    user_id = current_user.user_id
    
    request_id = user_cust_tasks_schema.request_id
    operation = user_cust_tasks_schema.operation
    slot = user_cust_tasks_schema.slot
    tasks = user_cust_tasks_schema.tasks
    
    customized_tasks_key = 'customized_tasks'
    
    
    if operation == 'fetch':
        res = await user_cust_tasks_table_ops.fetch_customized_tasks(user_id, request_id, slot)
        return {'res': res}
    
    elif operation == 'check_if_empty':
        res = await user_cust_tasks_table_ops.check_if_customized_tasks_empty(user_id, request_id)
        return {'res': res}
    
    elif operation == 'delete':
        await user_cust_tasks_table_ops.delete_customized_tasks(user_id, request_id, slot)
        return {'detail': 'delete customized tasks operation successful'}
    
    elif operation == 'update':
        if not customized_tasks_key in tasks:
            return HTTPException(status_code=400, detail='tasks cant be empty for update operation')
        tasks = json.dumps(tasks)
        await user_cust_tasks_table_ops.update_customized_tasks(user_id, request_id, slot, tasks)
        return {'detail': 'update customized tasks operation successful'}
        

    

class SetImportedTasksSchema(BaseModel):
    request_id: str
    task_ids: list

@app.post('/set_imported_task_ids')
async def set_imported_task_ids(set_imported_tasks_schema: SetImportedTasksSchema, current_user=Depends(get_current_user), 
                                user_cust_tasks_table_ops: UserCustomizedTasksTableOperation = Depends(get_user_customized_tasks_table_ops)):
    user_id = current_user.user_id
    
    request_id = set_imported_tasks_schema.request_id
    task_ids = set_imported_tasks_schema.task_ids
    
    await user_cust_tasks_table_ops.set_imported_tasks(user_id, request_id, task_ids)
    
    return {'detail': 'set imported tasks operation successful'}

@app.get('/fetch_imported_task_ids/{request_id}')
async def fetch_imported_task_ids(request_id: str, current_user=Depends(get_current_user),
                                  user_cust_tasks_table_ops: UserCustomizedTasksTableOperation = Depends(get_user_customized_tasks_table_ops)):
    user_id = current_user.user_id
    
    res = await user_cust_tasks_table_ops.fetch_imported_tasks(user_id, request_id)
    
    return {'imported_task_ids': res}
    
        
    

################### USER ROUTES ###################

class GetOTPSchema(BaseModel):
    username: str

@app.post('/get_otp')
async def get_otp(get_otp_data: GetOTPSchema, background_tasks = Depends(get_background_tasks), user_table_ops: UserTableOperation = Depends(get_user_table_ops)):
    username = get_otp_data.username
    user = await user_table_ops.get_user(username)
    
    if not user:
        logger.warning(f'user {username} does not exist in db and tried to log in')
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials.")
    
    if user['last_otp_request'] is not None:
        last_otp_request = datetime.strptime(user['last_otp_request'], "%Y-%m-%d %H:%M:%S.%f%z")
        
        if datetime.now(timezone.utc) < last_otp_request + timedelta(minutes=1): # if one minute has not passed since the last otp request
            raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="You need to wait one minute before requesting another OTP")
    
    raw_otp, encrypted_otp = generate_random_otp()
    otp_expire = datetime.now(timezone.utc) + timedelta(minutes=OTP_EXPIRE_MINUTES)
    
    await user_table_ops.update_otp(username=username, otp=encrypted_otp, otp_expire=otp_expire)
    
    recipients = [user['email']]
    subject = 'OTP for Data Analysis Assistant app'
    body = f'Your OTP is {raw_otp}'
    
    background_tasks.add_task(send_email, subject, recipients, body)
    
    return {'detail': 'otp has been sent'}
    

class LoginSchema(BaseModel):
    username: str
    otp: str

@app.post('/login')
@limiter.limit(RATE_LIMIT_LOGIN)
async def login(request: Request, login_data: LoginSchema, user_table_ops: UserTableOperation = Depends(get_user_table_ops)):
    user = await user_table_ops.get_user(login_data.username)
    user_otp = user['otp']
    username = user['username']

    otp_expire = datetime.strptime(user['otp_expire'], "%Y-%m-%d %H:%M:%S.%f%z")

    if not user or not verify_otp(login_data.otp, user_otp):
        logger.warning(f'user {login_data.username} failed to log in')
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username")
    
    if datetime.now(timezone.utc) > otp_expire:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Expired OTP. Please generate a new one.")
    
    access_token = create_access_token(data={"sub": username}, 
                                       expire_minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    
    logger.info(f"user {username} logged in")
    
    # generate new otp to invalidate previous otp
    _, encrypted_otp = generate_random_otp()
    otp_expire = datetime.now(timezone.utc) + timedelta(minutes=OTP_EXPIRE_MINUTES)
    
    await user_table_ops.update_otp(username=username, otp=encrypted_otp, otp_expire=otp_expire)
    
    return {'access_token': access_token, 'token_type': 'bearer'}



@app.post('/register_user')
@limiter.limit(RATE_LIMIT_REGISTER)
async def register_user(request: Request, user_register_data: UserRegisterSchema, user_table_ops: UserTableOperation = Depends(get_user_table_ops)):
    try:
        username = user_register_data.username
        email = user_register_data.email
        first_name = user_register_data.first_name
        last_name = user_register_data.last_name
        
        await user_table_ops.create_user(username=username, email=email, first_name=first_name, 
                                         last_name=last_name)
        
        logger.info(f'account {username} successfully created')
        
        return {'detail': f'account {username} successfully created'}

    except IntegrityError:
        logger.warning(f'{username}/{email} failed to register because of conflicting username/email.')
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f'username {username} or email {email} already exists.'
        )
        
    except Exception as e:
        logger.exception(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="there is an internal error."
        )
        
        

        

    


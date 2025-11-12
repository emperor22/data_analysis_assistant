from fastapi import FastAPI, UploadFile, HTTPException, status, Depends, Form, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer
from starlette.concurrency import run_in_threadpool
from pydantic import BaseModel


from app.services import CsvReader, DatasetProcessorForPrompt, is_task_invalid_or_still_processing, get_background_tasks, split_and_validate_new_prompt
from app.tasks import get_prompt_result_task, data_processing_task, get_additional_analyses_prompt_result
from app.crud import PromptTableOperation, UserTableOperation, TaskRunTableOperation, DATABASE_URL_SYNC, get_conn, get_prompt_table_ops, get_task_run_table_ops, get_user_table_ops
from app.auth import create_access_token, get_current_user, generate_random_otp, ACCESS_TOKEN_EXPIRE_MINUTES, send_email
from app.schemas import UserRegisterSchema, DataTasks, ExecuteAnalysesSchema, AdditionalAnalysesRequestSchema, RunInfoSchema

from starlette.middleware.base import BaseHTTPMiddleware

from app.logger import logger

from celery import chain

from sqlalchemy.exc import IntegrityError

from ast import literal_eval

import time

import sentry_sdk

from datetime import datetime, timezone, timedelta

# sentry_sdk.init(
#     dsn="https://d3b5d645c124aaaf40eb877f11b8704a@o4510198724296704.ingest.us.sentry.io/4510198734520320",
#     send_default_pii=True,
#     enable_logs=True
# )

WARN_FOR_SLOW_RESPONSE_TIME = True
THRES_SLOW_RESPONSE_TIME_MS = 1000
DATASET_SAVE_PATH = 'app/datasets'
OTP_EXPIRE_MINUTES = 5

DEFAULT_PROMPT_VERSION = 3

PT1_PROMPT_TEMPLATE = 'app/prompts/split_prompt/prompt_part1.md'


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

app = FastAPI()

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
async def read_root():
    return {'Hello': 'World'}

# to do:
# check last user request time from prompt_and_result db and put it in pending if still within time limit

@app.post('/upload_dataset')
async def upload(file: UploadFile, model: str = Form(...), analysis_task_count: int = Form(...), current_user=Depends(get_current_user), 
                 prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops)):
    try:
    
        # these values needs to be obtained from user config
        user_id = current_user.user_id
        
        file_reader = CsvReader(upload_file=file)
        file_data = await run_in_threadpool(file_reader.get_dataframe_dict)
        dataset_dataframe = file_data['dataframe']
        dataset_filename = file_data['filename']
        dataset_columns_str = file_data['columns_str']
        
        data_processor = DatasetProcessorForPrompt(dataframe=dataset_dataframe, 
                                                    filename=dataset_filename, 
                                                    prompt_template_file=PT1_PROMPT_TEMPLATE)
        
        prompt_pt_1 = await run_in_threadpool(data_processor.create_prompt)
        
        request_id = await prompt_table_ops.add_task(user_id=user_id, prompt_version=DEFAULT_PROMPT_VERSION, filename=dataset_filename, 
                                                dataset_cols=dataset_columns_str, model=model)
        
        parquet_file = f'{DATASET_SAVE_PATH}/{request_id}.parquet'
        await run_in_threadpool(dataset_dataframe.to_parquet, path=parquet_file, index=False)
        
        run_info: RunInfoSchema = {'request_id': request_id, 'user_id': user_id, 'parquet_file': parquet_file}

        _ = chain(get_prompt_result_task.s(model=model, prompt_pt_1=prompt_pt_1, task_count=analysis_task_count, 
                                        request_id=request_id, user_id=user_id, dataset_cols=literal_eval(dataset_columns_str)), 
                data_processing_task.s(run_info=run_info, run_type='first_run_after_request')
                ).apply_async()
        
        logger.info(f'initial task request added: request_id {request_id}, user_id {user_id}')
        
        return {'detail': 'request task executed'}
        
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail=f"an error occurred")



@app.post('/execute_analyses')
async def execute_analyses(execute_analyses_data: ExecuteAnalysesSchema, current_user=Depends(get_current_user), 
                           prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops)):
    user_id = current_user.user_id
    request_id = execute_analyses_data.request_id
    parquet_file = f'app/datasets/{request_id}.parquet'

    task_still_in_initial_request_process = await is_task_invalid_or_still_processing(request_id=request_id, user_id=user_id, prompt_table_ops=prompt_table_ops)
    if task_still_in_initial_request_process:
        raise HTTPException(status_code=403, detail=f"this is an invalid task or you must run an initial analysis request first")
    
    run_info = {'request_id': request_id, 'user_id': user_id, 'parquet_file': parquet_file}
    
    data_tasks = execute_analyses_data.model_dump()
    del data_tasks['request_id']
    
    data_processing_task.delay(data_tasks=data_tasks, run_info=run_info, run_type='modified_tasks_execution')
    
    logger.info(f'modified task execution request added: request_id {request_id}, user_id {user_id}')
    
    return {'detail': 'analysis task executed'}
    

@app.post('/make_additional_analyses_request')
async def make_additional_analyses_request(additional_analyses_request_data: AdditionalAnalysesRequestSchema, current_user=Depends(get_current_user), 
                                           prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops)):
    
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
    


@app.get('/get_original_tasks_by_id/{request_id}')
async def get_original_tasks_by_id(request_id: str, current_user=Depends(get_current_user), prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops), 
                                   task_run_table_ops: TaskRunTableOperation = Depends(get_task_run_table_ops)):
    user_id = current_user.user_id

    task_still_in_initial_request_process = await is_task_invalid_or_still_processing(request_id=request_id, user_id=user_id, prompt_table_ops=prompt_table_ops)
    if task_still_in_initial_request_process:
        raise HTTPException(status_code=403, detail=f"this is an invalid task or you must run an initial analysis request first")

    res = await task_run_table_ops.get_original_tasks_by_id(user_id, request_id)

    if not res:
        raise HTTPException(status_code=404, detail=f"cannot find the requested original tasks")

    return res



@app.get('/get_modified_tasks_by_id/{request_id}')
async def get_modified_tasks_by_id(request_id: str, current_user=Depends(get_current_user), prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops), 
                                   task_run_table_ops: TaskRunTableOperation = Depends(get_task_run_table_ops)):
    user_id = current_user.user_id

    task_still_in_initial_request_process = await is_task_invalid_or_still_processing(request_id=request_id, user_id=user_id, prompt_table_ops=prompt_table_ops)
    if task_still_in_initial_request_process:
        raise HTTPException(status_code=403, detail=f"this is an invalid task or you must run an initial analysis request first")

    res = await task_run_table_ops.get_modified_tasks_by_id(user_id, request_id)
    
    if not res:
        raise HTTPException(status_code=404, detail=f"cannot find the requested modified tasks")

    return res

@app.get('/get_col_info_by_id/{request_id}')
async def get_col_info_by_id(request_id: str, current_user=Depends(get_current_user), prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops), 
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
async def get_dataset_snippet_by_id(request_id: str, current_user=Depends(get_current_user), prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops), 
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
async def get_request_ids(current_user=Depends(get_current_user), prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops)):
    user_id = current_user.user_id
    res = await prompt_table_ops.get_request_ids_by_user(user_id)
    
    if not res:
        raise HTTPException(status_code=404, detail=f"not authenticated or cannot find any request ids")
    
    return {'request_ids': res}

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
    
    otp = generate_random_otp()
    otp_expire = datetime.now(timezone.utc) + timedelta(minutes=OTP_EXPIRE_MINUTES)
    
    await user_table_ops.update_otp(username=username, otp=otp, otp_expire=otp_expire)
    
    recipients = [user['email']]
    subject = 'OTP for Data Analysis Assistant app'
    body = f'Your OTP is {otp}'
    
    background_tasks.add_task(send_email, subject, recipients, body)
    
    return {'detail': 'otp has been sent'}
    

class LoginSchema(BaseModel):
    username: str
    otp: str

@app.post('/login')
async def login(login_data: LoginSchema, user_table_ops: UserTableOperation = Depends(get_user_table_ops)):
    user = await user_table_ops.get_user(login_data.username)
    user_otp = user['otp']
    username = user['username']

    otp_expire = datetime.strptime(user['otp_expire'], "%Y-%m-%d %H:%M:%S.%f%z")

    
    if not user or not login_data.otp == user_otp:
        logger.warning(f'user {login_data.username} failed to log in')
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username")
    
    if datetime.now(timezone.utc) > otp_expire:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Expired OTP. Please generate a new one.")
    
    access_token = create_access_token(data={"sub": username}, 
                                       expire_minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    
    logger.info(f"user {username} logged in")
    
    # generate new otp to invalidate previous otp
    otp = generate_random_otp()
    otp_expire = datetime.now(timezone.utc) + timedelta(minutes=OTP_EXPIRE_MINUTES)
    
    await user_table_ops.update_otp(username=username, otp=otp, otp_expire=otp_expire)
    
    return {'access_token': access_token, 'token_type': 'bearer'}



@app.post('/register_user')
async def register_user(user_register_data: UserRegisterSchema, user_table_ops: UserTableOperation = Depends(get_user_table_ops)):
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
        

    


from fastapi import FastAPI, UploadFile, HTTPException, status, Depends, Form, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer
from starlette.concurrency import run_in_threadpool
from pydantic import BaseModel

from app.services import CsvReader, DatasetProcessorForPrompt, is_task_invalid_or_still_processing
from app.tasks import get_prompt_result_task, data_processing_task, get_additional_analyses_prompt_result
from app.crud import PromptTableOperation, UserTableOperation, TaskRunTableOperation, DATABASE_URL_SYNC, get_conn, get_prompt_table_ops, get_task_run_table_ops, get_user_table_ops
from app.auth import create_access_token, get_current_user, generate_random_otp, ACCESS_TOKEN_EXPIRE_MINUTES
from app.schemas import UserRegisterModel, DataTasks

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

OTP_EXPIRE_MINUTES = 5


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
    # try:
    
    # these values needs to be obtained from user config
    user_id = current_user.user_id
    prompt_version = 3  
    
    file_reader = CsvReader(file)
    file_data = await run_in_threadpool(file_reader.get_dataframe_dict)
    dataset_dataframe = file_data['dataframe']
    dataset_filename = file_data['filename']
    dataset_columns_str = file_data['columns_str']
    
    data_processor = DatasetProcessorForPrompt(dataframe=dataset_dataframe, 
                                                filename=dataset_filename, 
                                                prompt_template_file=f'app/prompts/split_prompt/prompt_part1.md')
    
    prompt_pt_1 = await run_in_threadpool(data_processor.create_prompt)
    
    request_id = await prompt_table_ops.add_task(user_id=user_id, prompt_version=prompt_version, filename=dataset_filename, 
                                            dataset_cols=dataset_columns_str, model=model)

    parquet_file = f'app/datasets/{request_id}.parquet'
    dataset_dataframe.to_parquet(parquet_file, index=False)
    
    run_info = {'request_id': request_id, 'user_id': user_id, 'parquet_file': parquet_file}
    
    _ = chain(get_prompt_result_task.s(model, prompt_pt_1, analysis_task_count, request_id, user_id, literal_eval(dataset_columns_str)), # accepts prompt, request_id, cols list, db_url
            data_processing_task.s(run_info, 'first_run_after_request') # accepts data_tasks (from prev task), run_info dct, run_type
            ).apply_async()
    
    logger.info(f'initial task request added: request_id {request_id}, user_id {user_id}')
    
    return {'detail': 'request task executed'}
        
    # except Exception as e:
    #     return {'detail': e.args}



@app.post('/execute_analyses')
async def execute_analyses(data_tasks: DataTasks, request_id: str, current_user=Depends(get_current_user), 
                           prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops)):
    user_id = current_user.user_id
    parquet_file = f'app/datasets/{request_id}.parquet'
    
    # check if this task was run by the user requesting the task
    task_still_in_initial_request_process = await is_task_invalid_or_still_processing(request_id=request_id, user_id=user_id, prompt_table_ops=prompt_table_ops)
    if task_still_in_initial_request_process:
        raise HTTPException(status_code=403, detail=f"this is an invalid task or you must run an initial analysis request first")
    
    run_info = {'request_id': request_id, 'user_id': user_id, 'parquet_file': parquet_file}
    data_tasks = data_tasks.model_dump()
    data_processing_task.delay(data_tasks, run_info, 'modified_tasks_execution') # accepts data_tasks, run_info dct, run_type
    
    logger.info(f'modified task execution request added: request_id {request_id}, user_id {user_id}')
    
    return {'detail': 'analysis task executed'}
    

@app.post('/make_additional_analyses_request')
async def make_additional_analyses_request(model: str = Form(...), new_tasks_prompt: str = Form(...), request_id: str = Form(...), 
                                           current_user=Depends(get_current_user), prompt_table_ops: PromptTableOperation = Depends(get_prompt_table_ops)):
    
    user_id = current_user.user_id
    
    task_still_in_initial_request_process = await is_task_invalid_or_still_processing(request_id=request_id, user_id=user_id, prompt_table_ops=prompt_table_ops)
    if task_still_in_initial_request_process:
        raise HTTPException(status_code=403, detail=f"this is an invalid task or you must run an initial analysis request first")
    
    additional_analyses_prompt_res = await prompt_table_ops.get_additional_analyses_prompt_result(request_id, user_id)
    
    if additional_analyses_prompt_res is not None: # if user already run additional analyses on this req_id previously
        raise HTTPException(status_code=400, detail=f"can only execute one additional analyses request for one dataset")
    
    new_tasks_prompt = new_tasks_prompt.split('\n')

    parquet_file = f'app/datasets/{request_id}.parquet'
    
    run_info = {'request_id': request_id, 'user_id': user_id, 'parquet_file': parquet_file}
    
    _ = chain(get_additional_analyses_prompt_result.s(model, new_tasks_prompt, request_id, user_id), # accepts prompt, request_id, cols list, db_url
              data_processing_task.s(run_info, 'additional_analyses_request') # accepts data_tasks (from prev task), run_info dct, run_request
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


from fastapi import FastAPI, BackgroundTasks
from fastapi_mail import FastMail, MessageSchema, ConnectionConfig, MessageType
from pydantic import EmailStr, BaseModel
from typing import List
import os 

conf = ConnectionConfig(
    MAIL_USERNAME='daaotpsender@gmail.com',
    MAIL_PASSWORD='rfww zlil bhbl emjz',
    MAIL_FROM='daaotpsender@gmail.com',
    MAIL_PORT=587,
    MAIL_SERVER='smtp.gmail.com',
    MAIL_STARTTLS=True,
    MAIL_SSL_TLS=False,
    USE_CREDENTIALS=True,
    VALIDATE_CERTS=True,
)

async def send_email(subject: str, recipients: list, body: str):
    message = MessageSchema(
        subject=subject,
        recipients=recipients,
        body=body,
        subtype=MessageType.plain
    )
    
    fm = FastMail(conf)
    
    await fm.send_message(message)
    
class LoginInfo(BaseModel):
    username: str
    otp: str

@app.post('/get_otp')
async def get_otp(username: str, background_tasks: BackgroundTasks, user_table_ops: UserTableOperation = Depends(get_user_table_ops)):

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
    



@app.post('/token')
async def login(login_info: LoginInfo, user_table_ops: UserTableOperation = Depends(get_user_table_ops)):
    user = await user_table_ops.get_user(login_info.username)
    user_otp = user['otp']

    otp_expire = datetime.strptime(user['otp_expire'], "%Y-%m-%d %H:%M:%S.%f%z")

    
    if not user or not login_info.otp == user_otp:
        logger.warning(f'user {login_info.username} failed to log in')
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username")
    
    if datetime.now(timezone.utc) > otp_expire:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Expired OTP. Please generate a new one.")
    
    access_token = create_access_token(data={"sub": user.username}, 
                                       expire_minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    
    logger.info(f'user {user.username} logged in')
    
    # generate new otp to invalidate previous otp
    otp = generate_random_otp()
    otp_expire = datetime.now(timezone.utc) + timedelta(minutes=OTP_EXPIRE_MINUTES)
    
    await user_table_ops.update_otp(username=user.username, otp=otp, otp_expire=otp_expire)
    
    return {'access_token': access_token, 'token_type': 'bearer'}



@app.post('/register_user')
async def register_user(reg_model: UserRegisterModel, user_table_ops: UserTableOperation = Depends(get_user_table_ops)):
    try:
        await user_table_ops.create_user(username=reg_model.username, email=reg_model.email, first_name=reg_model.first_name, 
                                         last_name=reg_model.last_name)
        
        logger.info(f'account {reg_model.username} successfully created')
        
        return {'detail': f'account {reg_model.username} successfully created'}

    except IntegrityError as e:
        print(e.args)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f'username {reg_model.username} or email {reg_model.email} already exists.'
        )
        
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="there is an internal error."
        )
        

    


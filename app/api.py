from fastapi import FastAPI, UploadFile, HTTPException, status, Depends, Form
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer
from starlette.concurrency import run_in_threadpool
from pydantic import BaseModel

from app.services import CsvReader, DatasetProcessorForPrompt
from app.tasks import get_prompt_result_task, data_processing_task, get_additional_analyses_prompt_result
from app.crud import PromptTableOperation, UserTableOperation, TaskRunTableOperation, DATABASE_URL_SYNC, get_conn
from app.auth import create_access_token, get_current_user, get_hashed_password, verify_password, ACCESS_TOKEN_EXPIRE_MINUTES
from app.schemas import UserRegisterModel, ChangePasswordModel, DataTasks

from celery import chain

from sqlalchemy.exc import IntegrityError

from ast import literal_eval

app = FastAPI()

@app.get('/')
async def read_root():
    return {'Hello': 'World'}

# to do:
# check last user request time from prompt_and_result db and put it in pending if still within time limit

@app.post('/upload_dataset')
async def upload(file: UploadFile, model: str = Form(...), analysis_task_count: int = Form(...), conn=Depends(get_conn), current_user=Depends(get_current_user)):
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
        
    prompt_table_ops = PromptTableOperation(conn=conn)
    request_id = await prompt_table_ops.add_task(user_id=user_id, prompt_version=prompt_version, filename=dataset_filename, 
                                            dataset_cols=dataset_columns_str, model=model)
    
    parquet_file = f'app/datasets/{request_id}.parquet'
    dataset_dataframe.to_parquet(parquet_file, index=False)
    
    run_info = {'request_id': request_id, 'user_id': user_id, 'parquet_file': parquet_file}
    
    _ = chain(get_prompt_result_task.s(model, prompt_pt_1, analysis_task_count, request_id, literal_eval(dataset_columns_str)), # accepts prompt, request_id, cols list, db_url
            data_processing_task.s(run_info, 'first_run_after_request') # accepts data_tasks (from prev task), run_info dct, db_url, first_run
            ).apply_async()
    
    return {'detail': 'request task executed'}
        
    # except Exception as e:
    #     return {'detail': e.args}
    
@app.post('/execute_analyses')
async def execute_analyses(data_tasks: DataTasks, request_id: int, current_user=Depends(get_current_user), 
                           conn=Depends(get_conn)):
    user_id = current_user.user_id
    parquet_file = f'app/datasets/{request_id}.parquet'
    
    # check if this task was run by the user requesting the task
    task_run_table_ops = TaskRunTableOperation(conn)
    res = await task_run_table_ops.get_task_by_id(user_id, request_id)
    
    if not res:
        return {'detail': 'the request id is not associated with the user id'}
    
    
    run_info = {'request_id': request_id, 'user_id': user_id, 'parquet_file': parquet_file}
    data_tasks = data_tasks.model_dump()
    data_processing_task.delay(data_tasks, run_info, False) # accepts data_tasks, run_info dct, db_url, first_run
    
    return {'detail': 'analysis task executed'}

@app.post('/make_additional_analyses_request')
async def make_additional_analyses_request(model: str = Form(...), new_tasks_prompt: str = Form(...), request_id: int = Form(...), 
                                           conn=Depends(get_conn), current_user=Depends(get_current_user)):
    
    
    additional_analyses_task_count = 5
    user_id = current_user.user_id
    
    prompt_table_ops = PromptTableOperation(conn)
    additional_analyses_prompt_res = await prompt_table_ops.get_additional_analyses_prompt_result(request_id, user_id)
    first_req_prompt_res = await prompt_table_ops.get_prompt_result(request_id, user_id)
    
    if not first_req_prompt_res or not first_req_prompt_res['prompt_result']:
        return {'detail': 'this request is allowed only after the first analysis tasks request'}
    
    if additional_analyses_prompt_res:
        res = additional_analyses_prompt_res['additional_analyses_prompt_result']

        if res is not None:
            return {'detail': 'can only execute one additional analyses request for one dataset'}
    
    new_tasks_prompt = new_tasks_prompt.split('\n')

    parquet_file = f'app/datasets/{request_id}.parquet'
    run_info = {'request_id': request_id, 'user_id': user_id, 'parquet_file': parquet_file}
    
    _ = chain(get_additional_analyses_prompt_result.s(model, additional_analyses_task_count, new_tasks_prompt, request_id, user_id), # accepts prompt, request_id, cols list, db_url
              data_processing_task.s(run_info, 'additional_analyses_request') # accepts data_tasks (from prev task), run_info dct, db_url, first_run
              ).apply_async()
    
    return {'detail': 'additional analyses request executed'}
    


@app.get('/get_task_by_id/{request_id}')
async def get_task_by_id(request_id: str, conn=Depends(get_conn), current_user=Depends(get_current_user)):
    user_id = current_user.user_id
    request_id = int(request_id)
    
    task_run_table_ops = TaskRunTableOperation(conn)
    res = await task_run_table_ops.get_task_by_id(user_id, request_id)

    if res:
        return res
    else:
        return {'detail': 'cannot find the requested task'}

################### USER ROUTES ###################

@app.post('/token')
async def login(form_data=Depends(OAuth2PasswordRequestForm), conn=Depends(get_conn)):
    user_table_ops = UserTableOperation(conn)
    user = await user_table_ops.get_user(form_data.username)
    password = await user_table_ops.get_password_by_username(form_data.username)
    
    if not user or not verify_password(form_data.password, password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token = create_access_token(data={"sub": user.username}, 
                                       expire_minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    return {'access_token': access_token, 'token_type': 'bearer'}



@app.post('/register_user')
async def register_user(reg_model: UserRegisterModel, conn=Depends(get_conn)):
    try:

        hashed_pw = get_hashed_password(reg_model.password)
    
        user_table_ops = UserTableOperation(conn=conn)
        await user_table_ops.create_user(username=reg_model.username, email=reg_model.email, first_name=reg_model.first_name, 
                                         last_name=reg_model.last_name, hashed_password=hashed_pw)
            
        return {'detail': f'account {reg_model.username} successfully created'}

    except IntegrityError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f'username {reg_model.username} or email {reg_model.email} already exists.'
        )
        
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="there is an internal error."
        )
        

    


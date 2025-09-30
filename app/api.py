from fastapi import FastAPI, UploadFile, HTTPException, status, Depends
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer
from starlette.concurrency import run_in_threadpool
from pydantic import BaseModel

from app.services import CsvReader, DatasetProcessorForPrompt
from app.tasks import get_prompt_result_task, data_processing_task
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

@app.post('/upload_dataset')
async def upload(file: UploadFile, conn=Depends(get_conn), current_user=Depends(get_current_user)):
    # to do:
    # check last user request time from prompt_and_result db and put it in pending if still within time limit
    
    # try:
    
    user_id = current_user.user_id
    prompt_version = 3
    model = 'gemma-3-27b-it'  
    
    
    file_reader = CsvReader(file)
    file_data = await run_in_threadpool(file_reader.get_dataframe_dict)
    dataset_dataframe = file_data['dataframe']
    dataset_filename = file_data['filename']
    dataset_columns_str = file_data['columns_str']
    
    data_processor = DatasetProcessorForPrompt(dataset_dataframe, dataset_filename, f'app/prompts/prompt{prompt_version}.md')
    final_prompt = await run_in_threadpool(data_processor.create_prompt)
    
    with open('final_prompt.txt', 'w') as f:
        f.write(final_prompt)
    
    # these values needs to be obtained from user config
      
    
    prompt_table_ops = PromptTableOperation(conn=conn)
    req_id = await prompt_table_ops.add_task(user_id=user_id, prompt_version=prompt_version, filename=dataset_filename, 
                                             dataset_cols=dataset_columns_str, model=model)
    
    parquet_file = f'app/datasets/{req_id}.parquet'
    dataset_dataframe.to_parquet(parquet_file, index=False)
    
    run_info = {'request_id': req_id, 'user_id': user_id, 'parquet_file': parquet_file}
    
    _ = chain(get_prompt_result_task.s(model, final_prompt, req_id, literal_eval(dataset_columns_str)), # accepts prompt, req_id, cols list, db_url # type: ignore
              data_processing_task.s(run_info, False, True) # accepts data_tasks (from prev task), run_info dct, db_url, common_tasks_only, first_run
              ).apply_async()
    
    return {'detail': 'request task executed'}
            
    # except Exception as e:
    #     return {'detail': e.args}
    
@app.post('/execute_analyses')
async def execute_analyses(data_tasks: DataTasks, req_id: int, current_user=Depends(get_current_user), 
                           conn=Depends(get_conn)):
    user_id = current_user.user_id
    parquet_file = f'app/datasets/{req_id}.parquet'
    
    run_info = {'request_id': req_id, 'user_id': user_id, 'parquet_file': parquet_file}
    data_tasks = data_tasks.model_dump()
    data_processing_task.delay(data_tasks, run_info, True, False) # accepts data_tasks, run_info dct, db_url, common_tasks_only, first_run
    
    return {'detail': 'analysis task executed'}
    
    
    

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
        

    


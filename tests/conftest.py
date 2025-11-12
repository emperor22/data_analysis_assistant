import pytest
import pytest_asyncio
from app.crud import (Base, UserTableOperation, PromptTableOperation, TaskRunTableOperation, 
                      get_prompt_table_ops, get_user_table_ops, get_task_run_table_ops, get_current_time_utc)
from app.schemas import DatasetAnalysisModelPartOne, DatasetAnalysisModelPartTwo, DataTasks

from app.auth import get_current_user
from app.services import get_background_tasks
from app.api import app
from app import tasks, api
from app.data_transform_utils import clean_dataset
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
import uuid
import os

import pandas as pd

from unittest.mock import MagicMock

from datetime import datetime, timezone

from fastapi import FastAPI

from celery import Celery

import json

from ast import literal_eval

from fastapi.testclient import TestClient
from httpx import AsyncClient, ASGITransport

TEST_DB_URL_ASYNC = 'sqlite+aiosqlite:///./testing_xx.sqlite'
TEST_DB_URL_SYNC = 'sqlite:///./testing_xx.sqlite'

TEST_UUID = 'xxxx-xxxx-xxxx-xxxx'

TEST_DEFAULT_MODEL = 'gemini-2.5-flash'
TEST_DEFAULT_DATASET_FILE = 'tests/test_files/1_netflix-rotten-tomatoes-metacritic-imdb.csv'
TEST_DEFAULT_CLEAN_DATASET_FILE = 'tests/test_files/clean_dataset.parquet'
TEST_DEFAULT_TASK_COUNT = 10
TEST_PROMPT_VERSION = 3

TEST_DEFAULT_OTP = '000000'

@pytest.fixture(scope='session')
def clean_dataset_df():
    filename = TEST_DEFAULT_CLEAN_DATASET_FILE
    df = pd.read_parquet(filename)
    
    return df

@pytest.fixture(scope='session')
def dataset_cols(clean_dataset_df):
    dataset_cols = str(sorted(clean_dataset_df.columns.tolist()))
    
    return dataset_cols

@pytest.fixture(scope='session')
def get_prompt_result_data(dataset_cols):
    pt1_resp_file = 'tests/test_files/resp_pt1.json'
        
    pt2_resp_file = 'tests/test_files/resp_pt2.json'
        
    data = {'resp_pt1_file': pt1_resp_file, 'resp_pt2_file': pt2_resp_file, 
            'model': TEST_DEFAULT_MODEL, 'task_count': TEST_DEFAULT_TASK_COUNT, 
            'request_id': TEST_UUID, 'user_id': TEST_UUID, 
            'dataset_cols': literal_eval(dataset_cols)}
    
    return data


@pytest.fixture(scope='session')
def get_prompt_result_task_exp_output(get_prompt_result_data):
    pt1_prompt_file = get_prompt_result_data['resp_pt1_file']
    pt2_prompt_file = get_prompt_result_data['resp_pt2_file']
    request_id = get_prompt_result_data['request_id']
    dataset_cols = get_prompt_result_data['dataset_cols']
    
    with open(pt1_prompt_file, 'r') as f:
        pt1_prompt_json = json.load(f)
        
    with open(pt2_prompt_file, 'r') as f:
        pt2_prompt_json = json.load(f)
    pt1_context = {'required_cols': dataset_cols, 'request_id': request_id}
    pt2_context = {'run_type': 'first_run_after_request', 'request_id': request_id}
    
    pt1_prompt_validated = DatasetAnalysisModelPartOne.model_validate(pt1_prompt_json, context=pt1_context)
    pt2_prompt_validated = DatasetAnalysisModelPartTwo.model_validate(pt2_prompt_json, context=pt2_context)
    prompt_validated = {**pt1_prompt_validated.model_dump(), **pt2_prompt_validated.model_dump()}
    
    expected_data_tasks = DataTasks(
                        columns=prompt_validated['columns'],
                        common_tasks=prompt_validated['common_tasks'], 
                        common_column_cleaning_or_transformation=prompt_validated['common_column_cleaning_or_transformation'],
                        common_column_combination=prompt_validated['common_column_combination']
                        )
    
    expected_res = expected_data_tasks.model_dump()
    
    return expected_res

@pytest.fixture(scope='session')
def data_processing_task_first_run_flow_data(get_prompt_result_task_exp_output):
    
    data_tasks_dict = get_prompt_result_task_exp_output
    
    TEST_CLEAN_DATASET_FILE = 'tests/test_files/clean_dataset.parquet'

    run_info = {'request_id': TEST_UUID, 'user_id': TEST_UUID, 'parquet_file': TEST_CLEAN_DATASET_FILE}
    
    data = {'user_id': TEST_UUID, 
            'request_id': TEST_UUID,
            'run_type': 'first_run_after_request', 
            'data_tasks_dict': data_tasks_dict, 
            'run_info': run_info}
    
    return data

@pytest_asyncio.fixture(scope='session')
async def async_conn():
    testing_db_engine = create_async_engine(TEST_DB_URL_ASYNC)
    
    async with testing_db_engine.connect() as conn:
        yield conn
        
    await testing_db_engine.dispose()
    
@pytest.fixture(scope='session')
def default_uuid():
    return TEST_UUID
    
@pytest.fixture(scope='session')
def sync_conn():
    testing_db_engine = create_engine(TEST_DB_URL_SYNC)
    
    with testing_db_engine.connect() as conn:
        yield conn
        
    testing_db_engine.dispose()
    
@pytest.fixture(scope='session', autouse=True)
def init_db():
    
    testing_db_engine = create_engine(TEST_DB_URL_SYNC)
    
    db_path = TEST_DB_URL_ASYNC.split('/')[-1]
    if os.path.exists(db_path):
        os.remove(db_path)
        
    Base.metadata.create_all(testing_db_engine)
    
    yield
    
    testing_db_engine.dispose()
    
    if os.path.exists(db_path):
        os.remove(db_path)

@pytest.fixture(scope='function')
def df_to_parquet_mock(mocker):
    mock_func = mocker.patch('pandas.DataFrame.to_parquet')
    yield mock_func

@pytest.fixture(scope='function')
def patch_uuid(mocker):
    mock_func = mocker.patch('app.crud.uuid.uuid4')
    mock_func.return_value = TEST_UUID

@pytest.fixture(scope='session')
def user_register_data():
    user_register_data = {
        'username': 'emperor22', 
        'first_name': 'andi', 
        'last_name': 'putra', 
        'email': 'algiffaryriony@gmail.com'
        }
    
    return user_register_data

@pytest.fixture(scope='session')
def get_otp_data(user_register_data):
    email = user_register_data['email']
    username = user_register_data['username']
    
    email_recipients = [email]
    email_subject = 'OTP for Data Analysis Assistant app'
    email_body = f'Your OTP is {TEST_DEFAULT_OTP}'
    
    data = {'otp': TEST_DEFAULT_OTP, 
            'username': username, 
            'email_recipients': email_recipients,
            'email_subject': email_subject,
            'email_body': email_body}
    
    return data

@pytest.fixture(scope='function')
def login_data(user_register_data):
    username = user_register_data['username']
    
    data = {'username': username, 
            'otp': TEST_DEFAULT_OTP, 
            'new_otp_for_invalidation': '111111',
            'otp_expire': '2100-01-01 00:00:00.000000+0000'}
    
    return data  



@pytest.fixture(scope='session')
def initial_request_data(dataset_cols):
    filename = TEST_DEFAULT_DATASET_FILE
    model = TEST_DEFAULT_MODEL
    task_count = 10
    
    with open(filename, 'rb') as f:
        file_content = f.read()
        
    return {'filename': filename, 'file_content': file_content, 'task_count': str(task_count), 'model': model, 'prompt_version': TEST_PROMPT_VERSION, 
            'dataset_cols': dataset_cols}

@pytest.fixture(scope='session')
def execute_analysis_data():
    filename = 'tests/test_files/execute_analyses_frontend_payload.json'
    
    with open(filename, 'r') as f:
        data = json.load(f)
        
    data['request_id'] = TEST_UUID
        
    return data

@pytest.fixture(scope='session')
def additional_analyses_request_data():
    prompt = '''top 10 movies from the US
                top 10 indian movies'''
    payload = {'model': TEST_DEFAULT_MODEL, 'new_tasks_prompt': prompt, 'request_id': TEST_UUID}
    return payload


@pytest.fixture(scope='session')
def get_current_user_dependency_data():
    class TestUser():
        username = 'emperor22'
        user_id = TEST_UUID
    
    return TestUser()
        

@pytest.fixture(scope='function')
def mock_celery_app():
    app = Celery('app')
    app.conf.update(task_always_eager=True)
    
    return app
    
@pytest.fixture(scope='function')
def celery_mock_task(mock_celery_app):
    
    @mock_celery_app.task
    def mock_task(*args, **kwargs):
        return True
    
    return mock_task

@pytest.fixture(scope='function')
def patch_celery_related_things(monkeypatch, mock_celery_app, celery_mock_task):
    monkeypatch.setattr(tasks, 'app', mock_celery_app)
    
    monkeypatch.setattr(api, 'get_prompt_result_task', celery_mock_task)
    monkeypatch.setattr(api, 'get_additional_analyses_prompt_result', celery_mock_task)
    monkeypatch.setattr(api, 'data_processing_task', celery_mock_task)
    
    # # Optional: Track calls for assertions
    # celery_mock_task.call_count = 0
    # original_call = celery_mock_task.__call__
    # def tracked_call(*args, **kwargs):
    #     celery_mock_task.call_count += 1
    #     return original_call(*args, **kwargs)
    # monkeypatch.setattr(celery_mock_task, '__call__', tracked_call)


@pytest.fixture(scope='function')
def patch_is_task_invalid_check(mocker):
    mock_func = mocker.patch('api.is_task_invalid_or_still_processing')
    mock_func.return_value = False

@pytest_asyncio.fixture(scope='function')
async def test_client(async_conn, get_current_user_dependency_data, patch_celery_related_things, patch_uuid):
    
    async def override_get_prompt_table_ops():
        return PromptTableOperation(async_conn)
    
    async def override_get_user_table_ops():
        return UserTableOperation(async_conn)
    
    async def override_get_task_run_table_ops():
        return TaskRunTableOperation(async_conn)
    
    async def override_get_current_user():
        return get_current_user_dependency_data
    
    bg_task_mock = MagicMock()
    async def override_get_background_tasks():
        return bg_task_mock
    
    app.dependency_overrides[get_prompt_table_ops] = override_get_prompt_table_ops
    app.dependency_overrides[get_user_table_ops] = override_get_user_table_ops
    app.dependency_overrides[get_task_run_table_ops] = override_get_task_run_table_ops
    app.dependency_overrides[get_current_user] = override_get_current_user
    app.dependency_overrides[get_background_tasks] = override_get_background_tasks
    
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url='http://test') as client:
        yield client, bg_task_mock
    
    app.dependency_overrides.clear()

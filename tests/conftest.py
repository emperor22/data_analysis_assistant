import pytest
import pytest_asyncio
from app.crud import (Base, UserTableOperation, PromptTableOperation, TaskRunTableOperation, 
                      get_prompt_table_ops, get_user_table_ops, get_task_run_table_ops, get_current_time_utc)
from app.auth import get_current_user
from app.api import app
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
import uuid
import os

from fastapi.testclient import TestClient
from httpx import AsyncClient, ASGITransport

TESTING_DB_URL_ASYNC = 'sqlite+aiosqlite:///./testing_xx.sqlite'
TESTING_DB_URL_SYNC = 'sqlite:///./testing_xx.sqlite'
TESTING_REQUEST_ID = 'xxxx-xxxx-xxxx-xxxx'
TESTING_USER_ID = 'xxxx-xxxx-xxxx-xxxx'

@pytest.fixture(scope='session', autouse=True)
def init_db():
    
    testing_db_engine = create_engine(TESTING_DB_URL_SYNC)
    
    db_path = TESTING_DB_URL_ASYNC.split('/')[-1]
    if os.path.exists(db_path):
        os.remove(db_path)
        
    Base.metadata.create_all(testing_db_engine)
    
    yield
    
    testing_db_engine.dispose()
    
    if os.path.exists(db_path):
        os.remove(db_path)
        
@pytest_asyncio.fixture(scope='session')
async def async_conn():
    testing_db_engine = create_async_engine(TESTING_DB_URL_ASYNC)
    
    async with testing_db_engine.connect() as conn:
        yield conn
        
    await testing_db_engine.dispose()
    
@pytest.fixture(scope='session')
def sync_conn():
    testing_db_engine = create_engine(TESTING_DB_URL_SYNC)
    
    with testing_db_engine.connect() as conn:
        yield conn
        
    testing_db_engine.dispose()

@pytest.fixture(scope='session')
def monkeypatch_():
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest_asyncio.fixture(scope='session')
async def test_client(async_conn, monkeypatch_):
    
    ########### TABLE OPS REPLACEMENT FUNCTIONS ##############
    async def override_get_prompt_table_ops():
        return PromptTableOperation(async_conn)
    
    async def override_get_user_table_ops():
        return UserTableOperation(async_conn)
    
    async def override_get_task_run_table_ops():
        return TaskRunTableOperation(async_conn)
    
    ########### INDIVIDUAL TABLE REPLACEMENT METHODS ##############
    
    async def override_add_task(self, user_id: int, prompt_version: str, filename: str, dataset_cols: str, model: str):
        
        req_id = TESTING_REQUEST_ID
        query = '''insert into prompt_and_result(id, user_id, prompt_version, filename, dataset_cols, model, created_at)
                    values (:id, :user_id, :prompt_version, :filename, :dataset_cols, :model, :created_at)'''
        await self.conn.execute(text(query), {'id': req_id, 'user_id': user_id, 'prompt_version': prompt_version, 
                                              'filename': filename, 'dataset_cols': dataset_cols, 'model': model, 
                                              'created_at': get_current_time_utc()})
        await self.conn.commit()

        return req_id
    
    monkeypatch_.setattr(PromptTableOperation, 'add_task', override_add_task)
    
    async def override_create_user(self, username, email, first_name, last_name):
        user_id = TESTING_USER_ID

        query = '''insert into user(id, username, email, first_name, last_name, created_at) 
                    values (:user_id, :username, :email, :first_name, :last_name, :created_at)'''
        await self.conn.execute(text(query), {'user_id': user_id, 'username': username, 'email': email, 'first_name': first_name, 'last_name': last_name, 
                                              'created_at': get_current_time_utc()})
        await self.conn.commit()
    
    monkeypatch_.setattr(UserTableOperation, 'create_user', override_create_user)
    
    ################################################################
    
    async def override_get_current_user():
        
        class TestUser():
            username = 'emperor22'
            user_id = TESTING_USER_ID
        
        return TestUser()
    
    app.dependency_overrides[get_prompt_table_ops] = override_get_prompt_table_ops
    app.dependency_overrides[get_user_table_ops] = override_get_user_table_ops
    app.dependency_overrides[get_task_run_table_ops] = override_get_task_run_table_ops
    app.dependency_overrides[get_current_user] = override_get_current_user
    
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url='http://test') as client:
        yield client
    
    app.dependency_overrides.clear()

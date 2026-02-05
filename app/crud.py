from sqlalchemy import text, create_engine
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

import asyncio
from fastapi import Depends

import uuid

from datetime import datetime, timezone, date, timedelta

from sqlalchemy import (
    ForeignKey,
    Integer,
    String,
    Text,
    DateTime,
    Column,
    Boolean
)

from sqlalchemy.orm import declarative_base

import redis

from app.config import Config
from app.services.utils import get_current_time_utc

import json


base_engine = create_async_engine(Config.DATABASE_URL_ASYNC, pool_size=10, max_overflow=2, pool_recycle=300, pool_pre_ping=True, pool_use_lifo=True)
base_engine_sync = create_engine(Config.DATABASE_URL_SYNC, pool_size=10, max_overflow=2, pool_recycle=300, pool_pre_ping=True, pool_use_lifo=True)

SessionLocal = async_sessionmaker(bind=base_engine, expire_on_commit=False, class_=AsyncSession, autoflush=False)

Base = declarative_base()


class AppUsers(Base):
    __tablename__ = 'app_user'

    id = Column(Text, primary_key=True)
    username = Column(String, unique=True, nullable=False)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    email = Column(String, unique=True, nullable=False)
    otp = Column(String, nullable=True)
    otp_expire = Column(DateTime(timezone=True), nullable=True)
    last_otp_request = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True))
    api_key_cerebras = Column(String)
    api_key_google = Column(String)

class PromptAndResult(Base):
    __tablename__ = 'prompt_and_result'

    id = Column(Text, primary_key=True)
    
    user_id = Column(Text, ForeignKey("app_user.id")) 
    
    run_name = Column(String)
    prompt_version = Column(Text)
    filename = Column(Text)
    dataset_cols = Column(Text)
    model = Column(String)
    prompt_result = Column(Text)
    additional_analyses_prompt_result = Column(Text) 
    status = Column(String)
    celery_task_id = Column(String)
    created_at = Column(DateTime(timezone=True))
    rate_limit_retry_count = Column(Integer, nullable=True, server_default=text('0'))
    last_accessed_at = Column(DateTime(timezone=True))
    
class TaskRun(Base):
    __tablename__ = 'task_run'

    request_id = Column(Text, ForeignKey("prompt_and_result.id"), primary_key=True)
    user_id = Column(Text, ForeignKey("app_user.id"), primary_key=True)
    
    original_common_tasks = Column(Text)
    common_tasks_w_result = Column(Text)
    column_transforms_status = Column(String)
    column_combinations_status = Column(String)
    columns_info = Column(Text)
    celery_task_id = Column(String)
    final_dataset_snippet = Column(Text)
    created_at = Column(DateTime(timezone=True))
    
class BlacklistedDatasets(Base):
    __tablename__ = 'blacklisted_datasets'
    
    dataset_id = Column(Text, primary_key=True)
    reason = Column(Text)
    failed_attempts = Column(Integer)
    last_failed_at = Column(DateTime(timezone=True))
    is_blacklisted = Column(Boolean)
    
class UserCustomizedTasks(Base):
    __tablename__ = 'user_customized_tasks'
    
    request_id = Column(Text, ForeignKey("prompt_and_result.id"), primary_key=True)
    user_id = Column(Text, ForeignKey("app_user.id"), primary_key=True)
    
    saved_tasks_slot_1 = Column(Text)
    saved_tasks_slot_2 = Column(Text)
    saved_tasks_slot_3 = Column(Text)
    
    imported_original_tasks = Column(Text)
        
    
def create_tables():
    Base.metadata.create_all(base_engine_sync)


class BlacklistedDatasetsTableOperation:
    def __init__(self, conn_sync):
        self.conn_sync = conn_sync
        self.table_name = 'blacklisted_datasets'

    def add_dataset_to_table(self, dataset_id):
        user_id = str(uuid.uuid4())

        query = f'''insert into {self.table_name}(dataset_id, reason, failed_attempts, last_failed_at, is_blacklisted) 
                    values (:dataset_id, :reason, 0, :last_failed_at, false)'''
        self.conn_sync.execute(text(query), {'dataset_id': dataset_id, 'reason': '', 'last_failed_at': get_current_time_utc()})
        self.conn_sync.commit()
    
    def remove_dataset_from_table(self, dataset_id):
        query = f'''delete from {self.table_name} where dataset_id = :dataset_id'''
        self.conn_sync.execute(text(query), {'dataset_id': dataset_id})
        self.conn_sync.commit()
    
    def increment_failed_attempt(self, dataset_id):
        cur_failed_attempt = self.get_failed_attempt_count(dataset_id)
        
        query = f'''update {self.table_name}
                   set failed_attempts = :new_failed_attempts
                   where dataset_id = :dataset_id'''
        self.conn_sync.execute(text(query), {'dataset_id': dataset_id, 'new_failed_attempts': cur_failed_attempt + 1})
        self.conn_sync.commit()
        
    def reset_failed_attempt_count(self, dataset_id):
        query = f'''update {self.table_name}
                   set failed_attempts = 0
                   where dataset_id = :dataset_id'''
        self.conn_sync.execute(text(query), {'dataset_id': dataset_id})
        self.conn_sync.commit()
    
    def get_failed_attempt_count(self, dataset_id):
        query = f'''select failed_attempts from {self.table_name} where dataset_id = :dataset_id'''

        res = self.conn_sync.execute(text(query), {'dataset_id': dataset_id})
        res = res.fetchone()
        return res._mapping['failed_attempts'] if res else None
    
    def check_if_blacklisted(self, dataset_id):
        query = f'''select is_blacklisted from {self.table_name} where dataset_id = :dataset_id'''

        res = self.conn_sync.execute(text(query), {'dataset_id': dataset_id})
        res = res.fetchone()
        
        if not res:
            return None
        
        return bool(res._mapping['is_blacklisted'])
    
    def mark_as_blacklisted(self, dataset_id):
        query = f'''update {self.table_name}
                   set is_blacklisted = true
                   where dataset_id = :dataset_id'''
        self.conn_sync.execute(text(query), {'dataset_id': dataset_id})
        self.conn_sync.commit()

class UserCustomizedTasksTableOperation:
    def __init__(self, conn):
        self.conn = conn
        self.table_name = 'user_customized_tasks'
        
    async def delete_task(self, user_id, request_id):
        query = f'''delete from {self.table_name} where user_id = :user_id and request_id = :request_id'''
        
        await self.conn.execute(text(query), {'user_id': user_id, 'request_id': request_id})
        
    async def fetch_customized_tasks(self, user_id, request_id, slot):
        col = f'saved_tasks_slot_{slot}'
        allowed_cols = ['saved_tasks_slot_1', 'saved_tasks_slot_2', 'saved_tasks_slot_3']
        
        if col not in allowed_cols:
            return None
        
        query = f'''select {col} from {self.table_name} where user_id = :user_id and request_id = :request_id'''

        res = await self.conn.execute(text(query), {'user_id': user_id, 'request_id': request_id})
        res = res.fetchone()
        return res._mapping[col] if res else None
    
    async def fetch_imported_tasks(self, user_id, request_id):
        query = f'''select imported_original_tasks from {self.table_name} where user_id = :user_id and request_id = :request_id'''
        
        res = await self.conn.execute(text(query), {'user_id': user_id, 'request_id': request_id})
        res = res.fetchone()
        return res._mapping['imported_original_tasks'] if res else None
    
    async def check_if_customized_tasks_empty(self, user_id, request_id):
        query = f'''select saved_tasks_slot_1, saved_tasks_slot_2, saved_tasks_slot_3 from {self.table_name} where user_id = :user_id and request_id = :request_id'''
        
        res = await self.conn.execute(text(query), {'user_id': user_id, 'request_id': request_id})
        res = res.fetchone()
        values = res._mapping.values()
        return all([i is None for i in values])
        
    async def update_customized_tasks(self, user_id, request_id, slot, tasks):
        col = f'saved_tasks_slot_{slot}'
        allowed_cols = ['saved_tasks_slot_1', 'saved_tasks_slot_2', 'saved_tasks_slot_3']
        
        if col not in allowed_cols:
            return None
        
        query = f'''update {self.table_name} set {col} = :tasks where user_id = :user_id and request_id = :request_id'''

        await self.conn.execute(text(query), {'user_id': user_id, 'request_id': request_id, 'tasks': tasks})
        
        
    async def delete_customized_tasks(self, user_id, request_id, slot):
        await self.update_customized_tasks(user_id=user_id, request_id=request_id, slot=slot, tasks=None)
        
    async def set_imported_tasks(self, user_id, request_id, imported_task_ids: list):
        imported_task_ids = json.dumps(imported_task_ids)
        query = f'''update {self.table_name} set imported_original_tasks = :imported_task_ids where user_id = :user_id and request_id = :request_id'''
        
        await self.conn.execute(text(query), {'user_id': user_id, 'request_id': request_id, 'imported_task_ids': imported_task_ids})
        
        
    async def add_request_id_to_table(self, user_id, request_id):
        query = f'''insert into {self.table_name}(user_id, request_id) 
                    values (:user_id, :request_id)'''
        await self.conn.execute(text(query), {'user_id': user_id, 'request_id': request_id})
        
        
class UserTableOperation:
    def __init__(self, conn=None, conn_sync=None):
        self.conn = conn
        self.conn_sync = conn_sync
        self.table_name = 'app_user'
        
    async def add_api_key(self, user_id, key, provider):
        if provider not in Config.LLM_PROVIDER_LIST:
            return None
        
        query = f'''update {self.table_name} set api_key_{provider} = :key where id = :user_id'''
        
        await self.conn.execute(text(query), {'key': key, 'user_id': user_id})
    
    async def delete_api_key(self, user_id, provider):
        if provider not in Config.LLM_PROVIDER_LIST:
            return None
        
        query = f'''update {self.table_name} set api_key_{provider} = NULL where id = :user_id'''
        
        await self.conn.execute(text(query), {'user_id': user_id})
    
    async def get_api_key(self, user_id, provider):
        if provider not in Config.LLM_PROVIDER_LIST:
            return None
        
        col = f'api_key_{provider}'
        query = f'''select {col} from {self.table_name} where id = :user_id'''
        res = await self.conn.execute(text(query), {'user_id': user_id})
        res = res.fetchone()
        
        return res._mapping[col] if res else None
    
    async def get_user(self, username):
        query = f'''select username, id, email, last_otp_request, otp, otp_expire from {self.table_name} where username = :username'''

        res = await self.conn.execute(text(query), {'username': username})
        res = res.fetchone()
        return res._mapping if res else None
    
    async def create_user(self, username, email, first_name, last_name):
        user_id = str(uuid.uuid4())

        query = f'''insert into {self.table_name}(id, username, email, first_name, last_name, created_at) 
                    values (:user_id, :username, :email, :first_name, :last_name, :created_at)'''
        await self.conn.execute(text(query), {'user_id': user_id, 'username': username, 'email': email, 'first_name': first_name, 'last_name': last_name, 
                                              'created_at': get_current_time_utc()})
        
        
    async def update_otp(self, username: str, otp: str, otp_expire: datetime):
        query = f'''update {self.table_name}
                   set otp = :otp, otp_expire = :otp_expire, last_otp_request = :last_otp_request
                   where username = :username'''
        await self.conn.execute(text(query), {'otp': otp, 'otp_expire': otp_expire, 'last_otp_request': get_current_time_utc(), 'username': username})
        
            
    async def delete_user(self, username):
        query = f'''delete from {self.table_name} where username = :username '''

        await self.conn.execute(text(query), {'username': username})
        
            
class PromptTableOperation:
    def __init__(self, conn=None, conn_sync=None):
        self.conn = conn
        self.conn_sync = conn_sync
        self.table_name = 'prompt_and_result'
    
    async def add_task(self, user_id: str, run_name:str, prompt_version: str, filename: str, dataset_cols: str, model: str):

        
        req_id = str(uuid.uuid4())
        query = f'''insert into {self.table_name}(id, user_id, run_name, prompt_version, filename, dataset_cols, model, created_at, last_accessed_at)
                    values (:id, :user_id, :run_name, :prompt_version, :filename, :dataset_cols, :model, :created_at, :last_accessed_at)'''
        await self.conn.execute(text(query), {'id': req_id, 'user_id': user_id, 'prompt_version': str(prompt_version), 'run_name': run_name,
                                              'filename': filename, 'dataset_cols': dataset_cols, 'model': model, 'created_at': get_current_time_utc(), 
                                              'last_accessed_at': get_current_time_utc()})
        

        return req_id
    
    async def delete_task(self, user_id, request_id):
        query = f'''delete from {self.table_name} where user_id = :user_id and id = :request_id'''
        
        await self.conn.execute(text(query), {'user_id': user_id, 'request_id': request_id})
            
    # is a synchronous function because will be used in gevent worker
    def insert_prompt_result_sync(self, request_id: str, prompt_result: str):

        query = f'''update {self.table_name}
                   set prompt_result = :prompt_result
                   where id = :request_id'''

        self.conn_sync.execute(text(query) ,{'request_id': request_id, 'prompt_result': prompt_result})
        self.conn_sync.commit()
        
    async def get_prompt_result(self, request_id: str):
        query = f'''select prompt_result from {self.table_name} where id = :request_id'''

        res = await self.conn.execute(text(query), {'request_id': request_id})
        res = res.fetchone()
        return res._mapping if res and res.prompt_result is not None else None
        
    def get_prompt_result_sync(self, request_id: str, user_id: str):
        
        query = f'''select prompt_result from {self.table_name} where user_id = :user_id and id = :request_id'''

        res = self.conn_sync.execute(text(query), {'request_id': request_id, 'user_id': user_id})
        res = res.fetchone()
        return res._mapping if res and res.prompt_result is not None else None

    def insert_additional_analyses_prompt_result_sync(self, request_id: str, additional_analyses_prompt_result: str):
        
        
        query = f'''update {self.table_name}
                   set additional_analyses_prompt_result = :additional_analyses_prompt_result
                   where id = :request_id'''

        self.conn_sync.execute(text(query) ,{'request_id': request_id, 'additional_analyses_prompt_result': additional_analyses_prompt_result})
        self.conn_sync.commit()
        
    async def get_additional_analyses_prompt_result(self, request_id: str, user_id: str):
        query = f'''select additional_analyses_prompt_result from {self.table_name} where user_id = :user_id and id = :request_id'''

        res = await self.conn.execute(text(query), {'request_id': request_id, 'user_id': user_id})
        res = res.fetchone()
        return res._mapping if res and res.additional_analyses_prompt_result is not None else None
        
    def change_request_status_sync(self, request_id, status):
        
        
        query = f'''update {self.table_name}
                   set status = :status
                   where id = :request_id'''

        self.conn_sync.execute(text(query), {'request_id': request_id,  'status': status})
        self.conn_sync.commit()
    
    async def get_request_status(self, request_id: str, user_id: str):
        query = f'''select status from {self.table_name} where user_id = :user_id and id = :request_id'''

        res = await self.conn.execute(text(query), {'request_id': request_id, 'user_id': user_id})
        res = res.fetchone()
        return res._mapping if res and res.status is not None else None
    
    async def get_dataset_filename(self, request_id: str, user_id: str):
        query = f'''select filename from {self.table_name} where user_id = :user_id and id = :request_id'''

        res = await self.conn.execute(text(query), {'request_id': request_id, 'user_id': user_id})
        res = res.fetchone()
        return res._mapping['filename'] if res else None
    
    async def get_run_name(self, request_id: str, user_id: str):
        query = f'''select run_name from {self.table_name} where user_id = :user_id and id = :request_id'''

        res = await self.conn.execute(text(query), {'request_id': request_id, 'user_id': user_id})
        res = res.fetchone()
        return res._mapping['run_name'] if res else None
    
    async def get_request_ids_by_user(self, user_id: str):
        query = f'''select id, run_name, filename, status from {self.table_name} where user_id = :user_id'''
        res = await self.conn.execute(text(query), {'user_id': user_id})
        res = res.fetchall()
        return [(i.id, i.run_name, i.filename, i.status) for i in res] if res else None
    
    async def get_dataset_columns_by_id(self, request_id: str, user_id: str):
        query = f'''select dataset_cols from {self.table_name} where user_id = :user_id and id = :request_id'''

        res = await self.conn.execute(text(query), {'request_id': request_id, 'user_id': user_id})
        res = res.fetchone()
        return res._mapping['dataset_cols'] if res and res.dataset_cols is not None else None
    
    def update_last_accessed_column_sync(self, update_dct):
        
        
        update_dct = {i: datetime.strptime(j, '%Y-%m-%d') for i, j in update_dct.items()} # update dct is {req_id: date_str}
        for req_id, date in update_dct.items():
            q = f'''update {self.table_name} set last_accessed_at = :date where id = :req_id'''
            self.conn_sync.execute(text(q), {'date': date, 'req_id': req_id})
            
        self.conn_sync.commit()
        
    def get_least_accessed_request_ids_sync(self, thres_days):
        
        
        date_filt = (date.today() - timedelta(days=thres_days)).srtftime('%Y-%m-%d')
        
        query = f'''select id from {self.table_name} where last_accessed_at < :date_filt'''
        
        res = self.conn_sync.execute(text(query), {'date_filt': date_filt})
        res = res.fetchall()
        return res._mapping['id'] if res else None
    
    def increment_rate_limit_retry_count_sync(self, user_id, request_id):
        
        current_retry_count = self.check_rate_limit_retry_count(user_id, request_id)
        
        query = f'''update {self.table_name} set rate_limit_retry_count = :new_count where user_id = :user_id and id = :request_id'''
        self.conn_sync.execute(text(query), {'user_id': user_id, 'request_id': request_id, 'new_count': current_retry_count + 1})
        self.conn_sync.commit()
    
    def check_rate_limit_retry_count(self, user_id, request_id):
        
        query = f'''select rate_limit_retry_count from {self.table_name} where user_id = :user_id and id = :request_id'''
        res = self.conn_sync.execute(text(query), {'user_id': user_id, 'request_id': request_id})
        res = res.fetchone()
        
        return res._mapping['rate_limit_retry_count'] if res else 0
    
    def reset_rate_limit_retry_count_sync(self, user_id, request_id):
        
        
        query = f'''update {self.table_name} set rate_limit_retry_count = 0 where user_id = :user_id and id = :request_id'''
        self.conn_sync.execute(text(query), {'user_id': user_id, 'request_id': request_id})
        self.conn_sync.commit()
        
class TaskRunTableOperation:
    def __init__(self, conn=None, conn_sync=None):
        self.conn = conn
        self.conn_sync = conn_sync
        self.table_name = 'task_run'
    
    async def empty_modified_task_result(self, request_id: str, user_id: str):
        query = f'''update {self.table_name} 
                    set common_tasks_w_result = NULL
                    where user_id = :user_id and request_id = :request_id'''
                    
        await self.conn.execute(text(query) ,{'request_id': request_id, 'user_id': user_id})
        
    async def delete_task(self, user_id, request_id):
        query = f'''delete from {self.table_name} where user_id = :user_id and request_id = :request_id'''
        
        await self.conn.execute(text(query), {'user_id': user_id, 'request_id': request_id})
    
    def add_task_result_sync(self, request_id: str, user_id: str):
        
        
        query = f'''insert into {self.table_name}(request_id, user_id, created_at) values (:request_id, :user_id, :created_at)'''

        self.conn_sync.execute(text(query) ,{'request_id': request_id, 'user_id': user_id, 'created_at': get_current_time_utc()})
        self.conn_sync.commit()
    
    def update_task_result_sync(self, request_id: str, tasks: str):
        
        
        query = f'''update {self.table_name}
                   set common_tasks_w_result = :tasks
                   where request_id = :request_id'''
            
        self.conn_sync.execute(text(query) ,{'request_id': request_id, 'tasks': tasks})
        self.conn_sync.commit()
        
    def update_original_common_task_result_sync(self, request_id: str, tasks: str):
        
        
        query = f'''update {self.table_name}
                   set original_common_tasks = :tasks
                   where request_id = :request_id'''
            
        self.conn_sync.execute(text(query) ,{'request_id': request_id, 'tasks': tasks})
        self.conn_sync.commit()
        
    def update_column_transform_task_status_sync(self, request_id, column_transforms_status):
        
        
        query = f'''update {self.table_name}
            set column_transforms_status = :column_transforms_status
            where request_id = :request_id'''
    
        self.conn_sync.execute(text(query) ,{'request_id': request_id, 'column_transforms_status': column_transforms_status})
        self.conn_sync.commit()
        
    def update_column_combination_task_status_sync(self, request_id, column_combinations_status):
        
        
        query = f'''update {self.table_name}
            set column_combinations_status = :column_combinations_status
            where request_id = :request_id'''
    
        self.conn_sync.execute(text(query) ,{'request_id': request_id, 'column_combinations_status': column_combinations_status})
        self.conn_sync.commit()
        
    def update_final_dataset_snippet_sync(self, request_id, dataset_snippet):
        
        
        query = f'''update {self.table_name}
            set final_dataset_snippet = :dataset_snippet
            where request_id = :request_id'''
    
        self.conn_sync.execute(text(query) ,{'request_id': request_id, 'dataset_snippet': dataset_snippet})
        self.conn_sync.commit()
        
    def update_columns_info_sync(self, request_id, columns_info):
        
        
        query = f'''update {self.table_name}
            set columns_info = :columns_info
            where request_id = :request_id'''
    
        self.conn_sync.execute(text(query) ,{'request_id': request_id, 'columns_info': columns_info})
        self.conn_sync.commit()
    
    async def get_original_tasks_by_id(self, user_id:int, request_id: str):
        query = f'''select original_common_tasks from {self.table_name} 
                   where user_id = :user_id and request_id = :request_id'''
    
        res = await self.conn.execute(text(query), {'user_id': user_id, 'request_id': request_id})
        res = res.fetchone()
        return res._mapping if res and res.original_common_tasks is not None else None
    
        
    async def get_columns_combinations_by_id(self, user_id:int, request_id: str):
        query = f'''select column_combinations_status from {self.table_name} 
                   where user_id = :user_id and request_id = :request_id'''
    
        res = await self.conn.execute(text(query), {'user_id': user_id, 'request_id': request_id})
        res = res.fetchone()
        return res._mapping if res else None
    
    
    async def get_columns_transformations_by_id(self, user_id:int, request_id: str):
        query = f'''select column_transforms_status from {self.table_name} 
                   where user_id = :user_id and request_id = :request_id'''
    
        res = await self.conn.execute(text(query), {'user_id': user_id, 'request_id': request_id})
        res = res.fetchone()
        return res._mapping if res else None
    
    async def get_modified_tasks_by_id(self, user_id:int, request_id: str):
        query = f'''select common_tasks_w_result from {self.table_name} 
                   where user_id = :user_id and request_id = :request_id'''
    
        res = await self.conn.execute(text(query), {'user_id': user_id, 'request_id': request_id})
        res = res.fetchone()
        return res._mapping if res and res.common_tasks_w_result is not None else None

    
    async def get_columns_info_by_id(self, user_id:int, request_id: str):
        query = f'''select columns_info from {self.table_name} 
                   where user_id = :user_id and request_id = :request_id'''
    
        res = await self.conn.execute(text(query), {'user_id': user_id, 'request_id': request_id})
        res = res.fetchone()
        return res._mapping if res else None
    
    async def get_dataset_snippet_by_id(self, user_id:int, request_id: str):
        query = f'''select final_dataset_snippet from {self.table_name} 
                   where user_id = :user_id and request_id = :request_id'''
    
        res = await self.conn.execute(text(query), {'user_id': user_id, 'request_id': request_id})
        res = res.fetchone()
        return res._mapping if res else None
    
    def get_task_by_id_sync(self, user_id:int, request_id: str):
        query = f'''select original_common_tasks, common_tasks_w_result from {self.table_name} where user_id = :user_id and request_id = :request_id'''

        res = self.conn_sync.execute(text(query), {'user_id': user_id, 'request_id': request_id})
        res = res.fetchone()
        return res._mapping if res else None
    
    def request_id_exists(self, request_id: str):
        query = f'''select request_id from {self.table_name} where request_id = :request_id'''

        res = self.conn_sync.execute(text(query), {'request_id': request_id})
        res = res.fetchone()
        return res is not None

async def get_session():
    async with SessionLocal() as session:
        async with session.begin():
            yield session
        



async def get_prompt_table_ops(conn=Depends(get_session)):
    yield PromptTableOperation(conn=conn)
        
async def get_task_run_table_ops(conn=Depends(get_session)):
    yield TaskRunTableOperation(conn=conn)
    
async def get_user_table_ops(conn=Depends(get_session)):
    yield UserTableOperation(conn=conn)
    
async def get_user_customized_tasks_table_ops(conn=Depends(get_session)):
    yield UserCustomizedTasksTableOperation(conn=conn)

def get_redis_client():
    conn_pool = redis.ConnectionPool.from_url(Config.REDIS_URL)

    return redis.Redis(connection_pool=conn_pool)
        

async def read_sql_async(query, conn, insert_or_delete=False):
        res = await conn.execute(text(query))
        if insert_or_delete:
            pass
        else:
            res = res.fetchone()
            return res._mapping if res else None
    
        
    

            
if __name__ == '__main__':
    # # import os
    # # if os.path.exists('test.sqlite'):
    # #     os.remove('test.sqlite')
    # # create_tables_sqlite()
    
    # CONFIG.DATABASE_URL_ASYNC = 'sqlite+aiosqlite:///./test.sqlite'
    # engine = create_async_engine(CONFIG.DATABASE_URL_ASYNC)
    
    
    async def func():
        async with base_engine.connect() as conn:
            # ops = UserTableOperation(conn)
            # await read_sql_async('delete from user', conn, True)
            await read_sql_async('delete from prompt_and_result', conn, True)
            await read_sql_async('delete from user_customized_tasks', conn, True)
            await read_sql_async('delete from task_run', conn, True)
    asyncio.run(func())
    # create_tables()
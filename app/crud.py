from sqlalchemy import text, create_engine, Table, MetaData, insert
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.exc import IntegrityError
import sqlite3
from string import Template
import asyncio

DATABASE_URL_ASYNC = 'sqlite+aiosqlite:///./test.sqlite'
DATABASE_URL_SYNC = 'sqlite:///./test.sqlite'

base_engine = create_async_engine(DATABASE_URL_ASYNC)
base_engine_sync = create_engine(DATABASE_URL_SYNC)

async def get_conn():
    async with base_engine.connect() as conn:
        yield conn


def create_tables_sqlite():
    con = sqlite3.connect('test.sqlite')
    cur = con.cursor()

    cur.execute('''create table user (
        id integer primary key,
        username text not null unique, 
        first_name text not null, 
        last_name text not null,
        email text not null unique,
        hashed_password text not null,
        verified integer not null default 0,
        created_at datetime default CURRENT_TIMESTAMP
        )
        
        ''')

    cur.execute('''create table prompt_and_result (
        id integer primary key,
        user_id integer references user(id),
        prompt_version text, 
        filename text,
        dataset_cols text,
        model text,  
        prompt_result text, 
        additional_analyses_prompt_result, 
        status text,
        celery_task_id text,
        created_at datetime default CURRENT_TIMESTAMP
        )
        
        ''')
    
    cur.execute('''create table task_run (
        request_id integer references prompt_and_result(id), 
        user_id integer references user(id), 
        original_common_tasks text,
        common_tasks_w_result text, 
        column_transforms_status text,
        column_combinations_status text, 
        columns_info text, 
        celery_task_id text, 
        final_dataset_snippet text,
        created_at datetime default CURRENT_TIMESTAMP
    )
        
        ''')
    con.commit()
    con.close()


class UserTableOperation:
    def __init__(self, conn):
        self.conn = conn
        
    async def get_user(self, username):
        query = '''select * from user where username = :username'''

        res = await self.conn.execute(text(query), {'username': username})
        res = res.fetchone()
        return res._mapping if res else None
    
    async def get_password_by_username(self, username):
        query = '''select hashed_password from user where username = :username'''

        res = await self.conn.execute(text(query), {'username': username})
        res = res.fetchone()
        return res._mapping['hashed_password'] if res else None
    
    async def create_user(self, username, email, first_name, last_name, hashed_password):
        query = '''insert into user(username, email, first_name, last_name, hashed_password) values (:username, :email, :first_name, :last_name, :hashed_password)'''
        await self.conn.execute(text(query), {'username': username, 'email': email, 'first_name': first_name, 'last_name': last_name,
                                                  'hashed_password': hashed_password})
        await self.conn.commit()
        
    async def make_user_verified(self, user_id):
        query = '''update user 
                   set verified = 1
                   where id = :user_id '''
                   
        await self.conn.execute(text(query), {'user_id': user_id})
        await self.conn.commit()
            
    async def delete_user(self, username):
        query = '''delete from user where username = :username '''

        await self.conn.execute(text(query), {'username': username})
        await self.conn.commit()
            
class PromptTableOperation:
    def __init__(self, conn=None, conn_sync=None):
        self.conn = conn
        self.conn_sync = conn_sync
        
    async def _get_table_obj(self):
        metadata_obj = MetaData()
        table_name = 'prompt_and_result'
        prompt_table = await self.conn.run_sync(lambda sync_conn: Table(table_name, metadata_obj, autoload_with=sync_conn))
        return prompt_table

    
    async def add_task(self, user_id: int, prompt_version: str, filename: str, dataset_cols: str, model: str):
        prompt_table = await self._get_table_obj()
        stmt = insert(prompt_table).values(user_id=user_id, prompt_version=prompt_version, filename=filename, 
                                           dataset_cols=dataset_cols, model=model)

        result = await self.conn.execute(stmt)
        await self.conn.commit()
        inserted_id = result.inserted_primary_key[0]
        return inserted_id
            
    # is a synchronous function because will be used in gevent worker
    def insert_prompt_result_sync(self, request_id: int, prompt_result: str):
        if not self.conn_sync:
            raise Exception('conn_sync object has not been instantiated')
        
        query = '''update prompt_and_result
                   set prompt_result = :prompt_result
                   where id = :request_id'''

        self.conn_sync.execute(text(query) ,{'request_id': request_id, 'prompt_result': prompt_result})
        self.conn_sync.commit()
        
    async def get_prompt_result(self, request_id: int, user_id: int):
        query = '''select prompt_result from prompt_and_result where user_id = :user_id and id = :request_id'''

        res = await self.conn.execute(text(query), {'request_id': request_id, 'user_id': user_id})
        res = res.fetchone()
        return res._mapping if res else None
        
    def get_prompt_result_sync(self, request_id: int, user_id: int):
        if not self.conn_sync:
            raise Exception('conn_sync object has not been instantiated')
        query = '''select prompt_result from prompt_and_result where user_id = :user_id and id = :request_id'''

        res = self.conn_sync.execute(text(query), {'request_id': request_id, 'user_id': user_id})
        res = res.fetchone()
        return res._mapping if res else None

    def insert_additional_analyses_prompt_result_sync(self, request_id: int, additional_analyses_prompt_result: str):
        if not self.conn_sync:
            raise Exception('conn_sync object has not been instantiated')
        
        query = '''update prompt_and_result
                   set additional_analyses_prompt_result = :additional_analyses_prompt_result
                   where id = :request_id'''

        self.conn_sync.execute(text(query) ,{'request_id': request_id, 'additional_analyses_prompt_result': additional_analyses_prompt_result})
        self.conn_sync.commit()
        
    async def get_additional_analyses_prompt_result(self, request_id: int, user_id: int):
        query = '''select additional_analyses_prompt_result from prompt_and_result where user_id = :user_id and id = :request_id'''

        res = await self.conn.execute(text(query), {'request_id': request_id, 'user_id': user_id})
        res = res.fetchone()
        return res._mapping if res else None
        
    def change_request_status_sync(self, request_id, status):
        if not self.conn_sync:
            raise Exception('conn_sync object has not been instantiated')
        
        query = '''update prompt_and_result
                   set status = :status
                   where id = :request_id'''

        self.conn_sync.execute(text(query), {'request_id': request_id,  'status': status})
        self.conn_sync.commit()
    
    async def get_request_status(self, request_id: int, user_id: int):
        query = '''select status from prompt_and_result where user_id = :user_id and id = :request_id'''

        res = await self.conn.execute(text(query), {'request_id': request_id, 'user_id': user_id})
        res = res.fetchone()
        return res._mapping if res else None
    
    async def get_dataset_snippet_by_id(self, request_id: int, user_id: int):
        query = '''select dataset_cols from prompt_and_result where user_id = :user_id and id = :request_id'''

        res = await self.conn.execute(text(query), {'request_id': request_id, 'user_id': user_id})
        res = res.fetchone()
        return res._mapping if res else None
    
    async def get_request_ids_by_user(self, user_id: int):
        query = '''select id, filename, status from prompt_and_result where user_id = :user_id'''
        res = await self.conn.execute(text(query), {'user_id': user_id})
        res = res.fetchall()
        return [(i.id, i.filename, i.status) for i in res] if res else None
        
        
class TaskRunTableOperation:
    def __init__(self, conn=None, conn_sync=None):
        self.conn = conn
        self.conn_sync = conn_sync
    
    def add_task_result_sync(self, request_id: int, user_id: int, original_common_tasks: str):
        if not self.conn_sync:
            raise Exception('conn_sync object has not been instantiated')
        
        query = '''insert into task_run(request_id, user_id, original_common_tasks) values (:request_id, :user_id, :original_common_tasks)'''

        self.conn_sync.execute(text(query) ,{'request_id': request_id, 'user_id': user_id, 'original_common_tasks': original_common_tasks})
        self.conn_sync.commit()
    
    def update_task_result_sync(self, request_id: int, common_tasks_w_result: str):
        if not self.conn_sync:
            raise Exception('conn_sync object has not been instantiated')
        
        query = '''update task_run
                   set common_tasks_w_result = :common_tasks_w_result
                   where request_id = :request_id'''
            
        self.conn_sync.execute(text(query) ,{'request_id': request_id, 'common_tasks_w_result': common_tasks_w_result})
        self.conn_sync.commit()
        
    def update_original_common_task_result_sync(self, request_id: int, original_common_tasks: str):
        if not self.conn_sync:
            raise Exception('conn_sync object has not been instantiated')
        
        query = '''update task_run
                   set original_common_tasks = :original_common_tasks
                   where request_id = :request_id'''
            
        self.conn_sync.execute(text(query) ,{'request_id': request_id, 'original_common_tasks': original_common_tasks})
        self.conn_sync.commit()
        
    def update_column_transform_task_status_sync(self, request_id, column_transforms_status):
        if not self.conn_sync:
            raise Exception('conn_sync object has not been instantiated')
        
        query = '''update task_run
            set column_transforms_status = :column_transforms_status
            where request_id = :request_id'''
    
        self.conn_sync.execute(text(query) ,{'request_id': request_id, 'column_transforms_status': column_transforms_status})
        self.conn_sync.commit()
        
    def update_column_combination_task_status_sync(self, request_id, column_combinations_status):
        if not self.conn_sync:
            raise Exception('conn_sync object has not been instantiated')
        
        query = '''update task_run
            set column_combinations_status = :column_combinations_status
            where request_id = :request_id'''
    
        self.conn_sync.execute(text(query) ,{'request_id': request_id, 'column_combinations_status': column_combinations_status})
        self.conn_sync.commit()
        
    def update_final_dataset_snippet_sync(self, request_id, dataset_snippet):
        if not self.conn_sync:
            raise Exception('conn_sync object has not been instantiated')
        
        query = '''update task_run
            set final_dataset_snippet = :dataset_snippet
            where request_id = :request_id'''
    
        self.conn_sync.execute(text(query) ,{'request_id': request_id, 'dataset_snippet': dataset_snippet})
        self.conn_sync.commit()
        
    def update_columns_info_sync(self, request_id, columns_info):
        if not self.conn_sync:
            raise Exception('conn_sync object has not been instantiated')
        
        query = '''update task_run
            set columns_info = :columns_info
            where request_id = :request_id'''
    
        self.conn_sync.execute(text(query) ,{'request_id': request_id, 'columns_info': columns_info})
        self.conn_sync.commit()
    
    async def get_task_by_id(self, user_id:int, request_id: int):
        query = '''select original_common_tasks, common_tasks_w_result from task_run 
                   where user_id = :user_id and request_id = :request_id'''
    
        res = await self.conn.execute(text(query), {'user_id': user_id, 'request_id': request_id})
        res = res.fetchone()
        return res._mapping if res else None
    
    async def get_columns_info_by_id(self, user_id:int, request_id: int):
        query = '''select columns_info from task_run 
                   where user_id = :user_id and request_id = :request_id'''
    
        res = await self.conn.execute(text(query), {'user_id': user_id, 'request_id': request_id})
        res = res.fetchone()
        return res._mapping if res else None
    
    async def get_dataset_snippet_by_id(self, user_id:int, request_id: int):
        query = '''select final_dataset_snippet from task_run 
                   where user_id = :user_id and request_id = :request_id'''
    
        res = await self.conn.execute(text(query), {'user_id': user_id, 'request_id': request_id})
        res = res.fetchone()
        return res._mapping if res else None
    
    def get_task_by_id_sync(self, user_id:int, request_id: int):
        query = '''select original_common_tasks, common_tasks_w_result from task_run where user_id = :user_id and request_id = :request_id'''

        res = self.conn_sync.execute(text(query), {'user_id': user_id, 'request_id': request_id})
        res = res.fetchone()
        return res._mapping if res else None
        
        

async def read_sql_async(query, conn, insert_or_delete=False):
        res = await conn.execute(text(query))
        if insert_or_delete:
            await conn.commit()
        else:
            res = res.fetchone()
            return res._mapping if res else None
    
        
    

            
if __name__ == '__main__':
    # import os
    # if os.path.exists('test.sqlite'):
    #     os.remove('test.sqlite')
    # create_tables_sqlite()
    
    DATABASE_URL_ASYNC = 'sqlite+aiosqlite:///./test.sqlite'
    engine = create_async_engine(DATABASE_URL_ASYNC)
    
    
    async def func():
        async with engine.connect() as conn:
            # ops = UserTableOperation(conn)
            # await ops.create_user(username='emperor22', email='xx@xx.com', first_name='andi', last_name='putra', hashed_password='satuduatiga')

            await read_sql_async('delete from prompt_and_result', conn, True)
            await read_sql_async('delete from task_run', conn, True)
    asyncio.run(func())
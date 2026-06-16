from fastapi import Depends



import redis

from app.core.config import Config

from app.crud.models import Base, base_engine_sync, SessionLocal
from app.crud.queries import PromptTableOperation, TaskRunTableOperation, UserTableOperation, UserCustomizedTasksTableOperation


def create_tables():
    Base.metadata.create_all(base_engine_sync)
    

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
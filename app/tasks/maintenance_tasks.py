from app.crud import (
    PromptTableOperation,
    base_engine_sync,
)

from app.schemas import (
    TaskStatus,
)

from app.logger import logger
from app.config import Config

from celery import shared_task

import redis
import shutil

import os



# runs every day at end of day
@shared_task(name='update_last_accessed_at_task')
def update_last_accessed_at_db():
    try:
        conn_pool = redis.ConnectionPool.from_url(Config.REDIS_URL)
        redis_client = redis.Redis(connection_pool=conn_pool, decode_responses=True)

        temp_hashtable_name = f"{Config.REDIS_LAST_ACCESSED_HASHTABLE_NAME}_temp"
        redis_client.rename(
            Config.REDIS_LAST_ACCESSED_HASHTABLE_NAME, temp_hashtable_name
        )
        req_id_last_accessed_dct = redis_client.hgetall(temp_hashtable_name)

        with base_engine_sync.begin() as conn:
            prompt_table_ops = PromptTableOperation(conn_sync=conn)
            prompt_table_ops.update_last_accessed_column_sync(req_id_last_accessed_dct)

        redis_client.delete(temp_hashtable_name)
    except redis.exceptions.ResponseError:
        logger.error(
            "cannot find last accessed hashtable on redis on update_last_accessed_at celery task"
        )
    finally:
        conn_pool.disconnect()


# runs at first of each month
@shared_task(name='cleanup_unused_datasets_task')
def cleanup_unused_datasets():

    with base_engine_sync.begin() as conn:
        prompt_table_ops = PromptTableOperation(conn_sync=conn)
        res = prompt_table_ops.get_least_accessed_request_ids_sync(
            Config.THRES_DELETE_UNUSED_DATASET_DAYS
        )

    if res:
        # change status to deleted
        with base_engine_sync.begin() as conn:
            for req_id in res:
                prompt_table_ops = PromptTableOperation(conn_sync=conn)
                prompt_table_ops.change_request_status_sync(
                    req_id, TaskStatus.deleted_because_not_accessed_recently.value
                )

        # delete files
        for req_id in res:
            path_delete = f"{Config.DATASET_SAVE_PATH}/{req_id}"

            if os.path.exists(path_delete):
                shutil.rmtree(path_delete)

            logger.info(f"deleted {req_id} files on cleanup function")
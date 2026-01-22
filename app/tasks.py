from celery import Celery
import json
import time

from app.services import (get_prompt_result, process_llm_api_response, DataAnalysisProcessor, insert_prompt_context, cleanup_agg_col_names, 
                          resp_pt1_loader, resp_pt2_loader, mock_resp_pt1_loader, mock_resp_pt2_loader, write_prompt_and_res, 
                          addt_req_resp_loader, mock_addt_req_resp)
from app.crud import TaskRunTableOperation, PromptTableOperation, BlacklistedDatasetsTableOperation, base_engine_sync

from app.schemas import DataTasks, DatasetAnalysisModelPartOne, DatasetAnalysisModelPartTwo, TaskStatus, RunInfoSchema, TaskProcessingRunType
from app.logger import logger
from app.config import Config
from pydantic import ValidationError
from requests.exceptions import RequestException
from typing import Literal

import redis
import shutil

from celery.schedules import crontab

from psycopg import OperationalError



app = Celery('tasks', backend=Config.REDIS_URL, broker=Config.REDIS_URL)
app.conf.task_routes = {'tasks.get_prompt_result_task': {'queue': 'get_prompt_res_queue'}, 
                        'tasks.get_additional_analyses_prompt_result': {'queue': 'get_prompt_res_queue'},
                        'tasks.data_processing_task': {'queue': 'data_processing_queue'}
                        }

@app.task
def periodic_task():
    logger.info('doing task')

app.conf.beat_schedule = {
    # 'update_last_accessed_at_db': {'task': 'tasks.update_last_accessed_at_db','schedule': crontab(hour=1)}, 
    # 'cleanup_unused_datasets': {'task': 'tasks.cleanup_unused_datasets', 'schedule': crontab(day_of_month=1)}
    'periodic_task': {'task': 'tasks.periodic_task', 'schedule': 10.0}
}
class DatabaseTask(app.Task):
    def get_engine(self):
        return base_engine_sync 

class BlacklistedDatasetException(Exception):
    pass

retry_for_exceptions_get_prompt_task = [ValidationError, RequestException, OperationalError]

@app.task(bind=True, base=DatabaseTask, name='tasks.get_prompt_result_task', acks_late=True, time_limit=200, max_retries=7, rate_limit='15/m', 
          retry_backoff=True, retry_backoff_max=60, autoretry_for=retry_for_exceptions_get_prompt_task)
def get_prompt_result_task(self, model, prompt_pt_1, task_count, dataset_id, request_id, user_id, 
                           dataset_cols, debug_prompt_and_res=False, mock_pt1_resp_file=None, mock_pt2_resp_file=None):
    
    logger.info(f'initial task request processed: request_id {request_id}, user_id {user_id}')
    
    
    start_time = time.perf_counter()
    
    
    # TO DO:
    # catch requestlimit error and throttle for 60 seconds
    # llm api request limit reset at midnight pacific time
    
    engine = self.get_engine() 
    
    with engine.connect() as conn:
        prompt_table_ops = PromptTableOperation(conn_sync=conn)
        prompt_table_ops.change_request_status_sync(request_id=request_id, status=TaskStatus.waiting_for_initial_request_prompt.value)
        
        blacklist_table_ops = BlacklistedDatasetsTableOperation(conn_sync=conn)
        is_blacklisted = blacklist_table_ops.check_if_blacklisted(dataset_id)
        
        if is_blacklisted:
            logger.warning(f'user uploaded blacklisted dataset: request_id {request_id}, user_id {user_id}')
            prompt_table_ops.change_request_status_sync(request_id=request_id, status=TaskStatus.failed_because_blacklisted_dataset.value)
            raise BlacklistedDatasetException

        if is_blacklisted is None:
            blacklist_table_ops.add_dataset_to_table(dataset_id)
    
    # getting result for prompt part 1
    
    if mock_pt1_resp_file:
        resp_pt_1 = mock_resp_pt1_loader(mock_pt1_resp_file)
    else:
        resp_pt_1 = resp_pt1_loader(prompt_pt_1, model)
    
    if debug_prompt_and_res:
        write_prompt_and_res(prompt_pt_1, resp_pt_1, 1, Config.DEBUG_PROMPT_AND_RES_SAVE_DIR)
    
    
    try:
        resp_pt_1 = DatasetAnalysisModelPartOne.model_validate(resp_pt_1, context={'required_cols': dataset_cols, 'request_id': request_id})
    except ValidationError:
        with engine.connect() as conn:
            blacklist_table_ops = BlacklistedDatasetsTableOperation(conn_sync=conn)
            blacklist_table_ops.increment_failed_attempt(dataset_id)
        
        logger.exception(f'failed 1st part response validation: request_id {request_id}, user_id {user_id}, resp -> {resp_pt_1}')
        raise
    
    resp_pt_1 = resp_pt_1.model_dump()

    # getting result for prompt part 2
    prompt_2_context = {'context_json': resp_pt_1, 'task_count': task_count}
    prompt_pt_2 = insert_prompt_context(prompt_file=Config.PT2_PROMPT_TEMPLATE, context=prompt_2_context)
    
    if mock_pt2_resp_file:
        resp_pt_2 = mock_resp_pt2_loader(mock_pt2_resp_file)
    else:
        resp_pt_2 = resp_pt2_loader(prompt_pt_2, model)
    
    if debug_prompt_and_res:
        write_prompt_and_res(prompt_pt_2, resp_pt_2, 2, Config.DEBUG_PROMPT_AND_RES_SAVE_DIR)
    
    try:
        resp_pt_2 = DatasetAnalysisModelPartTwo.model_validate(resp_pt_2, context={'run_type': TaskProcessingRunType.first_run_after_request.value, 
                                                                                   'request_id': request_id})
    except ValidationError:
        with engine.connect() as conn:
            blacklist_table_ops = BlacklistedDatasetsTableOperation(conn_sync=conn)
            blacklist_table_ops.increment_failed_attempt(dataset_id)
        
        logger.exception(f'failed 2nd part response validation: request_id {request_id}, user_id {user_id}, resp -> {resp_pt_2}')
        raise
    
    resp_pt_2 = resp_pt_2.model_dump()
    
    # handle cases where a step's column contains _{agg} suffix when its preceded by a groupby step
    resp_pt_2 = cleanup_agg_col_names(resp_pt_2=resp_pt_2, resp_pt_1=resp_pt_1)
    result = {**resp_pt_1, **resp_pt_2}
    
    with engine.connect() as conn:
        prompt_table_ops = PromptTableOperation(conn_sync=conn)
        prompt_table_ops.insert_prompt_result_sync(request_id=request_id, prompt_result=json.dumps(result))
        prompt_table_ops.change_request_status_sync(request_id=request_id, status=TaskStatus.initial_request_prompt_received.value)

    # write response, token_count to main table
    data_tasks_fields = DataTasks.model_fields.keys()
    data_tasks_dct = {k: v for k, v in result.items() if k in data_tasks_fields}

    data_tasks = DataTasks.model_validate(data_tasks_dct)
    
    with engine.connect() as conn:
        blacklist_table_ops = BlacklistedDatasetsTableOperation(conn_sync=conn)
        blacklist_table_ops.reset_failed_attempt_count(dataset_id)
    
    process_time_ms = round((time.perf_counter() - start_time) * 1000, 2)

    logger.info(f'initial task request finished in {process_time_ms} ms: request_id {request_id}, user_id {user_id}')
    
    if process_time_ms > Config.THRES_SLOW_INITIAL_REQUEST_PROCESS_TIME_MS:
        logger.warning(f'slow initial request processing time ({process_time_ms} ms): request_id {request_id}, user_id {user_id}')
    
    return data_tasks.model_dump()

@app.task(bind=True, base=DatabaseTask, name='tasks.get_additional_analyses_prompt_result', acks_late=True, time_limit=200, max_retries=3, rate_limit='15/m', 
          retry_backoff=True, retry_backoff_max=60, autoretry_for=retry_for_exceptions_get_prompt_task)
def get_additional_analyses_prompt_result(self, model, new_tasks_prompt, request_id, user_id, mock_addt_request_resp_file=None):

    logger.info(f'additional analyses task request processed: request_id {request_id}, user_id {user_id}')

    start_time = time.perf_counter()

    prompt_template = Config.ADDT_REQ_PROMPT_TEMPLATE
    
    engine = self.get_engine()
    
    with engine.connect() as conn:
        prompt_table_ops = PromptTableOperation(conn_sync=conn)
        prompt_table_ops.change_request_status_sync(request_id=request_id, status=TaskStatus.waiting_for_additional_analysis_prompt_result.value)
    
        resp_pt_1 = prompt_table_ops.get_prompt_result_sync(request_id=request_id, user_id=user_id)
    
    resp_pt_1 = json.loads(resp_pt_1['prompt_result'])
    
    context_json = {}
    for field in ['columns', 'common_column_cleaning_or_transformation', 'common_column_combination']:
        context_json[field] = resp_pt_1[field]
    
    
    context = {'context_json': json.dumps(context_json), 'new_tasks_prompt': new_tasks_prompt}
    prompt = insert_prompt_context(prompt_file=prompt_template, context=context)
        

            
    if mock_addt_request_resp_file:
        resp = mock_addt_req_resp(mock_addt_request_resp_file)
    else:
        resp = addt_req_resp_loader(prompt, model)
    
    try:
        resp = DatasetAnalysisModelPartTwo.model_validate(resp, context={'run_type': TaskProcessingRunType.additional_analyses_request.value, 
                                                                         'request_id': request_id})
    except ValidationError:
        logger.exception(f'failed additional analyses response validation: request_id {request_id}, user_id {user_id}, resp -> {resp}')
        raise
        
    resp = resp.model_dump()
    
    with engine.connect() as conn:
        prompt_table_ops = PromptTableOperation(conn_sync=conn)
        prompt_table_ops.insert_additional_analyses_prompt_result_sync(request_id=request_id, additional_analyses_prompt_result=json.dumps(resp))
        prompt_table_ops.change_request_status_sync(request_id=request_id, status=TaskStatus.additional_analysis_prompt_result_received.value)
    
    data_tasks = DataTasks(common_tasks=resp['common_tasks'], common_column_cleaning_or_transformation=[], common_column_combination=[])
    
    process_time_ms = round((time.perf_counter() - start_time) * 1000, 2)
    
    logger.info(f'additional analyses task request finished in {process_time_ms} ms: request_id {request_id}, user_id {user_id}')
    
    if process_time_ms > Config.THRES_SLOW_INITIAL_REQUEST_PROCESS_TIME_MS:
        logger.warning(f'slow initial request processing time ({process_time_ms} ms): request_id {request_id}, user_id {user_id}')
    

    return data_tasks.model_dump()

@app.task(bind=True, base=DatabaseTask, name='tasks.data_processing_task', acks_late=True, ignore_result=True, time_limit=20, max_retries=3)
def data_processing_task(self, data_tasks_dict, run_info: RunInfoSchema, 
                         run_type):
    request_id = run_info['request_id']
    user_id = run_info['user_id']
    
    logger.info(f"task execution request processed: run type {run_type}, request_id {request_id}, user_id {user_id}")
    
    start_time = time.perf_counter()
    
    starting_status_dct = {TaskProcessingRunType.first_run_after_request.value: TaskStatus.doing_initial_tasks_run.value, 
                            TaskProcessingRunType.modified_tasks_execution.value: TaskStatus.doing_customized_tasks_run.value, 
                            TaskProcessingRunType.additional_analyses_request.value: TaskStatus.doing_additional_tasks_run.value, 
                            TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value: TaskStatus.doing_customized_tasks_run.value}
    
    finished_status_dct = {TaskProcessingRunType.first_run_after_request.value: TaskStatus.initial_tasks_run_finished.value, 
                            TaskProcessingRunType.modified_tasks_execution.value: TaskStatus.customized_tasks_run_finished.value, 
                            TaskProcessingRunType.additional_analyses_request.value: TaskStatus.additional_tasks_run_finished.value, 
                            TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value: TaskStatus.doing_customized_tasks_run.value}
    
    engine = self.get_engine()
    
    with engine.connect() as conn:
        prompt_table_ops = PromptTableOperation(conn_sync=conn)
        prompt_table_ops.change_request_status_sync(request_id=request_id, status=starting_status_dct[run_type])
    
    data_tasks = DataTasks.model_validate(data_tasks_dict)
    
    with engine.connect() as conn:
        task_run_table_ops = TaskRunTableOperation(conn_sync=conn)
        if run_type == TaskProcessingRunType.first_run_after_request.value and not task_run_table_ops.request_id_exists(request_id):
            task_run_table_ops.add_task_result_sync(request_id=request_id, user_id=user_id)
        
        processor = DataAnalysisProcessor(data_tasks=data_tasks, run_info=run_info, task_run_table_ops=task_run_table_ops, run_type=run_type)
        processor.process_all_tasks()
        
    with engine.connect() as conn:
        prompt_table_ops = PromptTableOperation(conn_sync=conn)
        prompt_table_ops.change_request_status_sync(request_id=request_id, status=finished_status_dct[run_type])
    
    process_time_ms = round((time.perf_counter() - start_time) * 1000, 2)
    
    logger.info(f"task execution request finished in {process_time_ms} ms: run type {run_type}, request_id {request_id}, user_id {user_id}")

    if process_time_ms > Config.THRES_SLOW_TASK_EXECUTION_PROCESS_TIME_MS:
        logger.warning(f"slow task execution request processing time ({process_time_ms} ms): run_type {run_type}, request_id {request_id}, user_id {user_id}")
        

   


# runs every day at end of day
@app.task
def update_last_accessed_at_db(db_engine):
    conn_pool = redis.ConnectionPool.from_url(Config.REDIS_URL)
    redis_client = redis.Redis(connection_pool=conn_pool)
    
    temp_hashtable_name = f'{Config.REDIS_LAST_ACCESSED_HASHTABLE_NAME}_temp'
    redis_client.rename(Config.REDIS_LAST_ACCESSED_HASHTABLE_NAME, temp_hashtable_name)
    req_id_last_accessed_dct = redis_client.hgetall(temp_hashtable_name)

    with db_engine.connect() as conn:
        prompt_table_ops = PromptTableOperation(conn_sync=conn)
        prompt_table_ops.update_last_accessed_column(req_id_last_accessed_dct)
    
    redis_client.delete(temp_hashtable_name)
    
    conn_pool.disconnect()

# runs at first of each month
@app.task
def cleanup_unused_datasets(db_engine):
    THRES_DELETE_UNUSED_DATASETS_DAYS = 7
    
    with db_engine.connect() as conn:
        prompt_table_ops = PromptTableOperation(conn_sync=conn)
        res = prompt_table_ops.get_least_accessed_request_ids(THRES_DELETE_UNUSED_DATASETS_DAYS)
        
    if res:
        for req_id in res:
            path_delete = f'{Config.DATASET_SAVE_PATH}/{req_id}'
            shutil.rmtree(path_delete)
            
            with db_engine.connect() as conn:
                prompt_table_ops = PromptTableOperation(conn_sync=conn)
                res = prompt_table_ops.change_request_status_sync(req_id, TaskStatus.deleted_because_not_accessed_recently.value)
                
            logger.info(f'deleted {req_id} files on cleanup function')
            

    
        


    


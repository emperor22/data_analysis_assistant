from celery import Celery
import json
import time


from app.services.analysis import (DataAnalysisProcessor)
from app.services.infra import send_email_sync
from app.services.llm import (insert_prompt_context, cleanup_agg_col_names, resp_loader, mock_resp_loader, 
                              write_prompt_and_res)
from app.exceptions import BlacklistedDatasetException, RateLimitedException, RetryableRateLimitException, TerminalRateLimitException, RetryableValidationException

from app.crud import TaskRunTableOperation, PromptTableOperation, BlacklistedDatasetsTableOperation, base_engine_sync, UserTableOperation

from app.schemas import DataTasks, DatasetAnalysisModelPartOne, DatasetAnalysisModelPartTwo, TaskStatus, RunInfo, TaskProcessingRunType
from app.logger import logger
from app.config import Config, api_key_dct
from pydantic import ValidationError
from requests.exceptions import RequestException
from typing import Literal

import redis
import shutil

from celery.schedules import crontab

from psycopg import OperationalError

import smtplib

import psycopg
import os

from datetime import datetime



app = Celery('tasks', backend=Config.REDIS_URL, broker=Config.REDIS_URL)
app.conf.task_routes = {'tasks.get_prompt_result_task': {'queue': 'io_tasks_queue'}, 
                        'tasks.get_additional_analyses_prompt_result': {'queue': 'io_tasks_queue'},
                        'tasks.data_processing_task': {'queue': 'cpu_tasks_queue'}, 
                        'tasks.send_email_task': {'queue': 'io_tasks_queue'}
                        }

@app.task
def periodic_task():
    logger.info('doing task')

app.conf.beat_schedule = {
    # 'update_last_accessed_at_db': {'task': 'tasks.update_last_accessed_at_db','schedule': crontab(hour=1)}, 
    # 'cleanup_unused_datasets': {'task': 'tasks.cleanup_unused_datasets', 'schedule': crontab(day_of_month=1)}
    'periodic_task': {'task': 'tasks.periodic_task', 'schedule': 10.0}
}


# exception handlers

def handle_validation_error_prompt_task(error, user_id, request_id, resp, dataset_id, blacklist_table_ops: BlacklistedDatasetsTableOperation, 
                                        prompt_table_ops: PromptTableOperation):
    if blacklist_table_ops.check_if_blacklisted(dataset_id) is None:
        blacklist_table_ops.add_dataset_to_table(dataset_id)
    
    if blacklist_table_ops.get_failed_attempt_count(dataset_id) < Config.FAILED_ATTEMPT_THRESHOLD_FOR_BLACKLIST:
        blacklist_table_ops.increment_failed_attempt(dataset_id)
        logger.warning(f'failed llm response validation: request_id {request_id}, user_id {user_id}, resp -> {resp}')
        logger.exception(error)
        raise error
    
    
    prompt_table_ops.change_request_status_sync(request_id=request_id, status=TaskStatus.failed_because_blacklisted_dataset.value)
    blacklist_table_ops.mark_as_blacklisted(dataset_id)
    
    logger.warning(f'dataset blacklisted: dataset_id {dataset_id}, request_id {request_id}')
    
    raise BlacklistedDatasetException

def handle_rate_limit_exception_prompt_task(user_id, request_id, prompt_table_ops: PromptTableOperation):
    if prompt_table_ops.check_rate_limit_retry_count(user_id, request_id) < Config.RATE_LIMIT_RETRY_COUNT_CAP:
        logger.warning(f'retrying due to rate limit: request_id {request_id}, user_id {user_id}')   
        prompt_table_ops.increment_rate_limit_retry_count_sync(user_id, request_id)
        raise RetryableRateLimitException
    
    prompt_table_ops.change_request_status_sync(request_id=request_id, status=TaskStatus.failed_because_rate_limited.value)
    prompt_table_ops.reset_rate_limit_retry_count_sync(user_id=user_id, request_id=request_id)
    
    logger.warning(f'request hit llm endpoint rate limit: request_id {request_id}, user_id {user_id}')
    
    raise TerminalRateLimitException



####################


class DatabaseTask(app.Task):
    def get_engine(self):
        return base_engine_sync
    


retry_for_exceptions_send_email_task = [smtplib.SMTPServerDisconnected, TimeoutError, ConnectionResetError]

@app.task(bind=True, name='tasks.send_email_task', acks_late=True, max_retries=3, autoretry_for=retry_for_exceptions_send_email_task)
def send_email_task(self, subject: str, receiver: str, body: str, attachment_path: str | None = None):
    send_email_sync(receiver=receiver, subject=subject, body=body, attachment_path=attachment_path)



retry_for_exceptions_get_prompt_task = [ValidationError, RequestException, OperationalError, RetryableRateLimitException]

@app.task(bind=True, base=DatabaseTask, name='tasks.get_prompt_result_task', acks_late=True, time_limit=200, max_retries=Config.MAX_RETRIES_GET_PROMPT_RESULT_TASK, rate_limit='15/m', 
          retry_backoff=3, retry_backoff_max=60, autoretry_for=retry_for_exceptions_get_prompt_task)
def get_prompt_result_task(self, model, provider, api_key, prompt_pt_1, task_count, dataset_id, request_id, user_id, 
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
        
        dataset_blacklisted = blacklist_table_ops.check_if_blacklisted(dataset_id)
        if dataset_blacklisted is not None and dataset_blacklisted:
            raise BlacklistedDatasetException

    
        # getting result for prompt part 1
    
        if mock_pt1_resp_file:
            resp_pt_1 = mock_resp_loader(mock_pt1_resp_file, pt=1)
        else:
            try:
                resp_pt_1 = resp_loader(prompt_pt_1, model, provider, api_key)
            except RateLimitedException:
                handle_rate_limit_exception_prompt_task(user_id=user_id, request_id=request_id, prompt_table_ops=prompt_table_ops)
        
        
        if debug_prompt_and_res:
            write_prompt_and_res(prompt=prompt_pt_1, res=resp_pt_1, part=1, request_id=request_id, dir_=Config.DEBUG_PROMPT_AND_RES_SAVE_DIR)
        
        
        try:
            resp_pt_1 = DatasetAnalysisModelPartOne.model_validate(resp_pt_1, context={'run_type': TaskProcessingRunType.first_run_after_request.value,
                                                                                       'required_cols': dataset_cols, 
                                                                                       'request_id': request_id})
        except ValidationError as e:
            handle_validation_error_prompt_task(error=e, user_id=user_id, request_id=request_id, resp=resp_pt_1, dataset_id=dataset_id, 
                                                blacklist_table_ops=blacklist_table_ops, prompt_table_ops=prompt_table_ops)
        
        resp_pt_1 = resp_pt_1.model_dump()

        # getting result for prompt part 2
        prompt_2_context = {'context_json': resp_pt_1, 'task_count': task_count, 'current_time': datetime.now().strftime('%H:%M:%S'),}
        prompt_pt_2 = insert_prompt_context(prompt_file=Config.PT2_PROMPT_TEMPLATE, context=prompt_2_context)
        
        if mock_pt2_resp_file:
            resp_pt_2 = mock_resp_loader(mock_pt2_resp_file, pt=2)
        else:
            try:
                resp_pt_2 = resp_loader(prompt_pt_2, model, provider, api_key)
            except RateLimitedException:
                handle_rate_limit_exception_prompt_task(user_id=user_id, request_id=request_id, prompt_table_ops=prompt_table_ops)
        
        if debug_prompt_and_res:
            write_prompt_and_res(prompt=prompt_pt_2, res=resp_pt_2, part=2, request_id=request_id, dir_=Config.DEBUG_PROMPT_AND_RES_SAVE_DIR)

        
        try:
            resp_pt_2 = DatasetAnalysisModelPartTwo.model_validate(resp_pt_2, context={'run_type': TaskProcessingRunType.first_run_after_request.value, 
                                                                                       'request_id': request_id})
        except ValidationError as e:
            handle_validation_error_prompt_task(error=e, user_id=user_id, request_id=request_id, resp=resp_pt_1, dataset_id=dataset_id, 
                                                blacklist_table_ops=blacklist_table_ops, prompt_table_ops=prompt_table_ops)
        resp_pt_2 = resp_pt_2.model_dump()
        
        # handle cases where a step's column contains _{agg} suffix when its preceded by a groupby step
        resp_pt_2 = cleanup_agg_col_names(resp_pt_2=resp_pt_2, resp_pt_1=resp_pt_1)
        result = {**resp_pt_1, **resp_pt_2}

        prompt_table_ops.insert_prompt_result_sync(request_id=request_id, prompt_result=json.dumps(result))
        prompt_table_ops.change_request_status_sync(request_id=request_id, status=TaskStatus.initial_request_prompt_received.value)

        data_tasks_fields = DataTasks.model_fields.keys()
        data_tasks_dct = {k: v for k, v in result.items() if k in data_tasks_fields}

        data_tasks = DataTasks.model_validate(data_tasks_dct, context={'run_type': TaskProcessingRunType.first_run_after_request.value, 
                                                                       'is_from_data_tasks': True,
                                                                       'required_cols': dataset_cols,
                                                                       'request_id': request_id})
        
        blacklist_table_ops.reset_failed_attempt_count(dataset_id) # reset if prompt results are valid
    
        process_time_ms = round((time.perf_counter() - start_time) * 1000, 2)

        logger.info(f'initial task request finished in {process_time_ms} ms: request_id {request_id}, user_id {user_id}')
        
        if process_time_ms > Config.THRES_SLOW_INITIAL_REQUEST_PROCESS_TIME_MS:
            logger.warning(f'slow initial request processing time ({process_time_ms} ms): request_id {request_id}, user_id {user_id}')
        
        return data_tasks.model_dump()

retry_for_exceptions_addt_analyses_requset = [RequestException, OperationalError, RetryableRateLimitException]

@app.task(bind=True, base=DatabaseTask, name='tasks.get_additional_analyses_prompt_result', acks_late=True, time_limit=200, max_retries=Config.MAX_RETRIES_ADDT_ANALYSES_REQ_TASK, 
          rate_limit='15/m', retry_backoff=3, retry_backoff_max=60, autoretry_for=retry_for_exceptions_addt_analyses_requset)
def get_additional_analyses_prompt_result(self, model, provider, api_key, new_tasks_prompt, request_id, user_id, debug_prompt_and_res=True, 
                                          mock_addt_request_resp_file=None):

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
        
        
        context = {'context_json': json.dumps(context_json), 'new_tasks_prompt': new_tasks_prompt, 'current_time': datetime.now().strftime('%H:%M:%S')}
        prompt = insert_prompt_context(prompt_file=prompt_template, context=context)
            

                
        if mock_addt_request_resp_file:
            resp = mock_resp_loader(mock_addt_request_resp_file, pt='addt_analyses')
        else:
            try:
                resp = resp_loader(prompt, model, provider, api_key)
            except RateLimitedException:
                handle_rate_limit_exception_prompt_task(user_id=user_id, request_id=request_id, prompt_table_ops=prompt_table_ops)
            
        if debug_prompt_and_res:
            write_prompt_and_res(prompt=prompt, res=resp, part='addt_request', request_id=request_id, dir_=Config.DEBUG_PROMPT_AND_RES_SAVE_DIR)

        try:
            resp = DatasetAnalysisModelPartTwo.model_validate(resp, context={'run_type': TaskProcessingRunType.additional_analyses_request.value, 
                                                                            'request_id': request_id})
        except ValidationError:
            logger.exception(f'failed additional analyses response validation: request_id {request_id}, user_id {user_id}, resp -> {resp}')
            prompt_table_ops.change_request_status_sync(request_id=request_id, status=TaskStatus.additional_analysis_invalid_resp.value)
            raise
            
        resp = resp.model_dump()

        prompt_table_ops.insert_additional_analyses_prompt_result_sync(request_id=request_id, additional_analyses_prompt_result=json.dumps(resp))
        prompt_table_ops.change_request_status_sync(request_id=request_id, status=TaskStatus.additional_analysis_prompt_result_received.value)
    
        data_tasks_dct = {'common_tasks': resp['common_tasks']}
        data_tasks = DataTasks.model_validate(data_tasks_dct, context={'run_type': TaskProcessingRunType.additional_analyses_request.value, 
                                                                       'request_id': request_id, 
                                                                       'is_from_data_tasks': True})
        
        process_time_ms = round((time.perf_counter() - start_time) * 1000, 2)
        
        logger.info(f'additional analyses task request finished in {process_time_ms} ms: request_id {request_id}, user_id {user_id}')
        
        if process_time_ms > Config.THRES_SLOW_INITIAL_REQUEST_PROCESS_TIME_MS:
            logger.warning(f'slow initial request processing time ({process_time_ms} ms): request_id {request_id}, user_id {user_id}')
        

        return data_tasks.model_dump()



retry_for_exceptions_data_processing_task = [psycopg.OperationalError, psycopg.DatabaseError]

@app.task(bind=True, base=DatabaseTask, name='tasks.data_processing_task', acks_late=True, ignore_result=True, time_limit=20, max_retries=3, 
          autoretry_for=retry_for_exceptions_data_processing_task)
def data_processing_task(self, data_tasks_dict, run_info, run_type):
    run_info = RunInfo(**run_info)
    
    request_id = run_info.request_id
    user_id = run_info.user_id
    
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
        task_run_table_ops = TaskRunTableOperation(conn_sync=conn)
        
        prompt_table_ops.change_request_status_sync(request_id=request_id, status=starting_status_dct[run_type])
    
        data_tasks = DataTasks.model_validate(data_tasks_dict, context={'run_type': run_type, 
                                                                        'request_id': request_id, 
                                                                        'is_from_data_tasks': True})
        
        if run_type == TaskProcessingRunType.first_run_after_request.value and not task_run_table_ops.request_id_exists(request_id):
            task_run_table_ops.add_task_result_sync(request_id=request_id, user_id=user_id)
        
        processor = DataAnalysisProcessor(data_tasks=data_tasks, run_info=run_info, task_run_table_ops=task_run_table_ops, run_type=run_type)
        processor.process_all_tasks()

        prompt_table_ops.change_request_status_sync(request_id=request_id, status=finished_status_dct[run_type])
    
        process_time_ms = round((time.perf_counter() - start_time) * 1000, 2)
        
        logger.info(f"task execution request finished in {process_time_ms} ms: run type {run_type}, request_id {request_id}, user_id {user_id}")

        if process_time_ms > Config.THRES_SLOW_TASK_EXECUTION_PROCESS_TIME_MS:
            logger.warning(f"slow task execution request processing time ({process_time_ms} ms): run_type {run_type}, request_id {request_id}, user_id {user_id}")
            
        return
   


# runs every day at end of day
@app.task
def update_last_accessed_at_db():
    try:
        conn_pool = redis.ConnectionPool.from_url(Config.REDIS_URL)
        redis_client = redis.Redis(connection_pool=conn_pool)
        
        temp_hashtable_name = f'{Config.REDIS_LAST_ACCESSED_HASHTABLE_NAME}_temp'
        redis_client.rename(Config.REDIS_LAST_ACCESSED_HASHTABLE_NAME, temp_hashtable_name)
        req_id_last_accessed_dct = redis_client.hgetall(temp_hashtable_name)

        with base_engine_sync.connect() as conn:
            prompt_table_ops = PromptTableOperation(conn_sync=conn)
            prompt_table_ops.update_last_accessed_column_sync(req_id_last_accessed_dct)
        
        redis_client.delete(temp_hashtable_name)
    finally:
        conn_pool.disconnect()

# runs at first of each month
@app.task
def cleanup_unused_datasets():
    THRES_DELETE_UNUSED_DATASETS_DAYS = 7
    
    with base_engine_sync.connect() as conn:
        prompt_table_ops = PromptTableOperation(conn_sync=conn)
        res = prompt_table_ops.get_least_accessed_request_ids_sync(THRES_DELETE_UNUSED_DATASETS_DAYS)
        
    if res:
        for req_id in res:
            path_delete = f'{Config.DATASET_SAVE_PATH}/{req_id}'
            if os.path.exists(path_delete):
                shutil.rmtree(path_delete)
            
            with base_engine_sync.connect() as conn:
                prompt_table_ops = PromptTableOperation(conn_sync=conn)
                res = prompt_table_ops.change_request_status_sync(req_id, TaskStatus.deleted_because_not_accessed_recently.value)
                
            logger.info(f'deleted {req_id} files on cleanup function')
            

    
        


    


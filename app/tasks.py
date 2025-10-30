from celery import Celery
import json
import time

from app.services import get_prompt_result, process_llm_api_response, DataAnalysisProcessor, insert_prompt_context, cleanup_agg_col_names
from app.crud import TaskRunTableOperation, PromptTableOperation, base_engine_sync

from app.schemas import DataTasks, DatasetAnalysisModelPartOne, DatasetAnalysisModelPartTwo, TaskStatus
from app.logger import logger
from pydantic import ValidationError
from requests.exceptions import RequestException
from typing import Literal

THRES_SLOW_INITIAL_REQUEST_PROCESS_TIME_MS = 90 * 1000
THRES_SLOW_ADDITIONAL_ANALYSES_REQUEST_PROCESS_TIME_MS = 45 * 1000
THRES_SLOW_TASK_EXECUTION_PROCESS_TIME_MS = 7 * 1000
REDIS_URL = 'redis://localhost:6379/0'
# REDIS_URL = 'redis://redis:6379/0'

app = Celery('tasks', backend=REDIS_URL, broker=REDIS_URL)
app.conf.task_routes = {'tasks.get_prompt_result_task': {'queue': 'get_prompt_res_queue'}, 
                        'tasks.get_additional_analyses_prompt_result': {'queue': 'get_prompt_res_queue'},
                        'tasks.data_processing_task': {'queue': 'data_processing_queue'}
                        }



class DatabaseTask(app.Task):
    def get_engine(self):
        return base_engine_sync 


# if it persists, write 'not enough common tasks, probably invalid dataset' status to prompt_and_result and (maybe) blacklist the dataset

retry_for_exceptions_get_prompt_task = [ValidationError, RequestException]

@app.task(bind=True, base=DatabaseTask, name='tasks.get_prompt_result_task', acks_late=True, time_limit=200, max_retries=3, rate_limit='15/m', 
          retry_backoff=True, retry_backoff_max=60, autoretry_for=retry_for_exceptions_get_prompt_task)
def get_prompt_result_task(self, model, prompt_pt_1, task_count, request_id, user_id, dataset_cols):
    
    logger.info(f'initial task request processed: request_id {request_id}, user_id {user_id}')
    
    start_time = time.perf_counter()
    
    # TO DO:
    # catch requestlimit error and throttle for 60 seconds
    # llm api request limit reset at midnight pacific time
    engine = self.get_engine() 
    
    # with engine.connect() as conn:
    #     prompt_table_ops = PromptTableOperation(conn_sync=conn)
    #     prompt_table_ops.change_request_status_sync(request_id=request_id, status=TaskStatus.waiting_for_initial_request_prompt.value)
    
    # # getting result for prompt part 1
    # resp_pt_1 = get_prompt_result(prompt_pt_1, model)     
    # resp_pt_1 = process_llm_api_response(resp_pt_1)
    
    # with open(f'resp_jsons/resp_pt1.json', 'w') as f:       # writing to file to check result directly for debugging
    #     f.write(json.dumps(resp_pt_1, indent=4))
    
    with open(f'resp_jsons/resp_pt1.json', 'r') as f:      # mocking part 1 response
        resp_pt_1 = json.load(f)
    
    try:
        resp_pt_1 = DatasetAnalysisModelPartOne.model_validate(resp_pt_1, context={'required_cols': dataset_cols, 'request_id': request_id})
    except ValidationError:
        logger.exception(f'failed first part response validation: request_id {request_id}, user_id {user_id}, resp -> {resp_pt_1}')
        raise
    
    resp_pt_1 = resp_pt_1.model_dump()

    # # getting result for prompt part 2
    # prompt_pt_2_file = 'app/prompts/split_prompt/prompt_part2.md'
    # prompt_2_context = {'context_json': resp_pt_1, 'task_count': task_count}
    # prompt_pt_2 = insert_prompt_context(prompt_file=prompt_pt_2_file, context=prompt_2_context)
    
    # resp_pt_2 = get_prompt_result(prompt_pt_2, model)     
    # resp_pt_2 = process_llm_api_response(resp_pt_2)
    
    # with open(f'resp_jsons/resp_pt2.json', 'w') as f:     # writing to file to check result directly for debugging
    #     f.write(json.dumps(resp_pt_2, indent=4))
    
    with open(f'resp_jsons/resp_pt2.json', 'r') as f:        # mocking part 2 response
        resp_pt_2 = json.load(f)
    
    try:
        resp_pt_2 = DatasetAnalysisModelPartTwo.model_validate(resp_pt_2, context={'run_type': 'first_run_after_request', 'request_id': request_id})
    except ValidationError:
        logger.exception(f'failed first part response validation: request_id {request_id}, user_id {user_id}, resp -> {resp_pt_2}')
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
    
    data_tasks = DataTasks(
        columns=result['columns'],
        common_tasks=result['common_tasks'], 
        common_column_cleaning_or_transformation=result['common_column_cleaning_or_transformation'],
        common_column_combination=result['common_column_combination']
        )
    
    process_time_ms = round((time.perf_counter() - start_time) * 1000, 2)

    logger.info(f'initial task request finished in {process_time_ms} ms: request_id {request_id}, user_id {user_id}')
    
    if process_time_ms > THRES_SLOW_INITIAL_REQUEST_PROCESS_TIME_MS:
        logger.warning(f'slow initial request processing time ({process_time_ms} ms): request_id {request_id}, user_id {user_id}')
    
    return data_tasks.model_dump()

@app.task(bind=True, base=DatabaseTask, name='tasks.get_additional_analyses_prompt_result', acks_late=True, time_limit=200, max_retries=3, rate_limit='15/m', 
          retry_backoff=True, retry_backoff_max=60, autoretry_for=retry_for_exceptions_get_prompt_task)
def get_additional_analyses_prompt_result(self, model, additional_analyses_task_count, new_tasks_prompt, request_id, user_id):

    logger.info(f'additional analyses task request processed: request_id {request_id}, user_id {user_id}')

    start_time = time.perf_counter()

    prompt_file = 'app/prompts/split_prompt/additional_tasks_req_prompt.md'
    
    engine = self.get_engine()
    
    # with engine.connect() as conn:
    #     prompt_table_ops = PromptTableOperation(conn_sync=conn)
    #     prompt_table_ops.change_request_status_sync(request_id=request_id, status=TaskStatus.waiting_for_additional_analysis_prompt_result.value)
    
    #     resp_pt_1 = prompt_table_ops.get_prompt_result_sync(request_id=request_id, user_id=user_id)
        
    # if not resp_pt_1:
    #     raise Exception('the requested prompt result does not exist')
    
    # resp_pt_1 = json.loads(resp_pt_1['prompt_result'])
    
    # context_json = {}
    # for field in ['columns', 'common_column_cleaning_or_transformation', 'common_column_combination']:
    #     context_json[field] = resp_pt_1[field]
    
    
    # context = {'context_json': json.dumps(context_json), 'new_tasks_prompt': new_tasks_prompt}
    # prompt = insert_prompt_context(prompt_file=prompt_file, context=context)
    
    # resp = get_prompt_result(prompt, model)     
    # resp = process_llm_api_response(resp)
    
    # with open(f'resp_jsons/resp_additional_analyses.json', 'w') as f:
    #     f.write(json.dumps(resp, indent=4))
    
    with open(f'resp_jsons/resp_additional_analyses.json', 'r') as f:
        resp = json.load(f)
    
    try:
        resp = DatasetAnalysisModelPartTwo.model_validate(resp, context={'run_type': 'additional_analyses_request', 'request_id': request_id})
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
    
    if process_time_ms > THRES_SLOW_INITIAL_REQUEST_PROCESS_TIME_MS:
        logger.warning(f'slow initial request processing time ({process_time_ms} ms): request_id {request_id}, user_id {user_id}')
    

    return data_tasks.model_dump()

@app.task(bind=True, base=DatabaseTask, name='tasks.data_processing_task', acks_late=True, ignore_result=True, time_limit=20, max_retries=3)
def data_processing_task(self, data_tasks, run_info, 
                         run_type: Literal['first_run_after_request', 'modified_tasks_execution', 'additional_analyses_request']):
    logger.info(f"task execution request processed: run type {run_type}, request_id {run_info['request_id']}, user_id {run_info['user_id']}")

    
    start_time = time.perf_counter()
    
    starting_status_dct = {'first_run_after_request': TaskStatus.doing_initial_tasks_run.value, 
                            'modified_tasks_execution': TaskStatus.doing_customized_tasks_run.value, 
                            'additional_analyses_request': TaskStatus.doing_additional_tasks_run.value}
    
    finished_status_dct = {'first_run_after_request': TaskStatus.initial_tasks_run_finished.value, 
                            'modified_tasks_execution': TaskStatus.customized_tasks_run_finished.value, 
                            'additional_analyses_request': TaskStatus.additional_tasks_run_finished.value}
    
    engine = self.get_engine()
    
    with engine.connect() as conn:
        prompt_table_ops = PromptTableOperation(conn_sync=conn)
        prompt_table_ops.change_request_status_sync(request_id=run_info['request_id'], status=starting_status_dct[run_type])
    
    data_tasks = DataTasks.model_validate(data_tasks)
    common_tasks = [i.model_dump() for i in data_tasks.common_tasks]
    
    with engine.connect() as conn:
        task_run_table_ops = TaskRunTableOperation(conn_sync=conn)
        
        if run_type == 'first_run_after_request': # if the run is the first task run right after the llm resp request
            task_run_table_ops.add_task_result_sync(request_id=run_info['request_id'], user_id=run_info['user_id'], 
                                                    original_common_tasks=json.dumps({'original_common_tasks': common_tasks})
                                                    )
        
        processor = DataAnalysisProcessor(data_tasks=data_tasks, run_info=run_info, task_run_table_ops=task_run_table_ops, run_type=run_type)
        processor.process_all_tasks() # to do: check if all tasks are processed first. if not, raise a specific exception that you need to retry for
        
    with engine.connect() as conn:
        prompt_table_ops = PromptTableOperation(conn_sync=conn)
        prompt_table_ops.change_request_status_sync(request_id=run_info['request_id'], status=finished_status_dct[run_type])
        
        if run_type == 'first_run_after_request':
            final_dataset_snippet = processor.get_final_dataset_snippet()
            task_run_table_ops = TaskRunTableOperation(conn_sync=conn)
            task_run_table_ops.update_final_dataset_snippet_sync(request_id=run_info['request_id'], dataset_snippet=final_dataset_snippet)
    
    process_time_ms = round((time.perf_counter() - start_time) * 1000, 2)
    
    logger.info(f"task execution request finished in {process_time_ms} ms: run type {run_type}, request_id {run_info['request_id']}, user_id {run_info['user_id']}")

    if process_time_ms > THRES_SLOW_TASK_EXECUTION_PROCESS_TIME_MS:
        logger.warning(f"slow task execution request processing time ({process_time_ms} ms): run_type {run_type}, request_id {run_info['request_id']}, user_id {run_info['user_id']}")
    
        


    


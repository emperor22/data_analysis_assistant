from celery import Celery, Task
import json
import time
from sqlalchemy import create_engine

from app.services import get_prompt_result, process_llm_api_response, DataAnalysisProcessor
from app.crud import TaskRunTableOperation, PromptTableOperation, base_engine_sync
import asyncio

from app.schemas import DataTasks, DatasetAnalysisModel
from pydantic import ValidationError
from requests.exceptions import RequestException

app = Celery('tasks', backend='redis://localhost:6379/0', broker='redis://localhost:6379/0')
app.conf.task_routes = {'tasks.get_prompt_result_task': {'queue': 'get_prompt_res_queue'}, 
                        'tasks.data_processing_task': {'queue': 'data_processing_queue'}}

class DatabaseTask(app.Task):
    def get_engine(self):
        return base_engine_sync 


# if it persists, write 'not enough common tasks, probably invalid dataset' status to prompt_and_result and (maybe) blacklist the dataset

retry_for_exceptions_list_get_prompt_task = [ValidationError, RequestException]

@app.task(bind=True, base=DatabaseTask, name='tasks.get_prompt_result_task', acks_late=True, time_limit=120, max_retries=3, rate_limit='15/m', 
          retry_backoff=True, retry_backoff_max=60, autoretry_for=retry_for_exceptions_list_get_prompt_task)
def get_prompt_result_task(self, model, prompt, request_id, dataset_cols):
    # TO DO:
    # catch requestlimit error and throttle for 60 seconds
    # llm api request limit reset at midnight pacific time
    
    resp = get_prompt_result(prompt, model)     
    result = process_llm_api_response(resp)
    with open('resp.json', 'w') as f:
        f.write(json.dumps(result, indent=4))
    # mocking api call
    # with open('resp2.json', 'r') as f:
    #     result = json.load(f)
        
    DatasetAnalysisModel.model_validate(result, context={'required_cols': dataset_cols})

    result_str = json.dumps(result)
    
    engine = self.get_engine() # type: ignore
    with engine.connect() as conn:
        prompt_table_ops = PromptTableOperation(conn_sync=conn)
        prompt_table_ops.insert_prompt_result_sync(request_id=request_id, prompt_result=result_str)

    # write response, token_count to main table
    
    data_tasks = DataTasks(
        common_tasks=result['common_tasks'], 
        common_column_cleaning_or_transformation=result['common_column_cleaning_or_transformation'],
        common_column_combination=result['common_column_combination']
        )

    return data_tasks.model_dump()

@app.task(bind=True, base=DatabaseTask, name='tasks.data_processing_task', acks_late=True, ignore_result=True, time_limit=20, max_retries=3)
def data_processing_task(self, data_tasks, run_info, common_tasks_only, first_run_after_request):
    data_tasks = DataTasks.model_validate(data_tasks)
    common_tasks = [i.model_dump() for i in data_tasks.common_tasks]
    
    engine = self.get_engine() # type: ignore
    with engine.connect() as conn:
        conn_sync = conn
        task_run_table_ops = TaskRunTableOperation(conn_sync=conn_sync)
        
        if first_run_after_request: # if the run is the first task run right after the llm resp request
            task_run_table_ops.add_task_result_sync(request_id=run_info['request_id'], user_id=run_info['user_id'], 
                                                    original_common_tasks=json.dumps({'original_common_tasks': common_tasks})
                                                    )
        
        processor = DataAnalysisProcessor(data_tasks=data_tasks, run_info=run_info, task_run_table_ops=task_run_table_ops, 
                                          common_tasks_only=common_tasks_only, first_run_after_request=first_run_after_request)
        processor.process_all_tasks() # to do: check if all tasks are processed first. if not, raise a specific exception that you need to retry for
    


from celery import Celery, Task
import json
import time
from sqlalchemy import create_engine

from app.services import get_prompt_result, process_llm_api_response, DataAnalysisProcessor, insert_prompt_context
from app.crud import TaskRunTableOperation, PromptTableOperation, base_engine_sync
import asyncio

from app.schemas import DataTasks, DatasetAnalysisModelPartOne, DatasetAnalysisModelPartTwo
from pydantic import ValidationError
from requests.exceptions import RequestException
from typing import Literal

app = Celery('tasks', backend='redis://localhost:6379/0', broker='redis://localhost:6379/0')
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
def get_prompt_result_task(self, model, prompt_pt_1, task_count, request_id, dataset_cols):
    # TO DO:
    # catch requestlimit error and throttle for 60 seconds
    # llm api request limit reset at midnight pacific time
    
    # getting result for prompt part 1
    # resp_pt_1 = get_prompt_result(prompt_pt_1, model)     
    # resp_pt_1 = process_llm_api_response(resp_pt_1)
    
    # with open('resp_pt1.json', 'w') as f:       # writing to file to check result directly for debugging
    #     f.write(json.dumps(resp_pt_1, indent=4))
    
    with open('resp1.json', 'r') as f:      # mocking part 1 response
        resp_pt_1 = json.load(f)
        
    DatasetAnalysisModelPartOne.model_validate(resp_pt_1, context={'required_cols': dataset_cols})

    # getting result for prompt part 2
    # prompt_pt_2_file = 'app/prompts/split_prompt/prompt_part2.md'
    # prompt_2_context = {'context_json': resp_pt_1, 'task_count': task_count}
    # prompt_pt_2 = insert_prompt_context(prompt_file=prompt_pt_2_file, context=prompt_2_context)
    
    # resp_pt_2 = get_prompt_result(prompt_pt_2, model)     
    # resp_pt_2 = process_llm_api_response(resp_pt_2)
    
    # with open('resp_pt2.json', 'w') as f:     # writing to file to check result directly for debugging
    #     f.write(json.dumps(resp_pt_2, indent=4))
    
    with open('resp2.json', 'r') as f:        # mocking part 2 response
        resp_pt_2 = json.load(f)
    
    DatasetAnalysisModelPartTwo.model_validate(resp_pt_2, context={'run_type': 'first_run_after_request'})
    
    result = {**resp_pt_1, **resp_pt_2}
    
    engine = self.get_engine() # type: ignore
    with engine.connect() as conn:
        prompt_table_ops = PromptTableOperation(conn_sync=conn)
        prompt_table_ops.insert_prompt_result_sync(request_id=request_id, prompt_result=json.dumps(result))

    # write response, token_count to main table
    
    data_tasks = DataTasks(
        common_tasks=result['common_tasks'], 
        common_column_cleaning_or_transformation=result['common_column_cleaning_or_transformation'],
        common_column_combination=result['common_column_combination']
        )

    return data_tasks.model_dump()

@app.task(bind=True, base=DatabaseTask, name='tasks.get_additional_analyses_prompt_result', acks_late=True, time_limit=200, max_retries=3, rate_limit='15/m', 
          retry_backoff=True, retry_backoff_max=60, autoretry_for=retry_for_exceptions_get_prompt_task)
def get_additional_analyses_prompt_result(self, model, additional_analyses_task_count, new_tasks_prompt, request_id, user_id):

    # prompt_file = 'app/prompts/split_prompt/additional_tasks_req_prompt.md'
    # 
    # engine = self.get_engine()
    # with engine.connect() as conn:
    #     prompt_table_ops = PromptTableOperation(conn_sync=conn)
    #     resp_pt_1 = prompt_table_ops.get_prompt_result_sync(request_id=request_id, user_id=user_id)
        
    # if not resp_pt_1:
    #     raise Exception('the requested prompt result does not exist')
    
    # resp_pt_1 = json.loads(resp_pt_1['prompt_result'])
    
    # context_json = {}
    # for field in ['columns', 'common_column_cleaning_or_transformation', 'common_column_combination']:
    #     context_json[field] = resp_pt_1[field]
    
    
    # context = {'additional_analyses_task_count': additional_analyses_task_count, 'context_json': json.dumps(context_json), 'new_tasks_prompt': new_tasks_prompt}
    # prompt = insert_prompt_context(prompt_file=prompt_file, context=context)
    
    # resp = get_prompt_result(prompt, model)     
    # resp = process_llm_api_response(resp)
    
    # with open('resp_additional_analyses_req.json', 'w') as f:
    #     f.write(json.dumps(resp, indent=4))
    
    with open('resp_additional_analyses_req.json', 'r') as f:
        resp = json.load(f)
    
    DatasetAnalysisModelPartTwo.model_validate(resp, context={'run_type': 'additional_analyses_request'})
    
    
    # write result to prompt and result table
    # result = {}
    
    # engine = self.get_engine() # type: ignore
    # with engine.connect() as conn:
    #     prompt_table_ops = PromptTableOperation(conn_sync=conn)
    #     prompt_table_ops.insert_prompt_result_sync(request_id=request_id, prompt_result=json.dumps(result))

    # write response, token_count to main table
    
    data_tasks = DataTasks(
        common_tasks=resp['common_tasks'], 
        common_column_cleaning_or_transformation=[],
        common_column_combination=[]
        )

    return data_tasks.model_dump()

@app.task(bind=True, base=DatabaseTask, name='tasks.data_processing_task', acks_late=True, ignore_result=True, time_limit=20, max_retries=3)
def data_processing_task(self, data_tasks, run_info, 
                         run_type: Literal['first_run_after_request', 'modified_tasks_execution', 'additional_analyses_request']):
    data_tasks = DataTasks.model_validate(data_tasks)
    common_tasks = [i.model_dump() for i in data_tasks.common_tasks]
    
    engine = self.get_engine() # type: ignore
    with engine.connect() as conn:
        conn_sync = conn
        task_run_table_ops = TaskRunTableOperation(conn_sync=conn_sync)
        
        if run_type == 'first_run_after_request': # if the run is the first task run right after the llm resp request
            task_run_table_ops.add_task_result_sync(request_id=run_info['request_id'], user_id=run_info['user_id'], 
                                                    original_common_tasks=json.dumps({'original_common_tasks': common_tasks})
                                                    )
        
        processor = DataAnalysisProcessor(data_tasks=data_tasks, run_info=run_info, task_run_table_ops=task_run_table_ops, run_type=run_type)
        processor.process_all_tasks() # to do: check if all tasks are processed first. if not, raise a specific exception that you need to retry for
    


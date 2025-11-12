import pandas as pd
from pandas.api.types import is_numeric_dtype
import numpy as np
import io
from string import Template
import requests

from abc import ABC, abstractmethod
from fastapi import UploadFile, BackgroundTasks

import re
import ast
import json
from typing import Literal

from app.crud import TaskRunTableOperation, PromptTableOperation
from app.schemas import DataTasks, TaskStatus, RunInfoSchema
from app.data_transform_utils import (
    groupby_func, filter_func, get_proportion_func, get_column_statistics_func,
    get_top_or_bottom_n_entries_func ,apply_map_range_func, apply_map_func, 
    apply_date_op_func, apply_math_op_func, get_column_combination_func, 
    get_column_properties, col_transform_and_combination_parse_helper, 
    clean_dataset
)
from app.logger import logger


##### CONFIG #####
N_ROWS_READ_UPLOAD_FILE = 100
DATASET_SAVE_PATH = 'app/datasets'

##################

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return round(float(obj), 3)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)



class FileReader(ABC):
    def __init__(self, upload_file: UploadFile):
        self.file_content = upload_file.file.read()
        self.filename = upload_file.filename
        self.clean_dataset_func = clean_dataset
        self.df = None
        
    def get_dataframe_dict(self):
        self._read_file()
        
        clean_df = self.clean_dataset_func(self.df)
        self.df = clean_df
        
        if self._validate_dataset():
            sorted_unique_cols = sorted(list(set(self.df.columns)))
            return {'filename': self.filename, 'dataframe': self.df, 'columns_str': str(sorted_unique_cols)}
        else:
            raise Exception('The data does not fit the requirements')
    
    @abstractmethod
    def _read_file(self):
        '''Reads the file and returns a pandas dataframe'''
        pass
    
    def _validate_dataset(self):
        '''A method to validate the dataframe. Returns True if passes all the checks else returns False'''
        return True
    
    
    
class CsvReader(FileReader):        
    def _read_file(self) -> pd.DataFrame:
        self.df = pd.read_csv(io.BytesIO(self.file_content))
        
class DatasetProcessorForPrompt:
    def __init__(self, dataframe: pd.DataFrame, filename: str, prompt_template_file: str):
        self.dataframe = dataframe
        self.filename = filename
        self.prompt_template_file = prompt_template_file
    
    def _get_column_unique_values(self) -> dict:
        unique_val_dct = {}
        for col in self.dataframe.columns:
            unique_val_dct[col] = {}
            unique_val_dct[col]['num_of_unique_values'] = self.dataframe[col].nunique()
            unique_val_dct[col]['top_5_values'] = self.dataframe[col].value_counts().index[:5].tolist()
        return unique_val_dct
    
    def create_prompt(self) -> str:
        with open(self.prompt_template_file, 'r') as f:
            template = Template(f.read())
        
        context = {'dataset_name': self.filename, 
                   'dataset_snippet': get_dataset_snippet(self.dataframe), 
                   'dataset_column_unique_values': self._get_column_unique_values()}
        return template.substitute(context)
    
class DataAnalysisProcessor:
    def __init__(self, data_tasks: DataTasks, run_info: RunInfoSchema, task_run_table_ops: TaskRunTableOperation, 
                 run_type: Literal['first_run_after_request', 'modified_tasks_execution', 'additional_analyses_request']):
        self.run_type = run_type
        
        parquet_file = run_info['parquet_file']
        self.request_id = run_info['request_id']
        self.user_id = run_info['user_id']
        
        self.df: pd.DataFrame = pd.read_parquet(parquet_file)
        self.original_columns = self.df.columns.tolist()
        
        self.data_tasks = data_tasks
        
        self.common_tasks_only = False
        
        if len(data_tasks.common_tasks) > 0 and \
           len(data_tasks.common_column_cleaning_or_transformation) == 0 and \
           len(data_tasks.common_column_combination) == 0:
               self.common_tasks_only = True
        
        self.task_run_table_ops = task_run_table_ops
        
        self.common_tasks_fn_map = {
            'groupby': groupby_func, 
            'filter': filter_func, 
            'get_top_or_bottom_N_entries': get_top_or_bottom_n_entries_func, 
            'get_proportion': get_proportion_func, 
            'get_column_statistics': get_column_statistics_func
            }
        
        self.column_transform_fn_map = {
            'map_range': apply_map_range_func,
            'date_op': apply_date_op_func,
            'math_op': apply_math_op_func,
            'map': apply_map_func,
            }
        
        self.common_tasks_runtype_json_key = {
            'first_run_after_request': 'original_common_tasks', 
            'modified_tasks_execution': 'common_tasks_w_result', 
            'additional_analyses_request': 'original_common_tasks'
        }
        
        # only accessed when testing to validate task result written to db
        self.col_info = None
        self.common_tasks_modified = None
        self.col_transform_tasks_status = None
        self.col_combination_tasks_status = None

    def process_all_tasks(self):       
        if not self.common_tasks_only:
            self._process_column_transforms()
            self._process_column_combinations()
            self.df.to_parquet(f'{DATASET_SAVE_PATH}/{self.request_id}.parquet', index=False) # update saved dataset with added columns
        
        self._process_common_tasks()
        
        if self.run_type == 'first_run_after_request':
            self._process_columns_info()
            
            final_dataset_snippet = get_dataset_snippet(self.df)
            self.task_run_table_ops.update_final_dataset_snippet_sync(request_id=self.request_id, dataset_snippet=final_dataset_snippet)
    
    def _process_columns_info(self):
        df = self.df.copy()
        
        added_columns = list(set(df.columns) - set(self.original_columns))
        numerical_columns = df.select_dtypes(include=[np.number])
        date_cols = df.select_dtypes(include=[np.datetime64])
        
        columns_info = [col.model_dump() for col in self.data_tasks.columns]
        col_combination = [col.model_dump() for col in self.data_tasks.common_column_combination]
        col_transform = [col.model_dump() for col in self.data_tasks.common_column_cleaning_or_transformation]
        
        col_info_lst = []
        
        for col in df.columns:
            series = df[col]
            
            col_info_dct = {'name': col}
            col_info_dct['source'] = 'original' if col not in added_columns else 'added'
            
            # get llm resp col info if in original columns
            if col not in added_columns: 
                col_info_dct['inferred_info_prompt_res'] = next(dct for dct in columns_info if dct['name'] == col)
            else:
                col_combination_names = [col['name'] for col in col_combination]
                col_transform_names = [col['name'] for col in col_transform]
                
                if col in col_combination_names:
                    llm_resp_col_info = next(dct for dct in col_combination if dct['name'] == col)
                    llm_resp_col_info = col_transform_and_combination_parse_helper(llm_resp_col_info, is_col_combination=True)
                    col_info_dct['inferred_info_prompt_res'] = llm_resp_col_info
                    
                elif col in col_transform_names:
                    llm_resp_col_info = next(dct for dct in col_transform if dct['name'] == col)
                    llm_resp_col_info = col_transform_and_combination_parse_helper(llm_resp_col_info, is_col_combination=False)
                    col_info_dct['inferred_info_prompt_res'] = llm_resp_col_info
                    
                else:
                    col_info_dct['inferred_info_prompt_res'] = {}
                    
            
            is_numeric_column = col in numerical_columns
            is_datetime_column = col in date_cols
            col_properties = get_column_properties(series, is_numeric_column, is_datetime_column)
            
            col_info_dct.update(col_properties)
            
            col_info_lst.append(col_info_dct)
            
        col_info = {'columns_info': col_info_lst}
        
        self.task_run_table_ops.update_columns_info_sync(request_id=self.request_id, columns_info=json.dumps(col_info, cls=NpEncoder))
        
        self.col_info = col_info
    
    def _process_common_tasks(self):
        common_tasks = self.data_tasks.common_tasks
        common_tasks_modified = []
        for task in common_tasks:
            task_modified = task.model_dump()
            
            steps = task.steps
            tmp = self.df.copy()
            error_steps = []
            for step_num, step in enumerate(steps):
                try:
                    func = self.common_tasks_fn_map[step['function']]
                    kwargs = {i: j for i, j in step.items() if i != 'function'}
                    tmp: pd.DataFrame = func(tmp, **kwargs)
                    
                    if tmp.empty:
                        raise Exception('empty result dataset')
                    
                    datetime_columns = tmp.select_dtypes(include=['datetime64[ns]']).columns.tolist()
                    
                    # because timestamp object is not serializable
                    if datetime_columns:
                        for dt_col in datetime_columns:
                            tmp[dt_col] = tmp[dt_col].dt.strftime('%Y-%m-%d %H:%M:%S')
                            
                    # mostly for pure filtering task. idea: let user know this doesnt fail, but they can export the filter to use on their own
                    if step_num == len(steps) - 1 and (len(tmp) > 1000 or len(tmp.columns) > 20):
                        raise Exception('output of this analysis is too big')
                    
                except Exception as e:
                    error_steps.append(f"failed at step {step_num+1}: {str(e)}")
                    break
                    
            if error_steps:
                task_modified['status'] = '; '.join(error_steps)
                task_modified['result'] = {}
                
                logger.warning(f'some task from common_tasks failed: run_type {self.run_type}, request_id {self.request_id}, task_id {task.task_id}, errors ({"; ".join(error_steps)})')
            else:
                task_modified['status'] = 'successful'
                task_modified['result'] = tmp.to_dict('list')
                    
            common_tasks_modified.append(task_modified)
        
        runtype_json_key = self.common_tasks_runtype_json_key[self.run_type]
        common_task_modified_w_json_key = {runtype_json_key: common_tasks_modified}
        
        if self.run_type == 'first_run_after_request': # update original_common_tasks (now with results) at first task run after llm response
            self.task_run_table_ops.update_original_common_task_result_sync(request_id=self.request_id, 
                                                                            original_common_tasks=json.dumps(common_task_modified_w_json_key))
        elif self.run_type == 'modified_tasks_execution':
            self.task_run_table_ops.update_task_result_sync(request_id=self.request_id, 
                                                            common_tasks_w_result=json.dumps(common_task_modified_w_json_key), )
        elif self.run_type == 'additional_analyses_request':
            tasks_by_id = self.task_run_table_ops.get_task_by_id_sync(user_id=self.user_id, request_id=self.request_id)
            original_common_tasks = tasks_by_id['original_common_tasks']
            original_common_tasks = json.loads(original_common_tasks)['original_common_tasks'] # a list of common tasks
            common_tasks_merged = original_common_tasks + common_tasks_modified # merge the original tasks and the new one
            
            self.task_run_table_ops.update_original_common_task_result_sync(request_id=self.request_id, 
                                                                            original_common_tasks=json.dumps(common_task_modified_w_json_key))
            
        self.common_tasks_modified = common_task_modified_w_json_key
            
    def _process_col_transform_and_combination_helper(self, tasks, fn_map=None, is_col_combination_task= False):
        tmp = self.df.copy()
        tasks_status = {}
        
        for task in tasks:
            task = task.model_dump()
            name = task['name'].replace(' ', '_')
            operation = task['operation']

            try:
                func = fn_map[operation['type']] if not is_col_combination_task else get_column_combination_func
                tmp = func(df=tmp, name=name, operation=operation)
                    
                tasks_status[task['name']] = 'successful'
                
            except Exception as e:
                tasks_status[task['name']] = f'failed. error: {e.args}'
                run = 'column_combinations' if is_col_combination_task else 'column_transforms'
                logger.warning(f'some task from {run} failed: run_type {self.run_type}, request_id {self.request_id}, errors ({str(e)})')

                continue
            
        self.df = tmp.copy()
        return tasks_status
    
    def _process_column_transforms(self):
        column_transformation_tasks = self.data_tasks.common_column_cleaning_or_transformation
        
        task_status = self._process_col_transform_and_combination_helper(tasks=column_transformation_tasks, 
                                                                        fn_map=self.column_transform_fn_map, 
                                                                        is_col_combination_task=False)
        
        self.task_run_table_ops.update_column_transform_task_status_sync(request_id=self.request_id, column_transforms_status=json.dumps(task_status))
        
        self.col_transform_tasks_status = task_status
        
        
    def _process_column_combinations(self):
        column_combination_tasks = self.data_tasks.common_column_combination

        task_status = self._process_col_transform_and_combination_helper(tasks=column_combination_tasks,  
                                                                         is_col_combination_task=True)

        self.task_run_table_ops.update_column_combination_task_status_sync(request_id=self.request_id, column_combinations_status=json.dumps(task_status)) 
        
        self.col_combination_tasks_status = task_status
        
    def get_process_result(self):
        result_dct = {'col_combination_status': self.col_combination_tasks_status, 
                      'col_transform_status': self.col_transform_tasks_status, 
                      'common_tasks_result': self.common_tasks_modified, 
                      'col_info': self.col_info}
        return result_dct
  

def get_dataset_snippet(df: pd.DataFrame):
    return df.iloc[:5].to_csv(index=False)

def save_uploaded_file(file: UploadFile, save_dir: str):
        contents = file.file.read()
        filename = file.filename
        with open(f'{save_dir}/{filename}', 'wb') as f:
            f.write(contents)

def insert_prompt_context(prompt_file, context):
    with open(prompt_file, 'r') as f:
        template = Template(f.read())
    
    return template.substitute(context)

@logger.catch
def get_prompt_result(prompt, model):
    key = 'AIzaSyB0Kbhw7DpGlA_tAolL4tGdRajI9k4F_s4'
    url = f'https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent'
    
    payload_data = {
        "contents": [{"parts": [{"text": prompt}]}]
    }

    headers = {'Content-Type': 'application/json','X-goog-api-key': key}
    
    r = requests.post(url, json=payload_data, headers=headers)
    r.raise_for_status()
        
    return r.json()

@logger.catch
def process_llm_api_response(resp: dict):
    prompt_resp = resp['candidates'][0]['content']['parts'][0]['text']
    clean_json = re.search(r"```json\s*(.*?)\s*```", prompt_resp, re.DOTALL)
    clean_json = clean_json.group(1).strip()
    
    data = json.loads(clean_json)
    
    # prompt_token_count = resp['usage_metadata']['promptTokenCount']
    # total_token_count = resp['usage_metadata']['totalTokenCount']
    
    return data

def cleanup_agg_col_names(resp_pt_2, resp_pt_1):
    
    json_str = json.dumps(resp_pt_2)
    
    def remove_suffix(s):
        return '_'.join(s.split('_')[:-1])
    
    resp_pt_1 = resp_pt_1
    
    mrg_lst = [*resp_pt_1['columns'], *resp_pt_1['common_column_combination'], *resp_pt_1['common_column_cleaning_or_transformation']]
    original_columns = {i['name'] for i in mrg_lst}
    
    aggs = ['mean', 'median', 'min', 'max', 'count', 'size', 'sum']
    pattern = r'"([^"]*_(?:{}))"'.format('|'.join(aggs))
    
    cols_to_clean = re.findall(pattern, json_str)
    cols_to_clean = list(set(cols_to_clean))
    
    replace_dct = {}
    
    for col in cols_to_clean:
        if remove_suffix(col) in original_columns:
            replace_dct[col] = remove_suffix(col)
    
    for col_to_replace, replace in replace_dct.items():
        json_str = json_str.replace(col_to_replace, replace)
        
    return json.loads(json_str)

async def is_task_invalid_or_still_processing(request_id, user_id, prompt_table_ops):
    req_status = await prompt_table_ops.get_request_status(request_id=request_id, user_id=user_id)
    exclude_status = (TaskStatus.waiting_for_initial_request_prompt.value, TaskStatus.initial_request_prompt_received.value, 
                      TaskStatus.doing_initial_tasks_run.value)
    
    if not req_status or (req_status and req_status['status'] in exclude_status):
        return True
    return False

def get_background_tasks(background_tasks: BackgroundTasks):
    return background_tasks


def split_and_validate_new_prompt(prompt):
    
    def validate_value(s):
        min_char = 15
        max_char = 100
        return min_char <= len(s) <= max_char 
    
    regex = r'^[a-zA-Z0-9 \n\r]*$'
    
    if not bool(re.fullmatch(regex, prompt)):
        return
            
    values = [i.strip() for i in prompt.split('\n')]
    all_values_valid = all([validate_value(val) for val in values])
    
    if len(values) > 5 or not all_values_valid:
        return
    
    return prompt
    



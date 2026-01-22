import pandas as pd
from pandas.api.types import is_numeric_dtype
import numpy as np
import io
from string import Template
import requests

import base64

from abc import ABC, abstractmethod
from fastapi import UploadFile, BackgroundTasks
from fastapi.exceptions import HTTPException

from fastapi_mail import FastMail, MessageSchema, ConnectionConfig, MessageType

import seaborn as sns
import matplotlib.pyplot as plt

import re
import ast
import json
from typing import Literal

import zipfile

import os

import asyncio

from app.crud import TaskRunTableOperation, PromptTableOperation, get_current_time_utc
from app.schemas import DataTasks, TaskStatus, RunInfoSchema, TaskProcessingRunType
from app.data_transform_utils import (
    groupby_func, filter_func, get_proportion_func, get_column_statistics_func,
    get_top_or_bottom_n_entries_func, resample_data_func, apply_map_range_func, apply_map_func, 
    apply_date_op_func, apply_math_op_func, get_column_combination_func, 
    get_column_properties, col_transform_and_combination_parse_helper, 
    clean_dataset, handle_datetime_columns_serialization, 
    determine_result_output_type, get_granularity_map
)
from app.logger import logger
from app.config import Config

import zipfile
from fpdf import FPDF

import sentry_sdk

from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import Request
import time

from functools import wraps
from datetime import datetime

from glob import glob

import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders



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
        self.granularity_map = {}
        self.df = None
        
    def get_dataframe_dict(self):
        self._read_file()
        
        self.df = clean_dataset(self.df)
        self.granularity_map = get_granularity_map(self.df)
        
        if self._validate_dataset():
            sorted_unique_cols = sorted(list(set(self.df.columns)))
            return {'filename': self.filename, 'dataframe': self.df, 'columns_str': str(sorted_unique_cols), 
                    'dataset_id': get_dataset_id(self.df), 'granularity_map': self.granularity_map}
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
        self.df = pd.read_csv(io.BytesIO(self.file_content), encoding='unicode_escape')
        
class DatasetProcessorForPtOnePrompt:
    def __init__(self, dataframe: pd.DataFrame, filename: str, prompt_template_file: str, granularity_data: dict = {}):
        self.dataframe = dataframe
        self.dataset_row_count = len(dataframe)
        self.filename = filename
        self.prompt_template_file = prompt_template_file
        self.granularity_data = 'N/A' if not granularity_data else granularity_data
    
    def _get_column_unique_values(self) -> dict:
        unique_val_dct = {}
        for col in self.dataframe.columns:
            unique_val_dct[col] = {}
            unique_val_dct[col]['num_of_unique_values'] = self.dataframe[col].nunique()
            unique_val_dct[col]['unique_values_count_ratio_to_row_count'] = self.dataframe[col].nunique() / self.dataset_row_count
            unique_val_dct[col]['top_5_values'] = self.dataframe[col].value_counts().index[:5].tolist()
            
        return unique_val_dct
    
    def create_prompt(self) -> str:
        with open(self.prompt_template_file, 'r') as f:
            template = Template(f.read())
        
        context = {'dataset_name': self.filename, 
                   'dataset_row_count': self.dataset_row_count, 
                   'dataset_snippet': get_dataset_snippet(self.dataframe), 
                   'dataset_column_unique_values': self._get_column_unique_values(), 
                   'temporal_granularity_map': self.granularity_data}
        return template.substitute(context)


class DataAnalysisProcessor:
    def __init__(self, data_tasks: DataTasks, run_info: RunInfoSchema, task_run_table_ops: TaskRunTableOperation, 
                 run_type: str):
        self.run_type = run_type
        
        parquet_file = run_info['parquet_file']
        self.request_id = run_info['request_id']
        self.user_id = run_info['user_id']
        self.send_result_to_email = run_info['send_result_to_email']
        self.email = run_info['email']
        self.dataset_filename = run_info['filename']
        self.run_name = run_info['run_name']
        
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
            'get_column_statistics': get_column_statistics_func, 
            'resample_data': resample_data_func
            }
        
        self.column_transform_fn_map = {
            'map_range': apply_map_range_func,
            'date_op': apply_date_op_func,
            'math_op': apply_math_op_func,
            'map': apply_map_func,
            }
        
        # only accessed when testing to validate task result written to db
        self.col_info = None
        self.common_tasks_modified = None
        self.col_transform_tasks_status = None
        self.col_combination_tasks_status = None

    def process_all_tasks(self):       
        if self.run_type in (TaskProcessingRunType.first_run_after_request.value, TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value):
            self._process_column_transforms()
            self._process_column_combinations()
            
            # useful for when user run customized tasks on original dataset without running col transform or combination operations
            save_type_dct = {TaskProcessingRunType.first_run_after_request.value: 'original_dataset', 
                             TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value: 'new_dataset'}
            
            save_dataset_req_id(request_id=self.request_id, dataframe=self.df, save_type=save_type_dct[self.run_type])
        
        if self.run_type == TaskProcessingRunType.first_run_after_request.value:
            self._process_columns_info()
            
            final_dataset_snippet = get_dataset_snippet(self.df)
            self.task_run_table_ops.update_final_dataset_snippet_sync(request_id=self.request_id, dataset_snippet=final_dataset_snippet)
            
        self._process_common_tasks()
        self._update_task_result_in_db()
        
        if self.send_result_to_email:
            self._generate_report_archive_and_send_to_email()
    
    def _process_columns_info(self):
        df = self.df
        added_cols = set(df.columns) - set(self.original_columns)
        
        col_info_llm_resp = {c.name: c.model_dump() for c in self.data_tasks.columns}
        
        # inserting col info for derived columns
        for col in self.data_tasks.common_column_combination:
            col_info_llm_resp[col.name] = col_transform_and_combination_parse_helper(col.model_dump(), True)
        for col in self.data_tasks.common_column_cleaning_or_transformation:
            col_info_llm_resp[col.name] = col_transform_and_combination_parse_helper(col.model_dump(), False)

        col_info_lst = []
        
        for col in df.columns:
            col_info_dct = {
                'name': col,
                'source': 'original' if col not in added_cols else 'added',
                'inferred_info_prompt_res': col_info_llm_resp.get(col, {})
            }
            
            is_numeric_col = col in df.select_dtypes(include=[np.number])
            is_datetime_col = col in df.select_dtypes(include=[np.datetime64])
            props = get_column_properties(df[col], is_numeric=is_numeric_col, is_datetime=is_datetime_col)
            
            col_info_dct.update(props)
            col_info_lst.append(col_info_dct)

        self.col_info = {'columns_info': col_info_lst}
        self.task_run_table_ops.update_columns_info_sync(request_id=self.request_id, columns_info=json.dumps(self.col_info, cls=NpEncoder))
    
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
                    
                except Exception as e:
                    error_steps.append(f"failed at step {step_num+1}: {str(e)}")
                    break
                    
            if error_steps:
                task_modified['status'] = '; '.join(error_steps)
                task_modified['result'] = {}
                
                logger.warning(f'some task from common_tasks failed: run_type {self.run_type}, request_id {self.request_id}, task_id {task.task_id}, errors ({"; ".join(error_steps)})')
            else:
                # this should be refactored into a function which processes all task results all at once
                logger.debug(f'processing task {task.task_id}')
                
                res_type = determine_result_output_type(tmp)
                
                result_save_handler(df=tmp, run_type=self.run_type, task_id=task.task_id, task_name=task.name, request_id=self.request_id, res_type=res_type)
                
                if not (len(tmp) > Config.DATASET_ROW_THRESHOLD_BEFORE_EXPORT or len(tmp.columns) > Config.DATASET_COLUMNS_THRESHOLD_BEFORE_EXPORT):
                    tmp = handle_datetime_columns_serialization(tmp)
                    task_modified['status'] = 'successful'
                    task_modified['result'] = tmp.to_dict('list') # transform to dictionary for storing in db
                else:
                    task_modified['status'] = 'output of this analysis is too big'
                    task_modified['result'] = {}
                    
            common_tasks_modified.append(task_modified)
        
        
        if self.run_type == TaskProcessingRunType.additional_analyses_request.value:
            common_tasks_modified = self._get_original_tasks_and_merge(new_tasks=common_tasks_modified)

        
        self.common_tasks_modified = common_tasks_modified
            
    def _process_col_transform_and_combination_helper(self, tasks, fn_map=None, is_col_combination_task= False):
        tmp = self.df.copy()
        tasks_w_status = []
        
        for task in tasks:
            task = task.model_dump()
            name = task['name'].replace(' ', '_')
            operation = task['operation']

            try:
                func = fn_map[operation['type']] if not is_col_combination_task else get_column_combination_func
                tmp = func(df=tmp, name=name, operation=operation)
                    
                task['status'] = 'successful'
                tasks_w_status.append(task)
                
            except Exception as e:
                task['status'] = f'failed. error: {e.args}'
                tasks_w_status.append(task)
                
                run = 'column_combinations' if is_col_combination_task else 'column_transforms'
                logger.warning(f'some task from {run} failed: run_type {self.run_type}, request_id {self.request_id}, errors ({str(e)})')

                continue
            
        self.df = tmp.copy()
        return tasks_w_status
    
    def _process_column_transforms(self):
        column_transformation_tasks = self.data_tasks.common_column_cleaning_or_transformation
        
        tasks_w_status = self._process_col_transform_and_combination_helper(tasks=column_transformation_tasks, 
                                                                            fn_map=self.column_transform_fn_map, 
                                                                            is_col_combination_task=False)
        
        self.task_run_table_ops.update_column_transform_task_status_sync(request_id=self.request_id, 
                                                                         column_transforms_status=json.dumps({'column_transforms': tasks_w_status}))
        
        self.col_transform_tasks_status = tasks_w_status
        
        
    def _process_column_combinations(self):
        column_combination_tasks = self.data_tasks.common_column_combination

        tasks_w_status = self._process_col_transform_and_combination_helper(tasks=column_combination_tasks,  
                                                                         is_col_combination_task=True)

        self.task_run_table_ops.update_column_combination_task_status_sync(request_id=self.request_id, 
                                                                          column_combinations_status=json.dumps({'column_combinations': tasks_w_status})) 
        
        self.col_combination_tasks_status = tasks_w_status
        
    def _get_process_result(self):
        result_dct = {'col_combination_status': self.col_combination_tasks_status, 
                      'col_transform_status': self.col_transform_tasks_status, 
                      'common_tasks_result': self.common_tasks_modified, 
                      'col_info': self.col_info}
        return result_dct
    
    def _update_task_result_in_db(self):
        common_task_modified_dct = {'tasks': self.common_tasks_modified}
        
        if self.run_type == TaskProcessingRunType.first_run_after_request.value:
            self.task_run_table_ops.update_original_common_task_result_sync(request_id=self.request_id, 
                                                                            tasks=json.dumps(common_task_modified_dct))
        elif self.run_type == TaskProcessingRunType.modified_tasks_execution.value:
            self.task_run_table_ops.update_task_result_sync(request_id=self.request_id, 
                                                            tasks=json.dumps(common_task_modified_dct))
        elif self.run_type == TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value:
            self.task_run_table_ops.update_task_result_sync(request_id=self.request_id, 
                                                        tasks=json.dumps(common_task_modified_dct))
        elif self.run_type == TaskProcessingRunType.additional_analyses_request.value:
            self.task_run_table_ops.update_original_common_task_result_sync(request_id=self.request_id, 
                                                                            tasks=json.dumps(common_task_modified_dct))
            
    def _get_original_tasks_and_merge(self, new_tasks):
        tasks_by_id = self.task_run_table_ops.get_task_by_id_sync(user_id=self.user_id, request_id=self.request_id)
        original_common_tasks = tasks_by_id['original_common_tasks']
        original_common_tasks = json.loads(original_common_tasks)['tasks'] # a list of common tasks
        merged_tasks = original_common_tasks + new_tasks # merge the original tasks and the new one
        
        return merged_tasks
    
    def _generate_report_archive_and_send_to_email(self):
        result_dct = get_result_dct(self.common_tasks_modified)
        
        zip_file_dir = generate_report_and_archive(request_id=self.request_id, run_type=self.run_type, result_dct=result_dct, run_name=self.run_name)
        logger.debug('zip file created')
        send_task_result_email(recipient=self.email, zip_file_dir=zip_file_dir, run_type=self.run_type, dataset_name=self.dataset_filename, run_name=self.run_name)
        logger.debug('email sent')
    


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



@logger.catch(reraise=True)
def get_prompt_result(prompt, model):
    key = Config.LLM_API_KEY
    url = Config.LLM_ENDPOINT
    
    url = url.format(model)
    
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

def get_task_plot_results(request_id, run_type):
    
    def get_image_byte_as_str(image_path):
        with open(image_path, "rb") as f:
            data = f.read()

        encoded = base64.b64encode(data).decode("utf-8")
        
        return encoded
    
    save_dir_dct = {TaskProcessingRunType.first_run_after_request.value: 'original_tasks', 
                    TaskProcessingRunType.modified_tasks_execution.value: 'customized_tasks', 
                    TaskProcessingRunType.additional_analyses_request.value: 'original_tasks', 
                    TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value: 'customized_tasks'}
    
    save_dir = save_dir_dct[run_type]
    
    path = f'{Config.DATASET_SAVE_PATH}/{request_id}/{save_dir}/artifacts'
    plot_files = [i for i in os.listdir(path) if i.endswith('.png')]
    plot_task_ids = [i.split('.')[0] for i in plot_files]
    

    image_data = [get_image_byte_as_str(f'{path}/{plot_file}') for plot_file in plot_files]
    
    return {task_id: image for task_id, image in zip(plot_task_ids, image_data)}

def cleanup_agg_col_names(resp_pt_2, resp_pt_1):
    
    json_str = json.dumps(resp_pt_2)
    
    def remove_suffix(s):
        return '_'.join(s.split('_')[:-1])
    
    resp_pt_1 = resp_pt_1
    
    mrg_lst = [*resp_pt_1['columns'], *resp_pt_1['common_column_combination'], *resp_pt_1['common_column_cleaning_or_transformation']]
    original_columns = {i['name'] for i in mrg_lst}
    
    aggs = ['mean', 'median', 'min', 'max', 'count', 'size', 'sum']
    pattern = r'"([^"]*_(?:{}))"'.format('|'.join(aggs))
    
    # find columns with names ending with aggregation (e.g. sales_sum)
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
                      TaskStatus.doing_initial_tasks_run.value, TaskStatus.failed_because_blacklisted_dataset, 
                      TaskStatus.deleted_because_not_accessed_recently)
    
    if not req_status or (req_status and req_status['status'] in exclude_status):
        return True
    return False

def check_if_task_is_valid(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        
        request_id = kwargs.get('request_id')
        
        if not request_id:
            try:
                data_key = [i for i in kwargs.keys() if i.endswith('_data')][0]
                request_id = kwargs.get(data_key).request_id
            except (AttributeError, IndexError):
                return await func(*args, **kwargs)
            
        current_user = kwargs.get('current_user')
        user_id = current_user.user_id
        
        prompt_table_ops = kwargs.get('prompt_table_ops')
        
        if not is_task_invalid_or_still_processing(request_id, user_id, prompt_table_ops):
            raise HTTPException(status_code=403, detail=f"this is an invalid task or you must run an initial analysis request first")
                
        
        return await func(*args, **kwargs)
    
    return wrapper



def get_background_tasks(background_tasks: BackgroundTasks):
    return background_tasks

def init_sentry():
    sentry_sdk.init(
        dsn=Config.SENTRY_DSN,
        send_default_pii=True,
        enable_logs=True
    )

class LogRequestMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.perf_counter()
        
        response = await call_next(request)
        
        end_time = time.perf_counter()
        process_time = end_time - start_time
        process_time_ms = round(process_time * 1000, 2)
        
        if response.status_code == 200:
            logger.debug(f"{request.method} {request.url} | Completed with status {response.status_code} in {process_time_ms} ms")
        elif response.status_code >= 400:
            logger.warning(f"CLIENT ERROR 4XX | {request.method} {request.url} | Headers: {dict(request.headers)} | Completed with status {response.status_code} in {process_time_ms} ms")
        elif response.status_code >= 500:
            logger.error(f"SERVER ERROR 5XX | {request.method} {request.url} | Headers: {dict(request.headers)} | Completed with status {response.status_code} in {process_time_ms} ms")
        
        slow_routes_exclude = ['/upload_dataset']
        if process_time_ms > Config.THRES_SLOW_RESPONSE_TIME_MS and Config.WARN_FOR_SLOW_RESPONSE_TIME and not any(i in str(request.url) for i in slow_routes_exclude):
            logger.warning(f'SLOW RESPONSE TIME | {request.method} {request.url} | Headers: {dict(request.headers)} | Completed with status {response.status_code} in {process_time_ms} ms')
        
        return response
    


def update_last_accessed_at_when_called(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        request_id = kwargs.get('request_id')
        redis_client = kwargs.get('redis_client')
        
        if not request_id or not redis_client:
            logger.warning(f'update_last_accessed_at decorator called on function "{func.__name__}" with no request_id or redis_client parameter')
            return await func(*args, **kwargs)
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        hashtable_last_accessed = Config.REDIS_LAST_ACCESSED_HASHTABLE_NAME
        cooldown_key = f'{request_id}:last_write:{today}'
        
        logger.debug(f'cooldown key {cooldown_key}')
        
        if redis_client.set(cooldown_key, 'on_cooldown', ex=3600*24*2, nx=True):
            redis_client.hset(hashtable_last_accessed, request_id, today)
            
        return await func(*args, **kwargs)
    
    return wrapper

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

def get_dataset_id(df: pd.DataFrame):
    return json.dumps({col: str(df[col].dtype) for col in sorted(df.columns)})

def save_dataset_req_id(request_id: str, dataframe: pd.DataFrame, 
                        save_type: Literal['original_dataset', 'new_dataset']):
    save_path = f'{Config.DATASET_SAVE_PATH}/{request_id}'
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    
    filename_dct = {'original_dataset': 'original_dataset.parquet', 'new_dataset': 'new_dataset.parquet'}
    filename = filename_dct[save_type]
    
    file_dir = f'{save_path}/{filename}'
    dataframe.to_parquet(file_dir, index=False)
    
    return file_dir

class ReportPDF(FPDF):
    def header(self):
        self.set_font('helvetica', 'B', 15)
        self.cell(0, 10, 'Data Analysis Report', border=False, new_x="LMARGIN", new_y="NEXT", align='C')
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font('helvetica', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}/{{nb}}', align='C')

    def chapter_title(self, task_id):
        self.set_font('helvetica', 'B', 12)
        self.set_fill_color(200, 220, 255)
        self.cell(0, 10, f'Task Result: {task_id}', new_x="LMARGIN", new_y="NEXT", fill=True)
        self.ln(5)

    def chapter_body_text(self, text):
        self.set_font('helvetica', '', 10)
        self.multi_cell(0, 5, text)
        self.ln()

def generate_report_and_archive(request_id, result_dct, run_type, run_name):
            
    base_dir = f'{Config.DATASET_SAVE_PATH}/{request_id}'
    save_folder_dct = {TaskProcessingRunType.first_run_after_request.value: 'original_tasks', 
                       TaskProcessingRunType.modified_tasks_execution.value: 'customized_tasks', 
                       TaskProcessingRunType.additional_analyses_request.value: 'original_tasks', 
                       TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value: 'customized_tasks'}
    save_folder = save_folder_dct[run_type]
    artifacts_dir = f'{base_dir}/{save_folder}/artifacts'
    
    output_filename_dct = {TaskProcessingRunType.first_run_after_request.value: 'original_tasks', 
                           TaskProcessingRunType.modified_tasks_execution.value: 'customized_tasks', 
                           TaskProcessingRunType.additional_analyses_request.value: 'original_tasks', 
                           TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value: 'customized_tasks'}
    output_filename = output_filename_dct[run_type]
    
    today_date = get_current_time_utc().strftime('%Y-%m-%d %I:%M:%S %p')
    output_pdf_path = f'{base_dir}/{run_name}_{output_filename}_report.pdf'
    output_zip_path = f'{base_dir}/{run_name}_{output_filename}_{today_date}.zip'
    
    task_ids = set(result_dct.keys())
        
    sorted_task_ids = sorted(list(task_ids))
    


    pdf = ReportPDF()
    pdf.add_page()

    for task in sorted_task_ids:
        pdf.chapter_title(task)

        plot_path = f"{artifacts_dir}/{task}.png"
        if os.path.exists(plot_path):
            try:
                pdf.image(str(plot_path), w=pdf.epw) 
                pdf.ln(5)
            except Exception as e:
                logger.debug(f"would not load image for {task}: {e}")

        if task in result_dct:
            CHAR_THRESH_TASK_DESC = 200
            task_desc = result_dct[task]['description']
            
            if len(task_desc) > CHAR_THRESH_TASK_DESC:
                task_desc = task_desc[:CHAR_THRESH_TASK_DESC] + '...'
                
            pdf.set_font('helvetica', '', 6)
            pdf.cell(0, 8, f"Task description: {task_desc}")
            pdf.ln(10)
            
            df = pd.DataFrame(result_dct[task]['result'])
            if not df.empty:
                
                pdf.set_font('helvetica', 'B', 10)
                pdf.cell(0, 8, "Data Summary:", new_x="LMARGIN", new_y="NEXT")
                pdf.set_font('helvetica', '', 9)
                
                with pdf.table() as table:
                    row = table.row()
                    for col_name in df.columns:
                        row.cell(str(col_name).encode('utf-8').decode('latin-1'))

                    for _, data_row in df.iterrows():
                        row = table.row()
                        for item in data_row:
                            row.cell(str(item).encode('utf-8').decode('latin-1'))
                pdf.ln(10)
            else:
                pdf.set_font('helvetica', '', 7)
                pdf.cell(0, 8, f"This task has an empty result.")
                pdf.ln(10)

        excel_path = f"{artifacts_dir}/{task}.xlsx"
        if os.path.exists(excel_path):
            pdf.ln(5)
            pdf.set_text_color(200, 50, 50)
            pdf.set_font('helvetica', 'I', 10)
            msg = (f"The dataset for task '{task}' was too large to display here. "
                   f"Please refer to '{task}.xlsx' included in the attached archive.")
            pdf.multi_cell(0, 5, msg)
            pdf.set_text_color(0, 0, 0) # Reset color
            pdf.ln(10)

        pdf.set_draw_color(200, 200, 200)
        pdf.line(pdf.get_x(), pdf.get_y(), pdf.epw, pdf.get_y())
        pdf.ln(10)

    pdf.output(output_pdf_path)

    with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(output_pdf_path, arcname=os.path.basename(output_pdf_path))
        
        for file in glob(f'{artifacts_dir}/*.xlsx'):
            filename = os.path.basename(file)
            filename_without_ext = filename.rstrip('.xlsx')
            
            if filename_without_ext.isnumeric() and int(filename_without_ext) in result_dct:
                zipf.write(file, arcname=filename)
    
    return str(output_zip_path)

def remove_file_w_extension(dir_, extension):
    for file in glob(f'{dir_}/*.{extension}'):
        os.remove(file)

def result_save_handler(df, task_id, run_type, task_name, request_id, res_type=Literal['BAR_CHART', 'LINE_CHART', 'DISPLAY_TABLE', 'TABLE_EXPORT']):
    # this function gets the appropriate column names if the res_type is chart, create the chart and save it
    # if output is too big then it'll save the excel
    
    
    def get_bar_chart_cols(df):
        x_axis_col = [i for i in df.columns if 'object' in str(df[i].dtype)][0]
        y_axis_col = [i for i in df.columns if i != x_axis_col][0]
        
        return x_axis_col, y_axis_col
    
    def get_line_chart_cols(df):
        x_axis_col = [i for i in df.columns if 'datetime' in str(df[i].dtype)][0]
        y_axis_col = [i for i in df.columns if i != x_axis_col][0]
        
        return x_axis_col, y_axis_col
    logger.debug(f'run type is {run_type}')
    
    # refactor this into dictionary dispatch
    save_path_dct = {
                     TaskProcessingRunType.first_run_after_request.value: f'{Config.DATASET_SAVE_PATH}/{request_id}/original_tasks/artifacts',
                     TaskProcessingRunType.additional_analyses_request.value: f'{Config.DATASET_SAVE_PATH}/{request_id}/original_tasks/artifacts', 
                     TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value: f'{Config.DATASET_SAVE_PATH}/{request_id}/customized_tasks/artifacts', 
                     TaskProcessingRunType.modified_tasks_execution.value: f'{Config.DATASET_SAVE_PATH}/{request_id}/customized_tasks/artifacts', 
                     }
    
    save_path = save_path_dct[run_type]
    
    # this line handles the artifacts dir creation      
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    
    save_path_plot = f'{save_path}/{task_id}.png'
    save_path_table_export = f'{save_path}/{task_id}.xlsx'
    
    if os.path.exists(save_path_plot):
        os.remove(save_path_plot)
        
    if os.path.exists(save_path_table_export):
        os.remove(save_path_table_export)
    
    # refactor these two into their own functions
    if res_type == 'BAR_CHART':
        x_col, y_col = get_bar_chart_cols(df)
        fig, ax = plt.subplots(figsize=(12, 6))
        sns.barplot(df, x=x_col, y=y_col, ax=ax)
        plt.xticks(rotation=45, ha='right')
        plt.title(task_name)
        fig.tight_layout()
        fig.savefig(save_path_plot)
        plt.close(fig)
        
    elif res_type == 'LINE_CHART':
        x_col, y_col = get_line_chart_cols(df)
        fig, ax = plt.subplots(figsize=(12, 6))
        sns.lineplot(df, x=x_col, y=y_col, ax=ax)
        plt.xticks(rotation=45, ha='right')
        plt.title(task_name)
        fig.tight_layout()
        fig.savefig(save_path_plot)
        plt.close(fig)
        
    elif res_type == 'DISPLAY_TABLE':
        return
    
    elif res_type == 'TABLE_EXPORT':
        df.to_excel(save_path_table_export, index=False)

async def send_email(subject: str, recipients: list, body: str, attachment_path: str = None):
    attachments = []

    if attachment_path:
        attachments.append(attachment_path)

    message = MessageSchema(
        subject=subject,
        recipients=recipients,
        body=body,
        subtype=MessageType.plain,
        attachments=attachments
    )

    conf = ConnectionConfig(
        MAIL_USERNAME=Config.EMAIL_USERNAME,
        MAIL_PASSWORD=Config.EMAIL_PASSWORD,
        MAIL_FROM=Config.EMAIL_USERNAME,
        MAIL_PORT=587,
        MAIL_SERVER=Config.EMAIL_SERVER,
        MAIL_STARTTLS=True,
        MAIL_SSL_TLS=False,
        USE_CREDENTIALS=True,
        VALIDATE_CERTS=True,
    )

    fm = FastMail(conf)
    await fm.send_message(message)
    


def send_email_with_attachment_sync(receiver, subject, body, attachment_path):
    msg = MIMEMultipart()
    msg['From'] = Config.EMAIL_USERNAME
    msg['To'] = receiver
    msg['Subject'] = subject


    msg.attach(MIMEText(body, 'plain'))

    with open(attachment_path, "rb") as attachment:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())

    encoders.encode_base64(part)

    filename = os.path.basename(attachment_path)
    part.add_header("Content-Disposition",f"attachment; filename={filename}")


    msg.attach(part)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(Config.EMAIL_USERNAME, Config.EMAIL_PASSWORD)
        server.send_message(msg)
    # except smtplib.SMTPException as e:
    
def send_task_result_email(recipient, zip_file_dir, run_type, dataset_name, run_name):
    subject = f'Output for your analysis tasks. (Run name: {run_name})'
    body = f'Here is the result of your "{run_type}" analysis tasks using the dataset {dataset_name}.'
    
    asyncio.run(send_email(subject=subject, recipients=[recipient], body=body, attachment_path=zip_file_dir))
    

def get_result_dct(common_tasks_dct):
    res = {}
    for t in common_tasks_dct:
        task_id = t['task_id']
        task_dct = {}
        task_dct['result'] = t['result']
        task_dct['description'] = t['description']
        res[task_id] = task_dct

    return res

def resp_pt1_loader(prompt, model):
    resp_pt_1 = get_prompt_result(prompt, model)     
    resp_pt_1 = process_llm_api_response(resp_pt_1)
    
    return resp_pt_1

def mock_resp_pt1_loader(mock_resp_file):
    logger.info('part 2 prompt mocked')
    with open(mock_resp_file, 'r') as f:
        resp_pt_1 = json.load(f)
        
    return resp_pt_1
def resp_pt2_loader(prompt, model):
    resp_pt_2 = get_prompt_result(prompt, model)     
    resp_pt_2 = process_llm_api_response(resp_pt_2)
    
    return resp_pt_2

def mock_resp_pt2_loader(mock_resp_file):
    logger.info('part 2 prompt mocked')
    with open(mock_resp_file, 'r') as f:
        resp_pt_2 = json.load(f)
    
    return resp_pt_2

def write_prompt_and_res(prompt, res, part, dir_): 
    with open(f'{dir_}/prompt_pt{part}.txt', 'w', encoding='utf-8') as f:
        f.write(prompt)
    with open(f'{dir_}/res_pt{part}.json', 'w', encoding='utf-8') as f:
        json.dump(res, f, indent=4)
    
def addt_req_resp_loader(prompt, model):
    resp = get_prompt_result(prompt, model)     
    resp = process_llm_api_response(resp)
    
    return resp    

def mock_addt_req_resp(mock_resp_file):
    with open(mock_resp_file, 'r') as f:
        resp = json.load(f)
    return resp

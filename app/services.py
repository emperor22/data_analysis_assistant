import pandas as pd
from pandas.api.types import is_numeric_dtype
import numpy as np
import io
from string import Template
import requests

import base64

from abc import ABC, abstractmethod
from fastapi import UploadFile, BackgroundTasks

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

from app.crud import TaskRunTableOperation, PromptTableOperation
from app.schemas import DataTasks, TaskStatus, RunInfoSchema
from app.data_transform_utils import (
    groupby_func, filter_func, get_proportion_func, get_column_statistics_func,
    get_top_or_bottom_n_entries_func, resample_data_func, apply_map_range_func, apply_map_func, 
    apply_date_op_func, apply_math_op_func, get_column_combination_func, 
    get_column_properties, col_transform_and_combination_parse_helper, 
    clean_dataset, handle_datetime_columns_serialization, 
    determine_result_output_type
)
from app.logger import logger

import zipfile
from fpdf import FPDF
from pathlib import Path


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
        self.granularity_map = {}
        self.df = None
        
    def get_dataframe_dict(self):
        self._read_file()
        
        self.df, self.granularity_map = self.clean_dataset_func(self.df)
        
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
        self.df = pd.read_csv(io.BytesIO(self.file_content), encoding= 'unicode_escape')
        
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
    
    # def generate_time_series_context(self):
    #     timeseries_template = '''
    #     - This is a timeseries data and you may use the 'resample_data' function to accomplish an analysis task.
    #     - Date column for downsampling: $date_col_timeseries
    #     - Date granularity: $granularity_timeseries
    #     '''
        
    #     no_timeseries_instruction = "This is not a timeseries data. DO NOT generate tasks that will require using the 'resample_data' function in their steps."
        
    #     if all([self.is_timeseries_data, self.date_col, self.granularity_timeseries]):
    #         template = Template(timeseries_template)
    #         template = template.substitute({'date_col_timeseries': self.date_col_timeseries, 
    #                                         'granularity_timeseries': self.granularity_timeseries})
    #         return template
        
    #     return no_timeseries_instruction
    
class DataAnalysisProcessor:
    def __init__(self, data_tasks: DataTasks, run_info: RunInfoSchema, task_run_table_ops: TaskRunTableOperation, 
                 run_type: Literal['first_run_after_request', 'modified_tasks_execution', 'additional_analyses_request', 'modified_tasks_execution_with_new_dataset']):
        self.run_type = run_type
        
        parquet_file = run_info['parquet_file']
        self.request_id = run_info['request_id']
        self.user_id = run_info['user_id']
        self.send_result_to_email = run_info['send_result_to_email']
        self.email = run_info['email']
        self.dataset_filename = run_info['filename']
        
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
        
        self.common_tasks_runtype_json_key = {
            'first_run_after_request': 'original_common_tasks', 
            'modified_tasks_execution': 'common_tasks_w_result', 
            'modified_tasks_execution_with_new_dataset': 'common_tasks_w_result',
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
            
            # useful for when user run customized tasks on original dataset without running col transform or combination operations
            if self.run_type == 'first_run_after_request': 
                save_dataset_req_id(request_id=self.request_id, dataframe=self.df, save_type='original_dataset')
            
            elif self.run_type == 'modified_tasks_execution_with_new_dataset': # this may not be necessary because this dataset will not be accessed later
                save_dataset_req_id(request_id=self.request_id, dataframe=self.df, save_type='new_dataset')
                
            
            
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
                    
                except Exception as e:
                    error_steps.append(f"failed at step {step_num+1}: {str(e)}")
                    break
                    
            if error_steps:
                task_modified['status'] = '; '.join(error_steps)
                task_modified['result'] = {}
                
                logger.warning(f'some task from common_tasks failed: run_type {self.run_type}, request_id {self.request_id}, task_id {task.task_id}, errors ({"; ".join(error_steps)})')
            else:
                # code block to handle result here (i.e. categorize how to store them: chart, table, or export file)
                
                # for each task id, get output type (chart, table, or export file). if chart include the encoded byte as string for that task id, else empty
                # store this as a dict in class attribute accessible via a method from the get_common_tasks_result and get_customized_tasks_result endpoints
                # the frontend will display chart and table if have both, just table, or nothing for export type output 
                # table output is handled by the section below which is to store them in db
                # 
                # store these artifacts in the request_id folder, inside an 'artifacts' folder maybe. this is for embedding in the output pdf
                
                logger.debug(f'processing task {task.task_id}')
                
                
                res_type = determine_result_output_type(tmp)
                
                self.result_save_handler(df=tmp, run_type=self.run_type, task_id=task.task_id, task_name=task.name, request_id=self.request_id, res_type=res_type)

                DATASET_ROW_THRESHOLD_BEFORE_EXPORT = 20
                DATASET_COLUMNS_THRESHOLD_BEFORE_EXPORT = 5
                
                if not (len(tmp) > DATASET_ROW_THRESHOLD_BEFORE_EXPORT or len(tmp.columns) > DATASET_COLUMNS_THRESHOLD_BEFORE_EXPORT):
                    tmp = handle_datetime_columns_serialization(tmp)
                    task_modified['status'] = 'successful'
                    task_modified['result'] = tmp.to_dict('list') # transform to dictionary for storing in db
                else:
                    task_modified['status'] = 'output of this analysis is too big'
                    task_modified['result'] = {}
                    
            common_tasks_modified.append(task_modified)
        
        runtype_json_key = self.common_tasks_runtype_json_key[self.run_type]
        
        if self.run_type == 'additional_analyses_request':
            tasks_by_id = self.task_run_table_ops.get_task_by_id_sync(user_id=self.user_id, request_id=self.request_id)
            original_common_tasks = tasks_by_id['original_common_tasks']
            original_common_tasks = json.loads(original_common_tasks)['original_common_tasks'] # a list of common tasks
            common_tasks_modified = original_common_tasks + common_tasks_modified # merge the original tasks and the new one
        
        common_task_modified_w_json_key = {runtype_json_key: common_tasks_modified}
        
        if self.run_type == 'first_run_after_request': # update original_common_tasks (now with results) at first task run after llm response
            self.task_run_table_ops.update_original_common_task_result_sync(request_id=self.request_id, 
                                                                            original_common_tasks=json.dumps(common_task_modified_w_json_key))
        elif self.run_type == 'modified_tasks_execution':
            self.task_run_table_ops.update_task_result_sync(request_id=self.request_id, 
                                                            common_tasks_w_result=json.dumps(common_task_modified_w_json_key))
        elif self.run_type == 'modified_tasks_execution_with_new_dataset':
            self.task_run_table_ops.update_task_result_sync(request_id=self.request_id, 
                                                           common_tasks_w_result=json.dumps(common_task_modified_w_json_key))
        elif self.run_type == 'additional_analyses_request':
            self.task_run_table_ops.update_original_common_task_result_sync(request_id=self.request_id, 
                                                                            original_common_tasks=json.dumps(common_task_modified_w_json_key))
        
        if self.send_result_to_email:
            result_dct = get_result_dct(common_tasks_modified)
            
            zip_file_dir = generate_report_and_archive(request_id=self.request_id, run_type=self.run_type, result_dct=result_dct)
            logger.debug('zip file created')
            send_task_result_email(recipient=self.email, zip_file_dir=zip_file_dir, run_type=self.run_type, dataset_name=self.dataset_filename)
            logger.debug('email sent')
            
        self.common_tasks_modified = common_task_modified_w_json_key
            
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
        
    def get_process_result(self):
        result_dct = {'col_combination_status': self.col_combination_tasks_status, 
                      'col_transform_status': self.col_transform_tasks_status, 
                      'common_tasks_result': self.common_tasks_modified, 
                      'col_info': self.col_info}
        return result_dct
    
    @staticmethod
    def result_save_handler(df, task_id, run_type, task_name, request_id, res_type=Literal['BAR_CHART', 'LINE_CHART', 'DISPLAY_TABLE', 'TABLE_EXPORT']):
        
        def get_bar_chart_cols(df):
            x_axis_col = [i for i in df.columns if 'object' in str(df[i].dtype)][0]
            y_axis_col = [i for i in df.columns if i != x_axis_col][0]
            
            return x_axis_col, y_axis_col
        
        def get_line_chart_cols(df):
            x_axis_col = [i for i in df.columns if 'datetime' in str(df[i].dtype)][0]
            y_axis_col = [i for i in df.columns if i != x_axis_col][0]
            
            return x_axis_col, y_axis_col
        logger.debug(f'run type is {run_type}')
        if run_type == 'first_run_after_request':
            save_path = f'{DATASET_SAVE_PATH}/{request_id}/original_tasks/artifacts'
        elif run_type in ('modified_tasks_execution', 'modified_tasks_execution_with_new_dataset'):
            save_path = f'{DATASET_SAVE_PATH}/{request_id}/customized_tasks/artifacts'
        elif run_type == 'additional_analyses_request':
            save_path = f'{DATASET_SAVE_PATH}/{request_id}/original_tasks/artifacts'
                
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        
        save_path_plot = f'{save_path}/{task_id}.png'
        save_path_table_export = f'{save_path}/{task_id}.xlsx'
        
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
            df.to_excel(save_path_table_export)
  

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

def get_task_plot_results(request_id, run_type):
    
    def get_image_byte_as_str(image_path):
        with open(image_path, "rb") as f:
            data = f.read()

        encoded = base64.b64encode(data).decode("utf-8")
        
        return encoded
    
    save_dir_dct = {'first_run_after_request': 'original_tasks', 'modified_tasks_execution': 'customized_tasks', 
                    'additional_analyses_request': 'original_tasks', 'modified_tasks_execution_with_new_dataset': 'customized_tasks'}
    save_dir = save_dir_dct[run_type]
    
    path = f'{DATASET_SAVE_PATH}/{request_id}/{save_dir}/artifacts'
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
                      TaskStatus.doing_initial_tasks_run.value, TaskStatus.failed_because_blacklisted_dataset)
    
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

def get_dataset_id(df: pd.DataFrame):
    return json.dumps({col: str(df[col].dtype) for col in sorted(df.columns)})

def save_dataset_req_id(request_id: str, dataframe: pd.DataFrame, 
                        save_type: Literal['original_dataset', 'new_dataset']):
    save_path = f'{DATASET_SAVE_PATH}/{request_id}'
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
        self.set_fill_color(200, 220, 255)  # Light blue background
        self.cell(0, 10, f'Task Result: {task_id}', new_x="LMARGIN", new_y="NEXT", fill=True)
        self.ln(5)

    def chapter_body_text(self, text):
        self.set_font('helvetica', '', 10)
        self.multi_cell(0, 5, text)
        self.ln()

def generate_report_and_archive(request_id, result_dct, run_type):

    base_dir = Path(f'{DATASET_SAVE_PATH}/{request_id}')
    save_folder_dct = {'first_run_after_request': 'original_tasks', 'modified_tasks_execution': 'customized_tasks', 
                       'additional_analyses_request': 'original_tasks', 'modified_tasks_execution_with_new_dataset': 'customized_tasks'}
    save_folder = save_folder_dct[run_type]
    artifacts_dir = base_dir / save_folder / 'artifacts'
    
    output_filename_dct = {'first_run_after_request': 'original_tasks', 'modified_tasks_execution': 'customized_tasks', 
                      'additional_analyses_request': 'original_tasks', 'modified_tasks_execution_with_new_dataset': 'customized_tasks'}
    output_filename = output_filename_dct[run_type]
    
    output_pdf_path = base_dir / f'{output_filename}_report.pdf'
    output_zip_path = base_dir / f'{output_filename}_{request_id}.zip'
    
    task_ids = set(result_dct.keys())
        
    sorted_task_ids = sorted(list(task_ids))

    pdf = ReportPDF()
    pdf.add_page()

    for task in sorted_task_ids:
        pdf.chapter_title(task)

        plot_path = artifacts_dir / f"{task}.png"
        if plot_path.exists():
            try:
                pdf.image(str(plot_path), w=pdf.epw) 
                pdf.ln(5)
            except Exception as e:
                print(f"Warning: Could not load image for {task}: {e}")

        if task in result_dct:
            df = pd.DataFrame(result_dct[task])
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

        excel_path = artifacts_dir / f"{task}.xlsx"
        if excel_path.exists():
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
        zipf.write(output_pdf_path, arcname=output_pdf_path.name)
        
        for file in artifacts_dir.glob("*.xlsx"):
            zipf.write(file, arcname=file.name)

    print(f"Process Complete. Archive created: {output_zip_path}")
    return str(output_zip_path)
        
    
EMAIL_USERNAME = 'daaotpsender@gmail.com'
EMAIL_PASSWORD = 'rfww zlil bhbl emjz'
EMAIL_SERVER = 'smtp.gmail.com'


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
        MAIL_USERNAME=EMAIL_USERNAME,
        MAIL_PASSWORD=EMAIL_PASSWORD,
        MAIL_FROM=EMAIL_USERNAME,
        MAIL_PORT=587,
        MAIL_SERVER=EMAIL_SERVER,
        MAIL_STARTTLS=True,
        MAIL_SSL_TLS=False,
        USE_CREDENTIALS=True,
        VALIDATE_CERTS=True,
    )

    fm = FastMail(conf)
    await fm.send_message(message)
    
def send_task_result_email(recipient, zip_file_dir, run_type, dataset_name):
    subject = f'Output for your analysis tasks. (Dataset name: {dataset_name})'
    body = f'Here is the result of your "{run_type}" analysis tasks'
    
    asyncio.run(send_email(subject=subject, recipients=[recipient], body=body, attachment_path=zip_file_dir))
    

def get_result_dct(common_tasks_dct):
    res = {}
    for t in common_tasks_dct:
        task_id = t['task_id']
        res[task_id] = t['result']

    return res

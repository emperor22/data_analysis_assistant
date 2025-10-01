import pandas as pd
from pandas.api.types import is_numeric_dtype
import numpy as np
import io
from string import Template
import requests

from abc import ABC, abstractmethod
from fastapi import UploadFile

import re
import ast
import json

from app.crud import TaskRunTableOperation
from app.schemas import DataTasks


##### CONFIG #####
N_ROWS_READ_UPLOAD_FILE = 100


##################

class FileReader(ABC):
    def __init__(self, upload_file: UploadFile):
        self.file_content = upload_file.file.read()
        self.filename = upload_file.filename
        self.df = None
        
    def get_dataframe_dict(self):
        self._read_file()
        self._clean_dataset()
        
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
    
    def _clean_dataset(self):
        df = self.df.copy()

        # for column names, strip, make lowercase, replace space and dash with _, and remove non-alphanum characters
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_', regex=False).str.replace('-', '_') \
                               .str.replace('[^a-z0-9_]', '', regex=True)
        
        # cleaning column values
        for col in df.columns:
            # assume column is numeric and try to cast it to numeric type after cleaning
            if df[col].dtype == 'object':
                temp_series = pd.to_numeric(df[col].astype(str).str.replace(r'[^0-9.\-]', '', regex=True), errors='coerce')

                numeric_success_rate = 1 - (temp_series.isnull().sum() / len(temp_series))
                
                if numeric_success_rate > 0.5: # more than 50% of col values are successfully converted to numeric, so keep it 
                    df[col] = temp_series
                else: # col is probably a string column
                    df[col] = df[col].astype(str).str.strip().str.lower().replace('', np.nan) # clean the string column
            else:
                continue # column is already numeric type and doesnt require cleaning
                
        self.df = df
    
class CsvReader(FileReader):        
    def _read_file(self) -> pd.DataFrame:
        self.df = pd.read_csv(io.BytesIO(self.file_content))
        
class DatasetProcessorForPrompt:
    def __init__(self, dataframe: pd.DataFrame, filename: str, prompt_template_file: str, task_count: int):
        self.dataframe = dataframe
        self.filename = filename
        self.prompt_template_file = prompt_template_file
        self.task_count = task_count
        
    def create_prompt(self) -> str:
        with open(self.prompt_template_file, 'r') as f:
            template = Template(f.read())
        
        context = {'task_count': self.task_count,
                   'dataset_name': self.filename, 
                   'dataset_snippet': self._get_dataset_snippet(), 
                   'dataset_column_unique_values': self._get_column_unique_values()}
        return template.substitute(context)
    
    def _get_dataset_snippet(self) -> str:
        return self.dataframe.iloc[:5].to_csv(index=False)
    
    def _get_column_unique_values(self) -> dict:
        unique_val_dct = {}
        for col in self.dataframe.columns:
            unique_val_dct[col] = {}
            unique_val_dct[col]['num_of_unique_values'] = self.dataframe[col].nunique()
            unique_val_dct[col]['top_5_values'] = self.dataframe[col].value_counts().index[:5].tolist()
        return unique_val_dct

    
    
class DataAnalysisProcessor:
    def __init__(self, data_tasks: DataTasks, run_info: dict, task_run_table_ops: TaskRunTableOperation, 
                 common_tasks_only=False, first_run_after_request=True):
        self.common_tasks_only = common_tasks_only
        self.first_run_after_request = first_run_after_request
        
        parquet_file = run_info['parquet_file']
        self.request_id = run_info['request_id']
        self.user_id = run_info['user_id']
        
        self.df: pd.DataFrame = pd.read_parquet(parquet_file)
        self.columns = self.df.columns.tolist()
        
        self.data_tasks = data_tasks
        
        self.task_run_table_ops = task_run_table_ops
        
        self.common_tasks_fn_map = {
            'groupby': self._groupby_func, 
            'filter': self._filter_func, 
            'get_top_or_bottom_N_entries': self._get_top_or_bottom_n_entries_func, 
            'get_proportion': self._get_proportion_func, 
            'get_column_statistics': self._get_column_statistics_func
            }
        
        self.column_transform_fn_map = {
            'MAP_RANGE': self._apply_map_range,
            'DATE_OP': self._apply_date_op,
            'MATH_OP': self._apply_math_op,
            'MAP': self._apply_map,
            }
    
    def process_all_tasks(self):
        if not self.common_tasks_only:
            self._process_column_transforms()
            self._process_column_combinations()
        
        self._process_common_tasks()
    
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
                    step = step.model_dump()
                    func = self.common_tasks_fn_map[step['function']]
                    kwargs = {i: j for i, j in step.items() if i != 'function'}
                    tmp: pd.DataFrame = func(tmp, **kwargs)
                    
                    # mostly for pure filtering task. idea: let user know this doesnt fail, but they can export the filter to use on their own

                    if tmp.empty:
                        raise Exception('empty result dataset')
                    
                    datetime_columns = tmp.select_dtypes(include=['datetime64[ns]']).columns.tolist()
                    
                    # because timestamp object is not serializable
                    if datetime_columns:
                        for dt_col in datetime_columns:
                            tmp[dt_col] = tmp[dt_col].dt.strftime('%Y-%m-%d %H:%M:%S')
                    
                    if step_num == len(steps) - 1 and (len(tmp) > 1000 or len(tmp.columns) > 20):
                        raise Exception('output of this analysis is too big')
                    
                except Exception as e:
                    error_steps.append(f"failed at step {step_num+1}: {str(e)}")
                    break
                    
            if error_steps:
                task_modified['status'] = '; '.join(error_steps)
                task_modified['result'] = {}
            else:
                task_modified['status'] = 'successful'
                task_modified['result'] = tmp.to_dict('list')
                    
            common_tasks_modified.append(task_modified)
        
        if self.first_run_after_request: # update original_common_tasks (now with results) at first task run after llm response
            self.task_run_table_ops.update_original_common_task_result_sync(request_id=self.request_id, 
                                                                            original_common_tasks=json.dumps({'original_common_tasks': common_tasks_modified})
                                                                            )
        else:
            self.task_run_table_ops.update_task_result_sync(request_id=self.request_id, 
                                                            common_tasks_w_result=json.dumps({'common_tasks_w_result': common_tasks_modified}), 
                                                            )
    
    def _process_column_transforms(self):
        column_transformation_tasks = self.data_tasks.common_column_cleaning_or_transformation
        tmp = self.df.copy()
        tasks_status = {}
        
        for task in column_transformation_tasks:
            task = task.model_dump()
            type_ = task['formula'].split('(')[0]
            name = task['name'].replace(' ', '_')
            operation = task['operation']
            
            try:
                func = self.column_transform_fn_map[type_]
                tmp = func(df=tmp, name=name, operation=operation)
                tasks_status[task['name']] = 'successful'
                
            except Exception as e:
                tasks_status[task['name']] = f'failed. error: {e.args}'
                continue
        
        self.task_run_table_ops.update_column_transform_task_status_sync(request_id=self.request_id, column_transforms_status=json.dumps(tasks_status))   
        self.df = tmp.copy()
        self.columns = self.df.columns.tolist()
        
        
    def _process_column_combinations(self):
        column_combination_tasks = self.data_tasks.common_column_combination
        tmp = self.df.copy()
        tasks_status = {}
        
        for task in column_combination_tasks:
            task = task.model_dump()
            name = task['name'].replace(' ', '_')
            operation = task['operation']
            try:
                tmp = self._get_column_combination(df=tmp, name=name, operation=operation)
                tasks_status[name] = 'successful'
            except Exception as e:
                tasks_status[name] = f'failed. error: {e.args}'
                continue

        self.task_run_table_ops.update_column_combination_task_status_sync(request_id=self.request_id, column_combinations_status=json.dumps(tasks_status))   
        self.df = tmp.copy()
        self.columns = self.df.columns.tolist()
                
        
    ########### common tasks functions ###########
    
    def _validate_columns(self, cols_to_check: str | list):
        if not cols_to_check:
            return
        
        if isinstance(cols_to_check, str):
            cols_to_check = [cols_to_check]
            
        if not all(i in self.columns for i in cols_to_check):
            raise Exception('some columns mentioned in task do not exist')
    
    def _groupby_func(self, df: pd.DataFrame, columns_to_group_by: list, columns_to_aggregate: list, calculation: str):
        for cols_to_check in (columns_to_group_by, columns_to_aggregate):
            self._validate_columns(cols_to_check)
            
        df = df.copy()
        
        if len(calculation) > 1:
            agg_dct = {col: calculation for col in columns_to_aggregate}
            df = df.groupby(columns_to_group_by, as_index=False).agg(agg_dct)
            df.columns = [f'{i[0]}_{i[1]}' for i in df.columns] # merging the two level of column names into one
            
        elif len(calculation) == 1:
            calc = calculation[0]
            df = df.groupby(columns_to_group_by, as_index=False)[columns_to_aggregate].apply(calc)
            
        return df
    
    def _filter_func(self, df: pd.DataFrame, column_name: str, operator: str, values: list | str):
        self._validate_columns(column_name)
        df = df.copy()
        
        if len(values) == 0:
            raise Exception('need to have filter values')
        
        valid_operators = ['>', '<', '>=', '<=', '==', '!=', 'in', 'between']
        if operator not in valid_operators:
            raise Exception('invalid filter operator')
        
        if operator == 'in':
            query_string = f"{column_name} {operator} @values"
        elif operator == 'between':
            values = sorted(values)
            min_val, max_val = values
            query_string = f"{column_name} >= @min_val and {column_name} <= @max_val"
        else:
            flt = repr(values[0])
            query_string = f"{column_name} {operator} {flt}"
        
        df = df.query(query_string)

        return df
                    
    def _get_top_or_bottom_n_entries_func(self, df: pd.DataFrame, sort_by_column_name: str, order: str, number_of_entries: int, return_columns: list):
        for cols_to_check in sort_by_column_name, return_columns:
            self._validate_columns(cols_to_check)
        
        df = df.copy()
        
        asc_dct = {'bottom': True, 'top': False}
        ascending = asc_dct[order]
        return df.sort_values(sort_by_column_name, ascending=ascending)[return_columns].iloc[:number_of_entries]
    
    def _get_proportion_func(self, df: pd.DataFrame, column_name: str, values=None):
        self._validate_columns(column_name)
        df = df.copy()
        
        if isinstance(column_name, list):
            column_name = column_name[0]
            
        df = df[column_name].value_counts(normalize=True).reset_index()

        if values is not None and len(values) > 0:
            df = df[df[column_name].isin(values)]

        return df
    
    def _get_column_statistics_func(self, df: pd.DataFrame, column_name: str, calculation: str):
        self._validate_columns(column_name)
        df = df.copy()
        
        return df[column_name].agg(calculation)
    
    ####### column transform functions ########
      
    def _apply_map(self, df, name: str, operation: dict):
        self._validate_columns(operation['source_column'])

        df = df.copy()
        source_col = operation['source_column']
        mapping = operation['mapping']
        mapping = {str(i): j for i, j in mapping.items()} # standardizing mapped values to str
        
        df[name] = df[source_col].astype(str).map(mapping) # convert to string to get valid mapping
        
        return df

    def _apply_map_range(self, df, name: str, operation: dict):
        self._validate_columns(operation['source_column'])

        df = df.copy()
        source_col = operation['source_column']
        ranges = operation['ranges']

        ranges_lst = []
        for r_def in ranges:
            range_str = r_def['range']
            label = r_def['label']

            range_str = range_str.replace('inf', f'{1e20}') # replacing inf with big number for easier parsing
            start_str, end_str = range_str.split('-')
            
            start = float(start_str)
            end = float(end_str)
            
            ranges_lst.append({'start': start, 'end': end, 'label': label})


        def get_label(value):
            if pd.isna(value):
                return np.nan
            
            for r in ranges_lst:
                start, end, label = r['start'], r['end'], r['label']
                
                if start <= value <= end:
                    return label
            
            return np.nan
        
        df[name] = df[source_col].apply(get_label)
            
        return df

    def _apply_date_op(self, df, name: str, operation: dict):
        self._validate_columns(operation['source_column'])
        
        df = df.copy()
        source_col = operation['source_column']
        function = operation['function'].upper()
        
        if source_col in df.columns:
            date_series = pd.to_datetime(df[source_col], errors='coerce')
            
            if function == 'YEAR':
                df[name] = date_series.dt.year
            elif function == 'MONTH':
                df[name] = date_series.dt.month
            elif function == 'DAY':
                df[name] = date_series.dt.day
            elif function == 'WEEKDAY':
                df[name] = date_series.dt.weekday
            
        return df

    def _apply_math_op(self, df, name: str, operation: dict):
        self._validate_columns(operation['source_column'])

        df = df.copy()
        expression = operation['expression']

        try:
            df[name] = df.eval(expression)
        except Exception:
            print(f"invalid math_op expression")

        return df

    ########### column combination functions ###########
    
    def _get_column_combination(self, df: pd.DataFrame, name: str, operation: str):
        self._validate_columns(operation['source_columns'])

        df = df.copy()
        expression = operation['expression']

        try:
            df[name] = df.eval(expression)
        except Exception:
            print(f"invalid column combination expression")

        return df
    

    
def save_uploaded_file(file: UploadFile, save_dir: str):
        contents = file.file.read()
        filename = file.filename
        with open(f'{save_dir}/{filename}', 'wb') as f:
            f.write(contents)


def get_prompt_result(prompt, model):
    key = 'AIzaSyB0Kbhw7DpGlA_tAolL4tGdRajI9k4F_s4'
    url = f'https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent'
    
    payload_data = {
        "contents": [
        {
            "parts": [
            {
                "text": prompt
            }
            ]
        }
        ]
    }

    headers = {
        'Content-Type': 'application/json',
        'X-goog-api-key': key
    }
    
    r = requests.post(url, json=payload_data, headers=headers)
    r.raise_for_status()
        
    return r.json()

def process_llm_api_response(resp: dict):
    prompt_resp = resp['candidates'][0]['content']['parts'][0]['text']
    clean_json = re.search(r"```json\s*(.*?)\s*```", prompt_resp, re.DOTALL)
    clean_json = clean_json.group(1).strip()
    data = json.loads(clean_json)
    
    # prompt_token_count = resp['usage_metadata']['promptTokenCount']
    # total_token_count = resp['usage_metadata']['totalTokenCount']
    
    return data



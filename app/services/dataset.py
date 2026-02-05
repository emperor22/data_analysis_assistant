from app.data_transform_utils import clean_dataset, get_granularity_map, get_dataset_id, is_numeric
from app.exceptions import InvalidDatasetException, FileReadException
from app.config import Config
from app.schemas import TaskProcessingRunType

from abc import ABC, abstractmethod
from fastapi import UploadFile

import pandas as pd
import io
import csv
import os



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
        
        self._validate_dataset()
        sorted_unique_cols = sorted(list(set(self.df.columns)))
        return {'filename': self.filename, 'dataframe': self.df, 'columns_str': str(sorted_unique_cols), 
                'dataset_id': get_dataset_id(self.df), 'granularity_map': self.granularity_map}
    
    @abstractmethod
    def _read_file(self, nrows=None):
        '''Reads the file and returns a pandas dataframe'''
        pass
    
    def _validate_dataset(self):
        if (
            (self.df.shape[0] > Config.MAX_DATAFRAME_ROWS or self.df.shape[1] > Config.MAX_DATAFRAME_COLS)
            or not self._dataset_has_header
            ):
            raise InvalidDatasetException
        
        return True
    
    def _dataset_has_header(self):
        return not any(is_numeric(col) for col in self.df.columns)
    


    
class CsvReader(FileReader):        
    def _read_file(self, nrows=None) -> pd.DataFrame:
        try:
            self.df = pd.read_csv(io.BytesIO(self.file_content), encoding='unicode_escape', sep=None, nrows=nrows)
        except Exception:
            raise FileReadException
        
        
def get_row_count_csv(upload_file: UploadFile):
    file = csv.reader(upload_file.file.read())
    row_count = sum(1 for _ in file)
    return row_count

def get_column_names_csv(upload_file: UploadFile):
    try:
        reader = csv.reader(io.BytesIO(upload_file.file.read()))
        try:
            headers = next(reader)
            return headers
        except StopIteration:
            return 'this file has no headers'
    except Exception as e:
        return f'an error occured {e}'

def get_dataset_snippet(df: pd.DataFrame):
    return df.iloc[:5].to_csv(index=False)


def get_request_id_saved_dataset_dir(request_id, run_type):
    filename_dct = {TaskProcessingRunType.first_run_after_request.value: 'original_dataset.parquet', 
                    TaskProcessingRunType.modified_tasks_execution.value: 'original_dataset.parquet', 
                    TaskProcessingRunType.additional_analyses_request.value: 'original_dataset.parquet', 
                    TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value: 'new_dataset.parquet'}
    filename = filename_dct[run_type]
    
    save_path = f'{Config.DATASET_SAVE_PATH}/{request_id}'
    file_dir = f'{save_path}/{filename}'
    
    return file_dir
        

def save_dataset_req_id(request_id, dataframe: pd.DataFrame, run_type):
    save_path = f'{Config.DATASET_SAVE_PATH}/{request_id}'
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    
    file_dir = get_request_id_saved_dataset_dir(request_id, run_type)
    
    dataframe.to_parquet(file_dir, index=False)
    
    return file_dir
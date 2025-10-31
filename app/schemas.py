from pydantic import BaseModel, Field, ConfigDict, field_validator, conlist, constr, ValidationError, model_validator
from typing import List, Union, Dict, Any, Literal
import re
from typing import Optional
from enum import Enum
from app.logger import logger


MIN_VALID_COMMON_TASKS_FIRST_RUN = 5
MIN_VALID_COMMON_TASKS_SUBSEQUENT_RUNS = 1

class TaskStatus(Enum):
    waiting_for_initial_request_prompt = 'GETTING INITIAL REQUEST PROMPT RESULT'
    waiting_for_additional_analysis_prompt_result = 'GETTING ADDITIONAL ANALYSES REQUEST PROMPT RESULT'
    
    initial_request_prompt_received = 'INITIAL REQUEST PROMPT RESULT RECEIVED'
    additional_analysis_prompt_result_received = 'ADDITIONAL ANALYSES PROMPT RESULT RECEIVED'
    
    doing_initial_tasks_run = 'RUNNING INITIAL ANALYSES TASKS'
    doing_additional_tasks_run = 'RUNNING ADDITIONAL ANALYSES TASKS'
    doing_customized_tasks_run = 'RUNNING USER CUSTOMIZED ANALYSIS TASKS'
    
    initial_tasks_run_finished = 'INITIAL ANALYSIS TASKS FINISHED'
    additional_tasks_run_finished = 'ADDITIONAL ANALYSES TASKS FINISHED'
    customized_tasks_run_finished = 'USER CUSTOMIZED ANALYSIS TASKS FINISHED'
    
class GetCurrentUserModel(BaseModel):
    username: str
    user_id: str
    
class UserRegisterModel(BaseModel):
    username: str
    email: str
    first_name: str
    last_name: str


class CommonColumnCombinationOperation(BaseModel):
    source_columns: List[str] = Field(min_length=1)  # type: ignore
    expression: str = Field(pattern=r'^[a-zA-Z0-9_\s\+\-\*/\(\)\.]+$') # type: ignore
    
    model_config = ConfigDict(extra='forbid')
    
    @model_validator(mode='after')
    @classmethod
    def check_if_columns_match_expression(cls, model_instance):
        regex = r'\b(?![0-9]+(\.[0-9]+)?\b)([a-zA-Z0-9_]+)\b'
        expression = model_instance.expression
        source_cols = model_instance.source_columns
        
        matches = re.finditer(regex, expression)
        matches = [match.group(2) for match in matches]
        
        if not sorted(matches) == sorted(source_cols):
            raise ValueError('columns in expressions dont match source columns')
        return model_instance
    
class CommonColumnCombinationModel(BaseModel):
    name: str
    description: str
    operation: dict
    
    model_config = ConfigDict(extra='forbid')

class MapOperation(BaseModel):
    type: Literal['map'] = 'map'
    source_column: str
    mapping: Dict[Union[str, int, float], Union[str, int]]
    
    model_config = ConfigDict(extra='forbid')
    

class RangeItem(BaseModel):
    range: str = Field(pattern=r'^\d+(?:\.\d+)?[-](\d+(?:\.\d+)?|inf)$')
    label: str
    
    model_config = ConfigDict(extra='forbid')

class MapRangeOperation(BaseModel):
    type: Literal['map_range'] = 'map_range'
    source_column: str
    ranges: List[RangeItem] = Field(min_length=1)
    
    model_config = ConfigDict(extra='forbid')

class DateOpOperation(BaseModel):
    type: Literal['date_op'] = 'date_op'
    source_column: str
    function: Literal['YEAR','MONTH','DAY','WEEKDAY']
    
    model_config = ConfigDict(extra='forbid')

class MathOpOperation(BaseModel):
    type: Literal['math_op'] = 'math_op'
    source_columns: str | list = Field(min_length=1, max_length=1) 
    expression: str = Field(pattern=r'^[a-zA-Z0-9_\s\+\-\*/\(\)\.]+$')
    
    model_config = ConfigDict(extra='forbid')
    
    @model_validator(mode='after')
    @classmethod
    def check_if_columns_match_expression(cls, model_instance):
        regex = r'\b(?![0-9]+(\.[0-9]+)?\b)([a-zA-Z0-9_]+)\b'
        expression = model_instance.expression
        source_cols = model_instance.source_columns
        
        matches = re.finditer(regex, expression)
        matches = [match.group(2) for match in matches]
        
        if not sorted(matches) == sorted(source_cols):
            raise ValueError('columns in expressions dont match source columns')
        return model_instance

class CommonColumnCleaningOrTransformationModel(BaseModel):
    name: str
    description: str
    operation: dict # this will be validated in the outer class (DataAnalysisModel)
    
    model_config = ConfigDict(extra='forbid')


class ColumnModel(BaseModel):
    name: str
    classification: Literal[
        'Identifier', 'Dimensional', 'Metric', 'Temporal', 
        'Geospatial', 'Scientific', 'Descriptive', 'PII', 
        'System/Metadata', 'Unknown'
    ]
    confidence_score: Literal['low', 'medium', 'high', 'inapplicable']
    data_type: Literal['string', 'integer', 'float', 'datetime']
    type: Literal['Categorical', 'Numerical']
    unit: str = ''
    expected_values: str | List[str | int | float] = []
    
    model_config = ConfigDict(extra='forbid')

class FilterStepModel(BaseModel):
    function: Literal['filter']
    column_name: List[str] | str
    operator: Literal['in', '>', '<', '>=', '<=', '==', '=', '<>', '!=', 'between']
    values: List[Any] | str | int | float
    
    model_config = ConfigDict(extra='forbid')

class GroupByStepModel(BaseModel):
    function: Literal['groupby']
    columns_to_group_by: List[str] = Field(min_length=1)
    columns_to_aggregate: List[str] = Field(min_length=1)
    calculation: List[Literal['mean', 'median', 'min', 'max', 'count', 'size', 'sum']]

    model_config = ConfigDict(extra='forbid')
    
    @model_validator(mode='after')
    @classmethod
    def check_count_count_aggregation(cls, model_instance):
        group_cols = model_instance.columns_to_group_by
        agg_cols = model_instance.columns_to_aggregate
        
        if sorted(group_cols) == sorted(agg_cols):
            raise ValueError('similar columns for groupby-aggregate pair')
        return model_instance

class TopBottomNStepModel(BaseModel):
    function: Literal['get_top_or_bottom_N_entries']
    sort_by_column_name: List[str] | str
    order: Literal['top', 'bottom']
    number_of_entries: int
    return_columns: List[str]
    
    model_config = ConfigDict(extra='forbid')

class ProportionStepModel(BaseModel):
    function: Literal['get_proportion']
    column_name: List[str] | str
    values: List[str | int] = []
    
    model_config = ConfigDict(extra='forbid')

class ColStatsStepModel(BaseModel):
    function: Literal['get_column_statistics']
    column_name: List[str] | str
    calculation: List[Literal['mean', 'median', 'min', 'max', 'count', 'sum']]
    
    model_config = ConfigDict(extra='forbid')
    
class CommonTaskModel(BaseModel):
    name: str
    description: str
    steps: list # this will be validated in the outer validation (DataAnalysisModel)
    score: Literal['low', 'medium', 'high', 'inapplicable']
    task_id: int
    
    model_config = ConfigDict(extra='forbid')


STEP_MODELS = {'filter': FilterStepModel, 'groupby': GroupByStepModel, 'get_top_or_bottom_N_entries': TopBottomNStepModel, 
               'get_proportion': ProportionStepModel, 'get_column_statistics': ColStatsStepModel}
TRANSFORM_MODELS = {'map': MapOperation, 'map_range': MapRangeOperation, 'date_op': DateOpOperation, 'math_op': MathOpOperation}
COMBINATION_MODELS = {'column_combination': CommonColumnCombinationOperation}

def validate_model_wrapper(val, model):
    try:
        model.model_validate(val)
        return True
    except ValidationError:
        return False

def log_invalid_values(run, invalid_vals_num, req_id, common_task_id):
    invalid_vals_num = [str(i) for i in invalid_vals_num]
    if run == 'common_tasks':
        logger.warning(f'invalid common_tasks step: request_id {req_id}, task id {common_task_id}, steps {", ".join(invalid_vals_num)}')
    else:
        logger.warning(f'invalid {run} task: request_id {req_id}, task nums ({", ".join(invalid_vals_num)})')

def filter_out_invalid_values(values, model_key_func, model_map, run, req_id, common_task_id=None):
    valid_vals_num = []
    invalid_vals_num = []
    for i, val in enumerate(values):
        try:
            model_key = model_key_func(val)
            model = model_map[model_key]
        except (AttributeError, KeyError): # doesnt have a valid model
            invalid_vals_num.append(i)
            continue

        if validate_model_wrapper(val, model):
            valid_vals_num.append(i)
        else:
            invalid_vals_num.append(i)
    
    if len(invalid_vals_num) > 0:
        log_invalid_values(run=run, invalid_vals_num=invalid_vals_num, req_id=req_id, common_task_id=common_task_id)
            
    return valid_vals_num

class DatasetAnalysisModelPartOne(BaseModel):
    domain: str
    description: str
    columns: List[ColumnModel]
    common_column_cleaning_or_transformation: list[CommonColumnCleaningOrTransformationModel] = []
    common_column_combination: list[CommonColumnCombinationModel] = []
    
    model_config = ConfigDict(extra='forbid')
    
    @field_validator('common_column_cleaning_or_transformation', mode='after')
    @classmethod
    def filter_out_invalid_transforms(cls, values, info):
        req_id = info.context.get('request_id')
        
        model_key_func = lambda val: val['type']
        values_to_check = [val.operation for val in values]
        valid_vals_num = filter_out_invalid_values(values_to_check, model_key_func, TRANSFORM_MODELS, 'column_transformation', req_id)
        
        return [val for i, val in enumerate(values) if i in valid_vals_num]

    @field_validator('common_column_combination', mode='after')
    @classmethod
    def filter_out_invalid_combinations(cls, values, info):
        req_id = info.context.get('request_id')
        
        model_key_func = lambda _: 'column_combination'
        values_to_check = [val.operation for val in values]
        valid_vals_num = filter_out_invalid_values(values_to_check, model_key_func, COMBINATION_MODELS, 'column_combination', req_id)

        return [val for i, val in enumerate(values) if i in valid_vals_num]
    
    @field_validator('columns', mode='after')
    @classmethod
    def check_all_columns_exist(cls, value, info):
        req_cols = info.context.get('required_cols')
        req_id = info.context.get('request_id')
        resp_cols = [col.name for col in value]
        missing = set(req_cols) - set(resp_cols)
        if missing:
            missing_cols = {', '.join(missing)}
            logger.warning(f"some columns are missing from the 'columns' section. cols list {missing_cols}; request_id {req_id}")
            raise ValueError(f"some columns are missing from the response: {missing_cols}")
            
        return value
class DatasetAnalysisModelPartTwo(BaseModel): # smaller model for subsequent task runs
    common_tasks: list[CommonTaskModel] = Field(min_length=1)

    model_config = ConfigDict(extra='forbid')


    @field_validator('common_tasks', mode='after')
    @classmethod
    def filter_common_tasks(cls, values, info):
        req_id = info.context.get('request_id')
        
        valid_values = []
        
        for task in values:
            model_key_func = lambda val: val['function']
            valid_steps_num = filter_out_invalid_values(task.steps, model_key_func, STEP_MODELS, 'common_tasks', req_id, task.task_id)
            
            if len(valid_steps_num) < len(task.steps):
                logger.warning(f"task discarded: task id {task.task_id}") 
            
            if len(valid_steps_num) == len(task.steps):
                valid_values.append(task)
        
        run_type = info.context.get('run_type')
        
        min_valid_values = MIN_VALID_COMMON_TASKS_FIRST_RUN if run_type == 'first_run_after_request' else MIN_VALID_COMMON_TASKS_SUBSEQUENT_RUNS
        
        if len(valid_values) < min_valid_values:
            raise ValueError('too few valid common_tasks')
        return valid_values
    
class DataTasks(BaseModel):
    columns: list[ColumnModel] = []
    common_column_cleaning_or_transformation: list[CommonColumnCleaningOrTransformationModel] = []
    common_tasks: list[CommonTaskModel] = Field(min_length=1)
    common_column_combination: list[CommonColumnCombinationModel] = []

    model_config = ConfigDict(extra='forbid')  
    

if __name__ == '__main__':
    import json
    with open('resp1.json', 'r') as f:
        data = json.load(f)
        
    DatasetAnalysisModelPartOne.model_validate(data, context={
        'required_cols': 
        ['id', 'loan_amnt', 'term', 'int_rate', 'installment', 'home_ownership','annual_inc', 'verification_status', 'issue_d', 'loan_status','purpose', 'total_pymnt']})
    

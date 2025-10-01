from pydantic import BaseModel, Field, ConfigDict, field_validator, conlist, constr, ValidationError, model_validator
from typing import List, Union, Dict, Any, Literal
import re
from typing import Optional

TASK_COUNT_LLM_RESP = 10
class GetCurrentUserModel(BaseModel):
    username: str
    user_id: int
    
class UserRegisterModel(BaseModel):
    username: str
    email: str
    first_name: str
    last_name: str
    password: str
    
class ChangePasswordModel(BaseModel):
    current_pass: str
    new_pass: str
    new_pass_confirm: str    

class ColumnInfo:
    pass
# idea: {'original_columns': [response from llm from columns section + num of unique values], 
#        'added columns': [response from llm from column transform and column combination which are successfully created]}
# maybe add these additional info: missing value total, skewness, duplicate values (give option to enable duplicate value removal if key columns are provided)


class CommonColumnCombinationModel(BaseModel):
    name: str
    formula: str
    
    model_config = ConfigDict(extra='forbid')

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
    range: str = Field(pattern=r'^\d+(?:\.\d+)?[-\+](\d+|inf)$')
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
        source_cols = model_instance.source_colums
        
        matches = re.finditer(regex, expression)
        matches = [match.group(2) for match in matches]
        
        if not sorted(matches) == sorted(source_cols):
            raise ValueError('columns in expressions dont match source columns')
        return model_instance

class CommonColumnCleaningOrTransformationModel(BaseModel):
    name: str
    description: str
    operation: dict
    
    model_config = ConfigDict(extra='forbid')


class ColumnModel(BaseModel):
    name: str
    classification: Literal[
        'Identifier', 'Dimensional', 'Metric', 'Temporal', 
        'Geospatial', 'Scientific', 'Descriptive', 'PII', 
        'System/Metadata', 'Unknown'
    ]
    confidence_score: Literal['low', 'medium', 'high']
    data_type: Literal['string', 'integer', 'float', 'datetime']
    type: Literal['Categorical', 'Numerical']
    unit: str = ''
    expected_values: List[str | int | float] = []
    
    model_config = ConfigDict(extra='forbid')

class FilterStepModel(BaseModel):
    function: Literal['filter']
    column_name: str
    operator: Literal['in', '>', '<', '>=', '<=', '==', '!=', 'between']
    values: Union[List[Any], str]
    
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
    sort_by_column_name: str
    order: Literal['top', 'bottom']
    number_of_entries: int
    return_columns: List[str]
    
    model_config = ConfigDict(extra='forbid')

class ProportionStepModel(BaseModel):
    function: Literal['get_proportion']
    column_name: List[str]
    values: List[str] = []
    
    model_config = ConfigDict(extra='forbid')

class ColStatsStepModel(BaseModel):
    function: Literal['get_column_statistics']
    column_name: List[str]
    calculation: List[Literal['mean', 'median', 'min', 'max', 'count', 'sum']]
    
    model_config = ConfigDict(extra='forbid')

class CommonTaskModel(BaseModel):
    name: str
    description: str
    steps: list
    score: Literal['low', 'medium', 'high']
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
    except ValidationError as e:
        # need to log the error for the item here
        return False

def filter_out_invalid_values(values, model_key_func, model_map):
    valid_vals_num = []
    
    for i, val in enumerate(values):
        try:
            model_key = model_key_func(val)
            model = model_map[model_key]
        except (AttributeError, KeyError): # doesnt have a valid model
            # need to log the discarded item here
            continue

        if validate_model_wrapper(val, model):
            valid_vals_num.append(i)
            
    return valid_vals_num

class DataTasks(BaseModel): # smaller model for subsequent task runs
    common_tasks: list[CommonTaskModel] = Field(min_length=1)
    common_column_cleaning_or_transformation: list[CommonColumnCleaningOrTransformationModel] = []
    common_column_combination: list[CommonColumnCombinationModel] = []

    @field_validator('common_tasks', mode='after')
    @classmethod
    def filter_common_tasks(cls, values):
        valid_values = []
        
        for task in values:
            model_key_func = lambda val: val['function']
            valid_steps_num = filter_out_invalid_values(task.steps, model_key_func, STEP_MODELS)
            
            if len(valid_steps_num) == len(task.steps):
                valid_values.append(task)
                
        if len(valid_values) < 5:
            raise ValueError('too few valid common_tasks')
        return valid_values

    @field_validator('common_column_cleaning_or_transformation', mode='after')
    @classmethod
    def filter_transforms(cls, values):
        model_key_func = lambda val: val['type']
        values_to_check = [val.operation for val in values]
        valid_vals_num = filter_out_invalid_values(values_to_check, model_key_func, TRANSFORM_MODELS)
        
        return [val for i, val in enumerate(values) if i in valid_vals_num]

    @field_validator('common_column_combination', mode='after')
    @classmethod
    def filter_combinations(cls, values):
        model_key_func = lambda _: 'column_combination'
        values_to_check = [val.operation for val in values]
        valid_vals_num = filter_out_invalid_values(values_to_check, model_key_func, COMBINATION_MODELS)
        
        return [val for i, val in enumerate(values) if i in valid_vals_num]

class DatasetAnalysisModel(DataTasks): # model for the result of llm prompt
    domain: str
    description: str
    columns: List[ColumnModel]
    
    model_config = ConfigDict(extra='forbid')
    
    @field_validator('columns', mode='after')
    @classmethod
    def check_all_columns_exist(cls, value: list[dict], info):
        req_cols = info.context.get('required_cols', [])
        resp_cols = [col.name for col in value]
        missing = set(req_cols) - set(resp_cols)
        if missing:
            raise ValueError(
                f"some columns are missing from the response: {', '.join(missing)}"
            )
        return value

if __name__ == '__main__':
    import json
    with open('resp.json', 'r') as f:
        data = json.load(f)
        
    DatasetAnalysisModel.model_validate(data, context={
        'required_cols': 
        ['id', 'loan_amnt', 'term', 'int_rate', 'installment', 'home_ownership','annual_inc', 'verification_status', 'issue_d', 'loan_status','purpose', 'total_pymnt']})
    

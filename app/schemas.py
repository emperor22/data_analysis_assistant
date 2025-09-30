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
    expression: str = Field(pattern=r"^[a-zA-Z0-9_\s\+\-\*/\(\)\.]+$") # type: ignore
    
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
    type: Literal["map"] = "map"
    source_column: str
    mapping: Dict[Union[str, int, float], Union[str, int]]
    
    model_config = ConfigDict(extra='forbid')
    

class RangeItem(BaseModel):
    range: str = Field(pattern=r"^\d+(?:\.\d+)?[-\+](\d+|inf)$") # type: ignore
    label: str
    
    model_config = ConfigDict(extra='forbid')

class MapRangeOperation(BaseModel):
    type: Literal["map_range"] = "map_range"
    source_column: str
    ranges: List[RangeItem] = Field(min_length=1)  # type: ignore
    
    model_config = ConfigDict(extra='forbid')

class DateOpOperation(BaseModel):
    type: Literal["date_op"] = "date_op"
    source_column: str
    function: Literal['YEAR','MONTH','DAY','WEEKDAY']
    
    model_config = ConfigDict(extra='forbid')

class MathOpOperation(BaseModel):
    type: Literal["math_op"] = "math_op"
    source_columns: str | list = Field(min_length=1)  # type: ignore
    expression: str = Field(pattern=r"^[a-zA-Z0-9_\s\+\-\*/\(\)\.]+$") # type: ignore
    
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
        "Identifier", "Dimensional", "Metric", "Temporal", 
        "Geospatial", "Scientific", "Descriptive", "PII", 
        "System/Metadata", "Unknown"
    ]
    confidence_score: float
    data_type: Literal["string", "integer", "float", "datetime"]
    type: Literal["Categorical", "Numerical"]
    unit: str = ""
    expected_values: List[str | int | float] = []
    
    model_config = ConfigDict(extra='forbid')

class FilterStepModel(BaseModel):
    function: Literal["filter"]
    column_name: str
    operator: Literal['in', '>', '<', '>=', '<=', '==', '!=', 'between']
    values: Union[List[Any], str]
    
    model_config = ConfigDict(extra='forbid')

class GroupByStepModel(BaseModel):
    function: Literal["groupby"]
    columns_to_group_by: List[str] = Field(min_length=1)
    columns_to_aggregate: List[str] = Field(min_length=1)
    calculation: List[Literal["mean", "median", "min", "max", "count", "size", "sum"]]

    model_config = ConfigDict(extra='forbid')
    
    @model_validator(mode='after')
    @classmethod
    def check_count_count_aggregation(cls, model_instance):
        group_cols = model_instance.columns_to_group_by
        agg_cols = model_instance.columns_to_aggregate
        
        if sorted(group_cols) == sorted(agg_cols):
            raise ValueError('similar groupby-aggregate pair')
        return model_instance

class TopBottomNStepModel(BaseModel):
    function: Literal["get_top_or_bottom_N_entries"]
    sort_by_column_name: str
    order: Literal["top", "bottom"]
    number_of_entries: int
    return_columns: List[str]
    
    model_config = ConfigDict(extra='forbid')

class ProportionStepModel(BaseModel):
    function: Literal["get_proportion"]
    column_name: List[str]
    values: List[str] = []
    
    model_config = ConfigDict(extra='forbid')

class ColStatsStepModel(BaseModel):
    function: Literal["get_column_statistics"]
    column_name: List[str]
    calculation: List[Literal["mean", "median", "min", "max", "count", "sum"]]
    
    model_config = ConfigDict(extra='forbid')

class CommonTaskModel(BaseModel):
    name: str
    description: str
    steps: list
    score: float
    task_id: int
    
    model_config = ConfigDict(extra='forbid')

STEP_MODELS = [FilterStepModel, GroupByStepModel, TopBottomNStepModel, ProportionStepModel, ColStatsStepModel]
OPERATION_MODELS = [MapOperation, MapRangeOperation, DateOpOperation, MathOpOperation,]
COMBINATION_MODELS = [CommonColumnCombinationOperation]

def validate_model_wrapper(val, model):
    try:
        model.model_validate(val)
        return True
    except ValidationError:
        return False

def filter_out_invalid_values(values, models):
    valid_vals = []
    for v in values:
        if any(validate_model_wrapper(v, m) for m in models):
            valid_vals.append(v)
    return valid_vals        
class DatasetAnalysisModel(BaseModel):
    domain: str
    description: str
    columns: List[ColumnModel]
    common_column_combination: List[CommonColumnCombinationModel]
    common_column_cleaning_or_transformation: List[CommonColumnCleaningOrTransformationModel]
    common_tasks: List[CommonTaskModel] = Field(min_length=TASK_COUNT_LLM_RESP)
    
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

    @field_validator('common_tasks', mode='after')
    @classmethod
    def filter_common_tasks(cls, values):
        valid_values = []
        for task in values:
            steps = filter_out_invalid_values(task.steps, STEP_MODELS)
            if len(steps) == len(task.steps):
                valid_values.append(task)
                
        if len(valid_values) < 5:
            raise ValueError("too few valid common_tasks")
        return valid_values

    @field_validator('common_column_cleaning_or_transformation', mode='after')
    @classmethod
    def filter_transforms(cls, values):
        return filter_out_invalid_values(values, OPERATION_MODELS)

    @field_validator('common_column_combination', mode='after')
    @classmethod
    def filter_combinations(cls, values):
        return filter_out_invalid_values(values, COMBINATION_MODELS)


class DataTasks(BaseModel):
    common_tasks: list[CommonTaskModel] = Field(min_length=1)
    common_column_cleaning_or_transformation: list[CommonColumnCleaningOrTransformationModel] = []
    common_column_combination: list[CommonColumnCombinationModel] = []

    @field_validator('common_tasks', mode='after')
    @classmethod
    def filter_common_tasks(cls, values):
        valid_values = []
        for task in values:
            steps = filter_out_invalid_values(task.steps, STEP_MODELS)
            if len(steps) == len(task.steps):
                valid_values.append(task)
        if len(valid_values) < 5:
            raise ValueError("Too few valid common_tasks")
        return valid_values

    @field_validator('common_column_cleaning_or_transformation', mode='after')
    @classmethod
    def filter_transforms(cls, values):
        return filter_out_invalid_values(values, OPERATION_MODELS)

    @field_validator('common_column_combination', mode='after')
    @classmethod
    def filter_combinations(cls, values):
        return filter_out_invalid_values(values, COMBINATION_MODELS)
    
    
if __name__ == '__main__':
    import json
    with open('resp.json', 'r') as f:
        data = json.load(f)
        
    DatasetAnalysisModel.model_validate(data, context={
        'required_cols': 
        ['id', 'loan_amnt', 'term', 'int_rate', 'installment', 'home_ownership','annual_inc', 'verification_status', 'issue_d', 'loan_status','purpose', 'total_pymnt']})
    

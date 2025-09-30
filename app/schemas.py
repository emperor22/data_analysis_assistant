from pydantic import BaseModel, Field, ConfigDict, field_validator, conlist, constr
from typing import List, Union, Dict, Any, Literal
import re
from typing import Optional

    
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
    expected_values: List[str | int] = []
    
    model_config = ConfigDict(extra='forbid')

class CommonColumnCombinationModel(BaseModel):
    name: str
    formula: str
    
    model_config = ConfigDict(extra='forbid')

class CommonColumnCombinationOperation(BaseModel):
    source_columns: List[str] = Field(min_length=1)  # type: ignore
    expression: str = Field(pattern=r"^[a-zA-Z0-9_\s\+\-\*/\(\)\.]+$") # type: ignore
       
class CommonColumnCombinationModelV2(BaseModel):
    name: str
    description: str
    operation: CommonColumnCombinationOperation

class MapOperation(BaseModel):
    type: Literal["map"] = "map"
    source_column: str
    mapping: Dict[Union[str, int, float], Union[str, int]]
    

class RangeItem(BaseModel):
    range: str = Field(pattern=r"^\d+-(\d+|inf)$") # type: ignore
    label: str

class MapRangeOperation(BaseModel):
    type: Literal["map_range"] = "map_range"
    source_column: str
    ranges: List[RangeItem] = Field(min_length=1)  # type: ignore

class DateOpOperation(BaseModel):
    type: Literal["date_op"] = "date_op"
    source_column: str
    function: Literal['YEAR','MONTH','DAY','WEEKDAY']

class MathOpOperation(BaseModel):
    type: Literal["math_op"] = "math_op"
    source_columns: str = Field(min_length=1)  # type: ignore
    expression: str = Field(pattern=r"^[a-zA-Z0-9_\s\+\-\*/\(\)\.]+$") # type: ignore


class CommonColumnCleaningOrTransformationModelV2(BaseModel):
    name: str
    description: str
    operation: Union[MapOperation, MapRangeOperation, DateOpOperation, MathOpOperation]


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
    expected_values: List[str | int] = []
    
    model_config = ConfigDict(extra='forbid')

class CommonColumnCombinationModel(BaseModel):
    name: str
    formula: str
    
    model_config = ConfigDict(extra='forbid')

class CommonColumnCleaningOrTransformationModel(BaseModel):
    name: str
    description: str
    formula: Union[str, List[Dict[str, str]]]
    
    model_config = ConfigDict(extra='forbid')

class FilterStepModel(BaseModel):
    function: Literal["filter"]
    column_name: str
    operator: Literal['in', '>', '<', '>=', '<=', '==', '!=', 'between']
    values: Union[List[Any], str]
    
    model_config = ConfigDict(extra='forbid')

class GroupByStepModel(BaseModel):
    function: Literal["groupby"]
    columns_to_group_by: List[str]
    columns_to_aggregate: List[str]
    calculation: List[Literal["mean", "median", "min", "max", "count", "size", "sum"]]
    
    model_config = ConfigDict(extra='forbid')

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
    steps: List[Union[FilterStepModel, GroupByStepModel, TopBottomNStepModel, ProportionStepModel, ColStatsStepModel]]
    score: float
    task_id: int
    
    model_config = ConfigDict(extra='forbid')

class DatasetAnalysisModel(BaseModel):
    domain: str
    description: str
    columns: List[ColumnModel]
    common_column_combination: List[CommonColumnCombinationModelV2]
    common_column_cleaning_or_transformation: List[CommonColumnCleaningOrTransformationModelV2]
    common_tasks: List[CommonTaskModel] = Field(min_length=10)
    
    model_config = ConfigDict(extra='forbid')
    
    
    @field_validator('columns', mode='after')
    @classmethod
    def check_all_columns_exist(cls, value: list[dict], info):
        req_cols = info.context.get('required_cols')
        cols_from_resp = [i.name for i in value]
        
        cols_not_exist = set(req_cols) - set(cols_from_resp)
                
        if len(cols_not_exist) > 0:
            error_msg = f'the columns field doesnt contain all the columns from the dataset. missing cols: {", ".join(cols_not_exist)}'
            raise ValueError(error_msg)
        
        return value
    
class DataTasks(BaseModel):
    common_tasks: list[CommonTaskModel] = Field(min_length=1)
    common_column_cleaning_or_transformation: list[CommonColumnCleaningOrTransformationModel] = []
    common_column_combination: list[CommonColumnCombinationModel] = []
    
    
if __name__ == '__main__':
    import json
    with open('resp3.json', 'r') as f:
        data = json.load(f)
        
    DatasetAnalysisModel.model_validate(data, context={'required_cols': 
        ['id', 'loan_amnt', 'term', 'int_rate', 'installment', 'home_ownership','annual_inc', 'verification_status', 'issue_d', 'loan_status','purpose', 'total_pymnt']})
    

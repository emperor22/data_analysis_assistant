from pydantic import (
    BaseModel,
    Field,
    ConfigDict,
    field_validator,
    ValidationError,
    model_validator,
)
from typing import List, Union, Dict, Any, Literal
import re
from enum import Enum
from app.logger import logger
from app.config import Config, model_list_dct
from dataclasses import dataclass


class TaskStatus(Enum):
    waiting_for_initial_request_prompt = "GETTING INITIAL REQUEST PROMPT RESULT"
    waiting_for_additional_analysis_prompt_result = (
        "GETTING ADDITIONAL ANALYSES REQUEST PROMPT RESULT"
    )

    initial_request_prompt_received = "INITIAL REQUEST PROMPT RESULT RECEIVED"
    additional_analysis_prompt_result_received = (
        "ADDITIONAL ANALYSES PROMPT RESULT RECEIVED"
    )

    doing_initial_tasks_run = "RUNNING INITIAL ANALYSES TASKS"
    doing_additional_tasks_run = "RUNNING ADDITIONAL ANALYSES TASKS"
    doing_customized_tasks_run = "RUNNING USER CUSTOMIZED ANALYSIS TASKS"
    doing_customized_tasks_run_with_new_dataset = (
        "RUNNING USER CUSTOMIZED ANALYSIS TASKS WITH NEW DATASET"
    )

    initial_tasks_run_finished = "INITIAL ANALYSIS TASKS FINISHED"
    additional_tasks_run_finished = "ADDITIONAL ANALYSES TASKS FINISHED"
    customized_tasks_run_finished = "USER CUSTOMIZED ANALYSIS TASKS FINISHED"
    customized_tasks_run_with_new_dataset_finished = (
        "USER CUSTOMIZED ANALYSIS TASKS WITH NEW DATASET FINISHED"
    )

    # failed attempts
    failed_because_blacklisted_dataset = "TASK FAILED BECAUSE DATASET IS BLACKLISTED"

    deleted_because_not_accessed_recently = (
        "TASK DELETED BECAUSE IT IS NOT ACCESSED FOR SOME TIME"
    )

    failed_because_rate_limited = "LLM ENDPOINT IS RATE LIMITED"


class TaskProcessingRunType(Enum):
    first_run_after_request = "first_run_after_request"
    modified_tasks_execution = "modified_tasks_execution"
    additional_analyses_request = "additional_analyses_request"
    modified_tasks_execution_with_new_dataset = (
        "modified_tasks_execution_with_new_dataset"
    )


@dataclass
class RunInfo:
    request_id: str
    user_id: str
    parquet_file: str
    filename: str
    send_result_to_email: str
    email: str
    run_name: str


class GetCurrentUserModel(BaseModel):
    username: str
    user_id: str
    email: str


class UserRegisterSchema(BaseModel):
    username: str
    email: str
    first_name: str
    last_name: str


class ModelAndProviderSchema(BaseModel):
    provider: Literal[tuple(Config.LLM_PROVIDER_LIST)]  # type: ignore
    model: str

    @model_validator(mode="after")
    def check_if_model_is_valid(self):
        if self.model not in model_list_dct[self.provider]:
            raise ValueError("model not in the provider's model list")

        return self


class UploadDatasetSchema(ModelAndProviderSchema):
    run_name: str
    analysis_task_count: int = Field(lt=Config.MAX_TASK_COUNT + 1)
    send_result_to_email: bool


class UserCustomizedTasksSchema(BaseModel):
    request_id: str
    slot: int = Literal[1, 2, 3]
    tasks: dict = {}
    operation: Literal["fetch", "delete", "update", "check_if_empty"]


class SetImportedTasksSchema(BaseModel):
    request_id: str
    task_ids: list


class GetOTPSchema(BaseModel):
    username: str


class LoginSchema(BaseModel):
    username: str
    otp: str


class AdditionalAnalysesRequestSchema(ModelAndProviderSchema):
    new_tasks_prompt: str
    send_result_to_email: bool


class SetupAPIKeySchema(BaseModel):
    key: str
    provider: Literal[tuple(Config.LLM_PROVIDER_LIST)]  # type: ignore


class JoinDatasetSchema(BaseModel):
    join_method: Literal["inner", "outer", "left", "right"]
    join_keys: list[tuple]


##################################################################


class CommonColumnCombinationOperation(BaseModel):
    source_columns: List[str] = Field(min_length=1)  # type: ignore
    expression: str = Field(pattern=r"^[a-zA-Z0-9_\s\+\-\*/\(\)\.]+$")  # type: ignore

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def check_if_columns_match_expression(self):
        regex = r"\b(?![0-9]+(\.[0-9]+)?\b)([a-zA-Z0-9_]+)\b"
        expression = self.expression
        source_cols = self.source_columns

        matches = re.finditer(regex, expression)
        matches = [match.group(2) for match in matches]

        if not sorted(matches) == sorted(source_cols):
            raise ValueError("columns in expressions dont match source columns")
        return self


class CommonColumnCombinationModel(BaseModel):
    name: str
    description: str
    operation: dict

    model_config = ConfigDict(extra="forbid")


class MapOperation(BaseModel):
    type: Literal["map"] = "map"
    source_column: str
    mapping: Dict[Union[str, int, float], Union[str, int]]

    model_config = ConfigDict(extra="forbid")


class RangeItem(BaseModel):
    range: str = Field(pattern=r"^\d+(?:\.\d+)?[-](\d+(?:\.\d+)?|inf)$")
    label: str

    model_config = ConfigDict(extra="forbid")


class MapRangeOperation(BaseModel):
    type: Literal["map_range"] = "map_range"
    source_column: str
    ranges: List[RangeItem] = Field(min_length=1)

    model_config = ConfigDict(extra="forbid")


class DateOpOperation(BaseModel):
    type: Literal["date_op"] = "date_op"
    source_column: str
    function: Literal["YEAR", "MONTH", "DAY", "WEEKDAY"]

    model_config = ConfigDict(extra="forbid")


class MathOpOperation(BaseModel):
    type: Literal["math_op"] = "math_op"
    source_columns: str | list = Field(min_length=1, max_length=1)
    expression: str = Field(pattern=r"^[a-zA-Z0-9_\s\+\-\*/\(\)\.]+$")

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def check_if_columns_match_expression(self):
        regex = r"\b(?![0-9]+(\.[0-9]+)?\b)([a-zA-Z0-9_]+)\b"
        expression = self.expression
        source_cols = self.source_columns

        matches = re.finditer(regex, expression)
        matches = [match.group(2) for match in matches]

        if not sorted(matches) == sorted(source_cols):
            raise ValueError("columns in expressions dont match source columns")
        return self


class CommonColumnCleaningOrTransformationModel(BaseModel):
    name: str
    description: str
    operation: dict  # this will be validated in the outer class (DataAnalysisModel)

    model_config = ConfigDict(extra="forbid")


class ColumnModel(BaseModel):
    name: str
    classification: Literal[
        "Identifier",
        "Dimensional",
        "Metric",
        "Temporal",
        "Geospatial",
        "Scientific",
        "Descriptive",
        "PII",
        "System/Metadata",
        "Unknown",
    ]
    confidence_score: Literal["low", "medium", "high", "inapplicable"]
    data_type: Literal["string", "integer", "float", "datetime"]
    type: Literal["Categorical", "Numerical", "Temporal"]
    unit: str = ""
    expected_values: str | List[str | int | float] = []

    model_config = ConfigDict(extra="forbid")


class FilterStepModel(BaseModel):
    function: Literal["filter"]
    column_name: List[str] | str
    operator: Literal["in", ">", "<", ">=", "<=", "==", "=", "<>", "!=", "between"]
    values: List[Any] | str | int | float

    model_config = ConfigDict(extra="forbid")


groupby_allowed_calc = Literal["mean", "median", "min", "max", "count", "size", "sum"]


class GroupByStepModel(BaseModel):
    function: Literal["groupby"]
    columns_to_group_by: List[str] = Field(min_length=1)
    columns_to_aggregate: List[str] = Field(min_length=1)
    calculation: groupby_allowed_calc | List[groupby_allowed_calc]

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def check_count_count_aggregation(self):
        group_cols = self.columns_to_group_by
        agg_cols = self.columns_to_aggregate

        if sorted(group_cols) == sorted(agg_cols):
            raise ValueError("similar columns for groupby-aggregate pair")
        return self


class TopBottomNStepModel(BaseModel):
    function: Literal["get_top_or_bottom_N_entries"]
    sort_by_column_name: List[str] | str
    order: Literal["top", "bottom"]
    number_of_entries: int
    return_columns: List[str]

    model_config = ConfigDict(extra="forbid")


class ProportionStepModel(BaseModel):
    function: Literal["get_proportion"]
    column_name: List[str] | str
    values: List[str | int] = []

    model_config = ConfigDict(extra="forbid")


class ColStatsStepModel(BaseModel):
    function: Literal["get_column_statistics"]
    column_name: List[str] | str
    calculation: List[Literal["mean", "median", "min", "max", "count", "sum"]]

    model_config = ConfigDict(extra="forbid")


resample_allowed_calc = Literal[
    "sum", "mean", "median", "min", "max", "first", "last", "count"
]


class ResampleDataStepModel(BaseModel):
    function: Literal["resample_data"]
    date_column: List[str] | str
    frequency: Literal["day", "week", "month", "year", "quarter"]
    static_group_cols: List[str]
    columns_to_aggregate: List[str] | str
    calculation: resample_allowed_calc | List[resample_allowed_calc]

    model_config = ConfigDict(extra="forbid")


class CommonTaskModel(BaseModel):
    name: str
    description: str
    steps: list  # this will be validated in the outer validation (DataAnalysisModel)
    score: Literal["low", "medium", "high", "inapplicable"]
    task_id: int

    model_config = ConfigDict(extra="forbid")


STEP_MODELS = {
    "filter": FilterStepModel,
    "groupby": GroupByStepModel,
    "get_top_or_bottom_N_entries": TopBottomNStepModel,
    "get_proportion": ProportionStepModel,
    "get_column_statistics": ColStatsStepModel,
    "resample_data": ResampleDataStepModel,
}
TRANSFORM_MODELS = {
    "map": MapOperation,
    "map_range": MapRangeOperation,
    "date_op": DateOpOperation,
    "math_op": MathOpOperation,
}
COMBINATION_MODELS = {"column_combination": CommonColumnCombinationOperation}


def validate_model_wrapper(val, model):
    try:
        model.model_validate(val)
        return True
    except ValidationError as e:
        logger.debug(f"{e}")
        return False


def log_invalid_values(run, invalid_vals_num, req_id, common_task_id):
    invalid_vals_num = [
        str(i + 1) for i in invalid_vals_num
    ]  # offset by one for easier debugging
    if run == "common_tasks":
        logger.warning(
            f"invalid common_tasks step: request_id {req_id}, task id {common_task_id}, steps {', '.join(invalid_vals_num)}"
        )
    else:
        logger.warning(
            f"invalid {run} task: request_id {req_id}, task nums ({', '.join(invalid_vals_num)})"
        )


def filter_out_invalid_values(
    values, model_key_func, model_map, run, req_id, common_task_id=None
):
    valid_vals_num = []
    invalid_vals_num = []
    for i, val in enumerate(values):
        try:
            model_key = model_key_func(val)
            model = model_map[model_key]
        except (AttributeError, KeyError):  # doesnt have a valid model
            invalid_vals_num.append(i)
            continue

        if validate_model_wrapper(val, model):
            valid_vals_num.append(i)
        else:
            invalid_vals_num.append(i)

    if len(invalid_vals_num) > 0:
        log_invalid_values(
            run=run,
            invalid_vals_num=invalid_vals_num,
            req_id=req_id,
            common_task_id=common_task_id,
        )

    return valid_vals_num


class ColumnInfoAndOperations(BaseModel):
    columns: list[ColumnModel] = []
    common_column_cleaning_or_transformation: list[
        CommonColumnCleaningOrTransformationModel
    ] = []
    common_column_combination: list[CommonColumnCombinationModel] = []

    @field_validator("columns", mode="after")
    @classmethod
    def check_all_columns_exist(cls, value, info):
        is_from_data_tasks = info.context.get("is_from_data_tasks")

        if is_from_data_tasks:
            return value

        req_cols = info.context.get("required_cols")
        req_id = info.context.get("request_id")
        resp_cols = [col.name for col in value]
        missing = set(req_cols) - set(resp_cols)
        if missing:
            missing_cols = {", ".join(missing)}
            logger.warning(
                f"some columns are missing from the 'columns' section. cols list {missing_cols}; request_id {req_id}"
            )
            raise ValueError(
                f"some columns are missing from the response: {missing_cols}"
            )

        return value

    @field_validator("common_column_cleaning_or_transformation", mode="after")
    @classmethod
    def filter_out_invalid_transforms(cls, values, info):
        req_id = info.context.get("request_id")

        def model_key(val):
            return val["type"]
        
        model_key_func = model_key
        values_to_check = [val.operation for val in values]
        valid_vals_num = filter_out_invalid_values(
            values_to_check,
            model_key_func,
            TRANSFORM_MODELS,
            "column_transformation",
            req_id,
        )

        return [val for i, val in enumerate(values) if i in valid_vals_num]

    @field_validator("common_column_combination", mode="after")
    @classmethod
    def filter_out_invalid_combinations(cls, values, info):
        req_id = info.context.get("request_id")

        def model_key(val):
            return "column_combination"
            
        model_key_func = model_key
        values_to_check = [val.operation for val in values]
        valid_vals_num = filter_out_invalid_values(
            values_to_check,
            model_key_func,
            COMBINATION_MODELS,
            "column_combination",
            req_id,
        )

        return [val for i, val in enumerate(values) if i in valid_vals_num]


class DatasetAnalysisModelPartOne(ColumnInfoAndOperations):
    domain: str
    description: str
    is_time_series: bool
    inferred_granularity: str

    model_config = ConfigDict(extra="forbid")


class DatasetAnalysisModelPartTwo(BaseModel):  # smaller model for subsequent task runs
    common_tasks: list[CommonTaskModel] = Field(min_length=1)

    model_config = ConfigDict(extra="forbid")

    @field_validator("common_tasks", mode="after")
    @classmethod
    def filter_common_tasks(cls, values, info):

        req_id = info.context.get("request_id")

        valid_values = []

        for task in values:
            
            def model_key(val):
                return val["function"]
            
            model_key_func = model_key
            valid_steps_num = filter_out_invalid_values(
                task.steps,
                model_key_func,
                STEP_MODELS,
                "common_tasks",
                req_id,
                task.task_id,
            )

            if len(valid_steps_num) < len(task.steps):
                logger.warning(
                    f"task discarded: task_id {task.task_id} request_id {req_id}"
                )
                continue

            valid_values.append(task)

        run_type = info.context.get("run_type")

        min_valid_values = (
            Config.MIN_VALID_COMMON_TASKS_FIRST_RUN
            if run_type == "first_run_after_request"
            else Config.MIN_VALID_COMMON_TASKS_SUBSEQUENT_RUNS
        )

        if len(valid_values) < min_valid_values:
            raise ValueError("too few valid common_tasks")
        return valid_values


class DataTasks(ColumnInfoAndOperations, DatasetAnalysisModelPartTwo):
    common_column_cleaning_or_transformation: list[
        CommonColumnCleaningOrTransformationModel
    ] = []
    common_column_combination: list[CommonColumnCombinationModel] = []
    common_tasks: list[CommonTaskModel] = Field(min_length=1)

    model_config = ConfigDict(extra="ignore")


class ExecuteAnalysesSchema(DataTasks):
    send_result_to_email: bool

from pydantic import (
    BaseModel,
    Field,
    model_validator,
    EmailStr,
)

from app.schemas.llm_validation import DataTasks

from typing import Literal

from app.core.config import Config, model_list_dct
from dataclasses import dataclass
from ast import literal_eval


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
    username: str = Field(min_length=3, max_length=10)
    email: EmailStr = Field(min_length=5, max_length=40)
    first_name: str = Field(min_length=3, max_length=20)
    last_name: str = Field(min_length=3, max_length=20)


class ModelAndProviderSchema(BaseModel):
    provider: Literal[tuple(literal_eval(Config.LLM_PROVIDER_LIST))]  # type: ignore
    model: str

    @model_validator(mode="after")
    def check_if_model_is_valid(self):
        if self.model not in model_list_dct[self.provider]:
            raise ValueError("model not in the provider's model list")

        return self


class UploadDatasetSchema(ModelAndProviderSchema):
    run_name: str = Field(min_length=1)
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
    username: str = Field(min_length=1)


class LoginSchema(BaseModel):
    username: str = Field(min_length=1)
    otp: str = Field(min_length=1)


class AdditionalAnalysesRequestSchema(ModelAndProviderSchema):
    new_tasks_prompt: str = Field(min_length=1)
    send_result_to_email: bool


class SetupAPIKeySchema(BaseModel):
    key: str = Field(min_length=1)
    provider: Literal[tuple(literal_eval(Config.LLM_PROVIDER_LIST))]  # type: ignore


class JoinDatasetSchema(BaseModel):
    join_method: Literal["inner", "outer", "left", "right"]
    join_keys: list[tuple]


class ExecuteAnalysesSchema(DataTasks):
    send_result_to_email: bool

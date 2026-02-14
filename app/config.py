from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


######################################

# streamlit_app/utils.py

# URL = 'https://nginx/api'
URL = "http://localhost:8000"

DEFAULT_SEND_EMAIL = True

DEFAULT_VERSION_CUSTOMIZED_TASKS = 1


#####################################


# conftest.py

TEST_DB_URL_ASYNC = "sqlite+aiosqlite:///./testing_xx.sqlite"
TEST_DB_URL_SYNC = "sqlite:///./testing_xx.sqlite"

TEST_UUID = "xxxx-xxxx-xxxx-xxxx"

TEST_DEFAULT_MODEL = "gemini-2.5-flash"
TEST_DEFAULT_DATASET_FILE = (
    "tests/test_files/1_netflix-rotten-tomatoes-metacritic-imdb.csv"
)
TEST_DEFAULT_CLEAN_DATASET_FILE = "tests/test_files/clean_dataset.parquet"
TEST_DEFAULT_TASK_COUNT = 10
TEST_PROMPT_VERSION = 3

TEST_DEFAULT_OTP = "000000"


class Configs(BaseSettings):
    # api.py
    SENTRY_DSN: str

    WARN_FOR_SLOW_RESPONSE_TIME: bool = True
    THRES_SLOW_RESPONSE_TIME_MS: int = 1000
    DATASET_SAVE_PATH: str = "app/datasets"
    OTP_EXPIRE_MINUTES: int = 5

    REDIS_LAST_ACCESSED_HASHTABLE_NAME: str = "req_id_last_accessed"

    DEFAULT_PROMPT_VERSION: int = 3

    PT1_PROMPT_TEMPLATE: str = "app/prompts/split_prompt/prompt_part1.md"
    ADDT_REQ_PROMPT_TEMPLATE: str = (
        "app/prompts/split_prompt/additional_tasks_req_prompt.md"
    )
    PT2_PROMPT_TEMPLATE: str = "app/prompts/split_prompt/prompt_part2.md"

    RATE_LIMIT_GET_ENDPOINTS: str = "60/minute"
    RATE_LIMIT_TASK_ENDPOINTS: str = "5/minute"
    RATE_LIMIT_LOGIN: str = "5/5minute"
    RATE_LIMIT_REGISTER: str = "2/hour"

    # tasks.py

    THRES_SLOW_INITIAL_REQUEST_PROCESS_TIME_MS: int = 90 * 1000
    THRES_SLOW_ADDITIONAL_ANALYSES_REQUEST_PROCESS_TIME_MS: int = 45 * 1000
    THRES_SLOW_TASK_EXECUTION_PROCESS_TIME_MS: int = 7 * 1000

    API_URL: str

    REDIS_PASSWORD: str
    REDIS_URL: str
    # REDIS_URL: str = 'redis://redis:6379/0'

    REDIS_LAST_ACCESSED_HASHTABLE_NAME: str = "req_id_last_accessed"

    DATASET_SAVE_PATH: str = "app_data/result/datasets"

    DEBUG_PROMPT_AND_RES_SAVE_DIR: str = "resp_jsons"

    # crud.py
    # DATABASE_URL_ASYNC: str = 'sqlite+aiosqlite:///./test.sqlite'
    # DATABASE_URL_SYNC: str = 'sqlite:///./test.sqlite'

    DATABASE_URL_ASYNC: str
    DATABASE_URL_SYNC: str

    MAX_RETRIES_GET_PROMPT_RESULT_TASK: int = 10
    MAX_RETRIES_ADDT_ANALYSES_REQ_TASK: int = 6

    FAILED_ATTEMPT_THRESHOLD_FOR_BLACKLIST: int = 5
    RATE_LIMIT_RETRY_COUNT_CAP: int = 5

    # services.py
    LLM_API_KEY_GOOGLE: str
    LLM_ENDPOINT_GOOGLE: str = (
        "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent"
    )

    LLM_API_KEY_CEREBRAS: str
    LLM_ENDPOINT_CEREBRAS: str = "https://api.cerebras.ai/v1/chat/completions"

    DEFAULT_LLM_PROVIDER: str = "cerebras"

    LLM_PROVIDER_LIST: list = ["google", "cerebras"]

    EMAIL_USERNAME: str
    EMAIL_PASSWORD: str
    EMAIL_SERVER: str

    N_ROWS_READ_UPLOAD_FILE: int = 100
    DATASET_SAVE_PATH: str = "app/datasets"

    DATASET_ROW_THRESHOLD_BEFORE_EXPORT: int = 20
    DATASET_COLUMNS_THRESHOLD_BEFORE_EXPORT: int = 5

    MAX_DATASET_ROW_RESULT: int = 10000
    MAX_DATASET_COLS_RESULT: int = 15

    MAX_DATAFRAME_ROWS: int = 500000
    MAX_DATAFRAME_COLS: int = 100

    MAX_DATAFRAME_ROWS_JOIN_UTIL: int = 100000
    MAX_DATAFRAME_COLS_JOIN_UTIL: int = 10

    # logger.py
    LOG_LEVEL_STDERR: str = "DEBUG"
    LOG_LEVEL_fILE: str = "DEBUG"

    # auth.py
    SECRET_KEY: str
    ALGORITHM: str = "HS256"
    API_KEY_ENCRYPTION_KEY: str
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 120

    SECRET_KEY_OTP_ENCRYPT: str = "satuduatiga"

    ADMIN_EMAIL: str = "algiffaryriony@gmail.com"

    # data_transform_utils.py
    MAX_BAR_ROWS: int = 20
    MIN_LINE_POINTS: int = 5
    MAX_VISUAL_ROWS: int = 20
    MAX_VISUAL_COLS: int = 5

    DATATYPE_CONV_SUCCESS_RATE_CLEAN_DATASET: float = 0.5  # for datatype conversion

    # schemas.py
    MIN_VALID_COMMON_TASKS_FIRST_RUN: int = 5
    MIN_VALID_COMMON_TASKS_SUBSEQUENT_RUNS: int = 1

    LLM_MODEL_LIST_GOOGLE: list = [
        "gemini-2.5-flash",
        "gemma-3-27b-it",
        "gemini-2.5-flash-lite",
    ]
    LLM_MODEL_LIST_CEREBRAS: list = [
        "qwen-3-235b-a22b-instruct-2507",
        "qwen-3-32b",
        "gpt-oss-120b",
        "zai-glm-4.7",
    ]

    MAX_TASK_COUNT: int = 20

    model_config = SettingsConfigDict(env_file="app/.env.local")


@lru_cache
def get_config() -> Configs:
    return Configs()


Config = get_config()

model_list_dct = {
    "google": Config.LLM_MODEL_LIST_GOOGLE,
    "cerebras": Config.LLM_MODEL_LIST_CEREBRAS,
}

api_key_dct = {
    "google": Config.LLM_API_KEY_GOOGLE,
    "cerebras": Config.LLM_API_KEY_CEREBRAS,
}

PARAMS_MAP = {
    "groupby": {
        "template": "Group by column(s) $columns_to_group_by and calculate $calculation of column(s) $columns_to_aggregate",
        "columns_to_group_by": {
            "alias": "Column(s) to group by",
            "type": "multiselect",
        },
        "columns_to_aggregate": {
            "alias": "Column(s) to aggregate",
            "type": "multiselect",
        },
        "calculation": {
            "alias": "Calculation",
            "type": "multiselect",
            "options": ["mean", "median", "min", "max", "count", "size", "sum"],
        },
    },
    "filter": {
        "template": "Filter column $column_name where condition $operator $values",
        "column_name": {"alias": "Filter column", "type": "selectbox"},
        "operator": {
            "alias": "Condition",
            "type": "selectbox",
            "options": [">", "<", ">=", "<=", "==", "!=", "in", "between"],
        },
        "values": {
            "alias": "Filter value(s)",
            "type": {"numerical": "number_input", "text": "text_input"},
        },
    },
    "get_top_or_bottom_N_entries": {
        "template": "Get the $order $number_of_entries entries, sorted by $sort_by_column_name. Return column(s): $return_columns",
        "sort_by_column_name": {"alias": "Column to sort by", "type": "selectbox"},
        "order": {"alias": "Ordering", "type": "radio", "options": ["top", "bottom"]},
        "number_of_entries": {"alias": "Number of entries", "type": "number_input"},
        "return_columns": {
            "alias": "Column(s) included in result",
            "type": "multiselect",
        },
    },
    "get_proportion": {
        "template": "Calculate the proportion/percentage of value(s) $values in column $column_name",
        "column_name": {"alias": "Column to get proportion of", "type": "selectbox"},
        "values": {"alias": "Value(s) to get proportion of", "type": "text_input"},
    },
    "get_column_statistics": {
        "template": "Calculate the statistic ($calculation) for column $column_name",
        "column_name": {"alias": "Column to get statistics from", "type": "selectbox"},
        "calculation": {
            "alias": "Calculation",
            "type": "selectbox",
            "options": ["mean", "median", "min", "max", "count", "sum"],
        },
    },
    "resample_data": {
        "template": "Change data frequency to frequency $frequency, group by $static_group_cols, and calculate $calculation of column(s) $columns_to_aggregate",
        "date_column": {"alias": "Date column", "type": "selectbox"},
        "frequency": {
            "alias": "Resample frequency",
            "type": "selectbox",
            "options": ["day", "week", "month", "year", "quarter"],
        },
        "static_group_cols": {"alias": "Column(s) to group by", "type": "multiselect"},
        "columns_to_aggregate": {
            "alias": "Column(s) to aggregate",
            "type": "multiselect",
        },
        "calculation": {
            "alias": "Calculation",
            "type": "selectbox",
            "options": ["sum", "mean", "median", "min", "max", "first", "last"],
        },
    },
}

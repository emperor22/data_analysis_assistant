from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache






######################################

# streamlit_app/utils.py

# URL = 'https://nginx/api'
URL = 'http://localhost:8000'

DEFAULT_SEND_EMAIL = True

DEFAULT_VERSION_CUSTOMIZED_TASKS = 1


#####################################


# conftest.py

TEST_DB_URL_ASYNC = 'sqlite+aiosqlite:///./testing_xx.sqlite'
TEST_DB_URL_SYNC = 'sqlite:///./testing_xx.sqlite'

TEST_UUID = 'xxxx-xxxx-xxxx-xxxx'

TEST_DEFAULT_MODEL = 'gemini-2.5-flash'
TEST_DEFAULT_DATASET_FILE = 'tests/test_files/1_netflix-rotten-tomatoes-metacritic-imdb.csv'
TEST_DEFAULT_CLEAN_DATASET_FILE = 'tests/test_files/clean_dataset.parquet'
TEST_DEFAULT_TASK_COUNT = 10
TEST_PROMPT_VERSION = 3

TEST_DEFAULT_OTP = '000000'





class Configs(BaseSettings):
    # api.py
    SENTRY_DSN: str
    
    WARN_FOR_SLOW_RESPONSE_TIME: bool = True
    THRES_SLOW_RESPONSE_TIME_MS: int = 1000
    DATASET_SAVE_PATH: str = 'app/datasets'
    OTP_EXPIRE_MINUTES: int = 5

    REDIS_LAST_ACCESSED_HASHTABLE_NAME: str = 'req_id_last_accessed'

    DEFAULT_PROMPT_VERSION: int = 3

    PT1_PROMPT_TEMPLATE: str = 'app/prompts/split_prompt/prompt_part1.md'

    RATE_LIMIT_GET_ENDPOINTS: str = '60/minute'
    RATE_LIMIT_TASK_ENDPOINTS: str = '5/minute'
    RATE_LIMIT_LOGIN: str = '5/5minute'
    RATE_LIMIT_REGISTER: str = '2/hour'


    # tasks.py

    THRES_SLOW_INITIAL_REQUEST_PROCESS_TIME_MS: int = 90 * 1000
    THRES_SLOW_ADDITIONAL_ANALYSES_REQUEST_PROCESS_TIME_MS: int = 45 * 1000
    THRES_SLOW_TASK_EXECUTION_PROCESS_TIME_MS: int = 7 * 1000

    REDIS_URL: str
    # REDIS_URL: str = 'redis://redis:6379/0'

    ADDT_REQ_PROMPT_TEMPLATE: str = 'app/prompts/split_prompt/additional_tasks_req_prompt.md'
    PT2_PROMPT_TEMPLATE: str = 'app/prompts/split_prompt/prompt_part2.md'

    REDIS_LAST_ACCESSED_HASHTABLE_NAME: str = 'req_id_last_accessed'

    DATASET_SAVE_PATH: str = 'app/datasets'
    
    DEBUG_PROMPT_AND_RES_SAVE_DIR: str = 'resp_jsons'

    # crud.py
    # DATABASE_URL_ASYNC: str = 'sqlite+aiosqlite:///./test.sqlite'
    # DATABASE_URL_SYNC: str = 'sqlite:///./test.sqlite'
    
    DATABASE_URL_ASYNC: str
    DATABASE_URL_SYNC: str
    
    


    FAILED_ATTEMPT_THRESHOLD_FOR_BLACKLIST: int = 5


    # services.py
    LLM_API_KEY: str
    LLM_ENDPOINT: str = 'https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent'
    
    EMAIL_USERNAME: str
    EMAIL_PASSWORD: str
    EMAIL_SERVER: str

    N_ROWS_READ_UPLOAD_FILE: int = 100
    DATASET_SAVE_PATH: str = 'app/datasets'

    DATASET_ROW_THRESHOLD_BEFORE_EXPORT: int = 20
    DATASET_COLUMNS_THRESHOLD_BEFORE_EXPORT: int = 5


    # logger.py
    LOG_LEVEL_STDERR: str = 'DEBUG'
    LOG_LEVEL_fILE: str = 'DEBUG'

    # auth.py
    SECRET_KEY: str
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 120

    SECRET_KEY_OTP_ENCRYPT: str = 'satuduatiga'
    
    ADMIN_EMAIL: str = 'algiffaryriony@gmail.com'



    # data_transform_utils.py
    MAX_BAR_ROWS: int = 20
    MIN_LINE_POINTS: int = 5
    MAX_VISUAL_ROWS: int = 50
    MAX_VISUAL_COLS: int = 10 

    DATATYPE_CONV_SUCCESS_RATE_CLEAN_DATASET: float = 0.5 # for datatype conversion



    # schemas.py
    MIN_VALID_COMMON_TASKS_FIRST_RUN: int = 5
    MIN_VALID_COMMON_TASKS_SUBSEQUENT_RUNS: int = 1
    
    LLM_MODEL_LIST_GOOGLE: list = ['gemini-2.5-flash', 'gemma-3-27b-it', 'gemini-2.5-flash-lite']
    
    MAX_TASK_COUNT: int = 20
    
    model_config = SettingsConfigDict(env_file='app/.env.local')

@lru_cache
def get_config() -> Configs:
    return Configs()

Config = get_config()
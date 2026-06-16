from enum import Enum


class TaskStatus(Enum):
    task_queued = "TASK QUEUED"

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

    failed_because_model_not_found = "TASK FAILED BECAUSE LLM MODEL IS NOT FOUND"

    deleted_because_not_accessed_recently = (
        "TASK DELETED BECAUSE IT IS NOT ACCESSED FOR SOME TIME"
    )

    failed_because_rate_limited = "TASK FAILED BECAUSE LLM ENDPOINT IS RATE LIMITED"


class TaskProcessingRunType(Enum):
    first_run_after_request = "first_run_after_request"
    modified_tasks_execution = "modified_tasks_execution"
    additional_analyses_request = "additional_analyses_request"
    modified_tasks_execution_with_new_dataset = (
        "modified_tasks_execution_with_new_dataset"
    )
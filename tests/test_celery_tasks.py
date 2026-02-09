import pytest
import json
from app.tasks import (
    get_prompt_result_task,
    data_processing_task,
    TaskStatus,
    TaskProcessingRunType,
)


def test_get_prompt_result_task(monkeypatch, mocker, get_prompt_result_data):

    model = get_prompt_result_data["model"]
    task_count = get_prompt_result_data["task_count"]
    request_id = get_prompt_result_data["request_id"]
    user_id = get_prompt_result_data["user_id"]
    dataset_cols = get_prompt_result_data["dataset_cols"]
    dataset_id = get_prompt_result_data["dataset_id"]
    resp_pt1_file = get_prompt_result_data["resp_pt1_file"]
    resp_pt2_file = get_prompt_result_data["resp_pt2_file"]
    prompt_pt_1 = ""

    mock_change_request_status_sync = mocker.patch(
        "app.tasks.PromptTableOperation.change_request_status_sync"
    )
    mocker.patch("app.tasks.PromptTableOperation.insert_prompt_result_sync")

    res = get_prompt_result_task(
        model=model,
        prompt_pt_1=prompt_pt_1,
        task_count=task_count,
        dataset_id=dataset_id,
        request_id=request_id,
        user_id=user_id,
        dataset_cols=dataset_cols,
        mock_pt1_resp_file=resp_pt1_file,
        mock_pt2_resp_file=resp_pt2_file,
    )

    last_update_status_call = mock_change_request_status_sync.call_args_list[-1].kwargs

    assert (
        last_update_status_call["status"]
        == TaskStatus.initial_request_prompt_received.value
    ), 'last status change must be "prompt received"'

    # assert if res looks correct
    assert isinstance(res, dict), "task result must be a dict"
    assert len(res) > 0, "task result cannot be an empty dict"


def read_json(json_str):
    try:
        dct = json.loads(json_str)
        return dct
    except ValueError:
        return None


run_types = [
    TaskProcessingRunType.first_run_after_request.value,
    TaskProcessingRunType.modified_tasks_execution.value,
    TaskProcessingRunType.additional_analyses_request.value,
    TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value,
]


@pytest.mark.parametrize("run_type", run_types)
def test_data_processing_task(
    mocker, data_processing_task_first_run_flow_data, run_type
):

    user_id = data_processing_task_first_run_flow_data["user_id"]
    request_id = data_processing_task_first_run_flow_data["request_id"]

    data_tasks_dict_first_req = data_processing_task_first_run_flow_data[
        "data_tasks_dict_first_req"
    ]
    data_tasks_dict_mdfd_tasks = data_processing_task_first_run_flow_data[
        "data_tasks_dict_mdfd_tasks"
    ]
    data_tasks_dict_addt_analyses = data_processing_task_first_run_flow_data[
        "data_tasks_dict_addt_analyses"
    ]
    data_tasks_dict_mdfd_tasks_new_dataset = data_processing_task_first_run_flow_data[
        "data_tasks_dict_mdfd_tasks_new_dataset"
    ]

    run_info = data_processing_task_first_run_flow_data["run_info"]

    mock_change_request_status_sync = mocker.patch(
        "app.tasks.PromptTableOperation.change_request_status_sync"
    )

    mock_task_run_table_ops = mocker.patch("app.tasks.TaskRunTableOperation")
    mock_task_run_table_ops.return_value.request_id_exists.return_value = False

    mocker.patch("app.services.save_dataset_req_id")
    mocker.patch("app.services.result_save_handler")

    data_tasks_dct = {
        TaskProcessingRunType.first_run_after_request.value: data_tasks_dict_first_req,
        TaskProcessingRunType.modified_tasks_execution.value: data_tasks_dict_mdfd_tasks,
        TaskProcessingRunType.additional_analyses_request.value: data_tasks_dict_addt_analyses,
        TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value: data_tasks_dict_mdfd_tasks_new_dataset,
    }

    finished_status_dct = {
        TaskProcessingRunType.first_run_after_request.value: TaskStatus.initial_tasks_run_finished.value,
        TaskProcessingRunType.modified_tasks_execution.value: TaskStatus.customized_tasks_run_finished.value,
        TaskProcessingRunType.additional_analyses_request.value: TaskStatus.additional_tasks_run_finished.value,
        TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value: TaskStatus.doing_customized_tasks_run.value,
    }

    update_task_result_func_dct = {
        TaskProcessingRunType.first_run_after_request.value: "update_original_common_task_result_sync",
        TaskProcessingRunType.modified_tasks_execution.value: "update_task_result_sync",
        TaskProcessingRunType.additional_analyses_request.value: "update_original_common_task_result_sync",
        TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value: "update_task_result_sync",
    }

    data_tasks = data_tasks_dct[run_type]

    _ = data_processing_task(data_tasks, run_info, run_type)

    mock_task_run_table_ops = mock_task_run_table_ops.return_value

    if run_type == TaskProcessingRunType.first_run_after_request.value:
        mock_task_run_table_ops.add_task_result_sync.assert_called_once_with(
            request_id=request_id, user_id=user_id
        )  # to check if the result is inserted into task_run table

    # check if status changed to finished
    last_change_status_calls_kwargs = mock_change_request_status_sync.call_args_list[
        -1
    ].kwargs

    assert last_change_status_calls_kwargs["status"] == finished_status_dct[run_type], (
        'last status change must be "task finished"'
    )

    update_task_result_call_kwargs = (
        getattr(mock_task_run_table_ops, update_task_result_func_dct[run_type])
        .call_args_list[-1]
        .kwargs
    )
    assert "tasks" in update_task_result_call_kwargs, (
        'last call of task_run_table_ops doesnt contain "tasks" argument'
    )

    tasks_dct = read_json(update_task_result_call_kwargs["tasks"])
    assert tasks_dct is not None, "task result must be a valid dictionary"
    assert list(tasks_dct.keys())[0] == "tasks", (
        'task dct must contain list of tasks as value of the key "tasks"'
    )
    assert len(list(tasks_dct.values())[0]) > 0, (
        "list of tasks in task dct cannot be empty"
    )

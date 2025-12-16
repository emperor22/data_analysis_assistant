import pytest
import json
from app.tasks import (get_prompt_result_task, get_additional_analyses_prompt_result, data_processing_task,
                       MOCK_PROMPT_RESULT, PT1_PROMPT_MOCK_FILE, PT2_PROMPT_MOCK_FILE, TaskStatus)

from app.schemas import DataTasks

from app import tasks

import pandas as pd

from unittest import mock

from app.services import get_dataset_snippet


def test_get_prompt_result_task(monkeypatch, mocker, get_prompt_result_data):
    
    pt1_prompt_file = get_prompt_result_data['resp_pt1_file']
    pt2_prompt_file = get_prompt_result_data['resp_pt2_file']
    model = get_prompt_result_data['model']
    task_count = get_prompt_result_data['task_count']
    request_id = get_prompt_result_data['request_id']
    user_id = get_prompt_result_data['user_id']
    dataset_cols = get_prompt_result_data['dataset_cols']
    prompt_pt_1 = ''
    
    monkeypatch.setattr(tasks, 'MOCK_PROMPT_RESULT', True)
    monkeypatch.setattr(tasks, 'PT1_PROMPT_MOCK_FILE', pt1_prompt_file)
    monkeypatch.setattr(tasks, 'PT2_PROMPT_MOCK_FILE', pt2_prompt_file)
    
    mock_change_request_status_sync = mocker.patch('app.tasks.PromptTableOperation.change_request_status_sync')
    mocker.patch('app.tasks.PromptTableOperation.insert_prompt_result_sync')
    
    mock_data_tasks_model_dump = mocker.patch('app.tasks.DataTasks.model_dump')

    res = get_prompt_result_task(model=model, prompt_pt_1=prompt_pt_1, task_count=task_count, request_id=request_id, user_id=user_id, dataset_cols=dataset_cols)
    
    change_status_calls = mock_change_request_status_sync.call_args_list
    
    assert len(change_status_calls) == 2
    assert change_status_calls[0][1]['status'] == TaskStatus.waiting_for_initial_request_prompt.value
    assert change_status_calls[1][1]['status'] == TaskStatus.initial_request_prompt_received.value
    # assert res == get_prompt_result_task_exp_output
    mock_data_tasks_model_dump.assert_called_once()    



def test_data_processing_task(mocker, data_processing_task_first_run_flow_data):
        
    user_id = data_processing_task_first_run_flow_data['user_id']
    request_id = data_processing_task_first_run_flow_data['request_id']
    data_tasks_dict = data_processing_task_first_run_flow_data['data_tasks_dict']
    run_type = data_processing_task_first_run_flow_data['run_type']
    run_info = data_processing_task_first_run_flow_data['run_info']

    
    mock_change_request_status_sync = mocker.patch('app.tasks.PromptTableOperation.change_request_status_sync')
    mock_add_task_result_sync = mocker.patch('app.services.TaskRunTableOperation.add_task_result_sync')
    mock_save_dataset_req_id = mocker.patch('app.services.save_dataset_req_id')
    mock_update_final_dataset_snippet_sync = mocker.patch('app.services.TaskRunTableOperation.update_final_dataset_snippet_sync')
    mock_update_original_common_task_result_sync = mocker.patch('app.services.TaskRunTableOperation.update_original_common_task_result_sync')
    mock_update_column_transform_task_status_sync = mocker.patch('app.services.TaskRunTableOperation.update_column_transform_task_status_sync')
    mock_update_column_combination_task_status_sync = mocker.patch('app.services.TaskRunTableOperation.update_column_combination_task_status_sync')
    mock_update_columns_info_sync = mocker.patch('app.services.TaskRunTableOperation.update_columns_info_sync')
    
    _ = data_processing_task(data_tasks_dict, run_info, run_type)
    
    change_status_calls = mock_change_request_status_sync.call_args_list
    
    assert len(change_status_calls) == 2
    assert change_status_calls[0][1]['status'] == TaskStatus.doing_initial_tasks_run.value
    assert change_status_calls[1][1]['status'] == TaskStatus.initial_tasks_run_finished.value
    
    mock_add_task_result_sync.assert_called_once_with(request_id=request_id, user_id=user_id, original_common_tasks=mock.ANY)
    
    mock_update_final_dataset_snippet_sync.assert_called_once_with(request_id=request_id, dataset_snippet=mock.ANY)
    
    mock_update_original_common_task_result_sync.assert_called_once_with(request_id=request_id, original_common_tasks=mock.ANY)
    mock_update_column_transform_task_status_sync.assert_called_once_with(request_id=request_id, column_transforms_status=mock.ANY)
    mock_update_column_combination_task_status_sync.assert_called_once_with(request_id=request_id, column_combinations_status=mock.ANY)
    mock_update_columns_info_sync.assert_called_once_with(request_id=request_id, columns_info=mock.ANY)
    mock_save_dataset_req_id.assert_called_once_with(save_path=mock.ANY, request_id=request_id, dataframe=mock.ANY, save_type='original_dataset')
    

    
    

    
    

# import pytest
# import json
# from unittest import mock

# from app.crud import PromptTableOperation
# from datetime import datetime

# @pytest.mark.asyncio
# async def test_register_user_endpoint(test_client, user_register_data):
#     endpoint = '/register_user'
#     client, _ = test_client
#     res = await client.post(endpoint, json=user_register_data)
#     username = user_register_data['username']
#     expected_detail_msg = f'account {username} successfully created'
#     assert res.status_code == 200
#     assert res.json()['detail'] == expected_detail_msg
    
    
# @pytest.mark.asyncio
# async def test_get_otp_endpoint(test_client, mocker, get_otp_data):
#     username = get_otp_data['username']
#     otp = get_otp_data['otp']
    
#     endpoint = '/get_otp'
    
#     mock_generate_random_otp = mocker.patch('app.api.generate_random_otp')
#     mock_generate_random_otp.return_value = otp
    
#     mock_update_otp = mocker.patch('app.crud.UserTableOperation.update_otp')
    
#     mock_send_email = mocker.patch('app.api.send_email')
    
#     email_recipients = get_otp_data['email_recipients']
#     email_subject = get_otp_data['email_subject']
#     email_body = get_otp_data['email_body']
    
#     client, bg_task = test_client
    
#     data = {'username': username}
#     res = await client.post(endpoint, json=data)
    
#     assert res.status_code == 200
#     mock_update_otp.assert_called_once_with(username=username, otp=otp, otp_expire=mock.ANY)
#     bg_task.add_task.assert_called_once_with(mock_send_email, email_subject, email_recipients, email_body)
    



# @pytest.mark.asyncio
# async def test_login_endpoint(test_client, mocker, login_data):
#     otp = login_data['otp']
#     otp_expire = login_data['otp_expire']
#     username = login_data['username']
#     new_otp_for_invalidation = login_data['new_otp_for_invalidation']
    
#     mock_get_user = mocker.patch('app.crud.UserTableOperation.get_user')
#     mock_get_user.return_value = {'username': username, 'otp': otp, 'otp_expire': otp_expire}
    
#     mock_update_otp = mocker.patch('app.crud.UserTableOperation.update_otp')
    
#     mock_generate_random_otp = mocker.patch('app.api.generate_random_otp')
#     mock_generate_random_otp.return_value = new_otp_for_invalidation
    
#     client, _ = test_client
    
#     endpoint = '/login'
    
#     data = {'username': username, 'otp': otp}
#     res = await client.post(endpoint, json=data)
    
#     assert res.status_code == 200
#     mock_update_otp.assert_called_once_with(username=username, otp=new_otp_for_invalidation, otp_expire=mock.ANY)

# @pytest.mark.asyncio
# async def test_initial_request_endpoint(test_client, initial_request_data, mocker, default_uuid, get_current_user_dependency_data):
#     mock_add_task = mocker.patch('app.crud.PromptTableOperation.add_task')
#     mock_add_task.return_value = default_uuid
    
#     filename = initial_request_data['filename']
#     file_content = initial_request_data['file_content']
#     model = initial_request_data['model']
#     task_count = initial_request_data['task_count']
#     prompt_version = initial_request_data['prompt_version']
#     dataset_cols = initial_request_data['dataset_cols']

#     user_id = get_current_user_dependency_data.user_id
    
#     endpoint = '/upload_dataset'
#     file = {'file': (filename, file_content)}

#     data = {'model': model, 'analysis_task_count': task_count}
    
#     client, _ = test_client
#     res = await client.post(endpoint, files=file, data=data)
    
#     PromptTableOperation.add_task.assert_called_once_with(user_id=user_id, prompt_version=prompt_version, 
#                                                           filename=filename, dataset_cols=dataset_cols, model=model)
    
#     assert res.status_code == 200


# @pytest.mark.asyncio
# async def test_execute_analyses_endpoint(test_client, execute_analysis_data, patch_is_task_invalid_check):
#     client, _ = test_client
#     res = await client.post('/execute_analyses', data=json.dumps(execute_analysis_data))
    
#     assert res.status_code == 200
    


# @pytest.mark.asyncio
# async def test_additional_analyses_request_endpoint(test_client, additional_analyses_request_data, patch_is_task_invalid_check):
#     client, _ = test_client
#     res = await client.post('/make_additional_analyses_request', data=json.dumps(additional_analyses_request_data))
    
#     assert res.status_code == 200

# @pytest.mark.xfail
# @pytest.mark.asyncio
# async def test_get_original_task_endpoint(test_client, default_uuid, patch_is_task_invalid_check):
#     client, _ = test_client
#     res = await client.get(f'/get_original_tasks_by_id/{default_uuid}')
    
#     assert res.status_code == 200

# @pytest.mark.xfail
# @pytest.mark.asyncio
# async def test_get_modified_task_endpoint(test_client, default_uuid, patch_is_task_invalid_check):
#     client, _ = test_client
#     res = await client.get(f'/get_modified_tasks_by_id/{default_uuid}')
    
#     assert res.status_code == 200

# @pytest.mark.xfail
# @pytest.mark.asyncio
# async def test_get_col_info_endpoint(test_client, default_uuid, patch_is_task_invalid_check):
#     client, _ = test_client
#     res = await client.get(f'/get_col_info_by_id/{default_uuid}')
    
#     assert res.status_code == 200

# @pytest.mark.xfail
# @pytest.mark.asyncio
# async def test_get_dataset_snippet_endpoint(test_client, default_uuid, patch_is_task_invalid_check):
#     client, _ = test_client
#     res = await client.get(f'/get_dataset_snippet_by_id/{default_uuid}')
    
#     assert res.status_code == 200

# @pytest.mark.xfail
# @pytest.mark.asyncio
# async def test_get_request_ids_endpoint(test_client, patch_is_task_invalid_check):
#     client, _ = test_client
#     res = await client.get(f'/get_request_ids')
#     assert res.status_code == 200


    











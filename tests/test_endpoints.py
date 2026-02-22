import pytest
import json


@pytest.mark.asyncio
async def test_register_user_endpoint(test_client, user_register_data):
    endpoint = "/register_user"
    res = await test_client.post(endpoint, json=user_register_data)
    username = user_register_data["username"]

    assert res.status_code == 200
    assert username in res.json()["detail"]


@pytest.mark.asyncio
async def test_get_otp_endpoint(test_client, mocker, get_otp_data):
    username = get_otp_data["username"]
    otp = get_otp_data["otp"]
    encrypted_otp = get_otp_data["encrypted_otp"]

    endpoint = "/get_otp"

    mock_generate_random_otp = mocker.patch("app.api.generate_random_otp")
    mock_generate_random_otp.return_value = (otp, encrypted_otp)  # raw, encrypted

    mock_send_email = mocker.patch("app.api.send_email_task")

    data = {"username": username}
    res = await test_client.post(endpoint, json=data)

    assert res.status_code == 200
    assert mock_send_email.delay.called


@pytest.mark.asyncio
async def test_login_endpoint(test_client, mocker, login_data):
    otp = login_data["otp"]
    username = login_data["username"]
    new_otp_for_invalidation = login_data["new_otp_for_invalidation"]
    new_otp_encrypted = login_data["new_otp_encrypted"]

    mock_generate_random_otp = mocker.patch("app.api.generate_random_otp")
    mock_generate_random_otp.return_value = new_otp_for_invalidation, new_otp_encrypted

    endpoint = "/login"

    data = {"username": username, "otp": otp}
    res = await test_client.post(endpoint, json=data)

    assert res.status_code == 200
    assert "access_token" in res.json()


@pytest.mark.asyncio
async def test_initial_request_endpoint(
    test_client,
    initial_request_data,
    mocker,
    default_uuid,
    get_current_user_dependency_data,
):
    filename = initial_request_data["filename"]
    file_content = initial_request_data["file_content"]
    model = initial_request_data["model"]
    task_count = initial_request_data["task_count"]
    provider = initial_request_data["provider"]
    run_name = initial_request_data["run_name"]
    send_result_to_email = initial_request_data["send_result_to_email"]

    endpoint = "/upload_dataset"
    file = {"file": (filename, file_content)}

    data = {
        "model": model,
        "provider": provider,
        "run_name": run_name,
        "analysis_task_count": task_count,
        "send_result_to_email": send_result_to_email,
    }
    data = {"upload_dataset_data": json.dumps(data)}

    res = await test_client.post(endpoint, files=file, data=data)

    assert res.status_code == 200


@pytest.mark.asyncio
async def test_execute_analyses_endpoint(
    test_client, execute_analysis_data, default_uuid
):
    data = {"execute_analyses_data": json.dumps(execute_analysis_data)}
    res = await test_client.post(f"/execute_analyses/{default_uuid}", data=data)

    assert res.status_code == 200


@pytest.mark.asyncio
async def test_execute_analyses_w_new_dataset_endpoint(
    mocker,
    test_client,
    execute_analysis_with_new_dataset_data,
    default_uuid,
    upload_file_payload,
    column_transform_and_combination_data,
):
    filename = upload_file_payload["filename"]
    file_content = upload_file_payload["file_content"]

    # mock_dataset_columns_match = mocker.patch('app.api.dataset_columns_match')
    # mock_dataset_columns_match.return_value = True

    mock_get_col_transform_and_combination = mocker.patch(
        "app.api.get_col_transform_and_combination"
    )  # no data in task_run table yet
    mock_get_col_transform_and_combination.return_value = (
        column_transform_and_combination_data
    )

    data = {"execute_analyses_data": json.dumps(execute_analysis_with_new_dataset_data)}
    file = {"file": (filename, file_content)}
    res = await test_client.post(
        f"/execute_analyses_with_new_dataset/{default_uuid}", files=file, data=data
    )

    assert res.status_code == 200


@pytest.mark.asyncio
async def test_additional_analyses_request_endpoint(
    test_client, additional_analyses_request_data, default_uuid
):
    res = await test_client.post(
        f"/make_additional_analyses_request/{default_uuid}",
        data=json.dumps(additional_analyses_request_data),
    )

    assert res.status_code == 200


@pytest.mark.asyncio
async def test_get_original_task_endpoint(test_client, default_uuid):
    res = await test_client.get(f"/get_original_tasks_by_id/{default_uuid}")

    if res.status_code == 404:
        pytest.xfail("result not found because theres no e2e tests yet")
    assert res.status_code == 200


@pytest.mark.asyncio
async def test_get_modified_task_endpoint(test_client, default_uuid):
    res = await test_client.get(f"/get_modified_tasks_by_id/{default_uuid}")

    if res.status_code == 404:
        pytest.xfail("result not found because theres no e2e tests yet")
    assert res.status_code == 200


@pytest.mark.asyncio
async def test_get_col_info_endpoint(test_client, default_uuid):
    res = await test_client.get(f"/get_col_info_by_id/{default_uuid}")

    if res.status_code == 404:
        pytest.xfail("result not found because theres no e2e tests yet")
    assert res.status_code == 200


@pytest.mark.asyncio
async def test_get_dataset_snippet_endpoint(test_client, default_uuid):
    res = await test_client.get(f"/get_dataset_snippet_by_id/{default_uuid}")

    if res.status_code == 404:
        pytest.xfail("result not found because theres no e2e tests yet")
    assert res.status_code == 200


@pytest.mark.asyncio
async def test_get_request_ids_endpoint(test_client):
    res = await test_client.get("/get_request_ids")

    if res.status_code == 404:
        pytest.xfail("result not found because theres no e2e tests yet")

    assert res.status_code == 200


@pytest.mark.asyncio
async def test_delete_task_endpoint(test_client, default_uuid):
    res = await test_client.post(f"delete_task/{default_uuid}")

    if res.status_code == 404:
        pytest.xfail("result not found because theres no e2e tests yet")

    assert res.status_code == 200


@pytest.mark.asyncio
async def test_download_excel_endpoint(test_client, download_excel_result_data):
    task_type = download_excel_result_data["task_type"]
    request_id = download_excel_result_data["request_id"]
    task_id = download_excel_result_data["task_id"]

    res = await test_client.get(
        f"/download_excel_result/{task_type}/{request_id}/{task_id}"
    )

    if res.status_code == 404:
        pytest.xfail("result not found because theres no e2e tests yet")

    assert res.status_code == 200


@pytest.mark.asyncio
async def test_setup_api_key(test_client, mocker, setup_api_key_data):
    key = setup_api_key_data["key"]
    provider = setup_api_key_data["provider"]

    mock_check_valid = mocker.patch("app.api.check_if_api_key_valid")

    mock_check_valid.return_value = "VALID_KEY"

    data = {"key": key, "provider": provider}

    res = await test_client.post("/setup_api_key", json=data)

    assert res.status_code == 200


@pytest.mark.asyncio
async def test_save_customized_tasks(test_client, save_customized_tasks_data):
    operation = save_customized_tasks_data["operation"]
    tasks = save_customized_tasks_data["tasks"]
    slot = save_customized_tasks_data["slot"]
    request_id = save_customized_tasks_data["request_id"]

    data = {
        "operation": operation,
        "tasks": tasks,
        "slot": slot,
        "request_id": request_id,
    }

    res = await test_client.post("/manage_user_cust_tasks", json=data)

    assert res.status_code == 200

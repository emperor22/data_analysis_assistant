import streamlit as st
import functools
import requests
import time
import json
from copy import deepcopy
from string import Template
import re

import base64
from PIL import Image
from io import BytesIO

from typing import Literal
import os

import pandas as pd

# import nltk
# nltk.download('punkt')
# nltk.download('averaged_perceptron_tagger_eng')

default_url = "http://localhost:8000"
URL = os.getenv("API_URL", default_url)


DEFAULT_VERSION_CUSTOMIZED_TASKS = 1


def force_refresh_page_once(key):
    state_key = f"{key}_already_refreshed"
    if state_key not in st.session_state:
        st.session_state[state_key] = 0

    if st.session_state[state_key] == 0:
        st.session_state[state_key] = 1
        st.rerun()


def register_user(username, first_name, last_name, email):
    body = {
        "username": username,
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
    }
    url = f"{URL}/register_user"
    res = requests.post(url, verify=False, json=body)

    if res.status_code == 409:
        return "username/email already exists"

    if res.status_code == 422:
        return "invalid username/first name/last name"

    if res.status_code == 429:
        rate_limit_error = res.json()["error"]
        st.error(rate_limit_error)

    return "success"


def submit_login_request(username, otp):
    body = {"username": username, "otp": otp}
    url = f"{URL}/login"
    res = requests.post(url, verify=False, json=body)

    if res.status_code == 401:
        return None

    if res.status_code == 429:
        rate_limit_error = res.json()["error"]
        st.error(rate_limit_error)
        st.stop()

    try:
        return res.json()
    except Exception:
        st.write(res.text)


def get_otp(username):
    url = f"{URL}/get_otp"
    data = {"username": username}
    res = requests.post(url, json=data, verify=False)

    if res.status_code == 401:
        return "invalid username"

    if res.status_code == 429:
        return "too many otp requests"

    if res.status_code != 200:
        return "internal error"

    return "success"


def show_unauthorized_error_and_redirect_to_login():
    st.session_state["authenticated"] = False
    st.session_state["access_token"] = None
    st.error("session expired. please log in again.")
    time.sleep(1)
    st.switch_page("Homepage.py")


def remove_duplicate_tasks(tasks):
    seen_steps = set()
    unique_list = []

    for item in tasks:
        item = deepcopy(item)
        steps_value = item["steps"]

        if steps_value not in seen_steps:
            unique_list.append(item)
            seen_steps.add(steps_value)

    return unique_list


# a decorator that intercepts the 'headers' argument and insert access token
def include_auth_header(func):

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        access_token = st.session_state.get("access_token")
        auth_header = {"Authorization": f"Bearer {access_token}"}
        if not access_token:
            show_unauthorized_error_and_redirect_to_login()

        if "headers" in kwargs:
            kwargs["headers"].update(auth_header)
        else:
            kwargs["headers"] = auth_header

        res = func(*args, **kwargs)

        if res.status_code == 200:
            try:
                return res.json()
            except requests.exceptions.JSONDecodeError:
                return res

        if res.status_code == 422:
            try:
                error_details = res.json()
                print(error_details)
                return
            except requests.exceptions.JSONDecodeError:
                print(res.text)
                return

        if res.status_code == 429:
            rate_limit_msg = res.json()["error"]
            st.error(rate_limit_msg)
            return

        if res.status_code == 401:
            show_unauthorized_error_and_redirect_to_login()

        st.error(f"{res.json()['detail']}")

    return wrapper


@include_auth_header
def get_original_tasks_by_id(task_id, headers=None):
    url = f"{URL}/get_original_tasks_by_id/{task_id}"

    res = requests.get(url, verify=False, headers=headers)

    return res


@include_auth_header
def manage_customized_tasks(
    request_id,
    operation: Literal["fetch", "delete", "update", "check_if_empty"],
    slot=None,
    tasks=None,
    headers=None,
):
    url = f"{URL}/manage_user_cust_tasks"

    data = {"request_id": request_id, "operation": operation}

    if tasks is not None:
        data["tasks"] = {"customized_tasks": tasks}

    if slot is not None:
        data["slot"] = slot

    res = requests.post(url, json=data, headers=headers, verify=False)

    return res


@include_auth_header
def get_modified_tasks_by_id(task_id, headers=None):
    url = f"{URL}/get_modified_tasks_by_id/{task_id}"

    res = requests.get(url, verify=False, headers=headers)

    return res


def is_task_still_processing(status):
    processing_status = (
        "TASK QUEUED",
        "GETTING INITIAL REQUEST PROMPT RESULT",
        "RUNNING INITIAL ANALYSES TASKS",
        "INITIAL REQUEST PROMPT RESULT RECEIVED",
    )

    return status in processing_status


@st.cache_data(scope="session")
@include_auth_header
def get_task_ids_by_user(headers=None):
    url = f"{URL}/get_request_ids"

    res = requests.get(
        url, verify=False, headers=headers
    )  # result is [task_id, filename, status]

    return res


@include_auth_header
def get_task_ids_by_user_uncached(headers=None):
    url = f"{URL}/get_request_ids"

    res = requests.get(
        url, verify=False, headers=headers
    )  # result is [task_id, filename, status]

    return res


def is_valid_email(email):
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

    if re.fullmatch(pattern, email):
        return True
    else:
        return False


def render_task_id_data(req_id_data):

    def pad_or_truncate(x, ln):
        if len(x) <= ln:
            pad = " " * (ln - len(x))
            return x + pad
        return x[: ln - 2] + "..."

    ln = 20

    name = req_id_data[1]
    name = pad_or_truncate(name, ln)

    filename = req_id_data[2]
    filename = pad_or_truncate(filename, ln)

    date = req_id_data[3][:10]  # get only the date from datetime str
    return f"{name} | {filename} | {date}"


def render_request_ids():

    col1, col2 = st.columns([12, 1])

    with col2:
        if st.button("⟳"):
            get_task_ids_by_user.clear()
            st.rerun()

    task_ids = get_task_ids_by_user()

    if not task_ids:
        st.stop()

    task_ids = task_ids.get("request_ids")

    with col1:
        if not task_ids:
            st.stop()

        task_ids = [i for i in task_ids if not is_task_still_processing(i[-1])]
        task_ids_choices = [""] + [render_task_id_data(i) for i in task_ids]

        task_ids_select = st.selectbox(
            "Select task", options=task_ids_choices, key="task_id_select"
        )

        if not task_ids_select:
            st.stop()

        task_id_idx = task_ids_choices.index(task_ids_select)
        task_ids_select = task_ids[task_id_idx - 1][0]

    return task_ids_select


def make_metric_font_smaller():
    st.markdown(
        """
    <style>
    [data-testid="stMetricValue"] {
        font-size: 15px;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )


def render_col_info(col_info):
    col_name = col_info["name"]
    source = col_info["source"]
    source_expander_suffix = "" if source == "original" else " | **DERIVED**"

    with st.expander(f"{col_name}{source_expander_suffix}", expanded=False):
        inf_res = col_info["inferred_info_prompt_res"]

        if source == "original":
            st.subheader("Inferred Column Information")
            # Display basic inferred info in columns for readability
            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("Classification", inf_res.get("classification", "N/A"))
                st.metric("Data Type", inf_res.get("data_type", "N/A"))

            with col2:
                st.metric("Type", inf_res.get("type", "N/A"))
                st.metric("Confidence", inf_res.get("confidence_score", "N/A").title())

            with col3:
                unit = inf_res.get("unit", "")
                st.metric("Unit", unit if unit else "None")

        if "operation" in inf_res:
            st.write("")
            st.write("Description:")
            st.write(inf_res.get("description", ""))
            st.write("Formula:")
            st.code(inf_res.get("formula", ""), language="text")

        with st.expander("Detailed Statistics", expanded=False):
            props = col_info["type_dependent_properties"]
            datatype = props["datatype"]

            # Common properties
            col_a, col_b, col_c = st.columns(3)

            with col_a:
                st.metric(
                    "Missing Values",
                    f"{col_info['missing_count']:,} ({col_info['missing_value_ratio']:.1%})",
                )

            with col_b:
                st.metric(
                    "Unique Values",
                    f"{col_info['unique_count']:,} ({col_info['uniqueness_ratio']:.1%})",
                )

            with col_c:
                is_cat = "Yes" if props.get("is_categorical", False) else "No"
                st.metric("Categorical?", is_cat)

            if datatype == "numerical":
                st.subheader("Numerical Properties")

                col1, col2, col3 = st.columns(3)

                with col1:
                    st.metric("Min", f"{props['min_value']:.2f}")
                    st.metric("Max", f"{props['max_value']:.2f}")

                with col2:
                    st.metric("Mean", f"{props['mean_value']:.2f}")
                    st.metric("Median", f"{props['median_value']:.2f}")

                with col3:
                    st.metric("Std Dev", f"{props['std']:.2f}")
                    skew = props["skewness"]
                    st.metric("Skewness", f"{skew:.2f}")

                # Quartiles
                q25, q75 = props["q_25th"], props["q_75th"]
                st.write(f"**IQR:** {q25:.2f} - {q75:.2f}")

            elif datatype == "string":
                st.subheader("String Properties")

                col1, col2 = st.columns(2)

                with col1:
                    st.metric("Max Length", props["max_length"])

                with col2:
                    st.metric("Avg Length", f"{props['mean_length']:.1f}")

            elif datatype == "datetime":
                st.subheader("Datetime Properties")

                col1, col2, col3 = st.columns(3)

                with col1:
                    st.metric("Min. date", props["date_min"])

                with col2:
                    st.metric("Max. date", props["date_max"])

                with col3:
                    st.metric("Date range (days)", props["range_days"])

            common_data = []
            for val, freq in props["most_common_5_values"].items():
                common_data.append({"Value": f"{val}", "Frequency (%)": f"{freq:.1%}"})
            st.subheader("Top 5 Most Common Values")
            st.table(common_data)


def render_delete_task_button(task_id):
    if st.button("Delete task"):
        _ = delete_task(task_id)
        get_task_ids_by_user.clear()  # clear task ids cache to reflect latest list of tasks

        st.success("Task deleted")
        st.rerun()


@include_auth_header
@st.cache_data(scope="session")
def get_col_info_by_id(task_id, headers=None):
    url = f"{URL}/get_col_info_by_id/{task_id}"

    res = requests.get(url, verify=False, headers=headers)

    return res


@include_auth_header
@st.cache_data(scope="session")
def get_dataset_snippet_by_id(task_id, headers=None):
    url = f"{URL}/get_dataset_snippet_by_id/{task_id}"

    res = requests.get(url, verify=False, headers=headers)

    return res


@include_auth_header
def send_tasks_to_process(data_tasks, request_id, send_result_to_email, headers=None):
    url = f"{URL}/execute_analyses/{request_id}"

    data_tasks["send_result_to_email"] = send_result_to_email

    data = {"execute_analyses_data": json.dumps(data_tasks)}

    res = requests.post(url, verify=False, data=data, headers=headers)

    return res


@include_auth_header
def send_tasks_to_process_w_new_dataset(
    uploaded_file, data_tasks, request_id, send_result_to_email=False, headers=None
):
    url = f"{URL}/execute_analyses_with_new_dataset/{request_id}"

    data_tasks["send_result_to_email"] = send_result_to_email

    data = {"execute_analyses_data": json.dumps(data_tasks)}

    file = {"file": (uploaded_file.name, uploaded_file.getvalue())}

    res = requests.post(url, verify=False, files=file, data=data, headers=headers)

    return res


@include_auth_header
def make_analysis_request(
    name, uploaded_file, model, task_count, send_result_to_email, headers=None
):
    url = f"{URL}/initial_analysis"
    file = {"file": (uploaded_file.name, uploaded_file.getvalue())}

    provider, model = model.split(":")

    params = {
        "run_name": name,
        "model": model,
        "provider": provider,
        "analysis_task_count": str(task_count),
        "send_result_to_email": send_result_to_email,
    }
    data = {"upload_dataset_data": json.dumps(params)}

    res = requests.post(url, verify=False, files=file, headers=headers, data=data)

    return res


@include_auth_header
def download_excel_result(request_id, task_id, task, headers=None):
    url = f"{URL}/download_excel_result/{task}/{request_id}/{task_id}"

    res = requests.get(url, headers=headers, verify=False)

    return res


def render_excel_download_button(
    request_id, task_id, task: Literal["original_tasks", "customized_tasks"]
):
    col1, col2 = st.columns(2)
    with col1:
        if st.button(
            "Download excel result", key=f"download_button_{request_id}_{task_id}"
        ):
            res = download_excel_result(
                request_id=request_id, task_id=task_id, task=task
            )

            if res is None:
                st.error("Cannot download excel result")
            else:
                with col2:
                    st.download_button(
                        "Download",
                        data=res.content,
                        file_name=f"{task}_req_id_{request_id}_task_id_{task_id}.xlsx",
                    )


@include_auth_header
def make_additional_analyses_request(
    model, new_tasks_prompt, request_id, send_result_to_email, headers=None
):
    url = f"{URL}/make_additional_analyses_request/{request_id}"
    provider, model = model.split(":")
    data = {
        "model": model,
        "provider": provider,
        "new_tasks_prompt": new_tasks_prompt,
        "send_result_to_email": send_result_to_email,
    }

    res = requests.post(url, verify=False, headers=headers, data=json.dumps(data))

    return res


@include_auth_header
def delete_task(request_id, headers=None):
    url = f"{URL}/delete_task/{request_id}"

    res = requests.post(url, verify=False, headers=headers)

    return res


@include_auth_header
def setup_api_key(provider, key, headers=None):
    url = f"{URL}/setup_api_key"

    data = {"key": key, "provider": provider}
    res = requests.post(url, verify=False, json=data, headers=headers)

    return res


def is_numerical(s):
    try:
        _ = float(s)
        return True
    except ValueError:
        return False


def split_and_validate_new_prompt(new_analysis_text):

    def validate_value(s):
        min_char = 15
        max_char = 100
        return min_char <= len(s) <= max_char

    regex = r"^[a-zA-Z0-9 \n\r]*$"

    if not bool(re.fullmatch(regex, new_analysis_text)):
        return

    values = [i.strip() for i in new_analysis_text.split("\n")]
    all_values_valid = all([validate_value(val) for val in values])

    if len(values) > 5 or not all_values_valid:
        return

    return new_analysis_text


def render_modified_task_box(
    request_id, param_info, all_columns, step_idx, step, step_param, task_idx
):
    """
    Arguments:
    param_info -> from PARAMS_MAP, obtained from function_name. to get alias, widget type, and options for widget
    all_columns -> columns of the dataset. include newly created columns from llm response
    step_idx -> get the step index on each task for widget key
    step -> the individual step to get necessary info
    step_param -> step's argument, to get the current value
    task_idx -> get the step index for widget key and to modify the final tasks dict
    """

    widget_type = param_info["type"]

    value = step.get(step_param, [""])

    if widget_type != "multiselect":
        value = value[0] if isinstance(value, list) and len(value) > 0 else value

    if step["function"] == "filter" and step_param == "values":
        num_ops = [">", "<", ">=", "<=", "==", "!="]

        num_or_text = (
            "numerical"
            if step["operator"] in num_ops and is_numerical(value)
            else "text"
        )
        widget_type = param_info["type"][num_or_text]

        if step["operator"] == "between":
            raise Exception("not implemented yet")

    if widget_type == "selectbox":
        options = param_info.get("options", all_columns)
        if step["function"] == "filter" and step_param == "operator":
            num_ops = [">", "<", ">=", "<=", "==", "!="]
            filter_value = (
                step["values"][0]
                if isinstance(step["values"], list)
                else step["values"]
            )

            options = (
                num_ops
                if step["operator"] in num_ops and is_numerical(filter_value)
                else ["in"]
            )
            value = (
                value
                if step["operator"] in num_ops and is_numerical(filter_value)
                else "in"
            )

            # this line forces replacing the operator with 'in' in case the operator is == with single string value
            st.session_state.modified_tasks[request_id][task_idx]["steps"][step_idx][
                step_param
            ] = value

        index = options.index(value)

        selected_value = st.selectbox(
            label=param_info["alias"],
            options=options,
            index=index,
            key=f"task_{task_idx}_step_{step_idx}_param_{step_param}",
        )

        return (
            [selected_value] if isinstance(step[step_param], list) else selected_value
        )  # uses step[step_param] to get original value format

    elif widget_type == "multiselect":
        selected_value = st.multiselect(
            label=param_info["alias"],
            options=param_info.get("options", all_columns),
            default=value,
            key=f"task_{task_idx}_step_{step_idx}_param_{step_param}",
        )

        return selected_value

    elif widget_type == "number_input":
        new_value = st.number_input(
            label=param_info["alias"],
            value=value,
            key=f"task_{task_idx}_step_{step_idx}_param_{step_param}",
        )

        return [new_value] if isinstance(step[step_param], list) else new_value

    elif widget_type == "radio":
        new_value = st.radio(
            label=param_info["alias"],
            options=param_info["options"],
            index=param_info["options"].index(value),
            key=f"task_{task_idx}_step_{step_idx}_param_{step_param}",
        )

        return [new_value] if isinstance(step[step_param], list) else new_value

    elif widget_type == "text_input":
        new_value = st.text_input(
            label=param_info["alias"],
            key=f"task_{task_idx}_step_{step_idx}_param_{step_param}",
            value=value,
        )
        st.warning("Please insert valid values from your selected column.")
        st.warning("If multiple values, separate them with semicolon (;)")

        return [val.strip() for val in new_value.split(";")]


def render_original_task_expander(request_id, task, task_idx, plots_dct, task_mode):
    task_status = task["status"]

    status_in_label = (
        f" ({task_status.split()[0].upper()})"
        if task_status.startswith("failed")
        else ""
    )
    expander_label = f"{task_idx + 1} - {task['name']}{status_in_label}"

    with st.expander(expander_label):
        st.write(f"**Status**: {task_status}")
        st.write(f"**Description**: {task['description']}")
        st.write(f"**Score**: {task['score']}")

        st.write("---")
        st.write("**Steps:**")

        for step_idx, step in enumerate(task["steps"]):
            render_task_step(step_idx, step)

        st.write("---")

        if "failed" not in task_status:
            task_id = str(task["task_id"])

            if "big" in task_status:
                render_excel_download_button(
                    request_id=request_id, task_id=task_id, task=task_mode
                )

            else:
                st.write("**Result**")
                st.write(pd.DataFrame(task["result"]))

                if task_id in plots_dct:
                    st.write("**Chart**")
                    display_b64_encoded_image(plots_dct[task_id])


def process_step_val(val):
    if val is None:
        return "()"
    if isinstance(val, list):
        if len(val) > 1:
            val = [f"**{v}**" for v in val]
            val = ", ".join(val)
            val = f"({val})"
            return val
        else:
            val = val[0] if len(val) > 0 else ""

    return f"**{val}**" if val else "[]"


def get_template_keys_to_be_substituted(s):
    return [i[1] for i in Template(s).pattern.findall(s) if i[1] is not None]


def render_task_step(step_idx, step):
    template_str = PARAMS_MAP[step["function"]]["template"]
    template = Template(template_str)
    args = {i: process_step_val(j) for i, j in step.items() if i != "function"}

    template_keys = get_template_keys_to_be_substituted(template_str)
    fill_missing_args = {i: "**[]**" for i in template_keys if i not in args.keys()}
    args.update(fill_missing_args)

    val = template.substitute(args)

    val = f"{step_idx + 1} - {val}"

    return st.write(val)


# def is_valid_sentence_nlp(text):
#     if not isinstance(text, str) or not text:
#         return False

#     sentences = sent_tokenize(text)  # type: ignore
#     print(sentences)
#     if len(sentences) != 1:
#         return False

#     words = word_tokenize(text)  # type: ignore
#     tagged_words = pos_tag(words)  # type: ignore

#     has_verb = any(tag.startswith("VB") for word, tag in tagged_words)

#     return has_verb


def display_b64_encoded_image(img_string):
    image_bytes = base64.b64decode(img_string)

    image = Image.open(BytesIO(image_bytes))

    st.image(image)


def render_progress_table():

    def truncate_text(text, max_len=15):
        if len(text) >= max_len:
            return text[:max_len] + ".."
        return text

    res = get_task_ids_by_user_uncached()

    if not res:
        st.error("You don't have any tasks.")
        return

    cols = st.columns(5)
    headers = [
        "**Request ID**",
        "**Name**",
        "**Filename**",
        "**Created At**",
        "**Progress**",
    ]
    [col.write(val) for col, val in zip(cols, headers)]
    st.write("***")

    res = res["request_ids"]

    for req_id, req_name, req_filename, req_date, req_status in res:
        cols = st.columns(5)
        cols[0].write(truncate_text(req_id))
        cols[1].write(truncate_text(req_name))
        cols[2].write(truncate_text(req_filename))
        cols[3].write(req_date[:10])  # only get date from datetime str

        if req_status not in failed_states:
            cols[4].progress(
                value=progress_value.get(req_status, 0),
                text=req_status if req_status else "",
            )
        else:
            st.error(req_status)
        st.write("***")


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

DEFAULT_PARAMS = {
    "groupby": ["columns_to_group_by", "columns_to_aggregate"],
    "filter": ["column_name", "operator", "values"],
    "get_top_or_bottom_N_entries": [
        "number_of_entries",
        "sort_by_column_name",
        "order",
    ],
    "get_proportion": ["column_name", "values"],
    "get_column_statistics": ["column_name"],
    "resample_data": ["frequency", "static_group_cols", "columns_to_aggregate"],
}

initial_request_flow = {
    "TASK QUEUED": 0.0,
    "GETTING INITIAL REQUEST PROMPT RESULT": 1 / 4,
    "INITIAL REQUEST PROMPT RESULT RECEIVED": 2 / 4,
    "RUNNING INITIAL ANALYSES TASKS": 3 / 4,
    "INITIAL ANALYSIS TASKS FINISHED": 4 / 4,
}

addt_analyses_flow = {
    "GETTING ADDITIONAL ANALYSES REQUEST PROMPT RESULT": 1 / 4,
    "ADDITIONAL ANALYSES PROMPT RESULT RECEIVED": 2 / 4,
    "RUNNING ADDITIONAL ANALYSES TASKS": 3 / 4,
    "ADDITIONAL ANALYSES TASKS FINISHED": 4 / 4,
}

execute_analyses_flow = {
    "RUNNING USER CUSTOMIZED ANALYSIS TASKS": 1 / 2,
    "USER CUSTOMIZED ANALYSIS TASKS FINISHED": 2 / 2,
}

execute_analyses_new_dataset_flow = {
    "RUNNING USER CUSTOMIZED ANALYSIS TASKS WITH NEW DATASET": 1 / 2,
    "USER CUSTOMIZED ANALYSIS TASKS WITH NEW DATASET FINISHED": 2 / 2,
}

progress_value = {
    **initial_request_flow,
    **addt_analyses_flow,
    **execute_analyses_flow,
    **execute_analyses_new_dataset_flow,
}

failed_states = [
    "TASK FAILED BECAUSE DATASET IS BLACKLISTED"
    "TASK DELETED BECAUSE IT IS NOT ACCESSED FOR SOME TIME"
    "TASK FAILED BECAUSE LLM ENDPOINT IS RATE LIMITED"
]


##########################################################################

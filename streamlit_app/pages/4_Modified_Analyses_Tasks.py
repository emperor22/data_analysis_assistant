from utils import (
    send_tasks_to_process,
    send_tasks_to_process_w_new_dataset,
    render_modified_task_box,
    render_task_step,
    process_step_val,
    PARAMS_MAP,
    DEFAULT_PARAMS,
    get_col_info_by_id,
    get_template_keys_to_be_substituted,
    get_original_tasks_by_id,
    render_request_ids,
    get_modified_tasks_by_id,
    render_original_task_expander,
    manage_customized_tasks,
)
import streamlit as st
import json
from copy import deepcopy
import time
from string import Template
from random import randint

cols_per_row = 1
max_task_count = 30


request_id = render_request_ids()

# to do: recreate selected_tasks_to_modify session state in original_tasks page. use this as the main source of truth for customized_tasks.
# if it disappears after reload, then fetch the tasks from the get_customized_tasks endpoint. additionally, provide a button 'load tasks from db' to override the customized tasks with ones from db (remove the selected_tasks_to_modify state first)

if "selected_tasks_to_modify" not in st.session_state:
    st.session_state.selected_tasks_to_modify = {}

# initialize 'modified_tasks' list if this is the first task import
if "modified_tasks" not in st.session_state:
    st.session_state.modified_tasks = {}

if "tasks" not in st.session_state:
    st.session_state.tasks = {}
    st.session_state.tasks_plots = {}

if request_id not in st.session_state.selected_tasks_to_modify:
    st.session_state.selected_tasks_to_modify[request_id] = []

if (
    request_id not in st.session_state.modified_tasks
    or len(st.session_state.modified_tasks) == 0
) and len(st.session_state.selected_tasks_to_modify[request_id]) == 0:
    saved_tasks_empty = manage_customized_tasks(request_id, operation="check_if_empty")[
        "res"
    ]

    if saved_tasks_empty:
        st.error("You need to import some original tasks first and save them.")
        st.stop()

if request_id not in st.session_state.tasks:
    res = get_original_tasks_by_id(request_id)

    if res:
        tasks = res["res"]["original_common_tasks"]
        tasks = json.loads(tasks)["tasks"]
        st.session_state.tasks[request_id] = tasks

        plots = res["plot_result"]
        st.session_state.tasks_plots[request_id] = plots


if (
    request_id not in st.session_state.modified_tasks
    or len(st.session_state.modified_tasks[request_id]) == 0
):
    imported_tasks_ids = st.session_state.selected_tasks_to_modify[request_id]
    loading_data_txt = st.empty()
    if len(imported_tasks_ids) == 0:
        loading_data_txt.write("Loading saved tasks from DB..")

        imported_tasks = manage_customized_tasks(request_id, "fetch", slot=1)["res"]
        imported_tasks = json.loads(imported_tasks)["customized_tasks"]

        time.sleep(1)
        loading_data_txt.empty()
    else:
        loading_data_txt.write("Loading imported original tasks..")

        tasks = deepcopy(st.session_state.tasks[request_id])

        for task in tasks:
            del task["status"]
            del task["result"]
            task["score"] = "inapplicable"

        imported_tasks = [
            task for task in tasks if task["task_id"] in imported_tasks_ids
        ]

        time.sleep(1)
        loading_data_txt.empty()

    st.session_state.modified_tasks[request_id] = imported_tasks

# need to get this info from db for each request_id
dataset_cols = get_col_info_by_id(request_id)
dataset_cols = json.loads(dataset_cols["columns_info"])
ALL_COLUMNS = [col["name"] for col in dataset_cols["columns_info"]]

customized_tasks = st.session_state.modified_tasks[request_id]
if len(customized_tasks) > 0:
    if st.button("Save customized tasks"):
        res = manage_customized_tasks(
            request_id, "update", tasks=customized_tasks, slot=1
        )

        if res:
            st.success("Customized tasks save successful")
            time.sleep(1)
            st.rerun()

task_edit_tab, task_overview_tab, task_result_tab = st.tabs(
    ["Customize tasks", "Task overview", "Task result"]
)

with task_edit_tab:
    st.subheader("Customize tasks")

    for task_idx, task in enumerate(st.session_state.modified_tasks[request_id]):
        if task_idx % cols_per_row == 0:
            st.write("")
            current_cols = st.columns(cols_per_row)

        with current_cols[task_idx % cols_per_row]:
            with st.container(border=True):
                col1, col2, col3, col4 = st.columns([10, 1, 1, 1])

                with col1:
                    if st.checkbox(
                        "Edit name/description", key=f"edit_name_desc_{task_idx}"
                    ):
                        with st.form(
                            f"{task_idx}_name_desc_change", enter_to_submit=False
                        ):
                            task_name_change = st.text_input(
                                "Task name", value=task["name"]
                            )
                            task_desc_change = st.text_input(
                                "Task description", value=task["description"]
                            )

                            if st.form_submit_button("💾", help="Save changes"):
                                st.session_state.modified_tasks[request_id][task_idx][
                                    "name"
                                ] = task_name_change
                                st.session_state.modified_tasks[request_id][task_idx][
                                    "description"
                                ] = task_desc_change

                    del_dup_msg = st.empty()

                with col3:
                    if st.button(
                        "🗑️",
                        key=f"del_task_{task_idx}",
                        type="primary",
                        help="Delete task",
                    ):
                        id_ = task["task_id"]
                        new_tasks = [
                            task
                            for task in st.session_state.modified_tasks[request_id]
                            if task["task_id"] != id_
                        ]
                        st.session_state.modified_tasks[request_id] = new_tasks

                        del_dup_msg.success("task deleted!")
                        time.sleep(1)
                        st.rerun()

                with col4:
                    if st.button(
                        "⿻",
                        key=f"duplicate_task_{task_idx}",
                        type="primary",
                        help="Duplicate task",
                    ):
                        if (
                            len(st.session_state.modified_tasks[request_id])
                            < max_task_count
                        ):
                            del_dup_msg.error(
                                f"total tasks cannot be more than {max_task_count}"
                            )

                        id_ = task["task_id"]
                        dup_task = next(
                            task
                            for task in st.session_state.modified_tasks[request_id]
                            if task["task_id"] == id_
                        )
                        dup_task = deepcopy(dup_task)
                        dup_task["task_id"] = randint(100, 9999)
                        dup_task["name"] = f"{dup_task['name']} (copy)"

                        st.session_state.modified_tasks[request_id].append(dup_task)

                        del_dup_msg.success(
                            "task duplicated! please scroll to the bottom to check it out."
                        )
                        time.sleep(2)
                        st.rerun()

                st.markdown(f"**{task_idx + 1} - {task['name']}**")
                st.caption(f"*{task['description']}*")

                for step_idx, step in enumerate(task["steps"]):
                    func_name = step["function"]
                    expander_name = f"Step {step_idx + 1}: {func_name}"

                    with st.expander(expander_name):
                        template_str = PARAMS_MAP[step["function"]]["template"]

                        step_args = {
                            i: process_step_val(j)
                            for i, j in step.items()
                            if i != "function"
                        }

                        template_keys = get_template_keys_to_be_substituted(
                            template_str
                        )
                        fill_missing_args = {
                            i: "**[]**"
                            for i in template_keys
                            if i not in step_args.keys()
                        }
                        step_args.update(fill_missing_args)

                        name = Template(template_str).substitute(step_args)

                        st.write(name)
                        st.write("")

                        for step_param in DEFAULT_PARAMS.get(func_name, []):
                            param_info = PARAMS_MAP[func_name][step_param]
                            form_key = f"form_{task_idx}_{step_idx}_{step_param}"

                            with st.form(form_key, enter_to_submit=False):
                                col1, col2 = st.columns([9, 1])

                                with col1:
                                    new_value = render_modified_task_box(
                                        request_id=request_id,
                                        param_info=param_info,
                                        all_columns=ALL_COLUMNS,
                                        step_idx=step_idx,
                                        step=step,
                                        step_param=step_param,
                                        task_idx=task_idx,
                                    )
                                    save_success_msg = st.empty()

                                with col2:
                                    if st.form_submit_button("💾", help="Save changes"):
                                        st.session_state.modified_tasks[request_id][
                                            task_idx
                                        ]["steps"][step_idx][step_param] = new_value
                                        save_success_msg.success("changes saved.")
                                        time.sleep(1)
                                        st.rerun()

                        with st.expander("Advanced Parameters"):
                            for step_param, param_value in step.items():
                                if (
                                    step_param in DEFAULT_PARAMS.get(func_name, [])
                                    or step_param == "function"
                                ):
                                    continue

                                param_info = PARAMS_MAP[func_name][step_param]
                                form_key = f"form_{task_idx}_{step_idx}_{step_param}"

                                with st.form(form_key, enter_to_submit=False):
                                    col1, col2 = st.columns([10, 1])
                                    with col1:
                                        new_value = render_modified_task_box(
                                            request_id=request_id,
                                            param_info=param_info,
                                            all_columns=ALL_COLUMNS,
                                            step_idx=step_idx,
                                            step=step,
                                            step_param=step_param,
                                            task_idx=task_idx,
                                        )
                                        save_success_msg = st.empty()

                                    with col2:
                                        if st.form_submit_button(
                                            "💾", help="Save changes"
                                        ):
                                            st.session_state.modified_tasks[request_id][
                                                task_idx
                                            ]["steps"][step_idx][step_param] = new_value
                                            save_success_msg.success("Changes saved.")
                                            time.sleep(1)
                                            st.rerun()


with task_overview_tab:
    st.subheader("Tasks overview")
    st.write("\n")

    new_dataset_task_req = None

    use_new_dataset_check = st.checkbox("Use new dataset")

    if use_new_dataset_check:
        new_dataset_task_req = st.file_uploader(
            "Select new dataset", accept_multiple_files=False, type=["csv"]
        )
        st.write("---")

    for task_idx, task in enumerate(st.session_state.modified_tasks[request_id]):
        if task_idx % cols_per_row == 0:
            st.write("")
            current_cols = st.columns(cols_per_row)

        id_ = task["task_id"]
        task_name = task["name"]
        score = task["score"]

        with current_cols[task_idx % cols_per_row]:
            expander_title = f"{task_idx + 1} - {task_name}"
            with st.expander(expander_title, expanded=False):
                st.write(f"**Description:** {task['description']}")
                st.markdown("**Steps:**")

                for step_num, step in enumerate(task.get("steps", [])):
                    render_task_step(step_num, step)

    if st.checkbox("Check tasks in raw JSON format", key="check_modified_tasks_json"):
        st.code(json.dumps(st.session_state.modified_tasks[request_id], indent=4))

    st.markdown("---")

    send_result_to_email = st.checkbox("Send result to email", value=True)

    if st.button("Process tasks"):
        data_tasks = {
            "common_tasks": st.session_state.modified_tasks[request_id],
            "common_column_cleaning_or_transformation": [],
            "common_column_combination": [],
        }

        if not use_new_dataset_check:
            res = send_tasks_to_process(data_tasks, request_id, send_result_to_email)
        else:
            res = send_tasks_to_process_w_new_dataset(
                new_dataset_task_req, data_tasks, request_id, send_result_to_email
            )

        if res:
            st.success("Tasks are being processed")
            time.sleep(1)
            st.rerun()


with task_result_tab:
    st.button("Refresh result")
    st.write("")

    res_modified_tasks = get_modified_tasks_by_id(request_id)

    if not res_modified_tasks:
        st.error(
            "you need to run the customized tasks first or the result is still being processed"
        )
        st.stop()

    modified_tasks_w_result = res_modified_tasks["res"]["common_tasks_w_result"]
    modified_tasks_w_result = json.loads(modified_tasks_w_result)["tasks"]

    modified_tasks_plots = res_modified_tasks["plot_result"]

    for task_idx, task in enumerate(modified_tasks_w_result):
        render_original_task_expander(
            request_id,
            task,
            task_idx,
            modified_tasks_plots,
            task_mode="customized_tasks",
        )

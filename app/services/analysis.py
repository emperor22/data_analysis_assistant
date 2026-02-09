from fpdf import FPDF

from app.schemas import DataTasks, RunInfo, TaskProcessingRunType
from app.crud import TaskRunTableOperation
from app.config import Config, PARAMS_MAP
from app.logger import logger

from app.services.dataset import get_dataset_snippet, save_dataset_req_id
from app.services.utils import get_current_time_utc


from app.data_transform_utils import (
    groupby_func,
    filter_func,
    get_proportion_func,
    get_column_statistics_func,
    get_top_or_bottom_n_entries_func,
    resample_data_func,
    apply_map_range_func,
    apply_map_func,
    apply_date_op_func,
    apply_math_op_func,
    get_column_combination_func,
    get_column_properties,
    col_transform_and_combination_parse_helper,
    handle_datetime_columns_serialization,
    determine_result_output_type,
)
from typing import Literal

from glob import glob

import pandas as pd
import numpy as np
import json
import os
import zipfile

import matplotlib.pyplot as plt
import seaborn as sns
from string import Template


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return round(float(obj), 3)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


class ReportPDF(FPDF):
    def header(self):
        self.set_font("helvetica", "B", 15)
        self.cell(
            0,
            10,
            "Data Analysis Report",
            border=False,
            new_x="LMARGIN",
            new_y="NEXT",
            align="C",
        )
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font("helvetica", "I", 8)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    def chapter_title(self, task_id):
        self.set_font("helvetica", "B", 12)
        self.set_fill_color(200, 220, 255)
        self.cell(
            0, 10, f"Task Result: {task_id}", new_x="LMARGIN", new_y="NEXT", fill=True
        )
        self.ln(5)

    def chapter_body_text(self, text):
        self.set_font("helvetica", "", 10)
        self.multi_cell(0, 5, text)
        self.ln()


class DataAnalysisProcessor:
    def __init__(
        self,
        data_tasks: DataTasks,
        run_info: RunInfo,
        task_run_table_ops: TaskRunTableOperation,
        run_type: str,
    ):
        self.run_type = run_type

        self.request_id = run_info.request_id
        self.user_id = run_info.user_id
        self.send_result_to_email = run_info.send_result_to_email
        self.email = run_info.email
        self.dataset_filename = run_info.filename
        self.run_name = run_info.run_name

        parquet_file = run_info.parquet_file
        self.df: pd.DataFrame = pd.read_parquet(parquet_file)
        self.original_columns = self.df.columns.tolist()

        self.data_tasks = data_tasks

        self.common_tasks_only = False

        if (
            len(data_tasks.common_tasks) > 0
            and len(data_tasks.common_column_cleaning_or_transformation) == 0
            and len(data_tasks.common_column_combination) == 0
        ):
            self.common_tasks_only = True

        self.task_run_table_ops = task_run_table_ops

        self.common_tasks_fn_map = {
            "groupby": groupby_func,
            "filter": filter_func,
            "get_top_or_bottom_N_entries": get_top_or_bottom_n_entries_func,
            "get_proportion": get_proportion_func,
            "get_column_statistics": get_column_statistics_func,
            "resample_data": resample_data_func,
        }

        self.column_transform_fn_map = {
            "map_range": apply_map_range_func,
            "date_op": apply_date_op_func,
            "math_op": apply_math_op_func,
            "map": apply_map_func,
        }

        # only accessed when testing to validate task result written to db
        self.col_info = None
        self.common_tasks_modified = None
        self.col_transform_tasks_status = None
        self.col_combination_tasks_status = None

    def process_all_tasks(self):
        if self.run_type in (
            TaskProcessingRunType.first_run_after_request.value,
            TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value,
        ):
            self._process_column_transforms()
            self._process_column_combinations()

            # useful for when user run customized tasks on original dataset without running col transform or combination operations

            save_dataset_req_id(
                request_id=self.request_id, dataframe=self.df, run_type=self.run_type
            )

        if self.run_type == TaskProcessingRunType.first_run_after_request.value:
            self._process_columns_info()

            final_dataset_snippet = get_dataset_snippet(self.df)
            self.task_run_table_ops.update_final_dataset_snippet_sync(
                request_id=self.request_id, dataset_snippet=final_dataset_snippet
            )

        self._process_common_tasks()
        self._update_task_result_in_db()

        if self.send_result_to_email:
            result_dct = get_result_dct(self.common_tasks_modified)
            generate_report_and_archive(
                request_id=self.request_id,
                run_type=self.run_type,
                result_dct=result_dct,
                run_name=self.run_name,
            )

    def _process_columns_info(self):
        df = self.df
        added_cols = set(df.columns) - set(self.original_columns)

        col_info_llm_resp = {c.name: c.model_dump() for c in self.data_tasks.columns}

        # inserting col info for derived columns
        for col in self.data_tasks.common_column_combination:
            col_info_llm_resp[col.name] = col_transform_and_combination_parse_helper(
                col.model_dump(), True
            )
        for col in self.data_tasks.common_column_cleaning_or_transformation:
            col_info_llm_resp[col.name] = col_transform_and_combination_parse_helper(
                col.model_dump(), False
            )

        col_info_lst = []

        for col in df.columns:
            col_info_dct = {
                "name": col,
                "source": "original" if col not in added_cols else "added",
                "inferred_info_prompt_res": col_info_llm_resp.get(col, {}),
            }

            is_numeric_col = col in df.select_dtypes(include=[np.number])
            is_datetime_col = col in df.select_dtypes(include=[np.datetime64])
            props = get_column_properties(
                df[col], is_numeric=is_numeric_col, is_datetime=is_datetime_col
            )

            col_info_dct.update(props)
            col_info_lst.append(col_info_dct)

        self.col_info = {"columns_info": col_info_lst}
        self.task_run_table_ops.update_columns_info_sync(
            request_id=self.request_id,
            columns_info=json.dumps(self.col_info, cls=NpEncoder),
        )

    def _process_common_tasks(self):
        common_tasks = self.data_tasks.common_tasks
        common_tasks_modified = []
        for task in common_tasks:
            task_modified = task.model_dump()

            steps = task.steps
            tmp = self.df.copy()
            error_steps = []
            for step_num, step in enumerate(steps):
                try:
                    func = self.common_tasks_fn_map[step["function"]]
                    kwargs = {i: j for i, j in step.items() if i != "function"}
                    tmp: pd.DataFrame = func(tmp, **kwargs)

                    if tmp.empty:
                        raise Exception("empty result dataset")

                except Exception as e:
                    error_steps.append(f"failed at step {step_num + 1}: {str(e)}")
                    break

            if error_steps:
                task_modified["status"] = "; ".join(error_steps)
                task_modified["result"] = {}

                logger.warning(
                    f"some task from common_tasks failed: run_type {self.run_type}, request_id {self.request_id}, task_id {task.task_id}, errors ({'; '.join(error_steps)})"
                )
            elif (
                len(tmp) > Config.MAX_DATASET_ROW_RESULT
                or len(tmp.columns) > Config.MAX_DATASET_COLS_RESULT
            ):
                task_modified["status"] = (
                    "task failed because output is too big to export"
                )
                task_modified["result"] = {}
            else:
                # this should be refactored into a function which processes all task results all at once
                logger.debug(f"processing task {task.task_id}")

                res_type = determine_result_output_type(tmp)

                result_save_handler(
                    df=tmp,
                    run_type=self.run_type,
                    task_id=task.task_id,
                    task_name=task.name,
                    request_id=self.request_id,
                    res_type=res_type,
                )

                if not (
                    len(tmp) > Config.DATASET_ROW_THRESHOLD_BEFORE_EXPORT
                    or len(tmp.columns) > Config.DATASET_COLUMNS_THRESHOLD_BEFORE_EXPORT
                ):
                    tmp = handle_datetime_columns_serialization(tmp)
                    task_modified["status"] = "successful"
                    task_modified["result"] = tmp.to_dict(
                        "list"
                    )  # transform to dictionary for storing in db
                else:
                    task_modified["status"] = (
                        "output of this analysis is too big and exported to excel"
                    )
                    task_modified["result"] = {}

            common_tasks_modified.append(task_modified)

        if self.run_type == TaskProcessingRunType.additional_analyses_request.value:
            common_tasks_modified = self._get_original_tasks_and_merge(
                new_tasks=common_tasks_modified
            )

        self.common_tasks_modified = common_tasks_modified

    def _process_col_transform_and_combination_helper(
        self, tasks, fn_map=None, is_col_combination_task=False
    ):
        tmp = self.df.copy()
        tasks_w_status = []

        for task in tasks:
            task = task.model_dump()
            name = task["name"].replace(" ", "_")
            operation = task["operation"]

            try:
                func = (
                    fn_map[operation["type"]]
                    if not is_col_combination_task
                    else get_column_combination_func
                )
                tmp = func(df=tmp, name=name, operation=operation)

                task["status"] = "successful"
                tasks_w_status.append(task)

            except Exception as e:
                task["status"] = f"failed. error: {e.args}"
                tasks_w_status.append(task)

                run = (
                    "column_combinations"
                    if is_col_combination_task
                    else "column_transforms"
                )
                logger.warning(
                    f"some task from {run} failed: run_type {self.run_type}, request_id {self.request_id}, errors ({str(e)})"
                )

                continue

        self.df = tmp.copy()
        return tasks_w_status

    def _process_column_transforms(self):
        column_transformation_tasks = (
            self.data_tasks.common_column_cleaning_or_transformation
        )

        tasks_w_status = self._process_col_transform_and_combination_helper(
            tasks=column_transformation_tasks,
            fn_map=self.column_transform_fn_map,
            is_col_combination_task=False,
        )

        self.task_run_table_ops.update_column_transform_task_status_sync(
            request_id=self.request_id,
            column_transforms_status=json.dumps({"column_transforms": tasks_w_status}),
        )

        self.col_transform_tasks_status = tasks_w_status

    def _process_column_combinations(self):
        column_combination_tasks = self.data_tasks.common_column_combination

        tasks_w_status = self._process_col_transform_and_combination_helper(
            tasks=column_combination_tasks, is_col_combination_task=True
        )

        self.task_run_table_ops.update_column_combination_task_status_sync(
            request_id=self.request_id,
            column_combinations_status=json.dumps(
                {"column_combinations": tasks_w_status}
            ),
        )

        self.col_combination_tasks_status = tasks_w_status

    def _get_process_result(self):
        result_dct = {
            "col_combination_status": self.col_combination_tasks_status,
            "col_transform_status": self.col_transform_tasks_status,
            "common_tasks_result": self.common_tasks_modified,
            "col_info": self.col_info,
        }
        return result_dct

    def _update_task_result_in_db(self):
        common_task_modified_dct = {"tasks": self.common_tasks_modified}

        if self.run_type == TaskProcessingRunType.first_run_after_request.value:
            self.task_run_table_ops.update_original_common_task_result_sync(
                request_id=self.request_id, tasks=json.dumps(common_task_modified_dct)
            )
        elif self.run_type == TaskProcessingRunType.modified_tasks_execution.value:
            self.task_run_table_ops.update_task_result_sync(
                request_id=self.request_id, tasks=json.dumps(common_task_modified_dct)
            )
        elif (
            self.run_type
            == TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value
        ):
            self.task_run_table_ops.update_task_result_sync(
                request_id=self.request_id, tasks=json.dumps(common_task_modified_dct)
            )
        elif self.run_type == TaskProcessingRunType.additional_analyses_request.value:
            self.task_run_table_ops.update_original_common_task_result_sync(
                request_id=self.request_id, tasks=json.dumps(common_task_modified_dct)
            )

    def _get_original_tasks_and_merge(self, new_tasks):
        tasks_by_id = self.task_run_table_ops.get_task_by_id_sync(
            user_id=self.user_id, request_id=self.request_id
        )
        original_common_tasks = tasks_by_id["original_common_tasks"]
        original_common_tasks = json.loads(original_common_tasks)[
            "tasks"
        ]  # a list of common tasks
        merged_tasks = (
            original_common_tasks + new_tasks
        )  # merge the original tasks and the new one

        return merged_tasks


def get_attachment_zip_filename(request_id, run_name, run_type):
    base_dir = f"{Config.DATASET_SAVE_PATH}/{request_id}"
    output_filename = get_task_category(run_type)

    today_date = get_current_time_utc().strftime("%Y-%m-%d")

    output_zip_path = f"{base_dir}/{run_name}_{output_filename}_{today_date}.zip"

    return output_zip_path


def get_task_category(run_type):
    task_category_dct = {
        TaskProcessingRunType.first_run_after_request.value: "original_tasks",
        TaskProcessingRunType.modified_tasks_execution.value: "customized_tasks",
        TaskProcessingRunType.additional_analyses_request.value: "original_tasks",
        TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value: "customized_tasks",
    }

    return task_category_dct[run_type]


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


def render_task_step(step, step_idx, pdf: ReportPDF):
    template_str = PARAMS_MAP[step["function"]]["template"]
    template = Template(template_str)
    args = {i: process_step_val(j) for i, j in step.items() if i != "function"}

    template_keys = get_template_keys_to_be_substituted(template_str)
    fill_missing_args = {i: "**[]**" for i in template_keys if i not in args.keys()}
    args.update(fill_missing_args)

    val = template.substitute(args)

    return pdf.cell(0, 8, f"{step_idx + 1} - {val}", markdown=True)


def generate_report_and_archive(request_id, result_dct, run_type, run_name):

    base_dir = f"{Config.DATASET_SAVE_PATH}/{request_id}"

    task_cat = get_task_category(run_type)
    artifacts_dir = f"{base_dir}/{task_cat}/artifacts"

    output_zip_path = get_attachment_zip_filename(request_id, run_name, run_type)

    output_pdf_path = f"{base_dir}/{run_name}_{task_cat}_report.pdf"

    task_ids = set(result_dct.keys())

    sorted_task_ids = sorted(list(task_ids))

    pdf = ReportPDF()
    pdf.add_page()

    for task in sorted_task_ids:
        pdf.chapter_title(task)

        plot_path = f"{artifacts_dir}/{task}.png"
        if os.path.exists(plot_path):
            try:
                pdf.image(str(plot_path), w=pdf.epw)
                pdf.ln(5)
            except Exception as e:
                logger.debug(f"would not load image for {task}: {e}")

        if task in result_dct:
            CHAR_THRESH_TASK_DESC = 200
            task_desc = result_dct[task]["description"]

            if len(task_desc) > CHAR_THRESH_TASK_DESC:
                task_desc = task_desc[:CHAR_THRESH_TASK_DESC] + "..."

            pdf.set_font("helvetica", "", 6)
            task_desc = task_desc.encode("latin-1", "replace").decode("latin-1")
            pdf.cell(0, 8, f"Task description: {task_desc}")
            pdf.ln(10)

            pdf.set_font("helvetica", "", 7)
            pdf.cell(0, 8, "Task steps:")
            pdf.ln(10)

            for step_idx, step in enumerate(result_dct[task]["steps"]):
                render_task_step(step, step_idx, pdf)
                pdf.ln(4)

            pdf.ln(10)

            df = pd.DataFrame(result_dct[task]["result"])
            if not df.empty:
                pdf.set_font("helvetica", "B", 10)
                pdf.cell(0, 8, "Data Summary:", new_x="LMARGIN", new_y="NEXT")
                pdf.set_font("helvetica", "", 9)

                with pdf.table() as table:
                    row = table.row()
                    for col_name in df.columns:
                        row.cell(str(col_name).encode("utf-8").decode("latin-1"))

                    for _, data_row in df.iterrows():
                        row = table.row()
                        for item in data_row:
                            row.cell(str(item).encode("utf-8").decode("latin-1"))
                pdf.ln(10)
            else:
                pdf.set_font("helvetica", "", 7)
                pdf.cell(0, 8, "This task has an empty result.")
                pdf.ln(10)

        excel_path = f"{artifacts_dir}/{task}.xlsx"
        if os.path.exists(excel_path):
            pdf.ln(5)
            pdf.set_text_color(200, 50, 50)
            pdf.set_font("helvetica", "I", 10)
            msg = (
                f"The dataset for task '{task}' was too large to display here. "
                f"Please refer to '{task}.xlsx' included in the attached archive."
            )
            pdf.multi_cell(0, 5, msg)
            pdf.set_text_color(0, 0, 0)  # Reset color
            pdf.ln(10)

        pdf.set_draw_color(200, 200, 200)
        pdf.line(pdf.get_x(), pdf.get_y(), pdf.epw, pdf.get_y())
        pdf.ln(10)

    pdf.output(output_pdf_path)

    with zipfile.ZipFile(output_zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(output_pdf_path, arcname=os.path.basename(output_pdf_path))

        for file in glob(f"{artifacts_dir}/*.xlsx"):
            filename = os.path.basename(file)
            filename_without_ext = filename.rstrip(".xlsx")

            if (
                filename_without_ext.isnumeric()
                and int(filename_without_ext) in result_dct
            ):
                zipf.write(file, arcname=filename)


def remove_file_w_extension(dir_, extension):
    for file in glob(f"{dir_}/*.{extension}"):
        os.remove(file)


def result_save_handler(
    df,
    task_id,
    run_type,
    task_name,
    request_id,
    res_type=Literal["BAR_CHART", "LINE_CHART", "DISPLAY_TABLE", "TABLE_EXPORT"],
):
    # this function gets the appropriate column names if the res_type is chart, create the chart and save it
    # if output is too big then it'll save the excel

    def get_bar_chart_cols(df):
        x_axis_col = [i for i in df.columns if "object" in str(df[i].dtype)][0]
        y_axis_col = [i for i in df.columns if i != x_axis_col][0]

        return x_axis_col, y_axis_col

    def get_line_chart_cols(df):
        x_axis_col = [i for i in df.columns if "datetime" in str(df[i].dtype)][0]
        y_axis_col = [i for i in df.columns if i != x_axis_col][0]

        return x_axis_col, y_axis_col

    logger.debug(f"run type is {run_type}")

    # refactor this into dictionary dispatch
    save_path_dct = {
        TaskProcessingRunType.first_run_after_request.value: f"{Config.DATASET_SAVE_PATH}/{request_id}/original_tasks/artifacts",
        TaskProcessingRunType.additional_analyses_request.value: f"{Config.DATASET_SAVE_PATH}/{request_id}/original_tasks/artifacts",
        TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value: f"{Config.DATASET_SAVE_PATH}/{request_id}/customized_tasks/artifacts",
        TaskProcessingRunType.modified_tasks_execution.value: f"{Config.DATASET_SAVE_PATH}/{request_id}/customized_tasks/artifacts",
    }

    save_path = save_path_dct[run_type]

    # this line handles the artifacts dir creation
    if not os.path.exists(save_path):
        os.makedirs(save_path)

    save_path_plot = f"{save_path}/{task_id}.png"
    save_path_table_export = f"{save_path}/{task_id}.xlsx"

    if os.path.exists(save_path_plot):
        os.remove(save_path_plot)

    if os.path.exists(save_path_table_export):
        os.remove(save_path_table_export)

    # refactor these two into their own functions
    try:
        if res_type == "BAR_CHART":
            x_col, y_col = get_bar_chart_cols(df)
            fig, ax = plt.subplots(figsize=(12, 6))
            sns.barplot(df, x=x_col, y=y_col, ax=ax)
            plt.xticks(rotation=45, ha="right")
            plt.title(task_name)
            fig.tight_layout()
            fig.savefig(save_path_plot)
            plt.close(fig)

        elif res_type == "LINE_CHART":
            x_col, y_col = get_line_chart_cols(df)
            fig, ax = plt.subplots(figsize=(12, 6))
            sns.lineplot(df, x=x_col, y=y_col, ax=ax)
            plt.xticks(rotation=45, ha="right")
            plt.title(task_name)
            fig.tight_layout()
            fig.savefig(save_path_plot)
            plt.close(fig)
    except Exception:
        res_type == "DISPLAY_TABLE"

    if res_type == "DISPLAY_TABLE":
        return

    if res_type == "TABLE_EXPORT":
        df.to_excel(save_path_table_export, index=False)


def get_result_dct(common_tasks_dct):
    res = {}
    for t in common_tasks_dct:
        task_id = t["task_id"]
        task_dct = {}
        task_dct["result"] = t["result"]
        task_dct["description"] = t["description"]
        task_dct["steps"] = t["steps"]
        res[task_id] = task_dct

    return res

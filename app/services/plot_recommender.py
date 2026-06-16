# app/services/plot_recommender.py

from typing import Any


VISUAL_FUNCTIONS = {
    "groupby",
    "get_top_or_bottom_N_entries",
    "get_proportion",
    "get_column_statistics",
    "resample_data",
}


def recommend_plot_from_plan(
    analysis_steps: list[dict[str, Any]],
) -> dict[str, Any]:
    if not analysis_steps:
        return {
            "chart_type": "table",
            "x_axis": None,
            "y_axis": None,
            "series": None,
            "reason": "No analysis steps provided.",
        }

    step = get_last_visual_step(analysis_steps)
    function_name = step.get("function")

    if function_name == "groupby":
        return recommend_groupby_plot(step)

    if function_name == "resample_data":
        return recommend_resample_plot(step)

    if function_name == "get_top_or_bottom_N_entries":
        return recommend_top_bottom_plot(step)

    if function_name == "get_proportion":
        return recommend_proportion_plot(step)

    if function_name == "get_column_statistics":
        return recommend_statistics_plot(step)

    return {
        "chart_type": "table",
        "x_axis": None,
        "y_axis": None,
        "series": None,
        "reason": "No visualization-producing step found.",
    }


def get_last_visual_step(analysis_steps: list[dict[str, Any]]) -> dict[str, Any]:
    for step in reversed(analysis_steps):
        if step.get("function") in VISUAL_FUNCTIONS:
            return step

    return analysis_steps[-1]


def recommend_groupby_plot(step: dict[str, Any]) -> dict[str, Any]:
    groupby_columns = ensure_list(step.get("columns_to_group_by"))

    x_axis = groupby_columns[0] if groupby_columns else None
    series = groupby_columns[1] if len(groupby_columns) > 1 else None

    return {
        "chart_type": "bar",
        "x_axis": x_axis,
        "y_axis": "value",
        "series": series,
        "reason": "Groupby result uses the groupby column as x-axis and standardized value column as y-axis.",
    }


def recommend_resample_plot(step: dict[str, Any]) -> dict[str, Any]:
    return {
        "chart_type": "line",
        "x_axis": step.get("date_column"),
        "y_axis": "value",
        "series": None,
        "reason": "Resample result uses the date column as x-axis and standardized value column as y-axis.",
    }


def recommend_top_bottom_plot(step: dict[str, Any]) -> dict[str, Any]:
    return_columns = ensure_list(step.get("return_columns"))
    sort_by_column = step.get("sort_by_column_name")

    # Prefer a returned column that is not the numeric sort column as the label.
    label_columns = [col for col in return_columns if col != sort_by_column]
    x_axis = label_columns[0] if label_columns else None

    return {
        "chart_type": "horizontal_bar",
        "x_axis": x_axis,
        "y_axis": sort_by_column or "value",
        "series": None,
        "reason": "Top/bottom result uses the label column as x-axis and sort column as y-axis.",
    }


def recommend_proportion_plot(step: dict[str, Any]) -> dict[str, Any]:
    column_names = ensure_list(step.get("column_name"))
    x_axis = column_names[0] if column_names else None

    return {
        "chart_type": "bar",
        "x_axis": x_axis,
        "y_axis": "value",
        "series": None,
        "reason": "Proportion result uses the category column as x-axis and standardized value column as y-axis.",
    }


def recommend_statistics_plot(step: dict[str, Any]) -> dict[str, Any]:
    column_names = ensure_list(step.get("column_name"))
    calculations = ensure_list(step.get("calculation"))

    if len(column_names) == 1 and len(calculations) == 1:
        return {
            "chart_type": "metric",
            "x_axis": None,
            "y_axis": "value",
            "series": None,
            "reason": "Single statistic is better shown as a metric.",
        }

    return {
        "chart_type": "bar",
        "x_axis": "statistic",
        "y_axis": "value",
        "series": "column_name" if len(column_names) > 1 else None,
        "reason": "Statistics result uses statistic as x-axis and standardized value column as y-axis.",
    }


def ensure_list(value: Any) -> list[Any]:
    if value is None:
        return []

    if isinstance(value, list):
        return value

    return [value]

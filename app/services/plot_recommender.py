from app.core.logger import logger
from typing import Literal

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from pandas.api.types import (
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
    is_string_dtype,
)


ChartType = Literal[
    "bar",
    "horizontal_bar",
    "line",
    "metric",
    "table",
    "table_export",
]


VISUAL_FUNCTIONS = {
    "groupby",
    "get_top_or_bottom_N_entries",
    "get_proportion",
    "get_column_statistics",
    "resample_data",
}


def resolve_plot_recommendation(
    df,
    analysis_steps,
):
    plan_recommendation = recommend_plot_from_plan(
        analysis_steps=analysis_steps,
    )

    if recommendation_is_valid(df, plan_recommendation):
        return plan_recommendation

    logger.warning(
        f"chart recommendation was invalid for dataframe columns. recommendation={plan_recommendation}, columns={df.columns.tolist()}",
    )

    fallback_recommendation = recommend_plot_from_dataframe(df)

    if recommendation_is_valid(df, fallback_recommendation):
        return fallback_recommendation

    return {
        "chart_type": "table",
        "x_axis": None,
        "y_axis": None,
        "series": None,
        "reason": "No valid chart mapping could be determined.",
    }


def recommend_plot_from_plan(
    analysis_steps,
):
    if not analysis_steps:
        return {
            "chart_type": "table",
            "x_axis": None,
            "y_axis": None,
            "series": None,
            "reason": "No analysis steps were provided.",
        }

    step = get_last_visual_step(analysis_steps)
    function_name = step.get("function")

    recommenders = {
        "groupby": recommend_groupby_plot,
        "resample_data": recommend_resample_plot,
        "get_top_or_bottom_N_entries": recommend_top_bottom_plot,
        "get_proportion": recommend_proportion_plot,
        "get_column_statistics": recommend_statistics_plot,
    }

    recommender = recommenders.get(function_name)

    if recommender is None:
        return {
            "chart_type": "table",
            "x_axis": None,
            "y_axis": None,
            "series": None,
            "reason": (f"No plot recommendation is registered for {function_name!r}."),
        }

    return recommender(step)


def get_last_visual_step(analysis_steps):
    for step in reversed(analysis_steps):
        if step.get("function") in VISUAL_FUNCTIONS:
            return step

    return analysis_steps[-1]


def recommend_groupby_plot(
    step,
):
    groupby_columns = ensure_list(step.get("columns_to_group_by"))

    x_axis = groupby_columns[0] if groupby_columns else None

    # only render grouped bar chart if there's only one extra column
    series = groupby_columns[1] if len(groupby_columns) == 2 else None

    if len(groupby_columns) > 2:
        return {
            "chart_type": "table",
            "x_axis": None,
            "y_axis": None,
            "series": None,
            "reason": "This result must be renderred as table because it has multiple columns as series",
        }

    y_axis = ensure_list(step.get("columns_to_aggregate"))

    return {
        "chart_type": "bar",
        "x_axis": x_axis,
        "y_axis": y_axis[0] if y_axis else None,
        "series": series,
        "reason": "Grouped values are best compared with a bar chart.",
    }


def recommend_resample_plot(
    step,
):
    static_group_columns = ensure_list(step.get("static_group_cols"))

    series = static_group_columns[0] if len(static_group_columns) == 1 else None

    if len(static_group_columns) > 1:
        return {
            "chart_type": "table",
            "x_axis": None,
            "y_axis": None,
            "series": None,
            "reason": "This result must be renderred as table because it has multiple columns as series",
        }

    y_axis = ensure_list(step.get("columns_to_aggregate"))

    return {
        "chart_type": "line",
        "x_axis": step.get("date_column"),
        "y_axis": y_axis[0] if y_axis else None,
        "series": series,
        "reason": "Resampled values represent an ordered time series.",
    }


# to do: make x axis value a list of columns instead of single column
# and concat those columns as x axis in the create_matplotlib_chart
def recommend_top_bottom_plot(
    step,
):
    return_columns = ensure_list(step.get("return_columns"))
    sort_by_column = step.get("sort_by_column_name")

    label_columns = [column for column in return_columns if column != sort_by_column]

    x_axis = label_columns[0] if label_columns else None

    ascending = True if step.get("order") and step.get("order") == "bottom" else False

    return {
        "chart_type": "horizontal_bar",
        "x_axis": x_axis,
        "y_axis": sort_by_column,
        "series": None,
        "sort_ascending": ascending,
        "reason": ("A horizontal bar chart is appropriate for ranked results."),
    }


def recommend_proportion_plot(
    step,
):
    column_names = ensure_list(step.get("column_name"))

    return {
        "chart_type": "bar",
        "x_axis": column_names[0] if column_names else None,
        "y_axis": "proportion",
        "series": None,
        "value_format": "percentage",
        "reason": "A bar chart clearly compares category proportions.",
    }


def recommend_statistics_plot(
    step,
):
    column_names = ensure_list(step.get("column_name"))
    calculations = ensure_list(step.get("calculation"))

    if len(column_names) == 1 and len(calculations) == 1:
        return {
            "chart_type": "metric",
            "x_axis": None,
            "y_axis": column_names[0],
            "series": None,
            "metric_label": (f"{calculations[0]} of {column_names[0]}"),
            "reason": ("A single statistic is best shown as a metric."),
        }

    return {
        "chart_type": "bar",
        "x_axis": "index",
        "y_axis": column_names[0] if column_names else None,
        "series": None,
        "reason": ("A bar chart compares multiple calculated statistics."),
    }


def recommend_plot_from_dataframe(
    df: pd.DataFrame,
):
    if df.empty:
        return {
            "chart_type": "table",
            "x_axis": None,
            "y_axis": None,
            "series": None,
            "reason": "The result dataframe is empty.",
        }

    datetime_columns = [
        column for column in df.columns if is_datetime64_any_dtype(df[column])
    ]

    numeric_columns = [column for column in df.columns if is_numeric_dtype(df[column])]

    category_columns = [
        column
        for column in df.columns
        if (
            is_object_dtype(df[column])
            or is_string_dtype(df[column])
            or isinstance(
                df[column].dtype,
                pd.CategoricalDtype,
            )
        )
    ]

    if datetime_columns and numeric_columns:
        return {
            "chart_type": "line",
            "x_axis": datetime_columns[0],
            "y_axis": numeric_columns[0],
            "series": (category_columns[0] if category_columns else None),
            "reason": ("Fallback: found datetime and numeric columns."),
        }

    if category_columns and numeric_columns:
        return {
            "chart_type": "bar",
            "x_axis": category_columns[0],
            "y_axis": numeric_columns[0],
            "series": None,
            "reason": ("Fallback: found categorical and numeric columns."),
        }

    if len(df) == 1 and numeric_columns:
        return {
            "chart_type": "metric",
            "x_axis": None,
            "y_axis": numeric_columns[0],
            "series": None,
            "metric_label": humanize_column_name(numeric_columns[0]),
            "reason": ("Fallback: found a single-row numeric result."),
        }

    return {
        "chart_type": "table",
        "x_axis": None,
        "y_axis": None,
        "series": None,
        "reason": (
            "Fallback dataframe inspection did not identify a reliable chart mapping."
        ),
    }


def recommendation_is_valid(
    df: pd.DataFrame,
    recommendation,
):
    chart_type = recommendation.get("chart_type")

    if chart_type in {"table", "table_export"}:
        return True

    if chart_type not in {
        "bar",
        "horizontal_bar",
        "line",
        "metric",
    }:
        return False

    required_columns = []

    if chart_type == "metric":
        required_columns.append(recommendation.get("y_axis"))
    else:
        required_columns.extend(
            [
                recommendation.get("x_axis"),
                recommendation.get("y_axis"),
            ]
        )

    series = recommendation.get("series")

    if series is not None:
        required_columns.append(series)

    return all(
        column is not None and column in df.columns for column in required_columns
    )


def make_unique_axis_labels(
    df: pd.DataFrame,
    x_col,
):
    labels = df[x_col].astype("string")

    occurrence = labels.groupby(labels, dropna=False).cumcount() + 1
    frequency = labels.groupby(labels, dropna=False).transform("size")

    return labels.where(
        frequency == 1,
        labels + "_" + occurrence.astype(str),
    )


def create_matplotlib_chart(
    df: pd.DataFrame,
    recommendation,
    title,
):
    df = df.copy()

    chart_type = recommendation["chart_type"]
    x_axis = recommendation.get("x_axis")
    y_axis = recommendation.get("y_axis")
    series = recommendation.get("series")

    fig, ax = create_styled_figure()

    if chart_type == "bar":
        sns.barplot(
            data=df,
            x=x_axis,
            y=y_axis,
            hue=series,
            ax=ax,
            errorbar=None,
        )

        format_vertical_bar_chart(
            ax=ax,
            category_count=df[x_axis].nunique(),
        )

    elif chart_type == "horizontal_bar":
        sort_ascending = recommendation.get(
            "sort_ascending",
            False,
        )

        plot_df = df.sort_values(
            by=y_axis,
            ascending=sort_ascending,
        )
        plot_df["_x_label"] = make_unique_axis_labels(plot_df, x_col=x_axis)

        sns.barplot(
            data=plot_df,
            x=y_axis,
            y="_x_label",
            hue=series,
            ax=ax,
            errorbar=None,
            order=plot_df["_x_label"].tolist(),
        )

        ax.grid(
            axis="x",
            alpha=0.2,
        )
        ax.grid(
            axis="y",
            visible=False,
        )

    elif chart_type == "line":
        plot_df = df.sort_values(x_axis)

        sns.lineplot(
            data=plot_df,
            x=x_axis,
            y=y_axis,
            hue=series,
            marker="o",
            ax=ax,
        )

        format_line_chart(ax)

    elif chart_type == "metric":
        render_metric(
            ax=ax,
            df=df,
            value_column=y_axis,
            label=recommendation.get(
                "metric_label",
                humanize_column_name(y_axis),
            ),
        )

    else:
        plt.close(fig)
        raise ValueError(f"Unsupported chart type: {chart_type!r}")

    ax.set_title(
        title,
        loc="left",
        pad=18,
    )

    if chart_type != "metric":
        ax.set_xlabel(
            humanize_column_name(y_axis if chart_type == "horizontal_bar" else x_axis)
        )
        ax.set_ylabel(
            humanize_column_name(x_axis if chart_type == "horizontal_bar" else y_axis)
        )

        format_numeric_axis(
            ax=ax,
            chart_type=chart_type,
            value_format=recommendation.get("value_format"),
        )

        clean_legend(ax)

    fig.tight_layout()

    return fig


def create_styled_figure():
    sns.set_theme(
        context="notebook",
        style="whitegrid",
    )

    fig, ax = plt.subplots(
        figsize=(10, 5),
        constrained_layout=False,
    )

    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    ax.grid(
        axis="y",
        alpha=0.2,
        linewidth=0.8,
    )
    ax.grid(
        axis="x",
        visible=False,
    )
    ax.set_axisbelow(True)

    return fig, ax


def format_vertical_bar_chart(
    ax,
    category_count,
):
    rotation = 45 if category_count > 5 else 0

    ax.tick_params(
        axis="x",
        rotation=rotation,
    )

    if rotation:
        for label in ax.get_xticklabels():
            label.set_horizontalalignment("right")


def format_line_chart(ax):
    ax.tick_params(
        axis="x",
        rotation=45,
    )

    for label in ax.get_xticklabels():
        label.set_horizontalalignment("right")


def render_metric(
    ax,
    df: pd.DataFrame,
    value_column,
    label,
):
    values = df[value_column].dropna()

    if values.empty:
        formatted_value = "No value"
    else:
        formatted_value = format_metric_value(values.iloc[0])

    ax.text(
        0.5,
        0.58,
        formatted_value,
        transform=ax.transAxes,
        ha="center",
        va="center",
        fontsize=38,
        fontweight="bold",
    )

    ax.text(
        0.5,
        0.38,
        label,
        transform=ax.transAxes,
        ha="center",
        va="center",
        fontsize=14,
    )

    ax.axis("off")


def format_numeric_axis(
    ax,
    chart_type,
    value_format,
):
    from matplotlib.ticker import (
        FuncFormatter,
        PercentFormatter,
    )

    if chart_type == "horizontal_bar":
        axis = ax.xaxis
    else:
        axis = ax.yaxis

    if value_format == "percentage":
        axis.set_major_formatter(PercentFormatter(xmax=1))
        return

    axis.set_major_formatter(
        FuncFormatter(lambda value, _: format_compact_number(value))
    )


def clean_legend(ax):
    legend = ax.get_legend()

    if legend is None:
        return

    legend.set_title(humanize_column_name(legend.get_title().get_text()))
    legend.set_frame_on(False)


def save_figure(
    fig,
    output_path,
):
    try:
        fig.savefig(
            output_path,
            format="png",
            dpi=180,
            bbox_inches="tight",
            facecolor="white",
        )
    finally:
        plt.close(fig)


def save_dataframe_export(
    df: pd.DataFrame,
    export_path,
):
    df.to_excel(
        export_path,
        index=False,
    )


def ensure_list(value):
    if value is None:
        return []

    if isinstance(value, list):
        return value

    if isinstance(value, tuple):
        return list(value)

    return [value]


def humanize_column_name(
    column,
):
    if column is None:
        return ""

    return str(column).replace("_", " ").strip().title()


def format_metric_value(value):
    if isinstance(value, float):
        return f"{value:,.2f}"

    if isinstance(value, int):
        return f"{value:,}"

    return str(value)


def format_compact_number(
    value,
):
    absolute_value = abs(value)

    if absolute_value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.1f}B"

    if absolute_value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"

    if absolute_value >= 1_000:
        return f"{value / 1_000:.1f}K"

    if float(value).is_integer():
        return f"{int(value):,}"

    return f"{value:,.2f}"

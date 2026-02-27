import streamlit as st
import pandas as pd
import json

from io import StringIO

from utils import (
    get_col_info_by_id,
    get_dataset_snippet_by_id,
    render_request_ids,
    render_col_info,
)

st.markdown(
    """
<style>
[data-testid="stMetricValue"] {
    font-size: 22px;
}
</style>
""",
    unsafe_allow_html=True,
)

task_id = render_request_ids()


col_infos = get_col_info_by_id(task_id=task_id)
col_infos = json.loads(col_infos["columns_info"])
col_infos = col_infos["columns_info"]

data_snippet = get_dataset_snippet_by_id(task_id=task_id)
data_snippet = data_snippet["final_dataset_snippet"]

st.subheader("Dataset Snippet")
st.write(pd.read_csv(StringIO(data_snippet)))
st.write("")

st.subheader("Columns overview")
st.write("")

search_col = None
if st.checkbox("Search column"):
    col_names = [i["name"] for i in col_infos]
    search_col = st.selectbox("Enter column name", options=[""] + col_names)

    if search_col:
        col_infos = [i for i in col_infos if i["name"] == search_col]

st.write("")

for col_info in col_infos:
    render_col_info(col_info)

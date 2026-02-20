from streamlit_autorefresh import st_autorefresh

from utils import render_progress_table

st_autorefresh(interval=5000)


render_progress_table()

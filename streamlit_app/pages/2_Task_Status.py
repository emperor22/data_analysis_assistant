from streamlit_autorefresh import st_autorefresh

from utils import render_progress_table

st_autorefresh(interval=2000)


render_progress_table()

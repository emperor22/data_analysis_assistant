import streamlit as st
from streamlit_local_storage import LocalStorage

ls = LocalStorage()

with st.form('set value'):
    key = st.text_input('key')
    val = st.text_input('value')
    
    if st.form_submit_button('set value'):
        ls.setItem(key, val)

if st.button('set dict'):
    dct = ['satu', 'dua']
    ls.setItem('lst_1', dct)

key_retrieve = st.text_input('Get value')
if key_retrieve:
    data = ls.getItem(key_retrieve)
    st.write(data)


# from streamlit_autorefresh import st_autorefresh
# from datetime import datetime

# st_autorefresh(interval=2000)

# st.write(str(datetime.now()))
import streamlit as st
from utils import submit_login_request

st.set_page_config(layout="centered")

if not 'authenticated' in st.session_state:
    st.session_state['authenticated'] = False

if not st.session_state.authenticated:
    st.markdown('**Login**')
    login_form = st.form('login_form', enter_to_submit=True)
    
    with login_form:
        username = st.text_input('Username', max_chars=10)
        password = st.text_input('Password', type='password')
        
        login_btn = st.form_submit_button('Submit')
        
        if login_btn:
            login_data = submit_login_request(username, password)
            if login_data:
                st.session_state['access_token'] = login_data['access_token']
                st.session_state['authenticated'] = True
                st.rerun()
            else:
                st.error('Invalid username/password')
else:
    st.write('You are logged in.')
        
        
    


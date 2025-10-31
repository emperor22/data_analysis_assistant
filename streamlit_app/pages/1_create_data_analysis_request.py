import streamlit as st
import pandas as pd
from utils import make_analysis_request, make_additional_analyses_request, render_task_ids
import time
import re

def split_and_validate_new_prompt(new_analysis_text):
    
    def validate_value(s):
        min_char = 15
        max_char = 100
        return min_char <= len(s) <= max_char 
    
    regex = r'^[a-zA-Z0-9 \n\r]*$'
    
    if not bool(re.fullmatch(regex, new_analysis_text)):
        return
            
    values = [i.strip() for i in new_analysis_text.split('\n')]
    all_values_valid = all([validate_value(val) for val in values])
    
    if len(values) > 5 or not all_values_valid:
        return
    
    return new_analysis_text
        



first_req_tab, additional_req_tab = st.tabs(['Create data analysis request', 'Request aditional analyses'])

with first_req_tab:
    analysis_req_form = st.form('analysis_req_form', enter_to_submit=False)

    with analysis_req_form:
        file = st.file_uploader('Select your dataset file', accept_multiple_files=False, type=['csv'])
        model = st.selectbox('Select model', ['gemini-2.5-flash', 'gemma-3-27b-it', 'gemini-2.5-flash-lite'])
        task_count = st.selectbox('Select number of analyses to output', [10, 20])
        
        if st.form_submit_button('Create Analysis Request'):
            if file and model and task_count:
                res = make_analysis_request(file, model, task_count)
                
                if res:
                    st.success('request task processed')
                    time.sleep(1)
                    st.rerun()
            else:
                st.error('Please upload the file and choose the parameters')
                time.sleep(1)
                st.rerun()
                


with additional_req_tab:
    task_id = render_task_ids()
    new_analysis_text = st.text_area('Type in the analyses you want here.', help='max 5 additional analyses; max character for each task is 60 characters, min character is 15; \
                                                                                  can only contain alphanumeric characters; separate each task by new line/enter')
    model = 'gemini-2.5-flash'
    
    new_analysis_text_val = split_and_validate_new_prompt(new_analysis_text=new_analysis_text)
    
    if st.button('Submit'):
        if len(new_analysis_text) > 0 and new_analysis_text_val:
            res = make_additional_analyses_request(model=model, new_tasks_prompt=new_analysis_text, request_id=task_id)
            
            if not res:
                st.error('you can only run additional analyses request once.')
            
        else:
            st.error('please make sure you follow all the requirements')
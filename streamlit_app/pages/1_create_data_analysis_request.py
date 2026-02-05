import streamlit as st
import pandas as pd
from utils import make_analysis_request, make_additional_analyses_request, render_request_ids, split_and_validate_new_prompt, setup_api_key
import time
import re


        
LLM_MODEL_LIST_GOOGLE = ['gemini-2.5-flash', 'gemma-3-27b-it', 'gemini-2.5-flash-lite']
LLM_MODEL_LIST_CEREBRAS = ['gpt-oss-120b', 'qwen-3-235b-a22b-instruct-2507', 'zai-glm-4.7']
LLM_PROVIDERS = ['google', 'cerebras']

DEFAULT_TASK_COUNT = 15

model_list_google = [f'google:{i}' for i in LLM_MODEL_LIST_GOOGLE]
model_list_cerebras = [f'cerebras:{i}' for i in LLM_MODEL_LIST_CEREBRAS]

model_list = [*model_list_cerebras, *model_list_google]



first_req_tab, additional_req_tab, api_key_setup_tab = st.tabs(['Create data analysis request', 'Request aditional analyses', 'Setup API Key'])

with first_req_tab:
    analysis_req_form = st.form('analysis_req_form', enter_to_submit=False)

    with analysis_req_form:
        name = st.text_input('Enter name for your run', max_chars=50)
        file = st.file_uploader('Select your dataset file', accept_multiple_files=False, type=['csv'])
        model = st.selectbox('Select model', model_list)
        
        send_result_to_email = st.checkbox('Send result to email', value=True)
        
        task_count = DEFAULT_TASK_COUNT
        
        if st.form_submit_button('Create Analysis Request'):
            if name and file and model and task_count:
                res = make_analysis_request(name=name, uploaded_file=file, task_count=task_count, model=model, send_result_to_email=send_result_to_email)
                
                if res:
                    st.success('request task processed')
                    time.sleep(1)
                    st.rerun()
            else:
                st.error('Please enter name, upload the dataset file, and choose the parameters')
                time.sleep(1)
                st.rerun()
                
with api_key_setup_tab:
    st.warning('This key will be encrypted and saved to your profile and will only be used for your requests. If rate limited, the app will resort back to using the global API key.')


    
    api_key_setup_form = st.form('api_key_setup_form', enter_to_submit=False)
    
    with api_key_setup_form:
        api_provider = st.selectbox('Select provider', LLM_PROVIDERS)
        api_key = st.text_input('API key')
        st.caption('Leaving API key empty will delete your stored key (if any)')
        if st.form_submit_button('Add API key'):
            res = setup_api_key(api_provider, api_key)
            
            if res:
                if 'deleted' in res['detail']:
                    st.success('API key delete successful')
                    time.sleep(1)
                    st.rerun()
                else:
                    st.success('API key add successful')
                    time.sleep(1)
                    st.rerun()

with additional_req_tab:
    task_id = render_request_ids()
    
    new_analysis_text = st.text_area('Type in the analyses you want here.', help='max 5 additional analyses; max character for each task is 60 characters, min character is 15; \
                                                                                  can only contain alphanumeric characters; separate each task by new line/enter')
    model_2 = st.selectbox('Select model', model_list, key='model_2')
    
    new_analysis_text_val = split_and_validate_new_prompt(new_analysis_text=new_analysis_text)
    
    if st.button('Submit'):
        if len(new_analysis_text) > 0 and new_analysis_text_val:
            res = make_additional_analyses_request(model=model_2, new_tasks_prompt=new_analysis_text, request_id=task_id)
            
            if not res:
                st.error('you can only run additional analyses request once.')
            
        else:
            st.error('please make sure you follow all the requirements')
            

    
    
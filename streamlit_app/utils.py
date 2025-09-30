import streamlit as st
import functools
import requests
import time
import pandas as pd
import json

URL = 'http://localhost:8000'

def submit_login_request(username, password):
    body = {'username': username, 'password': password}
    url = f'{URL}/token'
    res = requests.post(url, data=body)
    
    if res.status_code == 401:
        return None
    
    return res.json()

def show_unauthorized_error_and_redirect_to_login():
    st.session_state['authenticated'] = False
    st.session_state['access_token'] = None
    st.error('session expired. please log in again.')
    time.sleep(1)
    st.switch_page('home.py')

# a decorator that intercepts the 'headers' argument and insert access token
def include_auth_header(func):
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        access_token = st.session_state.get('access_token')
        auth_header = {'Authorization': f'Bearer {access_token}'}
        if not access_token:
            show_unauthorized_error_and_redirect_to_login()
        
        if 'headers' in kwargs:
            kwargs['headers'].update(auth_header)
        else:
            kwargs['headers'] = auth_header
        
        try:
            res = func(*args, **kwargs)

            if res.status_code == 401:
                show_unauthorized_error_and_redirect_to_login()

            elif res.status_code == 200:
                return res.json()
            
            else:
                raise Exception(f'request returned an error: status code: {res.status_code}')
                
        except Exception as e:
            st.error(f'An error occurred during the request: {e}')
        
    return wrapper
    
    
@include_auth_header
def get_task_by_id(task_id, headers=None):
    url = f'{URL}/get_task_by_id/{task_id}'
    
    res = requests.get(url, headers=headers)
    
    return res

@include_auth_header
def send_tasks_to_process(data_tasks, task_id, headers=None):
    url = f'{URL}/execute_analyses?req_id={task_id}'
    
    res = requests.post(url, data=json.dumps(data_tasks), headers=headers)
    
    return res
    

def render_modified_task_box(param_info, all_columns, step_idx, step, step_param, task_idx):
    '''
    Arguments:
    param_info -> from PARAMS_MAP, obtained from function_name. to get alias, widget type, and options for widget
    all_columns -> columns of the dataset. include newly created columns from llm response
    step_idx -> get the step index on each task for widget key
    step -> the individual step to get necessary info
    step_param -> step's argument, to get the current value
    task_idx -> get the step index for widget key and to modify the final tasks dict
    '''
    
    widget_type = param_info['type']
    
    value = step[step_param]
    
    if widget_type != 'multiselect':
        value = value[0] if isinstance(value, list) and len(value) > 0 else value
        
    
    if step['function'] == 'filter' and step_param == 'values':
        num_ops = ['>', '<', '>=', '<=', '==', '!=']
        
        num_or_text = 'numerical' if step['operator'] in num_ops else 'text'
        widget_type = param_info['type'][num_or_text]
        
        if step['operator'] == 'between':
            raise Exception('not implemented yet')

    if widget_type == 'selectbox':
        options = param_info.get('options', all_columns)
        if step['function'] == 'filter' and step_param == 'operator':
            num_ops = ['>', '<', '>=', '<=', '==', '!=']
            options = num_ops if step['operator'] in num_ops else ['in']
            
        index = options.index(value)
        
        selected_value = st.selectbox(
            label=param_info['alias'],
            options=options,
            index=index,
            key=f'task_{task_idx}_step_{step_idx}_param_{step_param}'
        )
        
        return [selected_value] if isinstance(step[step_param], list) else selected_value # uses step[step_param] to get original value format
            
    elif widget_type == 'multiselect':
        selected_value = st.multiselect(
            label=param_info['alias'],
            options=param_info.get('options', all_columns),
            default=value,
            key=f'task_{task_idx}_step_{step_idx}_param_{step_param}'
        )

        return selected_value

    elif widget_type == 'number_input':
        new_value = st.number_input(
            label=param_info['alias'],
            value=value,
            key=f'task_{task_idx}_step_{step_idx}_param_{step_param}'
        )
        
        return [new_value] if isinstance(step[step_param], list) else new_value
        
    elif widget_type == 'radio':
        new_value = st.radio(
            label=param_info['alias'],
            options=param_info['options'],
            index=param_info['options'].index(value),
            key=f'task_{task_idx}_step_{step_idx}_param_{step_param}'
        )
        
        return [new_value] if isinstance(step[step_param], list) else new_value
    
    elif widget_type == 'text_input':
        st.markdown('---')
        new_value = st.text_input(
            label=param_info['alias'], 
            key=f'task_{task_idx}_step_{step_idx}_param_{step_param}', 
            value=value
        )
        st.warning('Please insert valid values from your selected column.')
        st.warning('If multiple values, separate them with semicolon (;)')
        st.markdown('---')
        
        return [val.strip() for val in new_value.split(';')]
            
    
def render_original_task_expander(task) :   
    task_status = task['status']
    status_in_label = f' ({task_status.split()[0].upper()})' if task_status.startswith('failed') else ''
    expander_label = f"{task['task_id']} - {task['name']}{status_in_label}"
    
    with st.expander(expander_label):
        st.write(f"**Status**: {task_status}")
        st.write(f"**Description**: {task['description']}")
        st.write(f"**Score**: {task['score']}")
        st.write('\n')
        st.write('**Steps**')
        st.code(json.dumps({'steps': task['steps']}, indent=4))
        st.write('\n')
        
        if task_status == 'successful':
            st.write('**Result**')
            st.write(pd.DataFrame(task['result']))
                
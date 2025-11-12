import streamlit as st
import functools
import requests
import time
import pandas as pd
import json
from copy import deepcopy
from string import Template, Formatter

# import nltk
# nltk.download('punkt')
# nltk.download('averaged_perceptron_tagger_eng')

# URL = 'https://nginx/api'
URL = 'http://localhost:8000'

def register_user(username, first_name, last_name, email):
    body = {'username': username, 'first_name': first_name, 'last_name': last_name, 'email': email}
    url = f'{URL}/register_user'
    res = requests.post(url, verify=False, json=body)
    
    if res.status_code == 409:
        return 'username/email already exists'
    
    return 'success'

def submit_login_request(username, otp):
    body = {'username': username, 'otp': otp}
    url = f'{URL}/login'
    res = requests.post(url, verify=False, json=body)
    
    if res.status_code == 401:
        return None
    try:
        return res.json()
    except:
        st.write(res.text)

def get_otp(username):
    url = f'{URL}/get_otp'
    data = {'username': username}
    res = requests.post(url, json=data, verify=False)
    
    if res.status_code == 401:
        return 'invalid username'
    
    if res.status_code == 429:
        return 'too many otp requests'
    
    if res.status_code != 200:
        return 'internal error'
    
    return 'success'

def show_unauthorized_error_and_redirect_to_login():
    st.session_state['authenticated'] = False
    st.session_state['access_token'] = None
    st.error('session expired. please log in again.')
    time.sleep(1)
    st.switch_page('home.py')
    
def remove_duplicate_tasks(tasks):
    seen_steps = set()
    unique_list = []

    for item in tasks:
        item = deepcopy(item)
        steps_value = item['steps']

        if steps_value not in seen_steps:
            unique_list.append(item)
            seen_steps.add(steps_value)

    return unique_list

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
            
            if res.status_code == 422:
                try:
                    error_details = res.json()
                    print(error_details)
                    return
                except requests.exceptions.JSONDecodeError: 
                    print(res.text)
                    return

            if res.status_code == 401:
                show_unauthorized_error_and_redirect_to_login()
                
            if res.status_code == 404:
                return None
            
            if res.status_code == 400:
                return None

            if res.status_code == 200:
                return res.json()
            
            raise Exception(f'request returned an error: status code: {res.status_code}')
                
        except Exception as e:
            st.error(f'An error occurred during the request: {e}')
        
    return wrapper
    
    
@include_auth_header
def get_original_tasks_by_id(task_id, headers=None):
    url = f'{URL}/get_original_tasks_by_id/{task_id}'
    
    res = requests.get(url, verify=False,  headers=headers)
    
    return res

@include_auth_header
def get_modified_tasks_by_id(task_id, headers=None):
    url = f'{URL}/get_modified_tasks_by_id/{task_id}'
    
    res = requests.get(url, verify=False,  headers=headers)
    
    return res

def is_task_still_processing(status):
    processing_status = ('GETTING INITIAL REQUEST PROMPT RESULT', 'RUNNING INITIAL ANALYSES TASKS', 
                         'INITIAL REQUEST PROMPT RESULT RECEIVED')
    
    return status in processing_status

@st.cache_data
@include_auth_header
def get_task_ids_by_user(headers=None):
    url = f'{URL}/get_request_ids'
    
    res = requests.get(url, verify=False,  headers=headers) # result is [task_id, filename, status]
    
    return res

def render_task_ids():
    task_ids = get_task_ids_by_user()
    col1, col2 = st.columns([12, 1])
    
    with col2:
        if st.button('⟳'):
            get_task_ids_by_user.clear()
            st.rerun()
    with col1:
        if not task_ids:
            st.error('cannot find request ids')
            st.stop()
            
        task_ids = [i[0] for i in task_ids['request_ids'] if not is_task_still_processing(i[2])] # get first value which is the task id
        
        task_ids_select = st.selectbox('Select Task ID', options=[''] + task_ids, key='task_id_select')

        
        if not task_ids_select:
            st.stop()
    

    
    return task_ids_select
    

@include_auth_header
@st.cache_data
def get_col_info_by_id(task_id, headers=None):
    url = f'{URL}/get_col_info_by_id/{task_id}'
    
    res = requests.get(url, verify=False,  headers=headers)
    
    return res

@include_auth_header
@st.cache_data
def get_dataset_snippet_by_id(task_id, headers=None):
    url = f'{URL}/get_dataset_snippet_by_id/{task_id}'
    
    res = requests.get(url, verify=False,  headers=headers)
    
    return res


@include_auth_header
def send_tasks_to_process(data_tasks, task_id, headers=None):
    url = f'{URL}/execute_analyses'
    
    data_tasks['request_id'] = task_id
    
    res = requests.post(url, verify=False,  data=json.dumps(data_tasks), headers=headers)
    
    return res

@include_auth_header
def make_analysis_request(uploaded_file, model, task_count, headers=None):
    url = f'{URL}/upload_dataset'
    file = {'file': (uploaded_file.name, uploaded_file.getvalue())}

    data = {'model': model, 'analysis_task_count': str(task_count)}

    res = requests.post(url, verify=False, files=file, headers=headers, data=data)
    
    return res

@include_auth_header
def make_additional_analyses_request(model, new_tasks_prompt, request_id, headers=None):
    url = f'{URL}/make_additional_analyses_request'

    data = {'model': model, 'new_tasks_prompt': new_tasks_prompt, 'request_id': request_id}

    res = requests.post(url, verify=False, headers=headers, data=json.dumps(data))
    
    return res

def is_numerical(s):
    try:
        _ = float(s)
        return True
    except ValueError:
        return False

def render_modified_task_box(task_id, param_info, all_columns, step_idx, step, step_param, task_idx):
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
    
    value = step.get(step_param, [''])

    if widget_type != 'multiselect':
        value = value[0] if isinstance(value, list) and len(value) > 0 else value
        
    
    if step['function'] == 'filter' and step_param == 'values':
        num_ops = ['>', '<', '>=', '<=', '==', '!=']
        
        num_or_text = 'numerical' if step['operator'] in num_ops and is_numerical(value) else 'text'
        widget_type = param_info['type'][num_or_text]
        
        if step['operator'] == 'between':
            raise Exception('not implemented yet')

    if widget_type == 'selectbox':
        options = param_info.get('options', all_columns)
        if step['function'] == 'filter' and step_param == 'operator':
            num_ops = ['>', '<', '>=', '<=', '==', '!=']
            filter_value = step['values'][0] if isinstance(step['values'], list) else step['values']
            
            options = num_ops if step['operator'] in num_ops and is_numerical(filter_value) else ['in']
            value = value if step['operator'] in num_ops and is_numerical(filter_value) else 'in'
            
            # this line forces replacing the operator with 'in' in case the operator is == with single string value
            st.session_state.modified_tasks[task_id][task_idx]['steps'][step_idx][step_param] = value
            
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
        new_value = st.text_input(
            label=param_info['alias'], 
            key=f'task_{task_idx}_step_{step_idx}_param_{step_param}', 
            value=value,
        )
        st.warning('Please insert valid values from your selected column.')
        st.warning('If multiple values, separate them with semicolon (;)')
        
        return [val.strip() for val in new_value.split(';')]
            
    
def render_original_task_expander(task, task_idx) :   
    task_status = task['status']
    status_in_label = f' ({task_status.split()[0].upper()})' if task_status.startswith('failed') else ''
    expander_label = f"{task_idx+1} - {task['name']}{status_in_label}"
    
    with st.expander(expander_label):
        st.write(f"**Status**: {task_status}")
        st.write(f"**Description**: {task['description']}")
        st.write(f"**Score**: {task['score']}")
        
        st.write('---')
        st.write('**Steps:**')
        
        for step_idx, step in enumerate(task['steps']):
            render_task_step(step_idx, step)
        
        st.write('---')
        
        if task_status == 'successful':
            st.write('**Result**')
            st.write(pd.DataFrame(task['result']))

def process_step_val(val):
    if val is None:
        return '()'
    if isinstance(val, list):
        if len(val) > 1:
            val = [f"**{v}**" for v in val]
            val = ', '.join(val)
            val = f'({val})'
            return val
        else:
            val = val[0]
    
    return f'**{val}**'

def get_template_keys_to_be_substituted(s):
    return [i[1] for i in Template(s).pattern.findall(s)  if i[1] is not None]
            
def render_task_step(step_idx, step):
    template_str = PARAMS_MAP[step['function']]['template']
    template = Template(template_str)
    args = {i: process_step_val(j) for i, j in step.items() if i != 'function'}

    template_keys = get_template_keys_to_be_substituted(template_str)
    fill_missing_args = {i: '**[]**' for i in template_keys if i not in args.keys()}
    args.update(fill_missing_args)
    
    val = template.substitute(args)

    val = f'{step_idx+1} - {val}'

    return st.write(val)

def is_valid_sentence_nlp(text):
    if not isinstance(text, str) or not text:
        return False

    sentences = sent_tokenize(text) # type: ignore
    print(sentences)
    if len(sentences) != 1:
        return False

    words = word_tokenize(text) # type: ignore
    tagged_words = pos_tag(words) # type: ignore

    has_verb = any(tag.startswith('VB') for word, tag in tagged_words)

    return has_verb

            
            
PARAMS_MAP = {
    'groupby': {
        'template': 'Group by column(s) $columns_to_group_by and calculate $calculation of column(s) $columns_to_aggregate',
        'columns_to_group_by': {'alias': 'Column(s) to group by', 'type': 'multiselect'},
        'columns_to_aggregate': {'alias': 'Column(s) to aggregate', 'type': 'multiselect'},
        'calculation': {'alias': 'Calculation', 'type': 'multiselect', 'options': ['mean', 'median', 'min', 'max', 'count', 'size', 'sum']}
    },
    
    'filter': {
        'template': 'Filter column $column_name where condition $operator $values',
        'column_name': {'alias': 'Filter column', 'type': 'selectbox'},
        'operator': {'alias': 'Condition', 'type': 'selectbox', 'options': ['>', '<', '>=', '<=', '==', '!=', 'in', 'between']},
        'values': {'alias': 'Filter value(s)', 'type': {'numerical': 'number_input', 'text': 'text_input'}}
    },
    
    'get_top_or_bottom_N_entries': {
        'template': 'Get the $order $number_of_entries entries, sorted by $sort_by_column_name. Return column(s): $return_columns',
        'sort_by_column_name': {'alias': 'Column to sort by', 'type': 'selectbox'},
        'order': {'alias': 'Ordering', 'type': 'radio', 'options': ['top', 'bottom']},
        'number_of_entries': {'alias': 'Number of entries', 'type': 'number_input'},
        'return_columns': {'alias': 'Column(s) included in result', 'type': 'multiselect'},
    },
    
    'get_proportion': {
        'template': 'Calculate the proportion/percentage of value(s) $values in column $column_name',
        'column_name': {'alias': 'Column to get proportion of', 'type': 'selectbox'},
        'values': {'alias': 'Value(s) to get proportion of', 'type': 'text_input'}
    },
    
    'get_column_statistics': {
        'template': 'Calculate the statistic ($calculation) for column $column_name',
        'column_name': {'alias': 'Column to get statistics from', 'type': 'selectbox'},
        'calculation': {'alias': 'Calculation', 'type': 'selectbox', 'options': ['mean', 'median', 'min', 'max', 'count', 'sum']}
    },
}

DEFAULT_PARAMS = {
    'groupby': ['columns_to_group_by', 'columns_to_aggregate'],
    'filter': ['column_name', 'operator', 'values'],
    'get_top_or_bottom_N_entries': ['number_of_entries', 'sort_by_column_name', 'order'],
    'get_proportion': ['column_name', 'values'],
    'get_column_statistics': ['column_name'],
}

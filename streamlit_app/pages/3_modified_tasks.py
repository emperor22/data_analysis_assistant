from utils import send_tasks_to_process, render_modified_task_box
import streamlit as st
import json
from copy import deepcopy
import time

st.set_page_config(layout='wide')
cols_per_row = 3


task_id = 1 # this should be selectable with a selectbox with this format: 'req_id - dataset_name - date'

if 'tasks' not in st.session_state:
    st.error('you dont have any request.')
    st.stop()

if 'selected_tasks_to_modify' not in st.session_state or \
    task_id not in st.session_state.selected_tasks_to_modify or \
    len(st.session_state.selected_tasks_to_modify[task_id]) == 0:
        st.error('you need to import some tasks first.')
        st.stop()
        


# initialize 'imported' tracker if not exists        
if 'imported' not in st.session_state:
    st.session_state.imported = {}

if task_id not in st.session_state.imported:
    st.session_state.imported[task_id] = False

# initialize 'modified_tasks' list if this is the first import        
if 'modified_tasks' not in st.session_state:
    st.session_state.modified_tasks = {}
    
if task_id not in st.session_state.modified_tasks:
    tasks = deepcopy(st.session_state.tasks[task_id])
    
    for task in tasks:
        del task['status']
        del task['result']
        task['score'] = 0
    
    imported_tasks_ids = st.session_state.selected_tasks_to_modify[task_id]
    imported_tasks = [task for task in tasks if task['task_id'] in imported_tasks_ids]
    
    st.session_state.modified_tasks[task_id] = imported_tasks
    st.session_state.imported[task_id] = True


# need to get this info from db for each request_id
ALL_COLUMNS = ['id', 'loan_amnt', 'term', 'int_rate', 'installment', 'home_ownership',
               'annual_inc', 'verification_status', 'issue_d', 'loan_status',
               'purpose', 'total_pymnt', 'debt_to_income_ratio', 'interest_to_principal_ratio', 
               'monthly_income', 'installment_to_income_ratio', 'loan_size_category', 'income_bracket', 
               'risk_category', 'loan_term_category']

PARAMS_MAP = {
    'groupby': {
        'columns_to_group_by': {'alias': 'Column(s) to group by', 'type': 'multiselect'},
        'columns_to_aggregate': {'alias': 'Column(s) to aggregate', 'type': 'multiselect'},
        'calculation': {'alias': 'Calculation', 'type': 'multiselect', 'options': ['mean', 'median', 'min', 'max', 'count', 'size', 'sum']}
    },
    'filter': {
        'column_name': {'alias': 'Filter column', 'type': 'selectbox'},
        'operator': {'alias': 'Condition', 'type': 'selectbox', 'options': ['>', '<', '>=', '<=', '==', '!=', 'in', 'between']},
        'values': {'alias': 'Filter value(s)', 'type': {'numerical': 'number_input', 'text': 'text_input'}}
    },
    'get_top_or_bottom_N_entries': {
        'sort_by_column_name': {'alias': 'Column to sort by', 'type': 'selectbox'},
        'order': {'alias': 'Ordering', 'type': 'radio', 'options': ['top', 'bottom']},
        'number_of_entries': {'alias': 'Number of entries', 'type': 'number_input'},
        'return_columns': {'alias': 'Column(s) included in result', 'type': 'multiselect'},
    },
    'get_proportion': {
        'column_name': {'alias': 'Column to get proportion of', 'type': 'selectbox'},
        'values': {'alias': 'Value(s) to get proportion of', 'type': 'text_input'}
    },
    'get_columns_statistics': {
        'column_name': {'alias': 'Column to get statistics from', 'type': 'selectbox'},
        'calculation': {'alias': 'Calculation', 'type': 'selectbox', 'options': ['mean', 'median', 'min', 'max', 'count', 'sum']}
    },
}

DEFAULT_PARAMS = {
    'groupby': ['columns_to_group_by', 'columns_to_aggregate'],
    'filter': ['column_name', 'operator', 'values'],
    'get_top_or_bottom_N_entries': ['number_of_entries', 'sort_by_column_name', 'order'],
    'get_proportion': ['column_name', 'values'],
    'get_columns_statistics': ['column_name'],
}

task_edit_tab, task_overview_tab = st.tabs(['Customize tasks', 'Task overview'])

with task_edit_tab:
    st.subheader('Customize tasks')
    for task_idx, task in enumerate(st.session_state.modified_tasks[task_id]):

        if task_idx % cols_per_row == 0:
            st.write('')
            current_cols = st.columns(cols_per_row)
        
        with current_cols[task_idx % cols_per_row]:
            with st.container(border=True):
                st.markdown(f"**{task_idx+1}. {task['name']}**")
                st.markdown(f"*{task['description']}*")

                for step_idx, step in enumerate(task['steps']):
                    with st.expander(f"Step {step_idx + 1}: {step['function']}"):
                        func_name = step['function']
                        
                        st.markdown('**Core Parameters**')
                        for step_param in DEFAULT_PARAMS.get(func_name, []):
                            param_info = PARAMS_MAP[func_name][step_param]

                            new_value = render_modified_task_box(param_info=param_info, 
                                                                all_columns=ALL_COLUMNS, 
                                                                step_idx=step_idx, 
                                                                step=step, 
                                                                step_param=step_param, 
                                                                task_idx=task_idx)
                            
                            st.session_state.modified_tasks[task_id][task_idx]['steps'][step_idx][step_param] = new_value

                        with st.expander('Advanced Parameters'):
                            for step_param, param_value in step.items():
                                if step_param in DEFAULT_PARAMS.get(func_name, []) or step_param == 'function':
                                    continue
                                
                                param_info = PARAMS_MAP[func_name][step_param]

                                new_value = render_modified_task_box(param_info=param_info, 
                                                                    all_columns=ALL_COLUMNS, 
                                                                    step_idx=step_idx, 
                                                                    step=step, 
                                                                    step_param=step_param, 
                                                                    task_idx=task_idx)
                                
                                st.session_state.modified_tasks[task_id][task_idx]['steps'][step_idx][step_param] = new_value
            
                if st.button(f'Save changes', use_container_width=True, key=f'run_task_{task_idx}'):
                    st.success(f'Changes successfully saved')
                    time.sleep(2)
                    st.rerun()
                
with task_overview_tab:
    st.subheader('Tasks overview')
    st.write('\n')


    for task_idx, task in enumerate(st.session_state.modified_tasks[task_id]):
        if task_idx % cols_per_row == 0:
            st.write('')
            current_cols = st.columns(cols_per_row)
        
        id_ = task['task_id']
        task_name = task['name']
        score = task['score']
        with current_cols[task_idx % cols_per_row]:
            expander_title = f"{task_idx+1} - {task_name}"
            with st.expander(expander_title, expanded=False):
                
                st.write(f"**Description:** {task['description']}")
                st.markdown("**Steps:**")
                
                for step_num, step in enumerate(task.get('steps', [])):
                    func_name = step['function']
                    
                    st.markdown(f"**{step_num + 1}. Function:** `{func_name}`")
                    
                    step_args = {k: v for k, v in step.items() if k != 'function'}
                    
                    for arg_key, arg_value in step_args.items():
                        alias = PARAMS_MAP[func_name][arg_key]['alias']
                        
                        if isinstance(arg_value, (list, dict)):
                            value = json.dumps(arg_value).replace('[', '').replace(']', '').replace("'", '').replace('"', '').strip()
                            if value.startswith(','):
                                value = value[1:].lstrip()
                        else:
                            value = str(arg_value)
                            
                        st.markdown(f"*{alias}:* `{value}`")
                        
                    st.write('')

            st.write('')
                
                
    st.markdown('---')
    with st.expander('See raw JSON'):
        st.code(json.dumps({'modified_tasks': st.session_state.modified_tasks[task_id]}, indent=4))
    
    if st.button('Process tasks'):
        data_tasks = {'common_tasks': st.session_state.modified_tasks[task_id], 
                    'common_column_cleaning_or_transformation': [], 
                    'common_column_combination': []}
    
        res = send_tasks_to_process(data_tasks, task_id)
        st.write(res)
        
    
        
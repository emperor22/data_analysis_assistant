from utils import (send_tasks_to_process, render_modified_task_box, render_task_step, process_step_val, 
                   PARAMS_MAP, DEFAULT_PARAMS, get_col_info_by_id, get_template_keys_to_be_substituted, 
                   render_task_ids, get_modified_tasks_by_id)
import streamlit as st
import json
from copy import deepcopy
import time
from string import Template
from random import randint

cols_per_row = 1
max_task_count = 30


task_id = render_task_ids()

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

# initialize 'modified_tasks' list if this is the first task import        
if 'modified_tasks' not in st.session_state:
    st.session_state.modified_tasks = {}
    
if task_id not in st.session_state.modified_tasks:
    tasks = deepcopy(st.session_state.tasks[task_id])
    
    for task in tasks:
        del task['status']
        del task['result']
        task['score'] = 'inapplicable'
    
    imported_tasks_ids = st.session_state.selected_tasks_to_modify[task_id]
    imported_tasks = [task for task in tasks if task['task_id'] in imported_tasks_ids]
    
    st.session_state.modified_tasks[task_id] = imported_tasks
    st.session_state.imported[task_id] = True
    
    
# initialize 'modified_tasks_w_result' in session_state
if 'modified_tasks_w_result' not in st.session_state:
    st.session.modified_tasks_w_result = {}

if task_id not in st.session_state.modified_tasks_w_result:
    st.session_state.modified_tasks_w_result[task_id] = None
    
    


# need to get this info from db for each request_id
dataset_cols = get_col_info_by_id(task_id)
dataset_cols = json.loads(dataset_cols['columns_info'])
ALL_COLUMNS = [col['name'] for col in dataset_cols['columns_info']]


task_edit_tab, task_overview_tab, task_result_tab = st.tabs(['Customize tasks', 'Task overview'])

with task_edit_tab:
    st.subheader('Customize tasks')
    for task_idx, task in enumerate(st.session_state.modified_tasks[task_id]):

        if task_idx % cols_per_row == 0:
            st.write('')
            current_cols = st.columns(cols_per_row)
        
        with current_cols[task_idx % cols_per_row]:
            with st.container(border=True):
                
                col1, col2, col3, col4 = st.columns([10, 1, 1, 1])
                
                with col1:
                    if st.checkbox('Edit name/description', key=f'edit_name_desc_{task_idx}'):
                        with st.form(f'{task_idx}_name_desc_change', enter_to_submit=False):
                            
                            task_name_change = st.text_input('Task name')
                            task_desc_change = st.text_input('Task description')
                            
                            if st.form_submit_button('💾', help='Save changes'):
                                st.session_state.modified_tasks[task_id][task_idx]['name'] = task_name_change
                                st.session_state.modified_tasks[task_id][task_idx]['description'] = task_desc_change
                    
                    del_dup_msg = st.empty()
                    
                with col3:
                    if st.button('🗑️', key=f'del_task_{task_idx}', type='primary', help='Delete task'):
                        id_ = task['task_id']
                        new_tasks = [task for task in st.session_state.modified_tasks[task_id] if task['task_id'] != id_]
                        st.session_state.modified_tasks[task_id] = new_tasks
                        
                        del_dup_msg.success('task deleted!')
                        time.sleep(1)
                        st.rerun()
                        
                with col4:
                    if st.button('⿻', key=f'duplicate_task_{task_idx}', type='primary', help='Duplicate task'):
                        if len(st.session_state.modified_tasks[task_id]) < max_task_count:
                            del_dup_msg.error(f'total tasks cannot be more than {max_task_count}')
                        
                        id_ = task['task_id']
                        dup_task = next(task for task in st.session_state.modified_tasks[task_id] if task['task_id'] == id_)
                        dup_task = deepcopy(dup_task)
                        dup_task['task_id'] = randint(100, 9999)
                        dup_task['name'] = f"{dup_task['name']} (copy)"
                        
                        st.session_state.modified_tasks[task_id].append(dup_task)
                        
                        del_dup_msg.success('task duplicated! please scroll to the bottom to check it out.')
                        time.sleep(2)
                        st.rerun()
                    
                        
                                
                st.markdown(f"**{task_idx+1} - {task['name']}**")
                st.caption(f"*{task['description']}*")

                for step_idx, step in enumerate(task['steps']):
                    func_name = step['function']
                    expander_name = f'Step {step_idx+1}: {func_name}'
                    
                    with st.expander(expander_name):
                        template_str = PARAMS_MAP[step['function']]['template']
                        
                        step_args = {i: process_step_val(j) for i, j in step.items() if i != 'function'}

                        template_keys = get_template_keys_to_be_substituted(template_str)
                        fill_missing_args = {i: '**[]**' for i in template_keys if i not in step_args.keys()}
                        step_args.update(fill_missing_args)
                        
                        name = Template(template_str).substitute(step_args)
                        
                        st.write(name)
                        st.write('')

                        for step_param in DEFAULT_PARAMS.get(func_name, []):
                            param_info = PARAMS_MAP[func_name][step_param]
                            form_key = f"form_{task_idx}_{step_idx}_{step_param}" 
                            
                            with st.form(form_key, enter_to_submit=False):
                                col1, col2 = st.columns([9, 1])
                                
                                with col1:
                                    new_value = render_modified_task_box(task_id=task_id,
                                                                        param_info=param_info, 
                                                                        all_columns=ALL_COLUMNS, 
                                                                        step_idx=step_idx, 
                                                                        step=step, 
                                                                        step_param=step_param, 
                                                                        task_idx=task_idx)
                                    save_success_msg = st.empty()
                                    
                                with col2:
                                    if st.form_submit_button('💾', help='Save changes'):
                                        st.session_state.modified_tasks[task_id][task_idx]['steps'][step_idx][step_param] = new_value
                                        save_success_msg.success('changes saved.')
                                        time.sleep(1)
                                        st.rerun()

                        with st.expander('Advanced Parameters'):
                            for step_param, param_value in step.items():
                                if step_param in DEFAULT_PARAMS.get(func_name, []) or step_param == 'function':
                                    continue
                                
                                param_info = PARAMS_MAP[func_name][step_param]
                                form_key = f"form_{task_idx}_{step_idx}_{step_param}" 

                                with st.form(form_key, enter_to_submit=False):
                                    col1, col2 = st.columns([10, 1])
                                    with col1:
                                        new_value = render_modified_task_box(task_id=task_id,
                                                                            param_info=param_info, 
                                                                            all_columns=ALL_COLUMNS, 
                                                                            step_idx=step_idx, 
                                                                            step=step, 
                                                                            step_param=step_param, 
                                                                            task_idx=task_idx)
                                        save_success_msg = st.empty()

                                    with col2:
                                        if st.form_submit_button('💾', help='Save changes'):
                                            st.session_state.modified_tasks[task_id][task_idx]['steps'][step_idx][step_param] = new_value
                                            save_success_msg.success('Changes saved.')
                                            time.sleep(1)
                                            st.rerun()
                    

                
with task_overview_tab:
    st.subheader('Tasks overview')
    st.write('\n')
    
    if st.checkbox('Use new dataset'):
        new_dataset_task_req = st.file_uploader('Select new dataset', type=['csv'])
        st.write('---')


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
                    render_task_step(step_num, step)
                
                
    st.markdown('---')
    with st.expander('See raw JSON'):
        st.code(json.dumps({'modified_tasks': st.session_state.modified_tasks[task_id]}, indent=4))
    
    if st.button('Process tasks'):
        data_tasks = {'common_tasks': st.session_state.modified_tasks[task_id], 
                    'common_column_cleaning_or_transformation': [], 
                    'common_column_combination': []}
    
        res = send_tasks_to_process(data_tasks, task_id)
        st.write(res)
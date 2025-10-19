from utils import get_task_by_id, render_original_task_expander, render_task_ids
import streamlit as st
import json
import pandas as pd

cols_per_row = 1

task_id = render_task_ids()

score_threshold = ['low', 'medium', 'high']
if st.sidebar.checkbox('Filter score'):
    score_threshold = st.sidebar.multiselect('Select scores', ['low', 'medium', 'high'], default=['low', 'medium', 'high'])
    
show_failed_task = st.sidebar.checkbox('Show failed tasks')



# initialize 'imported' tracker if not exists        
if 'imported' not in st.session_state:
    st.session_state.imported = {}

if task_id not in st.session_state.imported:
    st.session_state.imported[task_id] = False
    
if st.session_state.imported[task_id]:
    if st.button('Reset task import'):
        st.session_state.imported[task_id] = False
        st.session_state.selected_tasks_to_modify[task_id] = []
        del st.session_state.modified_tasks
        st.rerun()
    
    st.error("you've already done an import")
    st.stop()

if 'tasks' not in st.session_state:
    st.session_state.tasks = {}
    
if task_id not in st.session_state.tasks:
    tasks = get_task_by_id(task_id)
    tasks = tasks['original_common_tasks']
    tasks = json.loads(tasks)['original_common_tasks']
    st.session_state.tasks[task_id] = tasks

if 'selected_tasks_to_modify' not in st.session_state:
    st.session_state.selected_tasks_to_modify = {}
    
if not task_id in st.session_state.selected_tasks_to_modify:
    st.session_state.selected_tasks_to_modify[task_id] = []


all_task_ids = [task['task_id'] for task in st.session_state.tasks[task_id] if task['score'] in score_threshold]


if st.session_state.selected_tasks_to_modify[task_id] != sorted(all_task_ids):
    if st.button('Import all tasks'):
        st.session_state.selected_tasks_to_modify[task_id] = all_task_ids
        st.rerun()
else:
    if st.button('Un-import all tasks'):
        st.session_state.selected_tasks_to_modify[task_id] = []
        st.rerun()

st.write('')
if st.button('Refresh task lists', help='This button is useful for when you run additional analyses and the new tasks have not appeared'):
    del st.session_state.tasks[task_id]
    st.rerun()

st.write('')
for task in st.session_state.tasks[task_id]:
    
    if task['task_id'] not in all_task_ids:
        continue
    
    col1, col2 = st.columns([3, 1])
    id_ = task['task_id']
    
    with col1:
        render_original_task_expander(task)
    
    with col2:
        if not id_ in st.session_state.selected_tasks_to_modify[task_id]:
            if st.button('Import task', key=f"import_task_{id_}"):
                st.session_state.selected_tasks_to_modify[task_id].append(id_)
                st.rerun()
            
        else:
            if st.button('Un-import task', key=f"remove_task_{id_}"):
                st.session_state.selected_tasks_to_modify[task_id].remove(id_)
                st.rerun()


                
            
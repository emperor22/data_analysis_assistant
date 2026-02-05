from utils import get_original_tasks_by_id, render_original_task_expander, render_request_ids
import streamlit as st
import json


cols_per_row = 1

request_id = render_request_ids()

score_threshold = ['low', 'medium', 'high']
if st.sidebar.checkbox('Filter score'):
    score_threshold = st.sidebar.multiselect('Select scores', ['low', 'medium', 'high'], default=['low', 'medium', 'high'])
    
show_failed_task = st.sidebar.checkbox('Show failed tasks')


if 'tasks' not in st.session_state:
    st.session_state.tasks = {}
    st.session_state.tasks_plots = {}

if request_id not in st.session_state.tasks:
    res = get_original_tasks_by_id(request_id)
    
    if res:
        tasks = res['res']['original_common_tasks']
        tasks = json.loads(tasks)['tasks']
        st.session_state.tasks[request_id] = tasks
        
        plots = res['plot_result']
        st.session_state.tasks_plots[request_id] = plots


if not st.session_state.tasks.get(request_id):
    st.stop()

if 'selected_tasks_to_modify' not in st.session_state:
    st.session_state.selected_tasks_to_modify = {}
    
if not request_id in st.session_state.selected_tasks_to_modify:
    st.session_state.selected_tasks_to_modify[request_id] = []
    

if 'modified_tasks' in st.session_state and request_id in st.session_state.modified_tasks:
    col1_rst, col2_rst = st.columns([8, 2])
    
    with col1_rst:
        st.warning("You already have imported tasks. If you want to do the import again in this page, you need to press the \
                    'Reset import' button for the changes to be reflected in the 'Modified Tasks' page.")
    
    with col2_rst:
        if st.button('Reset import'):
            del st.session_state.modified_tasks[request_id]
            st.rerun()


all_task_ids = [task['task_id'] for task in st.session_state.tasks[request_id] if task['score'] in score_threshold]


if st.session_state.selected_tasks_to_modify[request_id] != sorted(all_task_ids):
    if st.button('Import all tasks'):
        st.session_state.selected_tasks_to_modify[request_id] = all_task_ids
        st.rerun()
else:
    if st.button('Un-import all tasks'):
        st.session_state.selected_tasks_to_modify[request_id] = []
        st.rerun()

st.write('')
if st.button('Refresh task lists', help='This button is useful for when you run additional analyses and the new tasks have not appeared'):
    del st.session_state.tasks[request_id]
    st.rerun()

st.write('')
for task_idx, task in enumerate(st.session_state.tasks[request_id]):
    
    if task['task_id'] not in all_task_ids:
        continue
    
    col1, col2 = st.columns([3, 1])
    id_ = task['task_id']
    
    with col1:
        plots_dct = st.session_state.tasks_plots[request_id]
        render_original_task_expander(request_id, task, task_idx, plots_dct, task_mode='original_tasks')
    
    with col2:
        if not id_ in st.session_state.selected_tasks_to_modify[request_id]:
            if st.button('Import task', key=f"import_task_{id_}"):
                st.session_state.selected_tasks_to_modify[request_id].append(id_)
                st.rerun()
            
        else:
            if st.button('Un-import task', key=f"remove_task_{id_}"):
                st.session_state.selected_tasks_to_modify[request_id].remove(id_)
                st.rerun()

        


                
            
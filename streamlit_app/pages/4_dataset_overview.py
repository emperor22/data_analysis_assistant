import streamlit as st
import pandas as pd
import json

from io import StringIO

from utils import get_col_info_by_id, get_dataset_snippet_by_id, render_request_ids

st.markdown(
    """
<style>
[data-testid="stMetricValue"] {
    font-size: 22px;
}
</style>
""",
    unsafe_allow_html=True,
)

task_id = render_request_ids()


col_info = get_col_info_by_id(task_id=task_id)
col_info = json.loads(col_info['columns_info'])
col_info = col_info['columns_info']


data_snippet = get_dataset_snippet_by_id(task_id=task_id)
data_snippet = data_snippet['final_dataset_snippet']

st.subheader('Dataset Snippet')
st.write(pd.read_csv(StringIO(data_snippet)))
st.write('')

st.subheader('Columns overview')
st.write('')



search_col = None
if st.checkbox('Search column'):
    col_names = [i['name'] for i in col_info]
    search_col = st.selectbox('Enter column name', options=[''] + col_names)
    
    if search_col:
        col_info = [i for i in col_info if i['name'] == search_col]

st.write('')
for col_info in col_info:
    col_name = col_info['name']
    source = col_info['source']
    source_expander_suffix = '' if source == 'original' else f' | **DERIVED**'
    
    with st.expander(f"{col_name}{source_expander_suffix}", expanded=False):
        
        inf_res = col_info['inferred_info_prompt_res']
        
        if source == 'original':
            st.subheader("Inferred Column Information")
            # Display basic inferred info in columns for readability
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Classification", inf_res.get('classification', 'N/A'))
                st.metric("Data Type", inf_res.get('data_type', 'N/A'))
            
            with col2:
                st.metric("Type", inf_res.get('type', 'N/A'))
                st.metric("Confidence", inf_res.get('confidence_score', 'N/A').title())
            
            with col3:
                unit = inf_res.get('unit', '')
                st.metric("Unit", unit if unit else "None")
        
        if 'operation' in inf_res:
            st.write('')
            st.write('Description:')
            st.write(inf_res.get('description', ''))
            st.write('Formula:')
            st.code(inf_res.get('formula', ''), language='text')
        
        with st.expander("Detailed Statistics", expanded=False):
            props = col_info['type_dependent_properties']
            datatype = props['datatype']
            is_categorical = props['is_categorical']
            
            # Common properties
            col_a, col_b, col_c = st.columns(3)
            
            with col_a:
                st.metric("Missing Values", f"{col_info['missing_count']:,} ({col_info['missing_value_ratio']:.1%})",)
            
            with col_b:
                st.metric("Unique Values", f"{col_info['unique_count']:,} ({col_info['uniqueness_ratio']:.1%})")
            
            with col_c:
                is_cat = "Yes" if props.get('is_categorical', False) else "No"
                st.metric("Categorical?", is_cat)
            
            if datatype == 'numerical':
                st.subheader("Numerical Properties")
                
                col1, col2, col3 = st.columns(3)

                with col1:
                    st.metric("Min", f"{props['min_value']:.2f}")
                    st.metric("Max", f"{props['max_value']:.2f}")
                
                with col2:
                    st.metric("Mean", f"{props['mean_value']:.2f}")
                    st.metric("Median", f"{props['median_value']:.2f}")
                
                with col3:
                    st.metric("Std Dev", f"{props['std']:.2f}")
                    skew = props['skewness']
                    st.metric("Skewness", f"{skew:.2f}")
                
                # Quartiles
                q25, q75 = props['q_25th'], props['q_75th']
                st.write(f"**IQR:** {q25:.2f} - {q75:.2f}")
            
            elif datatype == 'string':
                st.subheader("String Properties")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.metric("Max Length", props['max_length'])
                
                with col2:
                    st.metric("Avg Length", f"{props['mean_length']:.1f}")
                    
            elif datatype == 'datetime':
                st.subheader('Datetime Properties')
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric('Min. date', props['date_min'])
                
                with col2:
                    st.metric('Max. date', props['date_max'])
                    
                with col3:
                    st.metric('Date range (days)', props['range_days'])
                    
            common_data = []
            for val, freq in props['most_common_5_values'].items():
                common_data.append({
                    'Value': f"{val}",
                    'Frequency (%)': f"{freq:.1%}"
                })
            st.subheader("Top 5 Most Common Values")
            st.table(common_data)
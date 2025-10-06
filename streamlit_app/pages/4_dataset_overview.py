import streamlit as st
import pandas as pd

columns_data  =[
        {
            "name": "id",
            "classification": "Identifier",
            "confidence_score": "high",
            "data_type": "integer",
            "type": "Numerical",
            "unit": "",
            "expected_values": [
                "87023",
                "1077501",
                "1077430",
                "1077175",
                "1076863"
            ]
        },
        {
            "name": "loan_amnt",
            "classification": "Metric",
            "confidence_score": "high",
            "data_type": "integer",
            "type": "Numerical",
            "unit": "USD",
            "expected_values": [
                "2400-15000"
            ]
        },
        {
            "name": "term",
            "classification": "Dimensional",
            "confidence_score": "high",
            "data_type": "integer",
            "type": "Categorical",
            "unit": "months",
            "expected_values": [
                "36",
                "60"
            ]
        },
        {
            "name": "int_rate",
            "classification": "Metric",
            "confidence_score": "high",
            "data_type": "float",
            "type": "Numerical",
            "unit": "percent",
            "expected_values": [
                "7.51-15.96"
            ]
        },
        {
            "name": "installment",
            "classification": "Metric",
            "confidence_score": "high",
            "data_type": "float",
            "type": "Numerical",
            "unit": "USD",
            "expected_values": [
                "59.83-368.45"
            ]
        },
        {
            "name": "home_ownership",
            "classification": "Dimensional",
            "confidence_score": "high",
            "data_type": "string",
            "type": "Categorical",
            "unit": "",
            "expected_values": [
                "rent",
                "mortgage",
                "own",
                "other",
                "none"
            ]
        },
        {
            "name": "annual_inc",
            "classification": "Metric",
            "confidence_score": "high",
            "data_type": "float",
            "type": "Numerical",
            "unit": "USD",
            "expected_values": [
                "12252.0-80000.0"
            ]
        },
        {
            "name": "verification_status",
            "classification": "Dimensional",
            "confidence_score": "high",
            "data_type": "string",
            "type": "Categorical",
            "unit": "",
            "expected_values": [
                "not verified",
                "verified",
                "source verified"
            ]
        },
        {
            "name": "issue_d",
            "classification": "Temporal",
            "confidence_score": "medium",
            "data_type": "integer",
            "type": "Numerical",
            "unit": "months",
            "expected_values": [
                "-11",
                "-10",
                "-9",
                "-8",
                "-7"
            ]
        },
        {
            "name": "loan_status",
            "classification": "Dimensional",
            "confidence_score": "high",
            "data_type": "string",
            "type": "Categorical",
            "unit": "",
            "expected_values": [
                "fully paid",
                "charged off",
                "current"
            ]
        },
        {
            "name": "purpose",
            "classification": "Dimensional",
            "confidence_score": "high",
            "data_type": "string",
            "type": "Categorical",
            "unit": "",
            "expected_values": [
                "debt_consolidation",
                "credit_card",
                "other",
                "home_improvement",
                "major_purchase"
            ]
        },
        {
            "name": "total_pymnt",
            "classification": "Metric",
            "confidence_score": "high",
            "data_type": "float",
            "type": "Numerical",
            "unit": "USD",
            "expected_values": [
                "0.0-12231.89"
            ]
        }
    ]


df = pd.DataFrame(columns_data)

# Simplify 'expected_values' for compact display
def format_expected_values(values):
    if not values:
        return "N/A"
    if len(values) == 1 and '-' in str(values[0]):
        return f"Range: {values[0]}"
    if len(values) <= 3:
        return ", ".join(values)
    return f"{len(values)} distinct values (e.g., {values[0]}, {values[1]}, ...)"

df['Expected Values'] = df['expected_values'].apply(format_expected_values)
df['Unit'] = df['unit'].apply(lambda x: x if x else 'N/A')

# Rename and reorder columns for clarity
df = df.rename(columns={
    'name': 'Column Name',
    'classification': 'Classification',
    'confidence_score': 'Confidence',
    'data_type': 'Data Type',
    'type': 'Type Group'
})

display_df = df[['Column Name', 'Classification', 'Confidence', 'Type Group', 'Data Type', 'Unit', 'Expected Values']].set_index('Column Name')



transposed_df = display_df.T

def style_confidence_row(s):
    # s is a Series, where index is 'Column Name'
    def get_color(val):
        if val == 'high':
            return 'background-color: #d4edda; color: #155724; font-weight: bold; border-radius: 4px; padding: 2px 5px;' # Green
        elif val == 'medium':
            return 'background-color: #fff3cd; color: #856404; font-weight: bold; border-radius: 4px; padding: 2px 5px;' # Yellow
        else:
            return ''

    if s.name == 'Confidence':
        return [get_color(val) for val in s]
    return [''] * len(s)

def style_type_group_row(s):
    def get_color(val):
        if val == 'Numerical':
            return 'background-color: #e0f7fa; color: #006064; font-weight: bold; border-radius: 4px; padding: 2px 5px;' # Cyan/Blue
        elif val == 'Categorical':
            return 'background-color: #ede7f6; color: #4527a0; font-weight: bold; border-radius: 4px; padding: 2px 5px;' # Purple
        else:
            return ''
            
    if s.name == 'Type Group':
        return [get_color(val) for val in s]
    return [''] * len(s)
    
def general_styling_transposed(s):
    return ['text-align: left; vertical-align: middle; white-space: normal;'] * len(s)

styled_transposed_df = transposed_df.style.apply(
    style_confidence_row, axis=1
).apply(
    style_type_group_row, axis=1
).apply(
    general_styling_transposed, axis=1 # Apply general styling row-wise
).set_table_styles([
    {'selector': 'th.col_heading', 'props': [
        ('font-size', '14px'), 
        ('text-align', 'center'), 
        ('background-color', '#f0f2f6')
    ]},
    
    {'selector': 'th.row_heading', 'props': [
        ('font-size', '14px'), 
        ('text-weight', 'bold'),
        ('text-align', 'left'),
        ('background-color', '#f0f2f6')
    ]},
    
    {'selector': 'td', 'props': [('font-size', '13px')]},
])


st.dataframe(styled_transposed_df, use_container_width=True, height=280)
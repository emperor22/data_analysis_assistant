import pandas as pd
import numpy as np

def validate_columns(cols_to_check: str | list, column_list):
    if not cols_to_check:
        return
    
    cols_to_check = [cols_to_check] if isinstance(cols_to_check, str) else cols_to_check
        
    if not all(i in column_list for i in cols_to_check):
        raise Exception('some columns mentioned in task do not exist')

def groupby_func(df: pd.DataFrame, columns_to_group_by: list, columns_to_aggregate: list, calculation: str):
    for cols_to_check in (columns_to_group_by, columns_to_aggregate):
        validate_columns(cols_to_check, df.columns.tolist())
        
    df = df.copy()
    
    if len(calculation) > 1:
        agg_dct = {col: calculation for col in columns_to_aggregate}
        df = df.groupby(columns_to_group_by, as_index=False).agg(agg_dct)
        df.columns = [f'{i[0]}_{i[1]}' for i in df.columns] # merging the two level of column names into one
        
    elif len(calculation) == 1:
        calc = calculation[0]
        df = df.groupby(columns_to_group_by, as_index=False)[columns_to_aggregate].apply(calc)
        
    return df

def filter_func(df: pd.DataFrame, column_name: str | list, operator: str, values: list | str):
    validate_columns(column_name, df.columns.tolist())
    df = df.copy()
    
    column_name = column_name[0] if isinstance(column_name, list) else column_name

    if len(values) == 0:
        raise Exception('need to have filter values')
    
    valid_operators = ['>', '<', '>=', '<=', '==', '=', '<>', '!=', 'in', 'between']
    if operator not in valid_operators:
        raise Exception('invalid filter operator')
    
    if operator == 'in':
        query_string = f"{column_name} {operator} @values"
    elif operator == 'between':
        values = sorted(values)
        min_val, max_val = values
        query_string = f"{column_name} >= @min_val and {column_name} <= @max_val"
    else:
        operator = '==' if operator == '=' else operator # replacing single equal op
        operator = '!=' if operator == '<>' else operator # replacing sql style not equal op
        
        flt = repr(values[0])
        query_string = f"{column_name} {operator} {flt}"
    
    df = df.query(query_string)

    return df
                
def get_top_or_bottom_n_entries_func(df: pd.DataFrame, sort_by_column_name: str | list, order: str, number_of_entries: int, return_columns: list):
    for cols_to_check in sort_by_column_name, return_columns:
        validate_columns(cols_to_check, df.columns.tolist())
    
    df = df.copy()
    
    sort_by_column_name = sort_by_column_name[0] if isinstance(sort_by_column_name, list) else sort_by_column_name

    asc_dct = {'bottom': True, 'top': False}
    ascending = asc_dct[order]
    return df.sort_values(sort_by_column_name, ascending=ascending)[return_columns].iloc[:number_of_entries]

def get_proportion_func(df: pd.DataFrame, column_name: str | list, values=None):
    validate_columns(column_name, df.columns.tolist())
    df = df.copy()
    
    column_name = column_name[0] if isinstance(column_name, list) else column_name
        
    df = df[column_name].value_counts(normalize=True).reset_index()

    if values is not None and len(values) > 0:
        df = df[df[column_name].isin(values)]

    return df

def get_column_statistics_func(df: pd.DataFrame, column_name: str | list, calculation: str):
    validate_columns(column_name, df.columns.tolist())
    df = df.copy()
    column_name = column_name[0] if isinstance(column_name, list) else column_name
    return df[column_name].agg(calculation).reset_index()

####### column transform functions ########
    
def apply_map_func(df, name: str, operation: dict):
    validate_columns(operation['source_column'], df.columns.tolist())

    df = df.copy()
    source_col = operation['source_column']
    mapping = operation['mapping']
    mapping = {str(i): j for i, j in mapping.items()} # standardizing mapped values to str
    
    df[name] = df[source_col].astype(str).map(mapping) # convert to string to get valid mapping
    
    return df

def apply_map_range_func(df, name: str, operation: dict):
    validate_columns(operation['source_column'], df.columns.tolist())

    df = df.copy()
    source_col = operation['source_column']
    ranges = operation['ranges']

    ranges_lst = []
    for rng in ranges:
        range_str = rng['range']
        label = rng['label']

        range_str = range_str.replace('inf', f'{1e20}') # replacing inf with big number for easier parsing
        start_str, end_str = range_str.split('-')
        
        start = float(start_str)
        end = float(end_str)
        
        ranges_lst.append({'start': start, 'end': end, 'label': label})


    def get_label(value):
        if pd.isna(value):
            return np.nan
        
        for r in ranges_lst:
            start, end, label = r['start'], r['end'], r['label']
            
            if start <= value <= end:
                return label
        
        return np.nan
    
    df[name] = df[source_col].apply(get_label)
        
    return df

def apply_date_op_func(df, name: str, operation: dict):
    validate_columns(operation['source_column'], df.columns.tolist())
    
    df = df.copy()
    source_col = operation['source_column']
    function = operation['function'].upper()
    
    if source_col in df.columns:
        date_series = pd.to_datetime(df[source_col], errors='coerce')
        
        if function == 'YEAR':
            df[name] = date_series.dt.year
        elif function == 'MONTH':
            df[name] = date_series.dt.month
        elif function == 'DAY':
            df[name] = date_series.dt.day
        elif function == 'WEEKDAY':
            df[name] = date_series.dt.weekday
        
    return df

def apply_math_op_func(df, name: str, operation: dict):
    validate_columns(operation['source_columns'], df.columns.tolist())

    df = df.copy()
    expression = operation['expression']

    try:
        df[name] = df.eval(expression)
    except Exception:
        print(f"invalid math_op expression")

    return df

########### column combination functions ###########

def get_column_combination_func(df: pd.DataFrame, name: str, operation: str):
    validate_columns(operation['source_columns'], df.columns.tolist())

    df = df.copy()
    expression = operation['expression']

    try:
        df[name] = df.eval(expression)
    except Exception:
        print(f"invalid column combination expression")

    return df
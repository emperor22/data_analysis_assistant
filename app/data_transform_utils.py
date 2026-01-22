import pandas as pd
from pandas.api.types import is_numeric_dtype, is_datetime64_any_dtype
import numpy as np
import json

from app.logger import logger
from app.config import Config




def determine_main_date_col(df):
    date_col_names = ['date', 'timestamp', 'updated_at', 'created_at'] # placeholder
    date_cols = []
    df_length = len(df)
    for col in df.columns:
        if any(i in col for i in date_col_names):
            date_cols.append(col)
    if len(date_cols) == 1:
        return date_cols[0]
    else:
        unique_vals = {}
        for col in date_cols:
            unique_vals[col] = df[col].nunique()
        max_unique_val_col = max(unique_vals, key=unique_vals.get)
        
        return max_unique_val_col # assuming one col with max number of unique values


def infer_granularity(series):
    hr_granularity = series.sort_values().diff().mode()[0] / pd.Timedelta(hours=1)
    
    MINUTE_H = 1/60
    
    HOUR_H = 1
    DAY_H = 24
    WEEK_H = 24 * 7
    
    MONTH_H_LOWER = 28 * 24
    MONTH_H_HIGHER = 31 * 24
    YEAR_H_LOWER = 365 * 24
    YEAR_H_HIGHER = 366 * 24
    
    if np.isclose(hr_granularity, MINUTE_H):
        return "minute"
    elif np.isclose(hr_granularity, 15 * MINUTE_H):
        return "15 minutes"
    elif np.isclose(hr_granularity, 30 * MINUTE_H):
        return "30 minutes"
    elif np.isclose(hr_granularity, HOUR_H):
        return "hour"
    elif np.isclose(hr_granularity, DAY_H):
        return "day"
    elif np.isclose(hr_granularity, WEEK_H):
        return "week"
    elif MONTH_H_LOWER <= hr_granularity <= MONTH_H_HIGHER:
        return "month"
    elif YEAR_H_LOWER <= hr_granularity <= YEAR_H_HIGHER:
        return "year"
    elif hr_granularity < MINUTE_H:
        return "less than a minute"
    elif hr_granularity > YEAR_H_HIGHER:
        return "greater than a year"
    else:
        return f"{round(hr_granularity, 2)} hours"

def handle_datetime_columns_serialization(df: pd.DataFrame):
    df = df.copy()
    datetime_columns = df.select_dtypes(include=['datetime64[ns]']).columns.tolist()

    if datetime_columns:
        for dt_col in datetime_columns:
            df[dt_col] = df[dt_col].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    return df

@logger.catch
def clean_dataset(df):
    df = df.copy()

    # for column names, strip, make lowercase, replace space and dash with _, and remove non-alphanum characters
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_', regex=False).str.replace('-', '_') \
                            .str.replace('[^a-z0-9_]', '', regex=True)
    
    # cleaning column values
    for col in df.columns:
        temp_series_nonull_idx = df[col].dropna().index # for later when getting conversion success rate only for non-null rows
        # assume column is numeric and try to cast it to numeric type after cleaning
        if df[col].dtype == 'object':
            
            # check if datetime
            date_col_contains = ['date', 'timestamp', 'updated_at', 'created_at']
            if any(s in col for s in date_col_contains):
                
                temp_series_dt = pd.to_datetime(df[col], errors='coerce')
                temp_series_dt_nonull = temp_series_dt[temp_series_dt.index.isin(temp_series_nonull_idx)]
                datetime_success_rate = 1 - (temp_series_dt_nonull.isna().sum() / len(temp_series_dt_nonull))
                
                if datetime_success_rate > Config.DATATYPE_CONV_SUCCESS_RATE_CLEAN_DATASET:
                    df[col] = temp_series_dt
                    continue

            # check if numeric
            temp_series_numeric = pd.to_numeric(df[col].astype(str).str.replace(r'[^a-zA-Z0-9\s.]', '', regex=True), errors='coerce')
            temp_series_numeric_nonull = temp_series_numeric[temp_series_numeric.index.isin(temp_series_nonull_idx)]

            numeric_success_rate = 1 - (temp_series_numeric_nonull.isna().sum() / len(temp_series_numeric_nonull))
            
            if numeric_success_rate > Config.DATATYPE_CONV_SUCCESS_RATE_CLEAN_DATASET: # more than x % of col values are successfully converted to numeric, so keep it 
                df[col] = temp_series_numeric
            else: # col is probably a string column
                null_string_replace = ['', 'nan', None, 'null', 'n/a', 'na', 'none']
                df[col] = df[col].astype(str).str.strip().str.lower().replace(null_string_replace, np.nan) # clean the string column
        else:
            continue # column is already numeric type and doesnt require cleaning
        
    return df

def get_granularity_map(df):
    granularity_map = {}
    dt_cols = df.select_dtypes(include=[np.datetime64])
    
    for col in dt_cols:
        granularity_map[col] = infer_granularity(df[col]) 
        
    return granularity_map
    

def validate_columns(cols_to_check: str | list, column_list):
    if not cols_to_check:
        return
    
    cols_to_check = [cols_to_check] if isinstance(cols_to_check, str) else cols_to_check
    
    missing_cols = [i for i in cols_to_check if i not in column_list]
    
    if len(missing_cols) > 0:
        raise Exception(f'some columns mentioned in task do not exist. columns ({", ".join(missing_cols)})')
    
    return

def groupby_func(df: pd.DataFrame, columns_to_group_by: list, columns_to_aggregate: list, calculation: str):
    for cols_to_check in (columns_to_group_by, columns_to_aggregate):
        validate_columns(cols_to_check, df.columns.tolist())
        
    df = df.copy()
    
    # handling non numeric aggregation columns by converting them to numeric type first
    for col in columns_to_aggregate:
        if not is_numeric_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    if isinstance(calculation, list) and len(calculation) > 1:
        agg_dct = {col: calculation for col in columns_to_aggregate}
        df = df.groupby(columns_to_group_by, as_index=False).agg(agg_dct)
        df.columns = [f'{i[0]}_{i[1]}' for i in df.columns] # merging the two level of column names into one
        
        return df
        
    if isinstance(calculation, list) and len(calculation) == 1:
        calculation = calculation[0]
        
    df = df.groupby(columns_to_group_by, as_index=False)[columns_to_aggregate].apply(calculation)
        
    return df

def is_numeric(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def filter_func(df: pd.DataFrame, column_name: str | list, operator: str, values: list | str):
    validate_columns(column_name, df.columns.tolist())
    df = df.copy()
    
    column_name = column_name[0] if isinstance(column_name, list) else column_name

    if len(values) == 0:
        raise Exception('need to have filter values')
    
    # replacing non standard operators
    operator = '==' if operator == '=' else operator
    operator = '!=' if operator == '<>' else operator

    # handling case of equal filter with single string  value
    flt_value = values[0] if isinstance(values, list) else values
    
    if isinstance(flt_value, str) and not is_numeric(flt_value) and operator in ('==', '!='):
        operator = 'in'
        
    if operator == 'in':
        values = [values] if not isinstance(values, list) else values
        query_string = f"{column_name} {operator} @values"
        
    elif operator == 'between':
        
        if not len(values) == 2:
            raise Exception('between operator must have exactly two filter values')
        
        values = sorted(values)
        min_val, max_val = values
        query_string = f"{column_name} >= @min_val and {column_name} <= @max_val"
        
    else:
        flt_value = values[0] if isinstance(values, list) else values
        
        if isinstance(flt_value, str):
            flt_value = float(flt_value)
            
        query_string = f"{column_name} {operator} {flt_value}"
    
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

    if values in ([], [''], ''):
        values = None
    
    if values is not None and len(values) > 0:
        df = df[df[column_name].isin(values)]

    return df

def get_column_statistics_func(df: pd.DataFrame, column_name: str | list, calculation: str):
    validate_columns(column_name, df.columns.tolist())
    df = df.copy()
    column_name = column_name[0] if isinstance(column_name, list) else column_name
    return df[column_name].agg(calculation).reset_index()

@logger.catch(reraise=True)
def resample_data_func(df: pd.DataFrame, columns_to_aggregate: list, static_group_cols: list,  frequency: str, date_column: str, 
                       calculation: str):
    for cols_to_check in [columns_to_aggregate, static_group_cols, date_column]:
        validate_columns(cols_to_check, df.columns.tolist())
    
    period_map = {'day': 'D', 'week': 'W', 'month': 'MS', 'year': 'YS', 'quarter': 'QS'}
    frequency = period_map[frequency]
    
    df = df.copy()
    
    if isinstance(calculation, list):
        calculation = calculation[0]
        
    if isinstance(date_column, list):
        date_column = date_column[0]
    
    if len(static_group_cols) > 0:
        resampled = df.groupby(static_group_cols).resample(frequency, on=date_column)[columns_to_aggregate].apply(calculation).reset_index()
    else:
        resampled = df.resample(frequency, on=date_column)[columns_to_aggregate].apply(calculation).reset_index()
    return resampled
    

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

########## col info utils #####################

@logger.catch(reraise=True)
def get_column_properties(series, is_numeric, is_datetime):
        row_count = len(series)
        
        col_info_dct = {}
        
        col_info_dct['missing_count'] = series.isna().sum()
        col_info_dct['missing_value_ratio'] = series.isna().sum() / row_count
        
        unique_count = series.nunique(dropna=True)
        uniqueness_ratio = unique_count / row_count if row_count > 0 else 0
        
        is_categorical = uniqueness_ratio < 0.05 or unique_count < 50
        
        col_info_dct['unique_count'] = unique_count
        col_info_dct['uniqueness_ratio'] = uniqueness_ratio
    
        if is_numeric:
            type_prop_dct = {'datatype': 'numerical', 'skewness': series.skew(), 
                                'min_value': series.min(), 'max_value': series.max(), 
                                'mean_value': series.mean(), 'median_value': series.median(), 
                                'std': series.std(), 'q_25th': series.quantile(0.25), 
                                'q_75th': series.quantile(0.75), 
                                'most_common_5_values': series.value_counts(normalize=True).head(5).to_dict(),
                                'is_categorical': is_categorical}
            
            col_info_dct['type_dependent_properties'] = type_prop_dct

        elif is_datetime:
            min_date = series.min()
            max_date = series.max()
            
            type_prop_dct = {'datatype': 'datetime', 'most_common_5_values': series.dt.strftime('%Y-%m-%d %H:%M:%S').value_counts(normalize=True).head(5).to_dict(), 
                             'date_min': min_date.strftime('%Y-%m-%d %H:%M:%S'), 'date_max': max_date.strftime('%Y-%m-%d %H:%M:%S'), 
                             'range_days': pd.Timedelta(max_date - min_date).days, 'is_categorical': is_categorical}
            
            col_info_dct['type_dependent_properties'] = type_prop_dct
        
        else:

            string_length = series.str.len()
            
            type_prop_dct = {'datatype': 'string', 'max_length': string_length.max(), 'mean_length': string_length.mean(),
                                'max_length': string_length.max(), 'mean_length': string_length.mean(), 
                                'most_common_5_values': series.value_counts(normalize=True).head(5).to_dict(), 
                                'is_categorical': is_categorical}
            
            col_info_dct['type_dependent_properties'] = type_prop_dct
            
        return col_info_dct
 
@logger.catch(reraise=True)   
def col_transform_and_combination_parse_helper(dct, is_col_combination):
    operation = dct['operation']
    
    if is_col_combination:
        formula = operation.get('expression', 'N/A')
        description = dct.get('description', 'N/A')
        
        return {'operation': 'column_combination', 'description': description, 'formula': formula}
    
    op_type = operation.get('type', 'N/A')
    description = dct.get('description', 'N/A')
    if op_type == "map":
        source_col = operation['source_column']
        mapping_str = json.dumps(operation['mapping'], separators=(',', ': '))
        formula = f"Category Mapping on {source_col}: {mapping_str}"

    elif op_type == "map_range":
        source_col = operation['source_column']
        ranges = operation['ranges']
        
        range_list = [f"{r['range']} -> {r['label']}" for r in ranges]
        formula = f"Binning on {source_col}: {'; '.join(range_list)}"

    elif op_type == "date_op":
        source_col = operation['source_column']
        function = operation['function'].upper()
        formula = f"{function}({source_col})"

    elif op_type == "math_op":
        expression = operation['expression']
        formula = expression
        
    return {'operation': op_type, 'description': description, 'formula': formula}

# refactor: improve this function for when there is more than 1 object column
def determine_result_output_type(df: pd.DataFrame):

    if df.shape[0] > Config.MAX_VISUAL_ROWS or df.shape[1] > Config.MAX_VISUAL_COLS:
        return 'TABLE_EXPORT'

    if df.shape[1] < 2:
        return 'DISPLAY_TABLE' 
    
    col_1_dtype = str(df.dtypes.iloc[0])
    col_2_dtype = str(df.dtypes.iloc[1])

    is_numeric_1 = ('int' in col_1_dtype or 'float' in col_1_dtype)
    is_numeric_2 = ('int' in col_2_dtype or 'float' in col_2_dtype)
    is_datetime_1 = ('datetime' in col_1_dtype)
    is_datetime_2 = ('datetime' in col_2_dtype)
    
    is_categorical_1 = ('object' in col_1_dtype or 'category' in col_1_dtype)
    is_categorical_2 = ('object' in col_2_dtype or 'category' in col_2_dtype)

    if df.shape[0] >= Config.MIN_LINE_POINTS:
        if (is_datetime_1 and is_numeric_2) or (is_datetime_2 and is_numeric_1):
            return 'LINE_CHART'

    if df.shape[0] <= Config.MAX_BAR_ROWS:
        if (is_categorical_1 and is_numeric_2) or (is_categorical_2 and is_numeric_1):
            return 'BAR_CHART'

    return 'DISPLAY_TABLE'
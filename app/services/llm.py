from app.services.dataset import get_dataset_snippet
from app.crud import UserTableOperation
from app.config import Config, api_key_dct
from app.logger import logger
from app.exceptions import RateLimitedException

from string import Template

import requests

import pandas as pd
import json
import re

from datetime import datetime

import httpx

from app.auth import decrypt_api_key

class DatasetProcessorForPtOnePrompt:
    def __init__(self, dataframe: pd.DataFrame, filename: str, prompt_template_file: str, granularity_data: dict = {}):
        self.dataframe = dataframe
        self.dataset_row_count = len(dataframe)
        self.filename = filename
        self.prompt_template_file = prompt_template_file
        self.granularity_data = 'N/A' if not granularity_data else granularity_data
    
    def _get_column_unique_values(self) -> dict:
        unique_val_dct = {}
        for col in self.dataframe.columns:
            unique_val_dct[col] = {}
            unique_val_dct[col]['num_of_unique_values'] = self.dataframe[col].nunique()
            unique_val_dct[col]['unique_values_count_ratio_to_row_count'] = self.dataframe[col].nunique() / self.dataset_row_count
            unique_val_dct[col]['top_5_values'] = self.dataframe[col].value_counts().index[:5].tolist()
            
        return unique_val_dct
    
    def create_prompt(self) -> str:
        with open(self.prompt_template_file, 'r') as f:
            template = Template(f.read())
            
        context = {'current_time': datetime.now().strftime('%H:%M:%S'), # so that prompt is not cached
                   'dataset_name': self.filename, 
                   'dataset_row_count': self.dataset_row_count, 
                   'dataset_snippet': get_dataset_snippet(self.dataframe), 
                   'dataset_column_unique_values': self._get_column_unique_values(), 
                   'temporal_granularity_map': self.granularity_data}
        return template.substitute(context)
    
    
def insert_prompt_context(prompt_file, context):
    with open(prompt_file, 'r') as f:
        template = Template(f.read())
    
    return template.substitute(context)


def generate_payload_cerebras(model, prompt):
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}]
    }
    
    return payload

def generate_headers_cerebras(key):
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    
    return headers


@logger.catch(reraise=True)
def get_prompt_result_cerebras(prompt, model, key):
    url = Config.LLM_ENDPOINT_CEREBRAS

    headers = generate_headers_cerebras(key)

    payload = generate_payload_cerebras(model, prompt)

    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 429:
        raise RateLimitedException
    
    response.raise_for_status()

    return response.json()

async def check_if_api_key_valid_cerebras(key):
    model='llama3.1-8b'
    prompt = 'what year is it? answer shortly.'
    
    headers = generate_headers_cerebras(key)
    payload = generate_payload_cerebras(model=model, prompt=prompt)


    async with httpx.AsyncClient() as client:
        response = await client.post(Config.LLM_ENDPOINT_CEREBRAS, headers=headers, json=payload)
    
    if response.status_code == 503:
        return 'TRY_LATER'
    
    if response.status_code in (400, 401, 403):
        return 'INVALID_KEY'
    
    return 'VALID_KEY'

def generate_payload_google(prompt):
    payload = {
        "contents": [
            {
                "parts": [{"text": prompt}]
             }
            ]
        }
    
    return payload
    
def generate_headers_google(key):
    headers = {'Content-Type': 'application/json','X-goog-api-key': key}
    return headers

async def check_if_api_key_valid_google(key):
    model = 'gemini-2.5-flash-lite'
    url = 'https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent'
    url = url.format(model)
    
    prompt = 'what year is it? answer promptly'
    
    payload = generate_payload_google(prompt)
    headers = generate_headers_google(key)

    async with httpx.AsyncClient() as client:
        response = await client.post(url, headers=headers, json=payload)
    
    if response.status_code == 503:
        return 'TRY_LATER'
    
    if response.status_code in (400, 401, 403):
        return 'INVALID_KEY'
    
    return 'VALID_KEY'
        


@logger.catch(reraise=True)
def get_prompt_result_google(prompt, model, key):
    url = Config.LLM_ENDPOINT_GOOGLE
    
    url = url.format(model)
    
    payload = generate_payload_google(prompt)

    headers = generate_headers_google(key)
    
    response = requests.post(url, json=payload, headers=headers)
    
    if response.status_code == 429:
        raise RateLimitedException
    
    response.raise_for_status()
        
    return response.json()




@logger.catch
def process_llm_api_response_google(resp: dict):
    resp['candidates'][0]['content']['parts'][0]['text']
    
    if '```json' in resp:
        resp = re.search(r"```json\s*(.*?)\s*```", resp, re.DOTALL)
        resp = resp.group(1).strip()
    
    resp = json.loads(resp)
    
    # prompt_token_count = resp['usage_metadata']['promptTokenCount']
    # total_token_count = resp['usage_metadata']['totalTokenCount']
    
    return resp


@logger.catch
def process_llm_api_response_cerebras(resp: dict):
    resp = resp["choices"][0]["message"]["content"]
    
    if '```json' in resp:
        resp = re.search(r"```json\s*(.*?)\s*```", resp, re.DOTALL)
        resp = resp.group(1).strip()
    
    resp = json.loads(resp)
    
    # prompt_token_count = resp['usage_metadata']['promptTokenCount']
    # total_token_count = resp['usage_metadata']['totalTokenCount']
    
    return resp

LLM_PROVIDER_DCT = {'google': get_prompt_result_google, 
                    'cerebras': get_prompt_result_cerebras}

LLM_RESP_PROCESSOR_DCT = {'google': process_llm_api_response_google, 
                          'cerebras': process_llm_api_response_cerebras}


def resp_loader(prompt, model, provider, key):
    get_prompt_result = LLM_PROVIDER_DCT[provider]
    process_llm_api_response = LLM_RESP_PROCESSOR_DCT[provider]
    resp = get_prompt_result(prompt, model, key)     
    resp = process_llm_api_response(resp)
    
    return resp

def mock_resp_loader(mock_resp_file, pt):
    logger.info(f'part {pt} prompt mocked')
    with open(mock_resp_file, 'r') as f:
        resp = json.load(f)
        
    return resp

def write_prompt_and_res(prompt, res, part, request_id, dir_): 
    with open(f'{dir_}/prompt_pt{part}_{request_id}.txt', 'w', encoding='utf-8') as f:
        f.write(prompt)
    with open(f'{dir_}/res_pt{part}_{request_id}.json', 'w', encoding='utf-8') as f:
        json.dump(res, f, indent=4)

def cleanup_agg_col_names(resp_pt_2, resp_pt_1):
    
    json_str = json.dumps(resp_pt_2)
    
    def remove_suffix(s):
        return '_'.join(s.split('_')[:-1])
    
    resp_pt_1 = resp_pt_1
    
    mrg_lst = [*resp_pt_1['columns'], *resp_pt_1['common_column_combination'], *resp_pt_1['common_column_cleaning_or_transformation']]
    original_columns = {i['name'] for i in mrg_lst}
    
    aggs = ['mean', 'median', 'min', 'max', 'count', 'size', 'sum']
    pattern = r'"([^"]*_(?:{}))"'.format('|'.join(aggs))
    
    # find columns with names ending with aggregation (e.g. sales_sum)
    cols_to_clean = re.findall(pattern, json_str)
    cols_to_clean = list(set(cols_to_clean))
    
    replace_dct = {}
    
    for col in cols_to_clean:
        if remove_suffix(col) in original_columns:
            replace_dct[col] = remove_suffix(col)
    
    for col_to_replace, replace in replace_dct.items():
        json_str = json_str.replace(col_to_replace, replace)
        
    return json.loads(json_str)


async def get_api_key(user_table_ops: UserTableOperation, user_id, provider):
    default_api_key = api_key_dct[provider]
    user_api_key = await user_table_ops.get_api_key(user_id, provider)
    
    if user_api_key:
        logger.debug('using user api key')
        return decrypt_api_key(user_api_key)
    
    return default_api_key

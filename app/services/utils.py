from datetime import datetime, timezone
import re

def split_and_validate_new_prompt(prompt):
    
    def validate_value(s):
        min_char = 15
        max_char = 100
        return min_char <= len(s) <= max_char 
    
    regex = r'^[a-zA-Z0-9 \n\r]*$'
    
    if not bool(re.fullmatch(regex, prompt)):
        return
            
    values = [i.strip() for i in prompt.split('\n')]
    all_values_valid = all([validate_value(val) for val in values])
    
    if len(values) > 5 or not all_values_valid:
        return
    
    return prompt.split('\n')

def get_current_time_utc():
    return datetime.now(timezone.utc)

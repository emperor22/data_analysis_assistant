import requests

url = 'http://127.0.0.1:8000/upload_dataset'
dataset1 = 'algerian_forest_fires_dataset.csv'
dataset2 = 'loan-default-risk.csv'
dataset3 = 'campus-placement-dataset.csv'

file = {'file': open(dataset2, 'rb')}
headers = {'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJlbXBlcm9yMjIiLCJleHAiOjE3NTkyMjMzNjF9.09te66DUoQXP2pT9EeMb9-r4kX4RwXXNfFwiqb-T0rw'}
resp = requests.post(url=url, files=file, headers=headers) 
print(resp)
print(resp.json())

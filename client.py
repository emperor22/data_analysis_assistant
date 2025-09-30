import requests

url = 'http://127.0.0.1:8000/upload_dataset'
dataset1 = 'algerian_forest_fires_dataset.csv'
dataset2 = 'loan-default-risk.csv'
dataset3 = 'campus-placement-dataset.csv'

file = {'file': open(dataset2, 'rb')}
headers = {'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJlbXBlcm9yMjIiLCJleHAiOjE3NTkyMzA5MzJ9.3CD_KJjJQrL3QJBQF7fa8b8L_aIpkg1VIVu8IXyVzTk'}
resp = requests.post(url=url, files=file, headers=headers) 
print(resp)
print(resp.json())

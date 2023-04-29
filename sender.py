import requests

url = "http://127.0.0.1/uploadfile"

with open("input/departments.csv", "rb") as f:
    r = requests.post(url, files={"file": f})
    print(r.json())

with open("input/hired_employees.csv", "rb") as f:
    r = requests.post(url, files={"file": f})
    print(r.json())

with open("input/jobs.csv", "rb") as f:
    r = requests.post(url, files={"file": f})
    print(r.json())

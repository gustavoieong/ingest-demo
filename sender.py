import requests
import sys

url = "http://localhost:8000/uploadfile"

try:
    with open("input/departments.csv", "rb") as f:
        r = requests.post(url, files={"file": f})
        print(r.json())

    with open("input/hired_employees.csv", "rb") as f:
        r = requests.post(url, files={"file": f})
        print(r.json())

    with open("input/jobs.csv", "rb") as f:
        r = requests.post(url, files={"file": f})
        print(r.json())

except Exception as e:
    print("Error: ", e)
    sys.exit("Fin de la ejecucion con error.")

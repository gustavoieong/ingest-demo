from fastapi import FastAPI, UploadFile, File
import csv
import mysql.connector

app = FastAPI()

def generate_table(filename):
    create_table_query = ""
    if (filename == "departments.csv"):
        create_table_query = """CREATE TABLE IF NOT EXISTS tb_departments (
                             id INT,
                             name VARCHAR(255),
                             datetime VARCHAR(255),
                             department_id INT,
                             job_id INT
                             );"""
    if (filename == "hired_employees.csv"):
        create_table_query = """CREATE TABLE IF NOT EXISTS tb_hired_employees (
                             id INT,
                             department VARCHAR(255)
                             );"""
    if (filename == "jobs.csv"):
        create_table_query = """CREATE TABLE IF NOT EXISTS tb_jobs (
                             id INT,
                             job VARCHAR(255)
                             );"""
    return create_table_query

def generate_query(filename):
    insert_query = ""
    if (filename == "departments.csv"):
        insert_query = """INSERT INTO tb_departments (id, name, datetime, department_id, job_id) VALUES (%d, %s, %s, %d, %d);"""
    if (filename == "hired_employees.csv"):
        insert_query = """INSERT INTO tb_hired_employes (id, job) VALUES (%d, %s);"""
    if (filename == "jobs.csv"):
        insert_query = """INSERT INTO tb_jobs (id, job) VALUES (%d, %s);"""
    return insert_query

# Endpoint to handle file upload
@app.post("/uploadfile/")
async def create_upload_file(file: UploadFile = File(...)):

    temp = ""

    # Connect to MySQL database
    cnx  = mysql.connector.connect(
        host = "database-api.conxcscqngr8.us-east-1.rds.amazonaws.com",
        user = "admin",
        password = "Azure123.",
        database = "database_rest_api"
    )

    # Create a cursor object to execute SQL queries
    cursor = cnx.cursor()
    temp = "cursor"

    # Open and read CSV file
    #if file.content_type == "text/csv":
    csv_data = file.file.read().decode("utf-8")
    temp = "csv_data decoded"
    csv_reader = csv.reader(csv_data.splitlines())
    #next(csv_reader)  # skip header row
    temp = "csv_reader created"

    # Insert data into MySQL table in batches of 1000
    batch_size = 1000
    rows = []
    for row in csv_reader:
        rows.append((int(row[0]), row[1]))
        temp = row
        if len(rows) == batch_size:
            insert_query = """INSERT INTO tb_jobs (id, job) VALUES (1, "ss");""" #generate_query(file.filename)
            cursor.executemany(insert_query, rows)
            rows = []

    # Insert any remaining rows
    if rows:
        insert_query = "INSERT INTO tb_jobs (id, job) VALUES (%d, %s);" #generate_query(file.filename)
        cursor.executemany(insert_query, rows)

    cnx.commit()
    cursor.close()
    cnx.close()
    return {"message": "File uploaded successfully."}
    #else:
    #   return temp #{"message": "Only CSV files are allowed."}

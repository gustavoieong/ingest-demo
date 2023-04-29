import csv
import mysql.connector
from fastapi import FastAPI, File, UploadFile
import traceback

app = FastAPI()

# MySQL configuration
db_config = {
    'host': 'database-api.conxcscqngr8.us-east-1.rds.amazonaws.com',
    'user': 'admin',
    'password': 'Azure123.',
    'database': 'database_rest_api',
}

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

@app.post("/uploadfile/")
async def create_upload_file(file: UploadFile = File(...)):
    # Connect to MySQL
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Get CSV file from request
    file = request.files['file']

    # Loop through the file and insert data into MySQL in batches of 1000 rows
    rows = []
    count = 0
    for line in file:
        # Convert bytes to string and split by comma
        row = line.decode().strip().split(',')
        # Append row to rows list
        rows.append(tuple(row))
        count += 1
        # Insert rows into MySQL in batches of 1000
        if count % 1000 == 0:
            insert_query = generate_query(file.filename)
            cursor.executemany(insert_query, rows)
            conn.commit()
            rows = []

    # Insert any remaining rows
    if rows:
        insert_query = generate_query(file.filename)
        cursor.executemany(insert_query, rows)
        conn.commit()

    # Close the cursor and connection
    cursor.close()
    cnx.close()

    return {"filename": file.filename}

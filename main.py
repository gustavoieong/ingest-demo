import csv
import mysql.connector
from fastapi import FastAPI, File, UploadFile
import traceback

app = FastAPI()

def generate_table(filename):
    create_table_query = ""
    if (file == "departments.csv"):
        create_table_query = """CREATE TABLE IF NOT EXISTS tb_departments (
                             id INT,
                             name VARCHAR(255),
                             datetime VARCHAR(255),
                             department_id INT,
                             job_id INT
                             );"""
    if (file == "hired_employees.csv"):
        create_table_query = """CREATE TABLE IF NOT EXISTS tb_hired_employees (
                             id INT,
                             department VARCHAR(255)
                             );"""
    if (file == "jobs.csv"):
        create_table_query = """CREATE TABLE IF NOT EXISTS tb_jobs (
                             id INT,
                             job VARCHAR(255)
                             );"""
    return create_table_query

def generate_query(filename):
    insert_query = ""
    if (file == "departments.csv"):
        insert_query = """INSERT INTO my_table (id, name, datetime, department_id, job_id) VALUES (%d, %s, %s, %d, %d);"""
    if (file == "departments.csv"):
        insert_query = """INSERT INTO my_table (id, job) VALUES (%d, %s);"""
    if (file == "departments.csv"):
        insert_query = """INSERT INTO my_table (id, job) VALUES (%d, %s);"""
    return insert_query

@app.post("/uploadfile/")
async def create_upload_file(file: UploadFile = File(...)):
    # Parse the CSV file
    contents = await file.read()
    csv_reader = csv.reader(contents.decode().splitlines())
    #print(contents.decode())
    print(file.filename)

    mysql_user = "admin"
    mysql_password = "Azure123."
    mysql_host = "database-api.conxcscqngr8.us-east-1.rds.amazonaws.com"
    mysql_database = "database_rest_api"

    # Connect to the MySQL database
    cnx = mysql.connector.connect(user = mysql_user, password = mysql_password, host = mysql_host, database = mysql_database)
    cursor = cnx.cursor()

    # Create table if it doesn't exist
    create_table_query = generate_table(file.filename)
    cursor.execute(create_table_query)

    # Set insert query
    insert_query = generate_query(file.filename)

    # Open the CSV file for reading
    with open('file.filename') as csvfile:
        reader = csv.DictReader(csvfile)

        # Insert rows in batches of 1000
        batch_size = 1000
        batch = []
        for row in reader:
            # Append the row to the batch
            batch.append(row)

            # If the batch is full, insert the rows and reset the batch
            if len(batch) == batch_size:
                insert_query = generate_query(file.filename)
                try:
                    cursor.executemany(insert_query)
                except Exception as e:
                    # log the error message to a file
                    damage_row = str(row)
                    with open('error_log.txt', 'a') as f:
                        f.write(f"Error message: {str(e)}\n")
                        f.write(f"Error message: {str(damage_row)}\n")
                        f.write(traceback.format_exc() + "\n")
                    # continue execution, skipping the line that raised the exception
                    pass
                cnx.commit()
                batch = []

        # Insert any remaining rows
        if batch:
            insert_query = generate_query(file.filename)
            try:
                cursor.executemany(insert_query)
            except Exception as e:
                # log the error message to a file
                damage_row = str(row)
                with open('error_log.txt', 'a') as f:
                    f.write(f"Error message: {str(e)}\n")
                    f.write(f"Error message: {str(damage_row)}\n")
                    f.write(traceback.format_exc() + "\n")
                # continue execution, skipping the line that raised the exception
                pass
            cnx.commit()

    # Close the cursor and connection
    cursor.close()
    cnx.close()

    return {"filename": file.filename}

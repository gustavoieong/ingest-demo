from fastapi import FastAPI, File, UploadFile
import mysql.connector

app = FastAPI()

# Define MySQL connection parameters
mysql_config = {
    'user': 'admin',
    'password': 'Azure123.',
    'host': 'database-api.conxcscqngr8.us-east-1.rds.amazonaws.com',
    'database': 'database_rest_api'
}

# Define the batch size
batch_size = 100

@app.post("/uploadfile/")
async def create_upload_file(file: UploadFile = File(...)):
    # Open a connection to MySQL
    cnx = mysql.connector.connect(**mysql_config)
    cursor = cnx.cursor()

    # Define the INSERT query to insert rows into the MySQL table
    insert_query = ""
    if(file.filename == 'departments.csv'):
        insert_query = """INSERT INTO tb_departments (id, department) VALUES (%s, %s);"""
    if(file.filename == 'hired_employees.csv'):
        insert_query = """INSERT INTO tb_hired_employees (id, name, datetime, department_id, job_id) VALUES (%s, %s, %s, %s, %s);"""
    if(file.filename == 'jobs.csv'):
        insert_query = """INSERT INTO tb_jobs (id, job) VALUES (%s, %s);"""

    # Read the CSV file in batches of 100 rows
    rows = []
    for line_no, line in enumerate(file.file):
        # Parse the line to get the values
        values = line.decode('utf-8').strip().split(',')

        if(file.filename == 'departments.csv'):
            id = int(values[0])
            department = values[1]
            # Append the row to the rows list
            rows.append((id, department))

        if(file.filename == 'hired_employees.csv'):
            id = int(values[0])
            name = values[1]
            datetime = values[2]
            department_id = int(values[3])
            job_id = int(values[4])
            # Append the row to the rows list
            rows.append((id, name, datetime, department_id, job_id))

        if(file.filename == 'jobs.csv'):
            id = int(values[0])
            job = values[1]
            # Append the row to the rows list
            rows.append((id, job))

        # If the rows list has reached the batch size, insert the rows into the table
        if len(rows) == batch_size:
            cursor.executemany(insert_query, rows)
            cnx.commit()
            rows = []
    
    # Insert any remaining rows
    if rows:
        cursor.executemany(insert_query, rows)
        cnx.commit()

    # Close the cursor and connection to MySQL
    cursor.close()
    cnx.close()
    return {"filename": file.filename}

# Report 1 endpoint
@app.get('/report_1')
async def get_report_1():
    cursor = cnx.cursor()
    query = "SELECT * FROM tb_jobs LIMIT 10"
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return result

# Report 2 endpoint
@app.get('/report_2')
async def get_report_2():
    cursor = cnx.cursor()
    query = "SELECT * FROM tb_hired_employees LIMIT 20"
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return result
    
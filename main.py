from fastapi import FastAPI, File, UploadFile
import mysql.connector
import datetime

app = FastAPI()

# Get the current date and time
current_time = datetime.datetime.now()

# Format the current date and time as a string
timestamp_str = current_time.strftime("%Y-%m-%d %H:%M:%S")

# Define MySQL connection parameters
mysql_config = {
    'user': 'admin',
    'password': 'Azure123.',
    'host': 'database-api.conxcscqngr8.us-east-1.rds.amazonaws.com',
    'database': 'database_rest_api'
}

# Define the batch size
batch_size = 100

# Query for report 1
query_report_1 = """
SELECT 
    dpt.department,
    jbs.job,
    COUNT(CASE WHEN QUARTER(DATE(hem.datetime)) = 1 THEN 1 END) Q1,
    COUNT(CASE WHEN QUARTER(DATE(hem.datetime)) = 2 THEN 1 END) Q2,
    COUNT(CASE WHEN QUARTER(DATE(hem.datetime)) = 3 THEN 1 END) Q3,
    COUNT(CASE WHEN QUARTER(DATE(hem.datetime)) = 4 THEN 1 END) Q4
FROM tb_hired_employees hem
INNER JOIN tb_departments dpt
    ON hem.department_id = dpt.id
INNER JOIN tb_jobs jbs
    ON hem.job_id = jbs.id
WHERE
    YEAR(DATE(hem.datetime)) = 2021
GROUP BY
    dpt.department, jbs.job
ORDER BY
    dpt.department, jbs.job ASC
;
"""

# Query for report 2
query_report_2 = """
WITH
summary_2021 AS
(
SELECT
    dpt.id,
    dpt.department,
    COUNT(*) hired
FROM tb_hired_employees hem
INNER JOIN tb_departments dpt
    ON hem.department_id = dpt.id
WHERE
    YEAR(DATE(hem.datetime)) = 2021
GROUP BY
    dpt.id, dpt.department
),
summary_years AS
(
SELECT
    dpt.id,
    dpt.department,
    COUNT(*) hired
FROM tb_hired_employees hem
INNER JOIN tb_departments dpt
    ON hem.department_id = dpt.id
WHERE
    YEAR(DATE(hem.datetime)) <> 2021
GROUP BY
    dpt.id, dpt.department
)
SELECT
    sy.id,
    sy.department,
    sy.hired
FROM summary_years sy
WHERE sy.hired > (SELECT avg(s21.hired) FROM summary_2021 s21)
ORDER
    BY hired DESC
;
"""

def write_error(values):
    with open("error_log.txt", "a") as log_file:
        # Write the log message with the timestamp
        for column in values:
            log_message = log_message + str(column) + ","
        time_stamp = f"{timestamp_str}\n"
        log_file.write(log_message, time_stamp)

@app.post("/uploadfile/")
async def create_upload_file(file: UploadFile = File(...)):
    # Open a connection to MySQL
    cnx = mysql.connector.connect(**mysql_config)
    cursor = cnx.cursor()

    # Define the INSERT query to insert rows into the MySQL table
    insert_query = ""
    if (file.filename == 'departments.csv'):
        insert_query = """INSERT INTO tb_departments (id, department) VALUES (%s, %s);"""
    if (file.filename == 'hired_employees.csv'):
        insert_query = """INSERT INTO tb_hired_employees (id, name, datetime, department_id, job_id) VALUES (%s, %s, %s, %s, %s);"""
    if (file.filename == 'jobs.csv'):
        insert_query = """INSERT INTO tb_jobs (id, job) VALUES (%s, %s);"""

    # Read the CSV file in batches of 100 rows
    rows = []
    for line_no, line in enumerate(file.file):
        # Parse the line to get the values
        values = line.decode('utf-8').strip().split(',')

        if (file.filename == 'departments.csv'):
            if (values[0].isdigit() and values[1] != ''):
                id = int(values[0])
                department = values[1]
                # Append the row to the rows list
                rows.append((id, department))
            else:
                write_error(values)

        if (file.filename == 'hired_employees.csv'):
            if (values[0].isdigit() and values[1] != '' and values[2] != '' and values[3].isdigit() and values[4].isdigit()):
                id = int(values[0])
                name = values[1]
                datetime = values[2]
                department_id = int(values[3])
                job_id = int(values[4])
                # Append the row to the rows list
                rows.append((id, name, datetime, department_id, job_id))
            else:
                write_error(values)

        if (file.filename == 'jobs.csv'):
            if (values[0].isdigit() and values[1] != ''):
                id = int(values[0])
                job = values[1]
                # Append the row to the rows list
                rows.append((id, job))
            else:
                # Open the text file in append mode
                write_error(values)

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
    cnx = mysql.connector.connect(**mysql_config)
    cursor = cnx.cursor()
    cursor.execute(query_report_1)
    result = cursor.fetchall()
    cursor.close()
    return result

# Report 2 endpoint
@app.get('/report_2')
async def get_report_2():
    cnx = mysql.connector.connect(**mysql_config)
    cursor = cnx.cursor()
    cursor.execute(query_report_2)
    result = cursor.fetchall()
    cursor.close()
    return result

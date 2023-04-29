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

# Define the INSERT query to insert rows into the MySQL table
insert_query = "INSERT INTO tb_jobs (id, job) VALUES (%d, %s)"

# Define the batch size
batch_size = 100

@app.post("/uploadfile/")
async def create_upload_file(file: UploadFile = File(...)):
    # Open a connection to MySQL
    cnx = mysql.connector.connect(**mysql_config)
    cursor = cnx.cursor()

    # Read the CSV file in batches of 100 rows
    rows = []
    for line_no, line in enumerate(file.file):
        # Skip the header line
        if line_no == 0:
            continue
        # Parse the line to get the values
        values = line.decode('utf-8').strip().split(',')
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

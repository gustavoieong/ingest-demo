import csv
import io
import mysql.connector
from fastapi import FastAPI, UploadFile, File

app = FastAPI()

# Define MySQL connection parameters
config = {
    'user': 'admin',
    'password': 'Azure123.',
    'host': 'database-api.conxcscqngr8.us-east-1.rds.amazonaws.com',
    'database': 'database_rest_api',
    'raise_on_warnings': True
}

# Define batch size
batch_size = 100

@app.post("/upload-csv/")
async def upload_csv(file: UploadFile = File(...)):

    # Connect to MySQL
    cnx = mysql.connector.connect(**config)

    # Create cursor
    cursor = cnx.cursor()

    # Read CSV file
    csv_file = io.StringIO(await file.read().decode())
    csv_reader = csv.reader(csv_file)

    # Skip header row
    next(csv_reader)

    # Initialize batch
    batch = []

    # Iterate over rows
    for row in csv_reader:
        # Extract data
        id = row[0]
        job = row[1]

        # Append data to batch
        batch.append((id, job))

        # If batch size reached, insert data into MySQL
        if len(batch) == batch_size:
            cursor.executemany("INSERT INTO youtb_jobsr_table (id, job) VALUES (%d, %s)", batch)
            batch = []

    # Insert any remaining rows
    if len(batch) > 0:
        cursor.executemany("INSERT INTO tb_jobs (id, job) VALUES (%d, %s)", batch)

    # Commit changes and close connection
    cnx.commit()
    cursor.close()
    cnx.close()

    return {"message": "CSV file uploaded and inserted into MySQL database."}

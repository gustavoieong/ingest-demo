import csv
import mysql.connector
from fastapi import FastAPI, UploadFile, File

app = FastAPI()

@app.post("/uploadfile/")
async def create_upload_file(file: UploadFile = File(...)):
    # connect to MySQL database
    cnx = mysql.connector.connect(
        host='database-api.conxcscqngr8.us-east-1.rds.amazonaws.com',
        user='admin',
        password='Azure123.',
        database='database_rest_api'
    )
    cursor = cnx.cursor()

    # read CSV file
    csv_reader = csv.DictReader(await file.read().decode('utf-8').splitlines())
    
    # define SQL statement
    insert_stmt = "INSERT INTO tb_jobs (id, job) VALUES (%d, %s)"
    
    # insert data in batches of 100 rows
    batch_size = 100
    batch = []
    for row in csv_reader:
        batch.append((row['id'], row['job']))
        if len(batch) >= batch_size:
            cursor.executemany(insert_stmt, batch)
            batch = []
    
    # insert any remaining rows
    if len(batch) > 0:
        cursor.executemany(insert_stmt, batch)
        
    # commit changes to database and close connection
    cnx.commit()
    cursor.close()
    cnx.close()

    return {"filename": file.filename}

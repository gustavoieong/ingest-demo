import mysql.connector
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

# Connect to MySQL
mydb = mysql.connector.connect(
  host="database-api.conxcscqngr8.us-east-1.rds.amazonaws.com",
  user="admin",
  password="Azure123.",
  database="database_rest_api"
)

# Avro schema definition
AVRO_SCHEMA = {
    "type": "record",
    "name": "TableBackup",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "department", "type": "string"}
    ]
}

# Define the batch size for your backup
batch_size = 1000

# Open the Avro file for writing
with open("backup.avro", "wb") as avro_file:
  writer = DataFileWriter(avro_file, DatumWriter(), AVRO_SCHEMA)

  # Start the backup by selecting the rows from the MySQL table in batches
  cursor = mydb.cursor()
  cursor.execute("SELECT * FROM tb_departments")
  rows = cursor.fetchmany(batch_size)

  while rows:
    # Write each batch of rows to the Avro file
    for row in rows:
      # Create a record for the current row
      record = {"id": row[0], "job": row[1]} # Replace with your column names

      # Write the record to the Avro file
      writer.append(record)

    # Fetch the next batch of rows
    rows = cursor.fetchmany(batch_size)

  # Close the Avro file
  writer.close()

# Close the MySQL connection
mydb.close()

import avro.schema
import avro.datafile
import avro.io
import mysql.connector

# Define the Avro schema for the table backup
schema = avro.schema.parse('''
    {
      "namespace": "example.avro",
      "type": "record",
      "name": "backup",
      "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "datetime", "type": "string"},
        {"name": "department_id", "type": "int"},
        {"name": "job_id", "type": "int"}
      ]
    }
''')

# Define MySQL connection parameters
db_config = {
    'host': 'database-api.conxcscqngr8.us-east-1.rds.amazonaws.com',
    'user': 'admin',
    'password': 'Azure123.',
    'database': 'database_rest_api'
}

# Define the name of the table to back up
table_name = 'tb_hired_employees'

# Define the batch size for the backup
batch_size = 1000

# Open the Avro data file for writing
avro_file = avro.datafile.DataFileWriter(open('backup_tb_hired_employees.avro', 'wb'), avro.io.DatumWriter(), schema)

# Connect to MySQL and retrieve the table data in batches
with mysql.connector.connect(**db_config) as conn:
    cursor = conn.cursor()
    cursor.execute(f'SELECT * FROM {table_name}')
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        # Convert the MySQL rows to Avro records and write them to the data file
        for row in rows:
            record = {'id': int(row[0]), 'name': str(row[1]), 'datetime': str(row[2]), 'department_id': int(row[3]), 'job_id': int(row[4])}
            avro_file.append(record)

# Close the Avro data file
avro_file.close()

print("Backup done")

import avro.schema
import avro.datafile
import avro.io
import mysql.connector

# Define the Avro schema for the table backup
schema = avro.schema.parse('''
    {
      "namespace": "example.avro",
      "type": "record",
      "name": "bckp_tb_department",
      "fields": [
        {"name": "id", "type": "int"},
        {"name": "department", "type": "string"}
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
table_name = 'tb_departments'

# Define the batch size for the backup
batch_size = 1000

# Open the Avro data file for writing
avro_file = avro.datafile.DataFileWriter(open('backup.avro', 'wb'), avro.io.DatumWriter(), schema)

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
            record = {'id': str(row[0]), 'department': str(row[1])}
            avro_file.append(record)

# Close the Avro data file
avro_file.close()

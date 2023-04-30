import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import mysql.connector

# MySQL database configuration
db_config = {
    'user': 'admin',
    'password': 'Azure123.',
    'host': 'database-api.conxcscqngr8.us-east-1.rds.amazonaws.com',
    'database': 'database_rest_api',
}

# Avro backup file path
backup_path = 'backup_tb_hired_employees.avro'

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

# Connect to MySQL database
cnx = mysql.connector.connect(**db_config)
cursor = cnx.cursor()

# Open Avro backup file
with open(backup_path, 'rb') as f:
    reader = DataFileReader(f, DatumReader())

    # Iterate over records in the backup file
    for record in reader:
        # Extract data from the Avro record
        id_value = record['id']
        name_value = record['name']
        datetime_value = record['datetime']
        department_id_value = record['department_id']
        job_id_value = record['job_id']

        # Insert the data into the MySQL database
        insert_query = 'INSERT INTO tb_hired_employees (id, name, datetime, department_id, job_id) VALUES (%s, %s, %s, %s, %s)'
        insert_values = (id_value, name_value, datetime_value, department_id_value, job_id_value)
        cursor.execute(insert_query, insert_values)

    # Commit changes to the database
    cnx.commit()

    # Close connections
    reader.close()
    cursor.close()
    cnx.close()
    print("Restore hired employees table")
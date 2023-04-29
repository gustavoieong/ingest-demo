import mysql.connector
import fastavro
import os

# MySQL connection details
MYSQL_HOST = 'database-api.conxcscqngr8.us-east-1.rds.amazonaws.com'
MYSQL_USER = 'admin'
MYSQL_PASSWORD = 'Azure123.'
MYSQL_DATABASE = 'database_rest_api'
MYSQL_TABLE = 'tb_deparments'

# Avro schema definition
AVRO_SCHEMA = {
    "type": "record",
    "name": "tb_departments",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "department", "type": "string"}
    ]
}

# Function to fetch data from MySQL table in batches
def fetch_data(connection, cursor, batch_size):
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        yield rows

# Function to backup MySQL table to Avro
def backup_table_to_avro():
    # Create MySQL connection
    conn = mysql.connector.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DATABASE)

    # Create MySQL cursor
    cursor = conn.cursor()

    temp = "cursor"

    # Fetch total rows count
    cursor.execute(f"SELECT COUNT(*) FROM {MYSQL_TABLE}")
    total_rows = cursor.fetchone()[0]

    temp = "fetch"

    # Create Avro file
    avro_file = f"{MYSQL_TABLE}.avro"
    with open(avro_file, 'wb') as f:
        # Write Avro schema to file
        fastavro.schemaless_writer(f, AVRO_SCHEMA)

        temp = "write"

        # Fetch data from MySQL table in batches and write to Avro file
        batch_size = 100
        offset = 0
        while offset < total_rows:
            cursor.execute(f"SELECT * FROM {MYSQL_TABLE} LIMIT {batch_size} OFFSET {offset}")
            rows = cursor.fetchall()
            fastavro.schemaless_writer(f, AVRO_SCHEMA, rows)
            offset += batch_size
            temp = "while" 

    # Close MySQL connection and cursor
    cursor.close()
    conn.close()
    
    temp = "fin"

    # Return Avro file path
    #return os.path.abspath(avro_file)

    return temp

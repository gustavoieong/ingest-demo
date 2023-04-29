import mysql.connector
import fastavro
import datetime

# Define schema
schema = {
    'namespace': 'my_namespace',
    'type': 'record',
    'name': 'my_record',
    'fields': [
        {'name': 'id', 'type': 'int'},
        {'name': 'department', 'type': 'string'}
    ]
}

def main():
    # Connect to MySQL database
    cnx = mysql.connector.connect(user='admin', password='Azure123.',
                                host='database-api.conxcscqngr8.us-east-1.rds.amazonaws.com', database='database_rest_api')
    cursor = cnx.cursor()

    # Execute SELECT query to retrieve data from table
    query = ("SELECT id, department FROM tb_departments")
    cursor.execute(query)

    # Open file for writing Avro data
    filename = 'backup_' + datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + '.avro'
    with open(filename, 'wb') as avro_file:

        # Initialize Avro writer
        avro_writer = fastavro.writer(avro_file, schema, codec='deflate')

        # Write data to Avro file in batches
        batch_size = 100
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            for row in rows:
                avro_writer.write({'id': row[0], 'department': row[1]})

    # Close cursor and database connection
    cursor.close()
    cnx.close()

if __name__ == "__main__":
    main()

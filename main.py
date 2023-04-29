from flask import Flask, request
import mysql.connector

app = Flask(__name__)

# MySQL configuration
db_config = {
    'host': 'database-api.conxcscqngr8.us-east-1.rds.amazonaws.com',
    'user': 'admin',
    'password': 'Azure123.',
    'database': 'database_rest_api',
}

# Endpoint to receive CSV file
@app.route('/upload_csv', methods=['POST'])
def upload_csv():
    # Connect to MySQL
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Get CSV file from request
    file = request.files['file']

    # Loop through the file and insert data into MySQL in batches of 1000 rows
    rows = []
    count = 0
    for line in file:
        # Convert bytes to string and split by comma
        row = line.decode().strip().split(',')
        # Append row to rows list
        rows.append(tuple(row))
        count += 1
        # Insert rows into MySQL in batches of 1000
        if count % 1000 == 0:
            insert_query = "INSERT INTO tb_jobs (id, job) VALUES (%d, %s);"
            cursor.executemany(insert_query, rows)
            conn.commit()
            rows = []

    # Insert any remaining rows
    if rows:
        insert_query = "INSERT INTO tb_jobs VALUES (%d, %s)"
        cursor.executemany(insert_query, rows)
        conn.commit()

    # Close MySQL connection
    cursor.close()
    conn.close()

    return 'CSV file uploaded and inserted into MySQL.'

if __name__ == '__main__':
    app.run(debug=True)

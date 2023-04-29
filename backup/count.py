import mysql.connector
import time

mysql_user = "admin"
mysql_password = "Azure123."
mysql_host = "database-api.conxcscqngr8.us-east-1.rds.amazonaws.com"
mysql_database = "database_rest_api"

# Connect to MySQL database
cnx = mysql.connector.connect(user = mysql_user, password = mysql_password,
                              host = mysql_host, database = mysql_database)

# Create a cursor object
#cursor = cnx.cursor()
cursor = cnx.cursor(buffered=True)

# Execute a SELECT query
query = """SELECT COUNT(*) cant FROM tb_departments;"""
cursor.execute(query)

# Fetch all rows and print them
for row in cursor.fetchall():
    print("cantidad tb_departments: ")
    print(row)

# Execute a SELECT query
query = """SELECT COUNT(*) cant FROM tb_hired_employees;"""
cursor.execute(query)

# Fetch all rows and print them
for row in cursor.fetchall():
    print("cantidad tb_hired_employees: ")
    print(row)

# Execute a SELECT query
query = """SELECT COUNT(*) cant FROM tb_jobs;"""
cursor.execute(query)

# Fetch all rows and print them
for row in cursor.fetchall():
    print("cantidad tb_jobs: ")
    print(row)

# Close the cursor and database connections
cursor.close()
cnx.close()

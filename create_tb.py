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
query = """DROP TABLE tb_departments;
DROP TABLE tb_hired_employees;
DROP TABLE tb_jobs;
CREATE TABLE IF NOT EXISTS tb_departments (
                             id INT,
                             department VARCHAR(255)
                             );
CREATE TABLE IF NOT EXISTS tb_hired_employees (
                             id INT,
                             name VARCHAR(255),
                             datetime VARCHAR(255),
                             department_id INT,
                             job_id INT
                             );
CREATE TABLE IF NOT EXISTS tb_jobs (
                             id INT,
                             job_id VARCHAR(255)
                             );
"""
cursor.execute(query)

# Close the cursor and database connections
cursor.close()
cnx.close()

import mysql.connector

def main():
    mysql_user = "admin"
    mysql_password = "Azure123."
    mysql_host = "database-api.conxcscqngr8.us-east-1.rds.amazonaws.com"
    mysql_database = "database_rest_api"
    # Connect to MySQL database
    mydb = mysql.connector.connect(
        host = mysql_host,
        user = mysql_user,
        password = mysql_password,
        database = mysql_database
    )

    # Create cursor object and execute SELECT query
    mycursor = mydb.cursor()
    query = "SELECT * FROM tb_departments;"
    mycursor.execute(query)

    # Fetch all rows and display in terminal
    rows = mycursor.fetchall()
    for row in rows:
        print(row)

    # Close cursor and database connection
    mycursor.close()
    mydb.close()

if __name__ == "__main__":
    main()

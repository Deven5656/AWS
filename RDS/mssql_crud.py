'''
    @Author: Deven Gupta
    @Date: 13-09-2024
    @Last Modified by: Deven Gupta
    @Last Modified time: 13-09-2024 
    @Title : Python program to perform CRUD Operation in MSSQL
    
'''

import pyodbc
import os
from dotenv import load_dotenv


def list_table(cursor):
    #List table in database
    list_table_query = """
    SELECT 
        TABLE_NAME 
    FROM 
        INFORMATION_SCHEMA.TABLES
    """

    cursor.execute(list_table_query)

    tables = cursor.fetchall()
    print("Tables in the database:")
    for table in tables:
        print(table.TABLE_NAME)

def table_schema(tablename,cursor):
    #table schema
    table_schema_query = f"""
    SELECT 
        COLUMN_NAME, 
        DATA_TYPE
    FROM 
        INFORMATION_SCHEMA.COLUMNS
    WHERE 
        TABLE_NAME = '{tablename}'
    """
    cursor.execute(table_schema_query)

    # Fetch and print results
    columns = cursor.fetchall()

    print(f"Table Structure for {tablename}:")
    print(f"{'Column Name':<30}{'Data Type':<20}")
    for column in columns:
        print(f"{column.COLUMN_NAME:<30}{column.DATA_TYPE:<20}")


def create_table(cursor):
        """
        Description :
            This function is used to Create a table with user-defined columns.
        Parameters :
            cursor : To execute query
        return :
            None 
        """
        table_name = input("Enter the name of the table to create: ")
        columns = []
        while True:
            column_name = input("Enter column name (or 'done' to finish): ")
            if column_name.lower() == 'done':
                break
            column_type = input(f"Enter data type for column '{column_name}' (e.g., INT, NVARCHAR(100)): ")
            columns.append(f"{column_name} {column_type}")
        
        columns_sql = ', '.join(columns)
        create_table_sql = f"CREATE TABLE {table_name} ({columns_sql})"
        
        try:
            cursor.execute(create_table_sql)
            print(f"Table '{table_name}' created successfully!")
        except Exception as e:
            print(f"Error: {e}")

def drop_table(cursor):
    """
    Description :
        This function is used to Drop a table.
    Parameters :
        cursor : To execute query
    return :
        None 
    """
    table_name = input("Enter the name of the table to drop: ")
    drop_table_sql = f"DROP TABLE {table_name}"
    try:
        cursor.execute(drop_table_sql)
        print(f"Table '{table_name}' dropped successfully!")
    except Exception as e:
        print(f"Error: {e}")

def insert_data(cursor):
    """
    Description :
        This function is used to Insert data into a table.
    Parameters :
        cursor : To execute query
    return :
        None 
    """
    table_name = input("Enter the name of the table to insert data into: ")
    
    # Retrieve column names to construct insert statement
    cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
    columns = [row.COLUMN_NAME for row in cursor.fetchall()]
    
    column_names = ', '.join(columns)
    placeholders = ', '.join(['?' for _ in columns])
    insert_data_sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
    
    values = [input(f"Enter value for column '{col}': ") for col in columns]
    
    try:
        cursor.execute(insert_data_sql, values)
        print("Data inserted successfully!")
    except Exception as e:
        print(f"Error: {e}")

def query_data(cursor):
    """
    Description :
        This function is used to Show data from a table.
    Parameters :
        cursor : To execute query
    return :
        None 
    """
    table_name = input("Enter the name of the table to query: ")
    query_sql = f"SELECT * FROM {table_name}"
    try:
        cursor.execute(query_sql)
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    except Exception as e:
        print(f"Error: {e}")

def main():

    load_dotenv()

    # Define connection parameters
    server = os.getenv('RDS_SERVER')  # RDS endpoint
    port = '1433'  # Default SQL Server port
    database = 'deven_demo_database' 
    username = os.getenv('RDS_USERNAME')  
    password = os.getenv('RDS_PASSWORD') 

    # Define the connection string
    conn_str = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER={server},{port};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password};'
        f'TrustServerCertificate=yes'
    )

    # Establish the connection
    try:
        connection = pyodbc.connect(conn_str,autocommit=True)
        print("Connection successful!")

    except pyodbc.Error as e:
        print(f"Error: {e}")

    while True:
        print(f"\nOperations for Database '{database}'")
        print("1. List table")
        print("2. Print table schema")
        print("3. Create Table")
        print("4. Drop Table")
        print("5. Insert Data")
        print("6. Query Data")
        print("7. Exit")

        choice = input("Enter your choice (1-7): ")

        cursor = connection.cursor()


        if choice == '1':
            list_table(cursor)

        elif choice == '2':
            tablename =input("Enter the table name : ")
            table_schema(tablename,cursor)    

        elif choice == '3':
            create_table(cursor)
        
        elif choice == '4':
            drop_table(cursor)
        
        elif choice == '5':
            insert_data(cursor)
        
        elif choice == '6':
            query_data(cursor)
        
        elif choice == '7':
            break
        
        else:
            print("Invalid choice. Please enter a number between 1 and 7.")


    connection.close()
    print("Connection Close")


if __name__ == "__main__":
    main()
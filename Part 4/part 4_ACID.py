def retrieve_tables_data(conn):
    
    try:
        connection = conn
        cursor = connection.cursor()

        if connection and cursor:
            for table_name in ['shipments', 'order_status']:
                cursor.execute(f"SELECT * FROM {table_name}")
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                data_list = [list(row) for row in data]
                
                print(f"Table: {table_name}")
                print(tabulate(data_list, headers=columns, tablefmt="grid"))
                print() 
                
    except Exception as error:
        print("Error selecting data from tables:", error)

def connect_postgres(dbname):
    try:
        user = 'postgres'
        port = '5433'
        host = 'localhost'
        password = 'postgresql_jrds'

        connection = psycopg2.connect(
            user=user,
            dbname=dbname,
            port=port,
            host=host,
            password=password
        )
        return connection

    except Exception as error:
        print("Error connecting to PostgreSQL:", error)
        return None

def create_database(dbname):

    try:
        connection = connect_postgres(dbname='postgres')
        cursor = connection.cursor()

        if connection and cursor:
            connection.autocommit = True
            cursor.execute("CREATE DATABASE "+dbname+";")

            connection.close()
            print("Database "+dbname+" created successfully.")

    except Exception as error:
        print("Error creating database "+dbname+":", error)

def create_tables(connection):
    # Example table schema for 'shipments'
    query_shipments = """
        DROP TABLE IF EXISTS shipments;
        CREATE TABLE shipments (
            shipment_id SERIAL PRIMARY KEY,
            order_id INT UNIQUE,
            shipping_address VARCHAR(255) NOT NULL
        );
    """

    # Example table schema for 'order_status'
    query_order_status = """
        DROP TABLE IF EXISTS order_status;
        CREATE TABLE order_status (
            order_id INT PRIMARY KEY,
            status VARCHAR(50) NOT NULL
        );
    """

    execute_query(connection, query_shipments)
    execute_query(connection, query_order_status)

def insert_shipment(connection, order_id, shipping_address):
    query = f"""
        INSERT INTO shipments (order_id, shipping_address)
        VALUES ({order_id}, '{shipping_address}');
    """
    execute_query(connection, query)


def insert_order_status(connection, order_id, status):
    query = f"""
        INSERT INTO order_status (order_id, status)
        VALUES ({order_id}, '{status}');
    """
    execute_query(connection, query)


def execute_query(connection, query):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
    except Exception as e:
        connection.rollback()
        raise e

def perform_distributed_transaction():
    try:
        # Connect to Node A and Node B
        connection_a = connect_postgres(DATABASE_NAME)
        connection_b = connect_postgres(DATABASE_NAME)

        # Create necessary tables if they don't exist
        create_tables(connection_a)
        create_tables(connection_b)

        # Start distributed transaction
        connection_a.autocommit = False
        connection_b.autocommit = False

        # Example: Insert a shipment on Node A
        insert_shipment(connection_a, 1, '123 Main Street')

        # Example: Insert order status on Node B
        insert_order_status(connection_b, 1, 'Shipped')

        # Commit the distributed transaction
        connection_a.commit()
        connection_b.commit()

        print("Distributed transaction committed successfully.")

    except Exception as e:
        # Rollback in case of any error
        connection_a.rollback()
        connection_b.rollback()
        print(f"Error in distributed transaction: {str(e)}")

    finally:
        # Close database connections
        connection_a.close()
        connection_b.close()

from random import random
from tabulate import tabulate

import psycopg2
import random
from datetime import date, timedelta
from datetime import datetime

DATABASE_NAME = 'brocodeproject'

if __name__ == '__main__':

    create_database(DATABASE_NAME)
    perform_distributed_transaction()
    connection_a = connect_postgres(DATABASE_NAME)
    connection_b = connect_postgres(DATABASE_NAME)
    retrieve_tables_data(connection_a)
    retrieve_tables_data(connection_b)


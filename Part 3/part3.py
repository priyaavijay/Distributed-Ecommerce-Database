from random import random
from tabulate import tabulate

import psycopg2
from psycopg2 import sql
import random
import time
from faker import Faker
from datetime import datetime

fake = Faker()

DATABASE_NAME = 'brocodeproject'

CATEGORIES_TABLE = 'Categories'
INVENTORY_TABLE = 'Inventory'
TRANSACTION_TABLE = 'Transaction'
ADDRESS_TABLE = 'Address'
CONTACT_DETAILS_TABLE = 'Contact_Details'
CUSTOMER_TABLE = 'Customer'
SUPPLIERS_TABLE = 'Suppliers'
DELIVERY_TABLE = 'Delivery'
PRODUCTS_TABLE = 'Products'
ORDER_TABLE = 'Orders'
ORDER_ITEMS_TABLE = 'Order_Items'
REVIEWS_TABLE = 'Reviews'


def connect_postgres(dbname):
    try:
        user = 'postgres'
        port = '5432'
        host = 'localhost'
        password = '1234'

        connection = psycopg2.connect(
            user=user,
            dbname=dbname,
            port=port,
            host=host,
            password=password
        )

        # connection = psycopg2.connect("postgresql://brocode:e8FiLxVsf7TQyTbnd5bd2g@bro-code-ecommerce-db-1-3907.g95.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full")

        return connection

    except Exception as error:
        print("Error connecting to PostgreSQL:", error)
        return None

def add_sequences_for_ids(conn):
    add_sequence_for_table(ADDRESS_TABLE,"address_id",conn)
    add_sequence_for_table(CUSTOMER_TABLE, "customer_id", conn)
    add_sequence_for_table(TRANSACTION_TABLE, "transaction_id", conn)
    add_sequence_for_table(CONTACT_DETAILS_TABLE, "contact_id", conn)
    add_sequence_for_table(SUPPLIERS_TABLE, "suppliers_id", conn)

def add_sequence_for_table(tableName,primaryKey,conn):
    # Create a cursor to execute SQL commands
    cursor = conn.cursor()

    # Execute SQL command to create a sequence
    cursor.execute(f"CREATE SEQUENCE IF NOT EXISTS {tableName}_pk_sequence START 11;")
    print(f"ALTER TABLE {tableName} ALTER COLUMN {primaryKey} SET DEFAULT NEXTVAL('{tableName}_pk_sequence');")
    # Execute ALTER TABLE query to set the default value using the sequence
    cursor.execute(f"ALTER TABLE {tableName} ALTER COLUMN {primaryKey} SET DEFAULT NEXTVAL('{tableName}_pk_sequence');")
    print(f'{tableName}_pk_sequence Sequence created successfully')
    # Commit the changes
    conn.commit()


def drop_all_indexes(connection):
    try:
        cursor = connection.cursor()

        # Query to retrieve all indexes
        query = """
            SELECT indexname, tablename
            FROM pg_indexes
            WHERE schemaname = 'public';
        """

        cursor.execute(query)

        # Fetch all rows
        indexes = cursor.fetchall()

        # Drop each index
        for index in indexes:
            index_name = index[0]
            table_name = index[1]
            if index_name.startswith("idx"):
                drop_statement = sql.SQL("DROP INDEX IF EXISTS {}").format(sql.Identifier(index_name))
                cursor.execute(drop_statement)
                print(f"Index {index_name} on table {table_name} dropped successfully.")

        # Commit the changes
        connection.commit()

    except Exception as e:
        print("Error dropping indexes:", str(e))


def createIndexes(connection):
    try:
        cursor = connection.cursor()



        # Create an index on category_name column
        cursor.execute("CREATE INDEX idx_category_name ON " + CATEGORIES_TABLE + " (category_name);")

        # Create an index on created_time column
        cursor.execute("CREATE INDEX idx_created_time ON " + CATEGORIES_TABLE + " (created_time);")

        # Create an index on modified_time column
        cursor.execute("CREATE INDEX idx_modified_time ON " + CATEGORIES_TABLE + " (modified_time);")

        cursor.execute("CREATE INDEX idx_quantity ON " + INVENTORY_TABLE + " (quantity);")

        # Indexes for TRANSACTION_TABLE
        cursor.execute("CREATE INDEX idx_order_id ON " + TRANSACTION_TABLE + " (order_id);")
        cursor.execute("CREATE INDEX idx_amount ON " + TRANSACTION_TABLE + " (amount);")

        # Indexes for ADDRESS_TABLE
        cursor.execute("CREATE INDEX idx_city ON " + ADDRESS_TABLE + " (city);")
        cursor.execute("CREATE INDEX idx_country ON " + ADDRESS_TABLE + " (country);")

        # Indexes for CONTACT_DETAILS_TABLE
        cursor.execute("CREATE INDEX idx_mobile_num ON " + CONTACT_DETAILS_TABLE + " (mobile_num);")
        cursor.execute("CREATE INDEX idx_email ON " + CONTACT_DETAILS_TABLE + " (email);")

        # Indexes for CUSTOMER_TABLE
        cursor.execute("CREATE INDEX idx_first_name ON " + CUSTOMER_TABLE + " (first_name);")
        cursor.execute("CREATE INDEX idx_last_name ON " + CUSTOMER_TABLE + " (last_name);")

        # Commit the changes
        connection.commit()

        print("Indexes created successfully.")

    except Exception as e:
        print("Error creating indexes:", str(e))

def query_analysis_index_creation(connection,before_or_after,query1):
    try:
        cursor = connection.cursor()

        # Query to retrieve data (replace with your actual query)

        #query5 = "SELECT * FROM " + CONTACT_DETAILS_TABLE + " WHERE mobile_num = '848-898-8084';"
        #query6 = "SELECT * FROM " + CUSTOMER_TABLE + " WHERE first_name = 'Rebecca';"

        # Measure execution time before creating i
        cursor = connection.cursor()
        cost_queryWithoutLimit = 0

        time_queryWithoutLimit = 0

        cursor.execute(query1)
        query_plan = cursor.fetchall()
        # Print the query plan
        for row in query_plan:
            cost_queryWithoutLimit = row[0][0]['Plan']['Total Cost']
            time_queryWithoutLimit = row[0][0]['Execution Time']

        print(f"Query 1 Executed {before_or_after} Index creation: {query1}")
        print(f" Cost of Executing Query: {cost_queryWithoutLimit}")
        print(f" Time taken to Execute Query 1: {time_queryWithoutLimit}")


    except Exception as e:
        print("Error retrieving data:", str(e))

def using_joins(connection):
    queryWithoutJoin = "Explain (FORMAT JSON, ANALYZE) (SELECT * FROM customer,address  WHERE customer.first_name = 'Rebecca' AND address.country = 'Honduras');"
    queryWithJoin = "Explain (FORMAT JSON, ANALYZE)(SELECT * FROM customer JOIN address on customer.address_id = address.address_id WHERE customer.first_name = 'Rebecca' AND address.country = 'Hzsonduras');"
    query_analysis(connection,queryWithoutJoin,queryWithJoin)

def using_Limit_for_pagination(connection):
    queryWithoutLimit = "EXPLAIN (FORMAT JSON, ANALYZE) (SELECT * FROM " + ADDRESS_TABLE + ");"
    queryWithLimit = "EXPLAIN (FORMAT JSON, ANALYZE) (SELECT * FROM " + ADDRESS_TABLE + " Limit 10)"
    query_analysis(connection,queryWithoutLimit,queryWithLimit)

def query_analysis(connection,query1,query2):
    try:
        cursor = connection.cursor()
        cost_queryWithoutLimit =0
        cost_queryWithLimit = 0
        time_queryWithoutLimit= 0
        time_queryWithLimit = 0


        cursor.execute(query1)
        query_plan = cursor.fetchall()
        # Print the query plan
        for row in query_plan:
            cost_queryWithoutLimit = row[0][0]['Plan']['Total Cost']
            time_queryWithoutLimit = row[0][0]['Execution Time']



        cursor.execute(query2)
        query_plan = cursor.fetchall()
        # Print the query plan
        for row in query_plan:
            cost_queryWithLimit = row[0][0]['Plan']['Total Cost']
            time_queryWithLimit = row[0][0]['Execution Time']

        print(f"Query 1 Executed: {query1}")
        print(f" Cost of Executing Query: {cost_queryWithoutLimit}")
        print(f" Time taken to Execute Query 1: {time_queryWithoutLimit}")
        print(f"Query 2 Executed: {query2}")
        print(f" Cost of Executing Query without Limit: {cost_queryWithLimit}")
        print(f" Time taken to Execute Query 2: {time_queryWithLimit}")

    except Exception as e:
        print("Error retrieving data:", str(e))


def retrieve_data_after(connection):
    try:
        cursor = connection.cursor()

        # Query to retrieve data (replace with your actual query)
        query4 = "EXPLAIN (SELECT * FROM " + ADDRESS_TABLE + " Limit 10);"
        query = "SELECT * FROM " + ADDRESS_TABLE + " WHERE country = 'USA';"
        cursor.execute(query4)
        query5 = "SELECT * FROM " + CONTACT_DETAILS_TABLE + " WHERE mobile_num = '732-878-8869';"
        query6 = "SELECT * FROM " + CUSTOMER_TABLE + " WHERE first_name = 'Patrick';"

        query_plan = cursor.fetchall()

        # Print the query plan
        for row in query_plan:
            print(row)

        start_time = time.time()
        cursor.execute(query)
        cursor.execute(query5)
        cursor.execute(query6)
        execution_time = time.time() - start_time



        return execution_time

    except Exception as e:
        print("Error retrieving data:", str(e))



# Function to generate fake data for the tables
def generate_fake_data(table_name, num_rows,conn):
    fake_data = []
    for _ in range(num_rows):
        if table_name == ADDRESS_TABLE:
            fake_data.append((
                fake.street_address(),
                fake.secondary_address(),
                fake.city(),
                fake.zipcode(),
                fake.country(),
                fake.word()  # Add other address attributes as needed
            ))
        elif table_name == CONTACT_DETAILS_TABLE:
            fake_data.append((
                fake.phone_number(),
                fake.phone_number(),
                fake.email(),
                fake.url()  # Add other contact details attributes as needed
            ))
        elif table_name == CUSTOMER_TABLE:
            address_id = insert_address(conn)
            contact_id = insert_contact_details(conn)
            fake_data.append((
                fake.first_name(),
                fake.last_name(),
                address_id,
                contact_id,
                datetime.now(),
                datetime.now()
            ))
        elif table_name == SUPPLIERS_TABLE:
            address_id = insert_address(conn)
            contact_id = insert_contact_details(conn)
            fake_data.append((
                fake.company(),
                fake.word(),
                address_id,
                contact_id
            ))
        # Add similar elif blocks for other tables

    return fake_data

# Function to insert data into the ADDRESS_TABLE
def insert_address(conn):
    cursor = conn.cursor()
    address_data = generate_fake_data(ADDRESS_TABLE, 1,conn)
    cursor.execute(f"INSERT INTO {ADDRESS_TABLE} (address_line1, address_line2, city, postal_code, country, address_type) VALUES (%s, %s, %s, %s, %s, %s) RETURNING address_id", address_data[0])
    return cursor.fetchone()[0]

# Function to insert data into the CONTACT_DETAILS_TABLE
def insert_contact_details(conn):
    cursor = conn.cursor()
    contact_data = generate_fake_data(CONTACT_DETAILS_TABLE, 1,conn)
    cursor.execute(f"INSERT INTO {CONTACT_DETAILS_TABLE} (mobile_num, telephone_num, email, website) VALUES (%s, %s, %s, %s) RETURNING contact_id", contact_data[0])
    return cursor.fetchone()[0]

def insert_fake_data(conn):
    cursor= conn.cursor()
    # Insert data into the ADDRESS_TABLE
    address_data = generate_fake_data(ADDRESS_TABLE, 2000,conn)
    for address in address_data:
        cursor.execute(f"INSERT INTO {ADDRESS_TABLE} (address_id, address_line1, address_line2, city, postal_code, country, address_type) VALUES (DEFAULT, %s, %s, %s, %s, %s, %s) RETURNING address_id", address)

    #cursor.executemany(f"INSERT INTO {ADDRESS_TABLE} (address_line1, address_line2, city, postal_code, country, address_type) VALUES (%s, %s, %s, %s, %s, %s) RETURNING address_id", address_data)

    # Insert data into the CONTACT_DETAILS_TABLE
    contact_data = generate_fake_data(CONTACT_DETAILS_TABLE, 2000,conn)
    cursor.executemany(f"INSERT INTO {CONTACT_DETAILS_TABLE} (contact_id,mobile_num, telephone_num, email, website) VALUES (DEFAULT,%s, %s, %s, %s) RETURNING contact_id", contact_data)

    # Insert data into the CUSTOMER_TABLE
    customer_data = generate_fake_data(CUSTOMER_TABLE, 2000,conn)
    cursor.executemany(f"INSERT INTO {CUSTOMER_TABLE} (customer_id,first_name, last_name, address_id, contact_id, created_time, modified_time) VALUES (DEFAULT,%s, %s, %s, %s, %s, %s) RETURNING customer_id", customer_data)

    # Insert data into the SUPPLIERS_TABLE
    suppliers_data = generate_fake_data(SUPPLIERS_TABLE, 2000,conn)
    cursor.executemany(f"INSERT INTO {SUPPLIERS_TABLE} (suppliers_id,suppliers_name, category_type, address_id, contact_id) VALUES (DEFAULT,%s, %s, %s, %s) RETURNING suppliers_id", suppliers_data)

    # Commit the changes
    conn.commit()



if __name__ == '__main__':

    with connect_postgres(dbname=DATABASE_NAME) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)


        add_sequences_for_ids(conn);
        print()
        insert_fake_data(conn)
        print()
        using_Limit_for_pagination(conn)
        print()
        using_joins(conn)
        print()
        drop_all_indexes(conn)
        print()
        query1 = "Explain (FORMAT JSON, ANALYZE) (SELECT * FROM " + ADDRESS_TABLE + " WHERE country = 'USA');"
        query_analysis_index_creation(conn,"before",query1)
        print()
        createIndexes(conn)
        print()
        query_analysis_index_creation(conn,"after",query1)
        print()
        drop_all_indexes(conn)
        query2 = "Explain (FORMAT JSON, ANALYZE) (SELECT * FROM " + CUSTOMER_TABLE + " WHERE first_name = 'Elizabeth');"
        print()
        query_analysis_index_creation(conn, "before",query2)
        print()
        createIndexes(conn)
        print()
        query_analysis_index_creation(conn, "after",query2)









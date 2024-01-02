from pyignite import Client
import psycopg2

import re
from functools import reduce
from pyignite.datatypes.cache_config import CacheAtomicityMode


# Connect to Apache Ignite
def connect_ignite():
    client = Client()
    client.connect('127.0.0.1', 10800)
    return client

# Create Ignite cache
def create_cache(client, cache_name):
    client.create_cache(cache_name,{'atomicity_mode': CacheAtomicityMode.TRANSACTIONAL})

# Insert data into Ignite cache
def insert_data(client, cache_name,cols, data):
    cache = client.get_cache(cache_name)

    for record in data:
        cache.put(record[0], record[1])
def retrieve_data(client, cache_name):
    cache = client.get_cache(cache_name)

    # Fetch all data from the cache using scan
    result = []
    with cache.scan() as cursor:
        for key, value in cursor:
            result.append((key, value))

    return result
def main():
    # Connect to PostgreSQL database
    # Add your PostgreSQL connection details here
    pg_conn = psycopg2.connect(
        host="localhost",
        database="brocodeproject",
        user="postgres",
        port=5432
    )

    # PostgreSQL cursor
    pg_cursor = pg_conn.cursor()

    # Define Ignite cache names
    IGNITE_CACHES = ["categories", "inventory", "transaction", "address",
                     "contact_details", "customer", "suppliers", "delivery",
                     "products", "orders", "order_items", "reviews"]

    # Connect to Ignite
    ignite_client = connect_ignite()

    try:
        # Iterate over each cache
        for cache_name in IGNITE_CACHES:
            # Create Ignite cache
            try:
                ignite_client.get_or_create_cache(cache_name)
            except KeyError:
                create_cache(ignite_client, cache_name)
            # Retrieve data from PostgreSQL
            pg_cursor.execute(f"SELECT (column_name) FROM information_schema.columns WHERE table_name = '{cache_name}';")
            columns=pg_cursor.fetchall()
            cols = list(columns)
            pg_cursor.execute(f"SELECT * FROM {cache_name};")
            data = pg_cursor.fetchall()

            cache = ignite_client.get_or_create_cache(cache_name)
            cache.clear()
            # Insert data into Ignite cache
            insert_data(ignite_client, cache_name,cols, data)

        print("Data migration to Apache Ignite completed successfully.")
    except Exception as e:
        print("Error during data migration:", e)
    try:
        # Specify the Ignite cache name
        IGNITE_CACHES = ["categories", "inventory", "transaction", "address",
                         "contact_details", "customer", "suppliers", "delivery",
                         "products", "orders", "order_items", "reviews"]
        cache_name = "inventory"  # Replace with your actual cache name
        for cache_name in IGNITE_CACHES:
            # Retrieve data from Ignite cache
            data = retrieve_data(ignite_client, cache_name)

            # Print the retrieved data
            print(f"Data from {cache_name} cache:")
            for item in data:
                print(item)

    except Exception as e:
        print("Error retrieving data:", e)
    finally:
        # Close PostgreSQL connection
        pg_cursor.close()
        pg_conn.close()

        # Close Ignite connection
        ignite_client.close()

if __name__ == "__main__":
    main()

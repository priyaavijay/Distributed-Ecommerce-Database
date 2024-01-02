from pyignite import Client
import time
from pyignite.datatypes import TransactionConcurrency, TransactionIsolation
from concurrent.futures import ThreadPoolExecutor
import threading
def connect_ignite(host, port):
    client = Client()
    client.connect(host, port)
    return client
def retrieve_data(client, cache_name):
    cache = client.get_cache(cache_name)

    # Fetch all data from the cache using scan
    result = []
    with cache.scan() as cursor:
        for key, value in cursor:
            result.append((key, value))
    return result
def disconnect_ignite(client):
    if client:
        client.close()

def perform_transaction(client, inventory_id, update_function, retries=5, wait_for_deadlock=True):
    for attempt in range(retries):
        with client.tx_start(
                concurrency=TransactionConcurrency.PESSIMISTIC,
                isolation=TransactionIsolation.SERIALIZABLE,
                timeout=5000  # Set an appropriate timeout in milliseconds
        ):
            try:
                current_quantity = client.get_cache('inventory').get(inventory_id)
                new_quantity = update_function(current_quantity)
                client.get_cache('inventory').put(inventory_id, new_quantity)
                return True
            except client.exceptions.CacheKeyError:
                # Handle key not found error
                print(f"Product with ID {inventory_id} not found in the inventory.")
                return False
            except client.exceptions.TransactionOptimisticException:
                # Handle optimistic concurrency conflict
                if wait_for_deadlock:
                    print(f"Optimistic concurrency conflict. Retrying... (Attempt {attempt + 1}/{retries})")
                    time.sleep(1)  # Introduce a delay to reduce contention
                    continue
                else:
                    print(f"Optimistic concurrency conflict. Aborting transaction.")
                    return False
            except Exception as e:
                print(f"Error during transaction: {e}")
                break

    print(f"Transaction failed after {retries} retries. Possible deadlock.")
    return False

if __name__ == "__main__":
    # Replace '127.0.0.1' and '10800' with your Ignite server's host and port
    ignite_host = '127.0.0.1'
    ignite_port = 10800

    ignite_client = connect_ignite(ignite_host, ignite_port)

    inventory_id_1 = 1
    inventory_id_2 = 2

    def update_function(current_quantity):
        # Replace this with your logic to update the quantity based on the current quantity
        return current_quantity + 500
    print('Inventory details before transaction:')
    try:
        # Specify the Ignite cache name
        cache_name = "inventory"  # Replace with your actual cache name

        # Retrieve data from Ignite cache
        data = retrieve_data(ignite_client, cache_name)

        # Print the retrieved data
        print(f"Data from {cache_name} cache:")
        for item in data:
            print(item)

    except Exception as e:
        print("Error retrieving data:", e)
    # Perform simultaneous transactions for different products
    with ThreadPoolExecutor(max_workers=2) as executor:
        # Submit two concurrent transactions for the same inventory_id
        success_1 = executor.submit(perform_transaction(ignite_client, inventory_id_1, update_function))
        success_2 = executor.submit(perform_transaction(ignite_client, inventory_id_2, update_function))
        print("Thread1 submitted")
        print("Thread2 submitted")
    #     threads = []
    #
    #     for _ in range(2):
    #         thread = threading.Thread(target=perform_transaction, args=(ignite_client, inventory_id_1, update_function))
    #         threads.append(thread)
    #         thread.start()
    #
    # for thread in threads:
    #     thread.join()
    #
    #     for thread in threads:
    #         thread.join()

    if success_1 and success_2:
        print("Simultaneous transaction succeeded.")
        try:
            print('Inventory details after transaction:')
            # Specify the Ignite cache name
            cache_name = "inventory"  # Replace with your actual cache name

            # Retrieve data from Ignite cache
            data = retrieve_data(ignite_client, cache_name)

            # Print the retrieved data
            print(f"Data from {cache_name} cache:")
            for item in data:
                print(item)
        except Exception as e:
            print("Error retrieving data:", e)
    # else:
    #     print("Simultaneous transactions failed.")

    disconnect_ignite(ignite_client)

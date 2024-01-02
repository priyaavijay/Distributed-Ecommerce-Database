from pymongo import MongoClient
from datetime import datetime, timedelta
import random

# Global declaration
client = MongoClient('mongodb://localhost:27017/')
product_names = [
    "Wireless Mouse", "Bluetooth Headphones", "Android Smartphone", "Laptop",
    "Smartwatch", "USB-C Charging Cable", "Webcam", "Ergonomic Keyboard",
    "External Hard Drive", "Gaming Monitor"
]
product_codes = [
    "W", "BH", "SP", "L",
    "SW", "USB-C", "WC", "EKeyboard",
    "HDDExt", "GMonitor"
]
categories = ["Electronics", "Computing", "Accessories", "Wearables", "Storage", "Gaming"]
customer_first_names = ["John", "Jane", "Alice", "Bob", "Emma", "Mike", "Laura", "Dave", "Sarah", "Chris"]
customer_last_names = ["Smith", "Johnson", "Brown", "Williams", "Jones", "Miller", "Davis", "Garcia", "Rodriguez",
                       "Wilson"]
supplier_names = ["Tech Supplies Co.", "Gadgets Unlimited", "Quality Electronics", "Computer Parts Corp.",
                  "Device Market", "Smart Tech"]
cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego",
          "Dallas", "San Jose"]
statuses = ["Completed", "Pending", "Cancelled", "Delivered", "Dispatched", "Processing"]
delivery_types = ["Standard", "Express"]
website_domains = ["techsupplies", "gadgetsunlimited", "qualityelectronics", "computerparts", "devicemarket",
                   "smarttech"]
products_ids = list(range(10))
customers_ids = list(range(10))
suppliers_ids = list(range(10))
addresses_ids = list(range(10))


# create operation
# Function to insert sample data into Products collection
def insert_sample_products():
    products_collection = db['Products']
    for i in range(10):
        products_collection.insert_one({
            'Product_ID': products_ids[i],
            'Product_Name': product_names[i],
            'Product_Description': f'{product_names[i]} with high performance',
            'Product_Code': f'{product_codes[i]}',
            'Price': random.uniform(100, 1500),
            'Inventory_Details': {
                'Quantity': random.randint(20, 100),
                'Created_Time': datetime.now(),
                'Updated_Time': datetime.now()
            },
            'Created_Time': datetime.now(),
            'Modified_Time': datetime.now(),
            'Supplier_ID': random.choice(suppliers_ids),
            'Category_Details': {
                'Category_ID': random.randint(1, 10),
                'Category_desc': random.choice(categories),
                'Created_Time': datetime.now(),
                'Updated_Time': datetime.now()
            },
            'Reviews': [
                {
                    'Rating': random.choice([1, 2, 3, 4, 5]),
                    'Comment': f'Great {product_names[i]}! Totally worth the price.',
                    'Review_Time': datetime.now(),
                    'Updated_Time': datetime.now(),
                    'Customer_ID': random.choice(customers_ids)
                }
                for _ in range(random.randint(1, 5))  # Each product can have multiple reviews
            ]
        })


# create operation
# Function to insert sample data into Customers collection
def insert_sample_customers():
    customers_collection = db['Customers']
    for i in range(10):
        customers_collection.insert_one({
            'Customer_ID': i,
            'First_Name': random.choice(customer_first_names),
            'Last_Name': random.choice(customer_last_names),
            'Created_Time': datetime.now(),
            'Modified_Time': datetime.now(),
            'Address_IDs': [random.choice(addresses_ids)],
            'Contact_Details': {
                'Email': f'customer{i}@example.com',
                'Mobile_Num': f'123-456-789{i}',
                'Website': f'http://{random.choice(website_domains)}.com'
            }
        })


# create operation
# Function to insert sample data into Orders collection
def insert_sample_orders():
    orders_collection = db['Orders']
    for i in range(10):
        orders_collection.insert_one({
            'Order_ID': i,
            'Order_Date': datetime.now(),
            'Delivery_ID': i,
            'Customer_ID': random.choice(customers_ids),
            'Transaction_Details': {
                'Amount': random.uniform(100, 1000),
                'Status': random.choice(statuses),
                'Type': random.choice(['Online Payment', 'COD']),
                'Created_Time': datetime.now(),
                'Updated_Time': datetime.now()
            },
            'Details': [
                {
                    'Product_ID': random.choice(products_ids),
                    'Quantity': random.randint(1, 5),
                    'Price': random.uniform(20, 1500)
                }
                for _ in range(random.randint(1, 5))  # Each order can have multiple products
            ],
            'Delivery': {
                'Type': random.choice(delivery_types),
                'Status': random.choice(statuses),
                'Address_ID': random.choice(addresses_ids),
                'Created_Time': datetime.now(),
                'Updated_Time': datetime.now()
            }
        })


# create operation
# Function to insert sample data into Suppliers collection
def insert_sample_suppliers():
    suppliers_collection = db['Suppliers']
    for i in range(10):
        suppliers_collection.insert_one({
            'Supplier_ID': suppliers_ids[i],
            'Supplier_Name': random.choice(supplier_names),
            'Category_type': random.choice(categories),
            'Contact_Details': {
                'Mobile_Num': f'+1-555-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}',
                'Email': f'supplier{i}@wholesale.com',
                'Website': f'http://www.supplier{i}wholesale.com'
            },
            'Address_IDs': [random.choice(addresses_ids)]
        })


# Function to insert sample data into Address_Details collection
def insert_sample_address_details():
    address_details_collection = db['Address_Details']
    for i in range(10):
        address_details_collection.insert_one({
            'Address_ID': addresses_ids[i],
            'Line': f'{random.randint(100, 9999)} {random.choice(["Maple", "Oak", "Pine", "Cedar", "Elm"])} St',
            'Apartment_Num': f'#{random.randint(1, 50)}',
            'City': random.choice(cities),
            'Zip_Code': f'{random.randint(10000, 99999)}',
            'Country': 'USA',
            'Address_Type': random.choice(['Home', 'Work'])
        })


# Read operation

# Function to find all products priced below $15
def find_products_above_500_dollars():
    products = {}
    products_collection = db['Products']
    prods = list(products_collection.find({'Price': {'$gt': 500}}))
    for each_product in prods:
        products[each_product['Product_ID']] = each_product['Product_Name']
    return products


# Function to find orders for a specific customer
def find_orders_by_customer(customer_id):
    orders_collection = db['Orders']
    return list(orders_collection.find({'Customer_ID': customer_id}))


# Function to find average rating for products
def avg_rating_result():
    products_collection = db['Products']
    average_rating_result = products_collection.aggregate([
        {'$unwind': '$Reviews'},  # Unwind the Reviews array
        {'$group': {'_id': '$Product_Name', 'average_rating': {'$avg': '$Reviews.Rating'}}}
    ])
    average_ratings = list(average_rating_result)
    return average_ratings


# Update operation
# Updating price of product by product_name
def update_product_price(product_name, new_price):
    products_collection = db['Products']

    print('Price details before update:')
    print(list(products_collection.find({'Product_Name' : 'Laptop'})))
    products_collection.update_one({'Product_Name': product_name}, {'$set': {'Price': new_price}})
    print('Price details after update:')
    print(list(products_collection.find({'Product_Name' : 'Laptop'})))


# Update contact details of customer by customer_id
def update_customer_contact_details(customer_id, new_email, new_mobile_num, new_website):
    customers_collection = db['Customers']
    print('Before updating customer details:')
    print(list(customers_collection.find({'Customer_ID' : customer_id})))
    customers_collection.update_one(
        {'Customer_ID': customer_id},
        {
            '$set': {
                'Contact_Details.Email': new_email,
                'Contact_Details.Mobile_Num': new_mobile_num,
                'Contact_Details.Website': new_website,
                'Modified_Time': datetime.now()
            }
        }
    )
    print('After updating customer details:')
    print(list(customers_collection.find({'Customer_ID' : customer_id})))
    print('')


# Update order status based on order ID
def update_order_status(order_id, new_status):
    orders_collection = db['Orders']
    print('Status of order before update: ')
    print(list(orders_collection.find({'Order_ID' : order_id})))
    orders_collection.update_one(
        {'Order_ID': order_id},
        {
            '$set': {
                'Transaction_Details.Status': new_status,
                'Transaction_Details.Updated_Time': datetime.now()
            }
        }
    )
    print('Status of order after update: ')
    print(list(orders_collection.find({'Order_ID' : order_id})))


def delete_customer_and_orders(customer_id):
    customers_collection = db['Customers']
    orders_collection = db['Orders']

    # Delete customer
    customers_collection.delete_one({'Customer_ID': customer_id})
    # Delete orders associated with the customer
    orders_collection.delete_many({'Customer_ID': customer_id})

#
# def delete_order(order_id):
#     orders_collection = db['Orders']
#     orders_collection.delete_one({'Order_ID': order_id})


def delete_product(product_id):
    products_collection = db['Products']
    products_collection.delete_one({'Product_ID': product_id})


if __name__ == '__main__':

    # database name company
    db = MongoClient().ECommerce
    # Delete all documents in the collection to clean up previous data
    db['Products'].delete_many({})
    db['Customers'].delete_many({})
    db['Orders'].delete_many({})
    db['Suppliers'].delete_many({})
    db['Address_Details'].delete_many({})

    # create operation
    # Insert sample data
    insert_sample_products()
    insert_sample_customers()
    insert_sample_orders()
    insert_sample_suppliers()
    insert_sample_address_details()

    # Read operation
    expensive_products = find_products_above_500_dollars()
    print('----------Printing products above 500 dollars----------')
    print(expensive_products)
    print('\n----------Printing orders of a given customer----------')
    customer_orders = find_orders_by_customer(7)
    for order in customer_orders:
        print(order['Details'])
        print('\n')
    print('----------Printing average rating of products in an e-commerce website----------')
    rating = avg_rating_result()
    for i in rating:
        print(i)
    print('###########################################################')

    # Update Operation
    # Update product price
    update_product_price('Laptop', 3000)
    update_customer_contact_details(3, 'mynewemail@gmail.com', '987-654-3210', 'http://mywebsite.com')
    update_order_status(3, 'Completed')


    # Delete Operation for Customers
    print("###################Delete Operation###################")
    print("\nBefore deletion:")
    customer = db['Customers'].find_one({'Customer_ID': 7})
    order = db['Orders'].find_one({'Customer_ID': 7})
    print(customer)
    print('\nOrder of the customer:  ')
    print(order)
    delete_customer_and_orders(7)
    customer = db['Customers'].find_one({'Customer_ID': 7})
    print("\nDeleted Customer:")
    print(customer)  # This should print None since the customer is deleted
    print("\nDeleted Order: ")
    order = db['Orders'].find_one({'Customer_ID': 7})
    print(order)

    # Delete Operation for Orders
    product = db['Products'].find_one({'Product_ID': 2})
    print("\nBefore deleting Product:")
    print(product)
    delete_product(2)
    deleted_product = db['Products'].find_one({'Product_ID': 2})
    print("\nDeleted Product:")
    print(deleted_product)  # This should print None since the product is deleted

    # Close the connection
    client.close()

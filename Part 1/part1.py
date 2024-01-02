from tabulate import tabulate
import psycopg2

DATABASE_NAME = 'brocodeproject'

tables = ['Categories', 'Inventory', 'Transaction', 'Address', 'Contact_Details', 'Customer', 'Suppliers', 'Delivery', 'Products', 'Orders', 'Order_Items', 'Reviews']

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

def create_tables(conn):
    
    try:
        connection = conn
        cursor = connection.cursor()

        if connection and cursor:
            cursor.execute("DROP TABLE IF EXISTS "+CATEGORIES_TABLE+" CASCADE;")
            cursor.execute("CREATE TABLE "+CATEGORIES_TABLE+" (category_id INT PRIMARY KEY, category_name TEXT NOT NULL, category_desc TEXT, created_time TIMESTAMP, modified_time TIMESTAMP);")

            cursor.execute("DROP TABLE IF EXISTS "+INVENTORY_TABLE+" CASCADE;")
            cursor.execute("CREATE TABLE "+INVENTORY_TABLE+" (inventory_id INT PRIMARY KEY, quantity INT NOT NULL, created_time TIMESTAMP, updated_time TIMESTAMP);")
            
            cursor.execute("DROP TABLE IF EXISTS "+TRANSACTION_TABLE+" CASCADE;")
            cursor.execute("CREATE TABLE "+TRANSACTION_TABLE+" (transaction_id INT PRIMARY KEY, order_id INT, amount INT, status TEXT, transaction_type TEXT);")
            
            cursor.execute("DROP TABLE IF EXISTS "+ADDRESS_TABLE+" CASCADE;")
            cursor.execute("CREATE TABLE "+ADDRESS_TABLE+" (address_id INT PRIMARY KEY, address_line1 TEXT, address_line2 TEXT, city TEXT, postal_code TEXT, country TEXT, address_type TEXT);")

            cursor.execute("DROP TABLE IF EXISTS "+CONTACT_DETAILS_TABLE+" CASCADE;")
            cursor.execute("CREATE TABLE "+CONTACT_DETAILS_TABLE+" (contact_id INT PRIMARY KEY, mobile_num TEXT, telephone_num TEXT, email TEXT UNIQUE, website TEXT);")

            cursor.execute("DROP TABLE IF EXISTS "+CUSTOMER_TABLE+" CASCADE;")
            cursor.execute("CREATE TABLE "+CUSTOMER_TABLE+" (customer_id INT PRIMARY KEY, first_name TEXT, last_name TEXT, address_id INT, contact_id INT, created_time TIMESTAMP, modified_time TIMESTAMP, FOREIGN KEY (address_id) REFERENCES "+ADDRESS_TABLE+" (address_id), FOREIGN KEY (contact_id) REFERENCES "+CONTACT_DETAILS_TABLE+" (contact_id));")
            
            cursor.execute("DROP TABLE IF EXISTS "+SUPPLIERS_TABLE+" CASCADE;")
            cursor.execute("CREATE TABLE "+SUPPLIERS_TABLE+" (suppliers_id INT PRIMARY KEY, suppliers_name TEXT NOT NULL, category_type TEXT, address_id INT, contact_id INT, FOREIGN KEY (address_id) REFERENCES "+ADDRESS_TABLE+" (address_id), FOREIGN KEY (contact_id) REFERENCES "+CONTACT_DETAILS_TABLE+" (contact_id));")

            cursor.execute("DROP TABLE IF EXISTS "+DELIVERY_TABLE+" CASCADE;")
            cursor.execute("CREATE TABLE "+DELIVERY_TABLE+" (delivery_id INT PRIMARY KEY, type TEXT NOT NULL, status TEXT, address_id INT, FOREIGN KEY (address_id) REFERENCES "+ADDRESS_TABLE+" (address_id));")

            cursor.execute("DROP TABLE IF EXISTS "+PRODUCTS_TABLE+" CASCADE;")
            cursor.execute("CREATE TABLE "+PRODUCTS_TABLE+" (product_id INT PRIMARY KEY, product_name TEXT, category_id INT, product_desc TEXT, product_code TEXT, price INT, inventory_id INT, created_time TIMESTAMP, modified_time TIMESTAMP, suppliers_id INT, FOREIGN KEY (category_id) REFERENCES "+CATEGORIES_TABLE+" (category_id), FOREIGN KEY (inventory_id) REFERENCES "+INVENTORY_TABLE+" (inventory_id), FOREIGN KEY (suppliers_id) REFERENCES "+SUPPLIERS_TABLE+" (suppliers_id));")

            cursor.execute("DROP TABLE IF EXISTS "+ORDER_TABLE+" CASCADE;")
            cursor.execute("CREATE TABLE "+ORDER_TABLE+" (order_id INT PRIMARY KEY, order_date TIMESTAMP, delivery_id INT, customer_id INT, transaction_id INT, FOREIGN KEY (delivery_id) REFERENCES "+DELIVERY_TABLE+" (delivery_id), FOREIGN KEY (customer_id) REFERENCES "+CUSTOMER_TABLE+" (customer_id), FOREIGN KEY (transaction_id) REFERENCES "+TRANSACTION_TABLE+" (transaction_id));")

            cursor.execute("DROP TABLE IF EXISTS "+ORDER_ITEMS_TABLE+" CASCADE;")
            cursor.execute("CREATE TABLE "+ORDER_ITEMS_TABLE+" (order_item_id INT PRIMARY KEY, order_id INT, product_id INT, quantity INT, FOREIGN KEY (order_id) REFERENCES "+ORDER_TABLE+" (order_id), FOREIGN KEY (product_id) REFERENCES "+PRODUCTS_TABLE+" (product_id));")

            cursor.execute("DROP TABLE IF EXISTS "+REVIEWS_TABLE+" CASCADE;")
            cursor.execute("CREATE TABLE "+REVIEWS_TABLE+" (review_id INT PRIMARY KEY, rating TEXT, comment TEXT, reviewed_date TIMESTAMP, updated_date TIMESTAMP, customer_id INT, product_id INT, FOREIGN KEY (customer_id) REFERENCES "+CUSTOMER_TABLE+" (customer_id), FOREIGN KEY (product_id) REFERENCES "+PRODUCTS_TABLE+" (product_id));")
            
            print("Tables created successfully.")
            print()
            print()
            
    except Exception as error:
        print("Error creating tables:", error)
        print()
        print()
        
def insert_tables_data(conn):
    
    try:
        connection = conn
        cursor = connection.cursor()

        if connection and cursor:
            
            cursor.execute("INSERT INTO " + CATEGORIES_TABLE + " (category_id, category_name, category_desc, created_time, modified_time) VALUES (1, 'Electronics', 'Electronic products', TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'))")
            cursor.execute("INSERT INTO " + CATEGORIES_TABLE + " (category_id, category_name, category_desc, created_time, modified_time) VALUES (2, 'Clothing', 'Apparel and fashion', TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'))")
            cursor.execute("INSERT INTO " + CATEGORIES_TABLE + " (category_id, category_name, category_desc, created_time, modified_time) VALUES (3, 'Books', 'Literature and publications', TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'))")
            cursor.execute("INSERT INTO " + CATEGORIES_TABLE + " (category_id, category_name, category_desc, created_time, modified_time) VALUES (4, 'Home Appliances', 'Household gadgets', TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'))")
            cursor.execute("INSERT INTO " + CATEGORIES_TABLE + " (category_id, category_name, category_desc, created_time, modified_time) VALUES (5, 'Toys', 'Children playthings', TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'))")

            cursor.execute("INSERT INTO " + INVENTORY_TABLE + " (inventory_id, quantity, created_time, updated_time) VALUES (1, 100, TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'))")
            cursor.execute("INSERT INTO " + INVENTORY_TABLE + " (inventory_id, quantity, created_time, updated_time) VALUES (2, 200, TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'))")
            cursor.execute("INSERT INTO " + INVENTORY_TABLE + " (inventory_id, quantity, created_time, updated_time) VALUES (3, 50, TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'))")
            cursor.execute("INSERT INTO " + INVENTORY_TABLE + " (inventory_id, quantity, created_time, updated_time) VALUES (4, 320, TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'))")
            cursor.execute("INSERT INTO " + INVENTORY_TABLE + " (inventory_id, quantity, created_time, updated_time) VALUES (5, 75, TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'))")
            
            cursor.execute("INSERT INTO " + TRANSACTION_TABLE + " (transaction_id, order_id, amount, status, transaction_type) VALUES (1, 101, 50, 'Completed', 'Sale')")
            cursor.execute("INSERT INTO " + TRANSACTION_TABLE + " (transaction_id, order_id, amount, status, transaction_type) VALUES (2, 102, 75, 'Pending', 'Refund')")
            cursor.execute("INSERT INTO " + TRANSACTION_TABLE + " (transaction_id, order_id, amount, status, transaction_type) VALUES (3, 103, 120, 'Completed', 'Sale')")
            cursor.execute("INSERT INTO " + TRANSACTION_TABLE + " (transaction_id, order_id, amount, status, transaction_type) VALUES (4, 104, 40, 'Completed', 'Sale')")
            cursor.execute("INSERT INTO " + TRANSACTION_TABLE + " (transaction_id, order_id, amount, status, transaction_type) VALUES (5, 105, 90, 'Pending', 'Refund')")
            
            cursor.execute("INSERT INTO " + ADDRESS_TABLE + " (address_id, address_line1, address_line2, city, postal_code, country, address_type) VALUES (1, '123 Main St', '', 'Cityville', '12345', 'USA', 'Billing')")
            cursor.execute("INSERT INTO " + ADDRESS_TABLE + " (address_id, address_line1, address_line2, city, postal_code, country, address_type) VALUES (2, '456 Elm St', 'Apt 23H', 'Townsville', '54321', 'USA', 'Shipping')")
            cursor.execute("INSERT INTO " + ADDRESS_TABLE + " (address_id, address_line1, address_line2, city, postal_code, country, address_type) VALUES (3, '789 Oak St', '', 'Villagetown', '98765', 'Canada', 'Billing')")
            cursor.execute("INSERT INTO " + ADDRESS_TABLE + " (address_id, address_line1, address_line2, city, postal_code, country, address_type) VALUES (4, '101 Pine Rd', 'Apt 5J', 'Hamlet City', '45678', 'USA', 'Shipping')")
            cursor.execute("INSERT INTO " + ADDRESS_TABLE + " (address_id, address_line1, address_line2, city, postal_code, country, address_type) VALUES (5, '321 Cedar Ave', '', 'Forestville', '65432', 'USA', 'Billing')")
            cursor.execute("INSERT INTO " + ADDRESS_TABLE + " (address_id, address_line1, address_line2, city, postal_code, country, address_type) VALUES (6, '611 Rural Rd', '', 'Cityville', '12345', 'USA', 'Billing')")
            cursor.execute("INSERT INTO " + ADDRESS_TABLE + " (address_id, address_line1, address_line2, city, postal_code, country, address_type) VALUES (7, '654 Forest Ave', 'Apt 2B', 'Townsville', '54321', 'USA', 'Shipping')")
            cursor.execute("INSERT INTO " + ADDRESS_TABLE + " (address_id, address_line1, address_line2, city, postal_code, country, address_type) VALUES (8, '987 Farmer Ave', '', 'Villagetown', '98765', 'Canada', 'Billing')")
            cursor.execute("INSERT INTO " + ADDRESS_TABLE + " (address_id, address_line1, address_line2, city, postal_code, country, address_type) VALUES (9, '201 Salado Rd', 'Suite 100', 'Hamlet City', '45678', 'USA', 'Shipping')")
            cursor.execute("INSERT INTO " + ADDRESS_TABLE + " (address_id, address_line1, address_line2, city, postal_code, country, address_type) VALUES (10, '712 Jentily Dr ', '', 'Forestville', '65432', 'USA', 'Billing')")
            
            cursor.execute("INSERT INTO " + CONTACT_DETAILS_TABLE + " (contact_id, mobile_num, telephone_num, email, website) VALUES (1, '123-456-7890', '789-123-4567', 'john.doe@example.com', 'www.johndoe.com')")
            cursor.execute("INSERT INTO " + CONTACT_DETAILS_TABLE + " (contact_id, mobile_num, telephone_num, email, website) VALUES (2, '555-555-5555', '', 'jane.smith@example.com', 'www.janesmith.com')")
            cursor.execute("INSERT INTO " + CONTACT_DETAILS_TABLE + " (contact_id, mobile_num, telephone_num, email, website) VALUES (3, '987-654-3210', '123-987-6543', 'james.wilson@example.com', 'www.jameswilson.com')")
            cursor.execute("INSERT INTO " + CONTACT_DETAILS_TABLE + " (contact_id, mobile_num, telephone_num, email, website) VALUES (4, '111-222-3333', '333-222-1111', 'emily.jones@example.com', 'www.emilyjones.com')")
            cursor.execute("INSERT INTO " + CONTACT_DETAILS_TABLE + " (contact_id, mobile_num, telephone_num, email, website) VALUES (5, '777-888-9999', '999-888-7777', 'michael.brown@example.com', 'www.michaelbrown.com')")
            cursor.execute("INSERT INTO " + CONTACT_DETAILS_TABLE + " (contact_id, mobile_num, telephone_num, email, website) VALUES (6, '113-466-7880', '999-133-4167', 'tech.co@example.com', 'www.techco.com')")
            cursor.execute("INSERT INTO " + CONTACT_DETAILS_TABLE + " (contact_id, mobile_num, telephone_num, email, website) VALUES (7, '565-565-5665', '', 'fashion.world@example.com', 'www.fashionworld.com')")
            cursor.execute("INSERT INTO " + CONTACT_DETAILS_TABLE + " (contact_id, mobile_num, telephone_num, email, website) VALUES (8, '917-454-3110', '613-917-6443', 'furniture.emporium@example.com', 'www.furnitureemporium.com')")
            cursor.execute("INSERT INTO " + CONTACT_DETAILS_TABLE + " (contact_id, mobile_num, telephone_num, email, website) VALUES (9, '121-422-1353', '353-232-1010', 'book.haven@example.com', 'www.bookhaven.com')")
            cursor.execute("INSERT INTO " + CONTACT_DETAILS_TABLE + " (contact_id, mobile_num, telephone_num, email, website) VALUES (10, '747-818-9397', '929-484-1117', 'toy.universe@example.com', 'www.toyuniverse.com')")

            cursor.execute("INSERT INTO " + CUSTOMER_TABLE + " (customer_id, first_name, last_name, address_id, contact_id, created_time, modified_time) VALUES (1, 'John', 'Doe', 1, 1, TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'))")
            cursor.execute("INSERT INTO " + CUSTOMER_TABLE + " (customer_id, first_name, last_name, address_id, contact_id, created_time, modified_time) VALUES (2, 'Jane', 'Smith', 2, 2, TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'))")
            cursor.execute("INSERT INTO " + CUSTOMER_TABLE + " (customer_id, first_name, last_name, address_id, contact_id, created_time, modified_time) VALUES (3, 'James', 'Wilson', 3, 3, TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'))")
            cursor.execute("INSERT INTO " + CUSTOMER_TABLE + " (customer_id, first_name, last_name, address_id, contact_id, created_time, modified_time) VALUES (4, 'Emily', 'Jones', 4, 4, TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'))")
            cursor.execute("INSERT INTO " + CUSTOMER_TABLE + " (customer_id, first_name, last_name, address_id, contact_id, created_time, modified_time) VALUES (5, 'Michael', 'Brown', 5, 5, TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'))")
            
            cursor.execute("INSERT INTO " + SUPPLIERS_TABLE + " (suppliers_id, suppliers_name, category_type, address_id, contact_id) VALUES (1, 'TechCo', 'Electronics', 6, 6)")
            cursor.execute("INSERT INTO " + SUPPLIERS_TABLE + " (suppliers_id, suppliers_name, category_type, address_id, contact_id) VALUES (2, 'Fashion World', 'Clothing', 7, 7)")
            cursor.execute("INSERT INTO " + SUPPLIERS_TABLE + " (suppliers_id, suppliers_name, category_type, address_id, contact_id) VALUES (3, 'Furniture Emporium', 'Furniture', 8, 8)")
            cursor.execute("INSERT INTO " + SUPPLIERS_TABLE + " (suppliers_id, suppliers_name, category_type, address_id, contact_id) VALUES (4, 'Book Haven', 'Books', 9, 9)")
            cursor.execute("INSERT INTO " + SUPPLIERS_TABLE + " (suppliers_id, suppliers_name, category_type, address_id, contact_id) VALUES (5, 'Toy Universe', 'Toys', 10, 10)")
            
            cursor.execute("INSERT INTO " + DELIVERY_TABLE + " (delivery_id, type, status, address_id) VALUES (1, 'Standard', 'Shipped', 1)")
            cursor.execute("INSERT INTO " + DELIVERY_TABLE + " (delivery_id, type, status, address_id) VALUES (2, 'Express', 'Delivered', 2)")
            cursor.execute("INSERT INTO " + DELIVERY_TABLE + " (delivery_id, type, status, address_id) VALUES (3, 'Standard', 'In Transit', 3)")
            cursor.execute("INSERT INTO " + DELIVERY_TABLE + " (delivery_id, type, status, address_id) VALUES (4, 'Express', 'Shipped', 4)")
            cursor.execute("INSERT INTO " + DELIVERY_TABLE + " (delivery_id, type, status, address_id) VALUES (5, 'Standard', 'Delivered', 5)")
            
            cursor.execute("INSERT INTO " + PRODUCTS_TABLE + " (product_id, product_name, category_id, product_desc, product_code, price, inventory_id, created_time, modified_time, suppliers_id) VALUES (1, 'iPhone 15', 1, 'High-end smartphone', 'PRD001', 799, 1, TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), 1)")
            cursor.execute("INSERT INTO " + PRODUCTS_TABLE + " (product_id, product_name, category_id, product_desc, product_code, price, inventory_id, created_time, modified_time, suppliers_id) VALUES (2, 'H&M T-shirt', 2, 'Cotton T-shirt', 'PRD002', 19, 2, TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), 2)")
            cursor.execute("INSERT INTO " + PRODUCTS_TABLE + " (product_id, product_name, category_id, product_desc, product_code, price, inventory_id, created_time, modified_time, suppliers_id) VALUES (3, 'The Alchemist', 3, 'Best-selling novel', 'PRD003', 12, 3, TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), 3)")
            cursor.execute("INSERT INTO " + PRODUCTS_TABLE + " (product_id, product_name, category_id, product_desc, product_code, price, inventory_id, created_time, modified_time, suppliers_id) VALUES (4, 'GE Appliances Washing Machine', 4, 'Front-load washer', 'PRD004', 789, 4, TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), 4)")
            cursor.execute("INSERT INTO " + PRODUCTS_TABLE + " (product_id, product_name, category_id, product_desc, product_code, price, inventory_id, created_time, modified_time, suppliers_id) VALUES (5, 'Hot Wheels Toy Car', 5, 'Remote-controlled car', 'PRD005', 29, 5, TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), 5)")
                        
            cursor.execute("INSERT INTO " + ORDER_TABLE + " (order_id, order_date, delivery_id, customer_id, transaction_id) VALUES (1, TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), 1, 1, 1)")
            cursor.execute("INSERT INTO " + ORDER_TABLE + " (order_id, order_date, delivery_id, customer_id, transaction_id) VALUES (2, TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), 2, 2, 2)")
            cursor.execute("INSERT INTO " + ORDER_TABLE + " (order_id, order_date, delivery_id, customer_id, transaction_id) VALUES (3, TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), 3, 3, 3)")
            cursor.execute("INSERT INTO " + ORDER_TABLE + " (order_id, order_date, delivery_id, customer_id, transaction_id) VALUES (4, TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), 4, 4, 4)")
            cursor.execute("INSERT INTO " + ORDER_TABLE + " (order_id, order_date, delivery_id, customer_id, transaction_id) VALUES (5, TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), 5, 5, 5)")
            
            cursor.execute("INSERT INTO " + ORDER_ITEMS_TABLE + " (order_item_id, order_id, product_id, quantity) VALUES (1, 1, 1, 1)")
            cursor.execute("INSERT INTO " + ORDER_ITEMS_TABLE + " (order_item_id, order_id, product_id, quantity) VALUES (2, 2, 2, 4)")
            cursor.execute("INSERT INTO " + ORDER_ITEMS_TABLE + " (order_item_id, order_id, product_id, quantity) VALUES (3, 3, 3, 1)")
            cursor.execute("INSERT INTO " + ORDER_ITEMS_TABLE + " (order_item_id, order_id, product_id, quantity) VALUES (4, 4, 4, 1)")
            cursor.execute("INSERT INTO " + ORDER_ITEMS_TABLE + " (order_item_id, order_id, product_id, quantity) VALUES (5, 5, 5, 3)")

            cursor.execute("INSERT INTO " + REVIEWS_TABLE + " (review_id, rating, comment, reviewed_date, updated_date, customer_id, product_id) VALUES (1, '5 stars', 'Great product!', TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), 1, 1)")
            cursor.execute("INSERT INTO " + REVIEWS_TABLE + " (review_id, rating, comment, reviewed_date, updated_date, customer_id, product_id) VALUES (2, '4 stars', 'Nice quality.', TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), 2, 2)")
            cursor.execute("INSERT INTO " + REVIEWS_TABLE + " (review_id, rating, comment, reviewed_date, updated_date, customer_id, product_id) VALUES (3, '3 stars', 'Average book.', TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), 3, 3)")
            cursor.execute("INSERT INTO " + REVIEWS_TABLE + " (review_id, rating, comment, reviewed_date, updated_date, customer_id, product_id) VALUES (4, '5 stars', 'Best washer ever!', TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), 4, 4)")
            cursor.execute("INSERT INTO " + REVIEWS_TABLE + " (review_id, rating, comment, reviewed_date, updated_date, customer_id, product_id) VALUES (5, '4 stars', 'Fun toy!', TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS'), 5, 5)")

            print("Inserted data into tables successfully.")
            print()
            print()

    except Exception as error:
        print("Error inserting data into tables:", error)
        print()
        print()
   
def retrieve_tables_data(conn):
    
    try:
        connection = conn
        cursor = connection.cursor()

        if connection and cursor:
            for table_name in tables:
                cursor.execute(f"SELECT * FROM {table_name}")
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                data_list = [list(row) for row in data]
                
                print(f"Table: {table_name}")
                print(tabulate(data_list, headers=columns, tablefmt="grid"))
                print() 
                
    except Exception as error:
        print("Error selecting data from tables:", error)
        
if __name__ == '__main__':

    create_database(DATABASE_NAME)

    with connect_postgres(dbname=DATABASE_NAME) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        create_tables(conn)
        insert_tables_data(conn)
        retrieve_tables_data(conn)

        print('Done')
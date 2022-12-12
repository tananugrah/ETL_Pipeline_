import os
import sys
sys.path.append(os.getcwd())
import time

import asyncio

from src import Connection
from TransactionData import DataOperation
from query.SqlCommand import QueryServices

start_time = time.time()
loop = asyncio.new_event_loop()

if __name__ == '__main__':
    #### set connection postgre ######
    conn_pg = loop.run_until_complete(
        Connection.DBClient.ConnectionDB()
    )

    CT_customers = loop.run_until_complete(
        DataOperation.CreateTable(
            connection = conn_pg,
            query = QueryServices.Create_Table.format(
                Schema = 'integration',
                Table = 'customers',
                Columns = '''customer_id VARCHAR(50) PRIMARY KEY NOT NULL,
                customer_unique_id VARCHAR(50) NOT NULL,
                customer_zip_code_prefix VARCHAR(10),
                customer_city VARCHAR(50) NOT NULL,
                customer_state VARCHAR(50) NOT NULL
                '''
            )
        )
    )
    print(CT_customers)
    print("--- state [customers] %s seconds ---" % (time.time() - start_time))


    CT_geolocation = loop.run_until_complete(
        DataOperation.CreateTable(
            connection = conn_pg,
            query = QueryServices.Create_Table.format(
                Schema = 'integration',
                Table = 'geolocation',
                Columns = '''
                geolocation_zip_code_prefix VARCHAR(10),
                geolocation_lat VARCHAR(50),
                geolocation_lng VARCHAR(50),
                geolocation_city VARCHAR(50),
                geolocation_state VARCHAR(5)
                '''
            )
        )
    )
    print(CT_geolocation)
    print("--- state [geolocation] %s seconds ---" % (time.time() - start_time))


    CT_sellers = loop.run_until_complete(
        DataOperation.CreateTable(
            connection = conn_pg,
            query = QueryServices.Create_Table.format(
                Schema = 'integration',
                Table = 'sellers',
                Columns = ''' 
                seller_id VARCHAR(50) PRIMARY KEY NOT NULL,
                seller_zip_code_prefix VARCHAR(50),
                seller_city VARCHAR(50),
                seller_state VARCHAR(5)
                '''
            )
        )
    )
    print(CT_sellers)
    print("--- state [sellers] %s seconds ---" % (time.time() - start_time))
   

    CT_product_category_name_translation = loop.run_until_complete(
        DataOperation.CreateTable(
            connection = conn_pg,
            query = QueryServices.Create_Table.format(
                Schema = 'integration',
                Table = 'product_category_name_translation',
                Columns = ''' 
                product_category_name VARCHAR(100) PRIMARY KEY,
                product_category_name_english VARCHAR(100)
                '''
            )
        )
    )
    print(CT_product_category_name_translation)
    print("--- state [product_category_name_translation] %s seconds ---" % (time.time() - start_time))


    CT_products = loop.run_until_complete(
        DataOperation.CreateTable(
            connection = conn_pg,
            query = QueryServices.Create_Table.format(
                Schema = 'integration',
                Table = 'products',
                Columns = ''' 
                product_id VARCHAR(50) PRIMARY KEY NOT NULL,
                product_category_name VARCHAR(50),
                product_name_lenght VARCHAR(50),
                product_description_lenght VARCHAR(50),
                product_photos_qty VARCHAR(10),
                product_weight_g VARCHAR(10),
                product_length_cm VARCHAR(10),
                product_height_cm VARCHAR(10),
                product_width_cm VARCHAR(10)
                '''
            )
        )
    )
    print(CT_products)
    print("--- state [products] %s seconds ---" % (time.time() - start_time))


    CT_orders = loop.run_until_complete(
        DataOperation.CreateTable(
            connection = conn_pg,
            query = QueryServices.Create_Table.format(
                Schema = 'integration',
                Table = 'orders',
                Columns = ''' 
                order_id VARCHAR(50) PRIMARY KEY NOT NULL,
                customer_id VARCHAR(50) NOT NULL,
                order_status VARCHAR(15) NOT NULL,
                order_purchase_timestamp TIMESTAMP,
                order_approved_at TIMESTAMP,
                order_delivered_carrier_date TIMESTAMP,
                order_delivered_customer_date TIMESTAMP,
                order_estimated_delivery_date TIMESTAMP
                ''' 
            )
        )
    )
    print(CT_orders)
    print("--- state [orders] %s seconds ---" % (time.time() - start_time))


    CT_order_items = loop.run_until_complete(
        DataOperation.CreateTable(
            connection = conn_pg,
            query = QueryServices.Create_Table.format(
                Schema = 'integration',
                Table = 'order_items',
                Columns = ''' 
                order_id VARCHAR(50) NOT NULL,
                order_item_id VARCHAR(50) NOT NULL,
                product_id VARCHAR(50) NOT NULL,
                seller_id VARCHAR(50) NOT NULL,
                shipping_limit_date TIMESTAMP NOT NULL,
                price VARCHAR(10),
                freight_value VARCHAR(10)
                '''
            )
        )
    )
    print(CT_order_items)
    print("--- state [order_items] %s seconds ---" % (time.time() - start_time))


    CT_order_payments = loop.run_until_complete(
        DataOperation.CreateTable(
            connection = conn_pg,
            query = QueryServices.Create_Table.format(
                Schema = 'integration',
                Table = 'order_payments',
                Columns = ''' 
                order_id VARCHAR(50) NOT NULL,
                payment_sequential VARCHAR(5) NOT NULL,
                payment_type VARCHAR(50) NOT NULL,
                payment_installments VARCHAR(5) NOT NULL,
                payment_value VARCHAR(10) NOT NULL
                ''' 
            )
        )
    )
    print(CT_order_payments)
    print("--- state [order_payments] %s seconds ---" % (time.time() - start_time))


    CT_order_reviews = loop.run_until_complete(
        DataOperation.CreateTable(
            connection = conn_pg,
            query = QueryServices.Create_Table.format(
                Schema = 'integration',
                Table = 'order_reviews',
                Columns = ''' 
                review_id VARCHAR(50) NOT NULL,
                order_id VARCHAR(50) NOT NULL,
                review_score VARCHAR(10),
                review_comment_title VARCHAR(255),
                review_comment_message VARCHAR(255),
                review_creation_date TIMESTAMP,
                review_answer_timestamp TIMESTAMP
                '''
            )
        )
    )
    print(CT_order_reviews)
    print("--- state [order_reviews] %s seconds ---" % (time.time() - start_time))


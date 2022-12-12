import os
import sys
sys.path.append(os.getcwd())
import time

import asyncio
import awswrangler as wr
import pandas as pd

from src import Connection
from TransactionData import DataOperation
from query.SqlCommand import QueryServices
from query.SqlCleansing import querycleaning

start_time = time.time()
loop = asyncio.new_event_loop()

if __name__ == '__main__':
    #### set connection postgre ######
    conn_pg = loop.run_until_complete(
        Connection.DBClient.ConnectionDB()
    )

    conn_pg1 = loop.run_until_complete(
        Connection.DBClient.ConnectionDBpg2()
    )

    # CT_geolocation = loop.run_until_complete(
    #     DataOperation.CreateTable(
    #         connection = conn_pg,
    #         query = QueryServices.Create_Table.format(
    #             Schema = 'public',
    #             Table = 'geolocation',
    #             Columns = '''
    #             geolocation_id serial PRIMARY KEY,
    #             geolocation_zip_code_prefix VARCHAR(10),
    #             latlong point,
    #             geolocation_lat VARCHAR(50),
    #             geolocation_lng VARCHAR(50),
    #             geolocation_city VARCHAR(50),
    #             geolocation_state VARCHAR(5),
    #             sequence_data int
    #             '''
    #         )
    #     )
    # )
    # print(CT_geolocation)
    # print("--- state [stag] %s seconds ---" % (time.time() - start_time))

    # CT_product_category_name_translation = loop.run_until_complete(
    #     DataOperation.CreateTable(
    #         connection = conn_pg,
    #         query = QueryServices.Create_Table.format(
    #             Schema = 'public',
    #             Table = 'product_category_name_translation',
    #             Columns = ''' 
    #             product_category_name VARCHAR(100) PRIMARY KEY,
    #             product_category_name_english VARCHAR(100)
    #             '''
    #         )
    #     )
    # )
    # print(CT_product_category_name_translation)
    # print("--- state [stag] %s seconds ---" % (time.time() - start_time))

    # CT_customers = loop.run_until_complete(
    #     DataOperation.CreateTable(
    #         connection = conn_pg,
    #         query = QueryServices.Create_Table.format(
    #             Schema = 'public',
    #             Table = 'customers',
    #             Columns = ''' 
    #             customer_id VARCHAR(50) PRIMARY KEY NOT NULL,
    #             customer_unique_id VARCHAR(50) NOT NULL,
    #             customer_zip_code_prefix INT,
    #             customer_city VARCHAR(50) NOT NULL,
    #             customer_state VARCHAR(50) NOT NULL
    #             '''
    #         )
    #     )
    # )
    # print(CT_customers)
    # print("--- state [stag] %s seconds ---" % (time.time() - start_time))

    # CT_sellers = loop.run_until_complete(
    #     DataOperation.CreateTable(
    #         connection = conn_pg,
    #         query = QueryServices.Create_Table.format(
    #             Schema = 'public',
    #             Table = 'sellers',
    #             Columns = ''' 
    #             seller_id VARCHAR(50) PRIMARY KEY NOT NULL,
    #             seller_zip_code_prefix INT,
    #             seller_city VARCHAR(50),
    #             seller_state VARCHAR(5)
    #             '''
    #         )
    #     )
    # )
    # print(CT_sellers)
    # print("--- state [sellers] %s seconds ---" % (time.time() - start_time))

    # CT_orders = loop.run_until_complete(
    #     DataOperation.CreateTable(
    #         connection = conn_pg,
    #         query = QueryServices.Create_Table.format(
    #             Schema = 'public',
    #             Table = 'orders',
    #             Columns = ''' 
    #             order_id VARCHAR(50) PRIMARY KEY NOT NULL,
    #             customer_id VARCHAR(50) NOT NULL,
    #             order_status VARCHAR(15) NOT NULL,
    #             order_purchase_timestamp TIMESTAMP,
    #             order_approved_at TIMESTAMP,
    #             order_delivered_carrier_date TIMESTAMP,
    #             order_delivered_customer_date TIMESTAMP,
    #             order_estimated_delivery_date TIMESTAMP
    #             '''
    #         )
    #     )
    # )
    # print(CT_orders)
    # print("--- state [orders] %s seconds ---" % (time.time() - start_time))

    # CT_products = loop.run_until_complete(
    #     DataOperation.CreateTable(
    #         connection = conn_pg,
    #         query = QueryServices.Create_Table.format(
    #             Schema = 'public',
    #             Table = 'products',
    #             Columns = ''' 
    #             product_id VARCHAR(50) PRIMARY KEY NOT NULL,
    #             product_category_name VARCHAR(50),
    #             product_name_lenght INT,
    #             product_description_lenght INT,
    #             product_photos_qty INT,
    #             product_weight_g INT,
    #             product_length_cm INT,
    #             product_height_cm INT,
    #             product_width_cm INT
    #             '''
    #         )
    #     )
    # )
    # print(CT_products)
    # print("--- state [products] %s seconds ---" % (time.time() - start_time))

    # CT_order_reviews = loop.run_until_complete(
    #     DataOperation.CreateTable(
    #         connection = conn_pg,
    #         query = QueryServices.Create_Table.format(
    #             Schema = 'public',
    #             Table = 'order_reviews',
    #             Columns = ''' 
    #             review_order_id VARCHAR(100) PRIMARY KEY NOT NULL,
    #             review_id VARCHAR(50) NOT NULL,
    #             order_id VARCHAR(50) NOT NULL,
    #             review_score INT,
    #             review_comment_title TEXT,
    #             review_comment_message TEXT,
    #             review_creation_date TIMESTAMP,
    #             review_answer_timestamp TIMESTAMP
    #             '''
    #         )
    #     )
    # )
    # print(CT_order_reviews)
    # print("--- state [order_reviews] %s seconds ---" % (time.time() - start_time))

    # CT_order_items = loop.run_until_complete(
    #     DataOperation.CreateTable(
    #         connection = conn_pg,
    #         query = QueryServices.Create_Table.format(
    #             Schema = 'public',
    #             Table = 'order_items',
    #             Columns = ''' 
    #             order_product_id serial PRIMARY KEY NOT NULL,
    #             order_id VARCHAR(50) NOT NULL,
    #             order_item_id INT NOT NULL,
    #             product_id VARCHAR(50) NOT NULL,
    #             seller_id VARCHAR(50) NOT NULL,
    #             shipping_limit_date TIMESTAMP NOT NULL,
    #             price DOUBLE PRECISION,
    #             freight_value VARCHAR(10)
    #             '''
    #         )
    #     )
    # )
    # print(CT_order_items)
    # print("--- state [order_items] %s seconds ---" % (time.time() - start_time))

    # CT_order_payments = loop.run_until_complete(
    #     DataOperation.CreateTableAs(
    #         connection = conn_pg,
    #         query = QueryServices.Create_Table.format(
    #             Schema = 'public',
    #             Table = 'order_payments',
    #             Columns = ''' 
    #             order_payments_id serial PRIMARY KEY not null,
    #             order_id varchar(50),
    #             payment_sequential INT,
    #             payment_type VARCHAR(50),
    #             payment_installments int,
    #             payment_value numeric 
    #             '''
    #         )
    #     )
    # )
    # print(CT_order_payments)
    # print("--- state [order_payments] %s seconds ---" % (time.time() - start_time))

    
    # CT_order_payments = loop.run_until_complete(
    #     DataOperation.CreateTableAs(
    #         connection = conn_pg,
    #         query = querycleaning.createtableas.format(
    #             Table_new = 'order_payments',
    #             Table = 'integration.order_payments',
    #             Columns = ''' 
    #             cast(order_payments_id as int) order_payments_id,
    #             cast(order_id as varchar(50)) order_id,
    #             cast(payment_sequential as int) payment_sequential,
    #             cast(payment_type as varchar(50)) payment_type,
    #             cast(payment_installments as int) payment_installments,
    #             cast(payment_value as float) payment_value
    #             ''',
    #             set_column = 'order_payments_id'
    #         )
    #     )
    # )
    # print(CT_order_payments)
    # print("--- state [order_payments] %s seconds ---" % (time.time() - start_time))

    
    



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



    CT_customers_clean = loop.run_until_complete(
        DataOperation.CreateTable(
            connection = conn_pg,
            query = querycleaning.createtableas.format(
                Table_new = 'customers',
                Table = 'integration.customers',
                Columns = ''' 
                cast(customer_id as varchar(50)) customer_id,
                cast(customer_unique_id as varchar(50)) customer_unique_id,
                cast(customer_zip_code_prefix as int) customer_zip_code_prefix,
                cast(customer_city as varchar(50)) customer_city,
                cast(customer_state as varchar(5)) customer_state
                ''',
                set_column_pk = 'customer_id'
            )
        )
    )
    print(CT_customers_clean)
    print("--- state [CT_customers_clean] %s seconds ---" % (time.time() - start_time))

    CT_geolocation_clean = loop.run_until_complete(
        DataOperation.CreateTable(
            connection = conn_pg,
            query = querycleaning.createtableas.format(
                Table_new = 'geolocation',
                Table = 'integration.geolocation',
                Columns = ''' 
                    geolocation_id,
                    cast(geolocation_zip_code_prefix as INT) geolocation_zip_code_prefix,
                    geolocation_lat,
                    geolocation_lng,
                    geolocation_city,
                    geolocation_state
                ''',
                set_column_pk = 'geolocation_id'
            )
        )
    )
    print(CT_geolocation_clean)
    print("--- state [tb_geolocation_clean] %s seconds ---" % (time.time() - start_time))

    CT_seller_clean = loop.run_until_complete(
        DataOperation.CreateTable(
            connection = conn_pg,
            query = querycleaning.createtableas.format(
                Table_new = 'sellers',
                Table = 'integration.sellers',
                Columns = ''' 
                    seller_id,
                    cast(seller_zip_code_prefix as INT) seller_zip_code_prefix,
                    seller_city,
                    seller_state
                ''',
                set_column_pk = 'seller_id'
            )
        )
    )
    print(CT_seller_clean)
    print("--- state [tb_seller_clean] %s seconds ---" % (time.time() - start_time))


    CT_product_category_name_clean = loop.run_until_complete(
        DataOperation.CreateTable(
            connection = conn_pg,
            query = querycleaning.createtableas.format(
                Table_new = 'product_category_name_translation',
                Table = 'integration.product_category_name_translation',
                Columns = ''' 
                    product_category_name,
                    product_category_name_english
                    ''',
                set_column_pk = 'product_category_name'
            )
        )
    )
    print(CT_product_category_name_clean)
    print("--- state [product_category_name_clean] %s seconds ---" % (time.time() - start_time))

    CT_products_clean = loop.run_until_complete(
        DataOperation.CreateTable(
            connection = conn_pg,
            query = querycleaning.createtableas.format(
                Table_new = 'products',
                Table = 'integration.products',
                Columns = ''' 
                    product_id,
                    product_category_name,
                    cast(product_name_lenght as INT) product_name_lenght,
                    cast(product_description_lenght as INT) product_description_lenght,
                    cast(product_photos_qty as INT) product_photos_qty,
                    cast(product_weight_g as INT) product_weight_g,
                    cast(product_length_cm as INT) product_length_cm,
                    cast(product_height_cm as INT) product_height_cm, 
                    cast(product_width_cm as INT) product_width_cm
                    ''',
                set_column_pk = 'product_id'
            )
        )
    )
    print(CT_products_clean)
    print("--- state [product_clean] %s seconds ---" % (time.time() - start_time))


    CT_orders_clean = loop.run_until_complete(
        DataOperation.CreateTable(
            connection = conn_pg,
            query = querycleaning.createtableas.format(
                Table_new = 'orders',
                Table = 'integration.orders',
                Columns = ''' 
                    order_id,
                    customer_id,
                    order_status,
                    cast(order_purchase_timestamp as DATE) order_purchase_timestamp,
                    cast(order_approved_at as DATE) order_approved_at,
                    cast(order_delivered_carrier_date as DATE) order_delivered_carrier_date,
                    cast(order_delivered_customer_date as DATE) order_delivered_customer_date,
                    cast(order_estimated_delivery_date as DATE) order_estimated_delivery_date
                    ''',
                set_column_pk = 'order_id'
            )
        )
    )
    print(CT_orders_clean)
    print("--- state [orders_clean] %s seconds ---" % (time.time() - start_time))

    CT_orders_items = loop.run_until_complete(
        DataOperation.CreateTable(
            connection = conn_pg,
            query = querycleaning.createtableas.format(
                Table_new = 'order_items',
                Table = 'integration.order_items',
                Columns = ''' 
                    order_product_id,
                    order_id,
                    cast(order_item_id as INT) order_item_id,
                    product_id,
                    seller_id,
                    cast(shipping_limit_date as DATE) shipping_limit_date,
                    cast(price as NUMERIC) price,
                    cast(freight_value as NUMERIC) freight_value
                    ''',
                set_column_pk = 'order_product_id'
            )
        )
    )
    print(CT_orders_items)
    print("--- state [orders_items] %s seconds ---" % (time.time() - start_time))


    CT_order_payments = loop.run_until_complete(
        DataOperation.CreateTableAs(
            connection = conn_pg,
            query = querycleaning.createtableas.format(
                Table_new = 'order_payments',
                Table = 'integration.order_payments',
                Columns = ''' 
                order_payment_id,
                cast(order_id as varchar(50)) order_id,
                cast(payment_sequential as int) payment_sequential,
                cast(payment_type as varchar(50)) payment_type,
                cast(payment_installments as int) payment_installments,
                cast(payment_value as NUMERIC) payment_value
                ''',
                set_column_pk = 'order_payment_id'
            )
        )
    )
    print(CT_order_payments)
    print("--- state [order_payments] %s seconds ---" % (time.time() - start_time))


    CT_order_items_reviews = loop.run_until_complete(
        DataOperation.CreateTable(
            connection = conn_pg,
            query = querycleaning.createtableas.format(
                Table_new = 'order_reviews',
                Table = 'integration.order_reviews',
                Columns = ''' 
                    review_order_id,
                    review_id,
                    order_id,
                    cast(review_score as INT) review_score,
                    review_comment_title,
                    review_comment_message,
                    cast(review_creation_date as DATE) review_creation_date,
                    cast(review_answer_timestamp as DATE) review_answer_timestamp
                    ''',
                set_column_pk = 'review_order_id'
            )
        )
    )
    print(CT_order_items_reviews)
    print("--- state [order_items_reviews] %s seconds ---" % (time.time() - start_time))

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
    

#################### import csv to database ########################
    Import_CSV_customers = loop.run_until_complete(
        DataOperation.Import_Csv(
            connection=conn_pg,
            table='customers',
            schema='integration',
            path = '/home/tananugrahr/data-2/customers_dataset.csv'
        )
    )
    print(f"Table Customers {Import_CSV_customers} rows")
    print("--- state [customers] %s seconds ---" % (time.time() - start_time))

    Import_CSV_geolocation = loop.run_until_complete(
        DataOperation.Import_Csv(
            connection=conn_pg,
            table='geolocation',
            schema='integration',
            path = '/home/tananugrahr/data-2/geolocation_dataset.csv'
        )
    )
    print(f"Table geolocation {Import_CSV_geolocation} rows")
    print("--- state [customers] %s seconds ---" % (time.time() - start_time))

    Import_CSV_sellers = loop.run_until_complete(
        DataOperation.Import_Csv(
            connection=conn_pg,
            table='sellers',
            schema='integration',
            path = '/home/tananugrahr/data-2/sellers_dataset.csv'
        )
    )
    print(f"Table sellers {Import_CSV_sellers} rows")
    print("--- state [sellers] %s seconds ---" % (time.time() - start_time))

    Import_CSV_product_category_name_translation = loop.run_until_complete(
        DataOperation.Import_Csv(
            connection=conn_pg,
            table='product_category_name_translation',
            schema='integration',
            path = '/home/tananugrahr/data-2/product_category_name_translation.csv'
        )
    )
    print(f"Table product_category_name_translation {Import_CSV_product_category_name_translation} rows")
    print("--- state [product_category_name_translation] %s seconds ---" % (time.time() - start_time))

    Import_CSV_products = loop.run_until_complete(
            DataOperation.Import_Csv(
                connection=conn_pg,
                table='products',
                schema='integration',
                path = '/home/tananugrahr/data-2/products_dataset.csv'
            )
        )
    print(f"Table products {Import_CSV_products} rows")
    print("--- state [products] %s seconds ---" % (time.time() - start_time))

    Import_CSV_orders = loop.run_until_complete(
        DataOperation.Import_Csv(
            connection=conn_pg,
            table='orders',
            schema='integration',
            path = '/home/tananugrahr/data-2/orders_dataset.csv'
        )
    )
    print(f"Table orders {Import_CSV_orders} rows")
    print("--- state [orders] %s seconds ---" % (time.time() - start_time))


    Import_CSV_order_items = loop.run_until_complete(
        DataOperation.Import_Csv(
            connection=conn_pg,
            table='order_items',
            schema='integration',
            path = '/home/tananugrahr/data-2/order_items_dataset.csv'
        )
    )
    print(f"Table order_items {Import_CSV_order_items} rows")
    print("--- state [order_items] %s seconds ---" % (time.time() - start_time))

    Import_CSV_order_payments = loop.run_until_complete(
        DataOperation.Import_Csv(
            connection=conn_pg,
            table='order_payments',
            schema='integration',
            path = '/home/tananugrahr/data-2/order_payments_dataset.csv'
        )
    )
    print(f"Table order_payments,{Import_CSV_order_payments} rows")
    print("--- state [order_payments] %s seconds ---" % (time.time() - start_time))

    Import_CSV_order_reviews = loop.run_until_complete(
        DataOperation.Import_Csv(
            connection=conn_pg,
            table='order_reviews',
            schema='integration',
            path = '/home/tananugrahr/data-2/order_reviews_dataset.csv'
        )
    )
    print(f"Table order_reviews {Import_CSV_order_reviews} rows")
    print("--- state [order_reviews] %s seconds ---" % (time.time() - start_time))




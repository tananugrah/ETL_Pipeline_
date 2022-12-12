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


################ hanya digunakan sekali #################
############################create Foreign Key ###################################
    FK_orders = loop.run_until_complete(
        DataOperation.Alter_tb(
            connection = conn_pg,
            query = QueryServices.Create_FK.format(
                Table = 'integration.orders',
                constraint_name = 'order_customers',
                fk_columns = 'customer_id',
                parent_table = 'integration.customers',
                fk_parent_key_columns = 'customer_id'
            )
        )
    )
    print(FK_orders)
    print("--- state [order] %s seconds ---" % (time.time() - start_time))


    FK_order_reviews = loop.run_until_complete(
        DataOperation.Alter_tb(
            connection = conn_pg,
            query = QueryServices.Create_FK.format(
                Table = 'integration.order_reviews',
                constraint_name = 'order_reviews',
                fk_columns = 'order_id',
                parent_table = 'integration.orders',
                fk_parent_key_columns = 'order_id'
            )
        )
    )
    print(FK_order_reviews)
    print("--- state [order_reviews] %s seconds ---" % (time.time() - start_time))

    FK_order_payments = loop.run_until_complete(
        DataOperation.Alter_tb(
            connection = conn_pg,
            query = QueryServices.Create_FK.format(
                Table = 'integration.order_payments',
                constraint_name = 'order_payments',
                fk_columns = 'order_id',
                parent_table = 'integration.orders',
                fk_parent_key_columns = 'order_id'
            )
        )
    )
    print(FK_order_payments)
    print("--- state [order_payments] %s seconds ---" % (time.time() - start_time))

    FK_order_items_orders = loop.run_until_complete(
        DataOperation.Alter_tb(
            connection = conn_pg,
            query = QueryServices.Create_FK.format(
                Table = 'integration.order_items',
                constraint_name = 'order_items_order',
                fk_columns = 'order_id',
                parent_table = 'integration.orders',
                fk_parent_key_columns = 'order_id'
            )
        )
    )
    print(FK_order_items_orders)
    print("--- state [order_items_order] %s seconds ---" % (time.time() - start_time))

    FK_order_items_product = loop.run_until_complete(
        DataOperation.Alter_tb(
            connection = conn_pg,
            query = QueryServices.Create_FK.format(
                Table = 'integration.order_items',
                constraint_name = 'order_items_product',
                fk_columns = 'product_id',
                parent_table = 'integration.products',
                fk_parent_key_columns = 'product_id'
            )
        )
    )
    print(FK_order_items_product)
    print("--- state [order_items_product] %s seconds ---" % (time.time() - start_time))


    FK_order_items_sellers = loop.run_until_complete(
        DataOperation.Alter_tb(
            connection = conn_pg,
            query = QueryServices.Create_FK.format(
                Table = 'integration.order_items',
                constraint_name = 'order_items_sellers',
                fk_columns = 'seller_id',
                parent_table = 'integration.sellers',
                fk_parent_key_columns = 'seller_id'
            )
        )
    )
    print(FK_order_items_sellers)
    print("--- state [order_items_seller] %s seconds ---" % (time.time() - start_time))



    ############## hanya digunakan sekali########
#################################create indexs on table####################
    CI_customers_raw = loop.run_until_complete(
        DataOperation.CreateTable(
            connection=conn_pg,
            query=QueryServices.Create_Index.format(
                Table = 'customers',
                Schema = 'integration',
                Columns = 'customer_id,customer_city'
            )
        )
    ) 
    print("Create Index on Table Customers",CI_customers_raw)   

    CI_geolocation_raw = loop.run_until_complete(
        DataOperation.CreateTable(
            connection=conn_pg,
            query=QueryServices.Create_Index.format(
                Table = 'geolocation',
                Schema = 'integration',
                Columns = 'geolocation_zip_code_prefix,geolocation_lat,geolocation_lng,geolocation_city'
            )
        )
    ) 
    print("Create Index on Table geolocation",CI_geolocation_raw) 

    CI_order_items_raw = loop.run_until_complete(
        DataOperation.CreateTable(
            connection=conn_pg,
            query=QueryServices.Create_Index.format(
                Table = 'order_items',
                Schema = 'integration',
                Columns = 'order_id,product_id,seller_id,price'
            )
        )
    ) 
    print("Create Index on Table order_items",CI_order_items_raw) 

    CI_order_payments_raw= loop.run_until_complete(
        DataOperation.CreateTable(
            connection=conn_pg,
            query=QueryServices.Create_Index.format(
                Table = 'order_payments',
                Schema = 'integration',
                Columns = 'order_id,payment_type,payment_value'
            )
        )
    ) 
    print("Create Index on Table order_payments_raw",CI_order_payments_raw) 

    CI_order_reviews_raw= loop.run_until_complete(
        DataOperation.CreateTable(
            connection=conn_pg,
            query=QueryServices.Create_Index.format(
                Table = 'order_reviews',
                Schema = 'integration',
                Columns = 'order_id,review_creation_date,review_answer_timestamp'
            )
        )
    ) 
    print("Create Index on Table order_reviews_raw",CI_order_reviews_raw) 

    CI_orders_raw= loop.run_until_complete(
        DataOperation.CreateTable(
            connection=conn_pg,
            query=QueryServices.Create_Index.format(
                Table = 'orders',
                Schema = 'integration',
                Columns = 'order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,order_delivered_carrier_date,order_delivered_customer_date,order_delivered_customer_date'
            )
        )
    ) 
    print("Create Index on Table orders_raw",CI_orders_raw) 

    CI_product_category_name_translation_raw= loop.run_until_complete(
        DataOperation.CreateTable(
            connection=conn_pg,
            query=QueryServices.Create_Index.format(
                Table = 'product_category_name_translation',
                Schema = 'integration',
                Columns = 'product_category_name'
            )
        )
    ) 
    print("Create Index on Table product_category_name_translation",CI_product_category_name_translation_raw) 

    CI_product_raw= loop.run_until_complete(
        DataOperation.CreateTable(
            connection=conn_pg,
            query=QueryServices.Create_Index.format(
                Table = 'products',
                Schema = 'integration',
                Columns = 'product_id,product_category_name'
            )
        )
    ) 
    print("Create Index on Table product_raw",CI_product_raw) 

    CI_sellers_raw= loop.run_until_complete(
        DataOperation.CreateTable(
            connection=conn_pg,
            query=QueryServices.Create_Index.format(
                Table = 'sellers',
                Schema = 'integration',
                Columns = 'seller_id,seller_city'
            )
        )
    ) 
    print("Create Index on Table sellers_raw",CI_sellers_raw) 

    
    

    



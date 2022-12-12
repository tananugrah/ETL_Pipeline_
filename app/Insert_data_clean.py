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
from query.SqlCommandMart import Query

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


    # ### extract data customers######
    extract_customers = loop.run_until_complete(
                        DataOperation.Extract(
                            connection= conn_pg, 
                            query= querycleaning.customers.format()
                        )
                    )
        
    print(len(extract_customers))
    df = pd.DataFrame(
            extract_customers,
            columns=[
                'customer_id',
                'customer_unique_id',
                'customer_zip_code_prefix',
                'customer_city',
                'customer_state' 
                ]
        )
    print(df)
    print(df.info())
    print("--- state [1] %s seconds ---" % (time.time() - start_time))


    ### insert data customers######
    con_insert = conn_pg1
    insert_data_customers = loop.run_until_complete(
                DataOperation.ExecutedInsertDataWrr(
                    df= df,
                    connection= con_insert,
                    schemas= 'public',
                    tables = 'customers',
                    primary_key_column=[
                        'customer_id'
                        ],
                     datatype = [{
                        'customer_id' : 'VARCHAR' ,
                        'customer_unique_id' : 'VARCHAR',
                        'customer_zip_code_prefix' : 'VARCHAR',
                        'customer_city': 'VARCHAR',
                        'customer_state' : 'VARCHAR'   
                    }]   
                )
            )

    print(insert_data_customers)
    print("--- state [customers] %s seconds ---" % (time.time() - start_time))



### extract data geolocation######
    extract_geolocation = loop.run_until_complete(
                        DataOperation.Extract(
                            connection= conn_pg, 
                            query= querycleaning.geolocation.format()
                        )
                    )
        
    print(len(extract_geolocation))
    df = pd.DataFrame(
            extract_geolocation,
            columns=[
                "geolocation_id",
                "geolocation_zip_code_prefix",
                "geolocation_lat",
                "geolocation_lng",
                "geolocation_city",
                "geolocation_state"
                ]
        )
    print(df)
    print(df.info())
    print("--- state [1] %s seconds ---" % (time.time() - start_time))


### insert data geolocation######
    con_insert = conn_pg1
    insert_data_geolocation = loop.run_until_complete(
                DataOperation.ExecutedInsertDataWrr(
                    df= df,
                    connection= con_insert,
                    schemas= 'public',
                    tables = 'geolocation',
                    primary_key_column=[
                        'geolocation_id'
                        ],
                    datatype = [{
                        "geolocation_id" : "VARCHAR",
                        "geolocation_zip_code_prefix": "INT",
                        "geolocation_lat" : "VARCHAR",
                        "geolocation_lng" : "VARCHAR",
                        "geolocation_city" : "VARCHAR",
                        "geolocation_state": "VARCHAR"  
                    }] 
                )
            )

    print(insert_data_geolocation)
    print(df.info())
    print("--- state [geolocation] %s seconds ---" % (time.time() - start_time))


### extract data sellers######
    extract_sellers = loop.run_until_complete(
                        DataOperation.Extract(
                            connection= conn_pg, 
                            query= querycleaning.sellers.format()
                        )
                    )
        
    print(len(extract_sellers))
    df = pd.DataFrame(
            extract_sellers,
            columns=[
                'seller_id',
                'seller_zip_code_prefix',
                'seller_city',
                'seller_state'
                ]
        )
    print(df)
    print(df.info())
    print("--- state [1] %s seconds ---" % (time.time() - start_time))


    ### insert data sellers######
    con_insert = conn_pg1
    insert_data_sellers = loop.run_until_complete(
                DataOperation.ExecutedInsertDataWrr(
                    df= df,
                    connection= con_insert,
                    schemas= 'public',
                    tables = 'sellers',
                    primary_key_column=[
                        'seller_id'
                        ],
                    datatype = [{
                        'seller_id' : 'VARCHAR',
                        'seller_zip_code_prefix': 'INT',
                        'seller_city' : 'VARCHAR',
                        'seller_state' : 'VARCHAR'
                    }] 
                )
            )

    print(insert_data_sellers)
    print("--- state [sellers] %s seconds ---" % (time.time() - start_time))

### extract data  product category name translate######
    extract_product_category_name = loop.run_until_complete(
                        DataOperation.Extract(
                            connection= conn_pg, 
                            query= querycleaning.pcn.format()
                        )
                    )
        
    print(len(extract_product_category_name))
    df = pd.DataFrame(
            extract_product_category_name,
            columns=[
                'product_category_name',
                'product_category_name_english'
                ]
        )
    print(df)
    print(df.info())
    print("--- state [1] %s seconds ---" % (time.time() - start_time))


    ### insert data product category name translate######
    con_insert = conn_pg1
    insert_data_product_category_name = loop.run_until_complete(
                DataOperation.ExecutedInsertDataWrr(
                    df= df,
                    connection= con_insert,
                    schemas= 'public',
                    tables = 'product_category_name_translation',
                    primary_key_column=[
                        'product_category_name'
                        ],
                    datatype = [{
                        'product_category_name': 'VARCHAR',
                        'product_category_name_english':'VARCHAR'
                    }] 
                )
            )

    print(insert_data_product_category_name)
    print("--- state [product_category_name] %s seconds ---" % (time.time() - start_time))



## extract data orders######
    extract_orders = loop.run_until_complete(
                        DataOperation.Extract(
                            connection= conn_pg, 
                            query= querycleaning.orders.format()
                        )
                    )
        
    print(len(extract_orders))
    df = pd.DataFrame(
            extract_orders,
            columns=[
                'order_id',
                'customer_id',
                'order_status',
                'order_purchase_timestamp',
                'order_approved_at',
                'order_delivered_carrier_date',
                'order_delivered_customer_date',
                'order_estimated_delivery_date'
                ]
        )
    print(df)
    print(df.info())
    print("--- state [1] %s seconds ---" % (time.time() - start_time))


    ### insert data orders######
    con_insert = conn_pg1
    insert_data_orders = loop.run_until_complete(
                DataOperation.ExecutedInsertDataWrr(
                    df= df,
                    connection= con_insert,
                    schemas= 'public',
                    tables = 'orders',
                    primary_key_column=[
                        'order_id'
                        ],
                    datatype =[{
                        'order_id':'VARCHAR',
                        'customer_id': 'VARCHAR',
                        'order_status': 'VARCHAR',
                        'order_purchase_timestamp': 'DATE',
                        'order_approved_at': 'DATE',
                        'order_delivered_carrier_date': 'DATE',
                        'order_delivered_customer_date': 'DATE',
                        'order_estimated_delivery_date': 'DATE'
                    }]
                )
            )

    print(insert_data_orders)
    print("--- state [orders] %s seconds ---" % (time.time() - start_time))

### extract data products######
    extract_products = loop.run_until_complete(
                        DataOperation.Extract(
                            connection= conn_pg, 
                            query= querycleaning.products.format()
                        )
                    )
        
    print(len(extract_products))
    df = pd.DataFrame(
            extract_products,
            columns=[
                'product_id',
                'product_category_name',
                'product_name_lenght',
                'product_description_lenght',
                'product_photos_qty',
                'product_weight_g',
                'product_length_cm',
                'product_height_cm',
                'product_width_cm'
                ]
        )
    print(df)
    print(df.info())
    print("--- state [1] %s seconds ---" % (time.time() - start_time))


    ### insert data products######
    con_insert = conn_pg1
    insert_data_products = loop.run_until_complete(
                DataOperation.ExecutedInsertDataWrr(
                    df= df,
                    connection= con_insert,
                    schemas= 'public',
                    tables = 'products',
                    primary_key_column=[
                        'product_id'
                        ],
                    datatype = [{
                        'product_id':'VARCHAR',
                        'product_category_name':'VARCHAR',
                        'product_name_lenght':'INT',
                        'product_description_lenght':'INT',
                        'product_photos_qty':'INT',
                        'product_weight_g':'INT',
                        'product_length_cm':'INT',
                        'product_height_cm':'INT',
                        'product_width_cm':'INT'
                    }]
                )
            )

    print(insert_data_products)
    print("--- state [products] %s seconds ---" % (time.time() - start_time))


 # ### extract data order_review######
    extract_order_reviews = loop.run_until_complete(
                        DataOperation.Extract(
                            connection= conn_pg, 
                            query= querycleaning.order_reviews.format()
                        )
                    )
        
    print(len(extract_order_reviews))
    df = pd.DataFrame(
            extract_order_reviews,
            columns=[
                'review_order_id',
                'review_id',
                'order_id',
                'review_score',
                'review_comment_title',
                'review_comment_message',
                'review_creation_date',
                'review_answer_timestamp'
                ]
        )
    print(df)
    print(df.info())
    print("--- state [1] %s seconds ---" % (time.time() - start_time))


    ### insert data ORDER REVIEWSs######
    con_insert = conn_pg1
    insert_data_order_reviews = loop.run_until_complete(
                DataOperation.ExecutedInsertDataWrr(
                    df= df,
                    connection= con_insert,
                    schemas= 'public',
                    tables = 'order_reviews',
                    primary_key_column=[
                        'review_order_id'
                        ],
                    datatype=[{
                        'review_order_id':'INT',
                        'review_id': 'VARCHAR',
                        'order_id': 'VARCHAR',
                        'review_score':'INT',
                        'review_comment_title': 'VARCHAR',
                        'review_comment_message': 'VARCHAR',
                        'review_creation_date': 'DATE',
                        'review_answer_timestamp': 'DATE'
                    }]
                )
            )

    print(insert_data_order_reviews)
    print("--- state [order_reviews] %s seconds ---" % (time.time() - start_time))

# ### extract data order_ITEMS######
    extract_order_items = loop.run_until_complete(
                        DataOperation.Extract(
                            connection= conn_pg, 
                            query= querycleaning.order_items.format()
                        )
                    )
        
    print(len(extract_order_items))
    df = pd.DataFrame(
            extract_order_items,
            columns=[
                'order_product_id',
                'order_id',
                'order_item_id',
                'product_id',
                'seller_id',
                'shipping_limit_date',
                'price',
                'freight_value'
                ]
        )
    print(df)
    print(df.info())
    print("--- state [1] %s seconds ---" % (time.time() - start_time))


    ## insert data ORDER ITEMSs######
    con_insert = conn_pg1
    insert_data_order_items = loop.run_until_complete(
                DataOperation.ExecutedInsertDataWrr(
                    df= df,
                    connection= con_insert,
                    schemas= 'public',
                    tables = 'order_items',
                    primary_key_column=[
                        'order_product_id'
                        ],
                    datatype=[{
                       'order_product_id':'INT',
                        'order_id': 'VARCHAR',
                        'order_item_id': 'INT',
                        'product_id': 'VARCHAR',
                        'seller_id': 'VARCHAR',
                        'shipping_limit_date': 'DATE',
                        'price': 'NUMERIC',
                        'freight_value': 'NUMERIC'
                    }]
                )
            )

    print(insert_data_order_items)
    print("--- state [order_items] %s seconds ---" % (time.time() - start_time))

     # ### extract data order_payments######
    extract_order_payments = loop.run_until_complete(
                        DataOperation.Extract(
                            connection= conn_pg, 
                            query= querycleaning.order_payments.format()
                        )
                    )
        
    print(len(extract_order_payments))
    df = pd.DataFrame(
            extract_order_payments,
            columns=[
                'order_payment_id',
                'order_id',
                'payment_sequential',
                'payment_type',
                'payment_installments',
                'payment_value'
                ]
        )
    print(df.info())
    print(df.head())
    print("--- state [1] %s seconds ---" % (time.time() - start_time))


    ## insert data order payments######
    con_insert = conn_pg1
    insert_data_order_payments = loop.run_until_complete(
                DataOperation.ExecutedInsertDataWrr(
                    df= df,
                    connection= con_insert,
                    schemas= 'public',
                    tables = 'order_payments',
                    primary_key_column=[
                        'order_payment_id'
                        ],
                    datatype = [{
                        'order_payment_id' : 'INTEGER',
                        'order_id' : 'VARCHAR',
                        'payment_sequential': 'INTEGER',
                        'payment_type' : 'VARCHAR',
                        'payment_installments' : 'INTEGER',
                        'payment_value' : 'NUMERIC'
                    }]
                )
            )

    print(insert_data_order_payments)
    print("--- state [order_payments] %s seconds ---" % (time.time() - start_time))







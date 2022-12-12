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

    CT_Mart_Customers_Orders = loop.run_until_complete(
        DataOperation.CreateTable(
            connection = conn_pg,
            query = QueryServices.Create_Table.format(
                Schema = 'data_mart',
                Table = 'customers_orders',
                Columns = ''' 
                order_purchase DATE,
                customer_id VARCHAR(50),
                customer_city VARCHAR(50),
                seller_id VARCHAR(50),
                seller_city VARCHAR(50),
                product_id VARCHAR(50),
                product_category_name VARCHAR(50),
                order_status VARCHAR(50),
                payment_type VARCHAR(50),
                total_gmv NUMERIC,
                total_order INT,
                total_customer INT   
                '''
            )
        )
    )
    print(CT_Mart_Customers_Orders)
    print("--- state [Mart_Customers_Orders] %s seconds ---" % (time.time() - start_time))
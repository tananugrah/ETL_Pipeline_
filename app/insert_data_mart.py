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

    extract_mart = loop.run_until_complete(
                        DataOperation.InsertData(
                            connection= conn_pg, 
                            query= Query.datamart_customers_orders.format(
                                from_date   ='2016-09-04', 
                                up_to_date  = '2018-10-17'
                            )
                        )
                    )
    print(extract_mart)
    print("--- state [insert_data_mart_customers_order] %s seconds ---" % (time.time() - start_time))


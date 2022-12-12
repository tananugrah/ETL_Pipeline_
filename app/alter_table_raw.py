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

    Add_Column_raw = loop.run_until_complete(
        DataOperation.Alter_tb(
            connection = conn_pg,
            query = QueryServices.Alter_Column.format(
                statement = """
                    ALTER TABLE integration.geolocation 
                        ADD COLUMN geolocation_id SERIAL PRIMARY KEY;

                    ALTER TABLE integration.order_items
                        ADD COLUMN order_product_id SERIAL PRIMARY KEY;

                    ALTER TABLE integration.order_payments
                        ADD COLUMN order_payment_id SERIAL PRIMARY KEY;

                    ALTER TABLE integration.order_reviews
                        ADD COLUMN review_order_id SERIAL PRIMARY KEY;
                    
                """
            )
        )
    )
    print(Add_Column_raw)
    print("--- state [Add_Column_raw] %s seconds ---" % (time.time() - start_time))
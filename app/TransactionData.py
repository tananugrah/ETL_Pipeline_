import os
import sys

import time

sys.path.append(os.getcwd())

import asyncpg
import asyncio

import awswrangler as wr
import pandas as pd


class DataOperation():
    async def CreateTable (connection,query):
        tr = connection.transaction()
        await tr.start() #Masukkan blok transaksi atau savepoint
        try:
            await connection.execute(query)
        except asyncpg.PostgresError as exc:
            return ("Failed to Create Table. ", exc)
        else:
            await tr.commit()
            return connection
            await connection.close()

    async def Alter_tb (connection,query):
        tr = connection.transaction()
        await tr.start() #Masukkan blok transaksi atau savepoint
        try:
            await connection.execute(query)
            print("Success Alter Table")
        except asyncpg.PostgresError as exc:
            return ("Failed to Alter Table. ", exc)
        else:
            await tr.commit()
            return connection
            await connection.close()
    
    async def CreateTableAs (connection,query):
        tr = connection.transaction()
        await tr.start() #Masukkan blok transaksi atau savepoint
        try:
            await connection.execute(query)
        except asyncpg.PostgresError as exc:
            return ("Failed to Create Table. ", exc)
        else:
            await tr.commit()
            return connection
            await connection.close()
    
    #https://magicstack.github.io/asyncpg/current/api/index.html?highlight=copy_to_table#asyncpg.connection.Connection.copy_to_table
    async def Import_Csv (connection,schema,table,path):
        tr = connection.transaction()
        await tr.start() #Masukkan blok transaksi atau savepoint
        try:
            data = await connection.copy_to_table(
                table,
                schema_name=schema,
                source= path,
                format='csv',
                delimiter = ',',
                header = True
            )
            print(f"[INFO] Success Import Data From CSV File.. ")
        except asyncpg.PostgresError as exc:
            print ("Failed to Import Data. ", exc)
        else:
            await tr.commit()
            return data
            await connection.close()

    async def Extract(connection,query):
        try: 
            data = await connection.fetch(query)
        except asyncpg.PostgresError as exc:
            return ("Failed to extract data ",exc)
        else:
            return data
            await connection.close()


#https://aws-sdk-pandas.readthedocs.io/en/stable/
    async def ExecutedInsertDataWrr (
        df,
        connection,
        schemas,
        tables,
        primary_key_column,
        datatype
    ):
        try:
            if len(df) != 0:
                # ingestion statemen
                wr.postgresql.to_sql(
                    df,
                    con=connection,
                    schema=schemas,
                    table=tables,
                    mode='upsert',
                    dtype = datatype,
                    upsert_conflict_columns = primary_key_column,
                    chunksize=200
                )
                print('Insert Data')
            else:
                print('Dataframe empty dataset')
        except:
            print('error in \n', sys.exc_info())
        else:
            print('success')
    
    async def InsertData (connection,query):
        tr = connection.transaction()
        await tr.start()
        try:
            await connection.execute(query)
        except asyncpg.PostgresError as exc:
            return ("Failed to Insert Data. ", exc)
        else:
            await tr.commit()
            return connection
            await connection.close()


   
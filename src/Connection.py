import os
import sys
sys.path.append(os.getcwd())

import asyncpg 
import asyncio
import pg8000.native
import sys

from src import Config

# Credential Postgres db
SERVER_DB = Config.DB_URL
DATABASE_DB = Config.DB_NAME
USERNAME_DB = Config.DB_USER
PASSWORD_DB = Config.DB_PASS
PORT_DB = Config.DB_PORT

class DBClient:
    async def ConnectionDB():
        conn = None
        if conn == None:
            try:
                conn = await asyncpg.connect(
                        user=USERNAME_DB,
                        password=PASSWORD_DB,
                        database=DATABASE_DB,
                        host=SERVER_DB,
                        port=PORT_DB,
                    )
                print(f"[INFO]success connect to postgresDB...")
                # await conn.close()
            except asyncpg.PostgresError as exc:
                return ("Failed to initialise database.", exc)
            else:
                pass
        return conn


    async def connection_postgresqlaws():
        conn_postgresqlaws = None
        if conn_postgresqlaws == None:

            try:
                con_postgresqlaws = redshift_connector.connect(
                    host=SERVER_DB,
                    database=DATABASE_DB,
                    user=USERNAME_DB,
                    password=PASSWORD_DB
                )

            except redshift_connector.Error as er:
                print("OOps: Something Else:", er)

            else:
                pass
    
        return con_postgresqlaws

    
    async def ConnectionDBpg2():
        conn = None
        if conn == None:
            try:
                conn = pg8000.connect(
                    host=SERVER_DB,
                    database=DATABASE_DB,
                    user=USERNAME_DB,
                    password=PASSWORD_DB
                    )
                cur = conn.cursor()
                cur.execute('SELECT version()')

                version = cur.fetchone()[0]
            except:
                return ("Failed to initialise database.")
            else:
                pass
        return conn
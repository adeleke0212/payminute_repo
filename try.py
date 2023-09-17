
import pandas as pd
import psycopg2
from ......Documents.Rhino_Storage.sql.create import Staging
from ......Documents.Rhino_Storage.loading import DB_SCHEMA
from ......Documents.Rhino_Storage.loading import STORAGE_TRANS, CUSTOMERS, DATES, ACTIONS, STORAGES
from ......Documents.Rhino_Storage.loading import DB_HOST, DB_PORT, DB_PASSWORD, DB_DATABASE, DB_USER as user
from ......Documents.Rhino_Storage.loading import database_connector

database_connector()


def createAllTablesAndSchema():
    conn = database_connector()
    cursor = conn.cursor()
    cursor.execute(Staging.format(DB_SCHEMA))
    cursor.execute(Staging.fact_table_storage_trans.format(
        DB_SCHEMA, STORAGE_TRANS))
    cursor.execute(Staging.customers.format(DB_SCHEMA, CUSTOMERS))
    cursor.execute(Staging.dates.format(DB_SCHEMA, DATES))
    cursor.execute(Staging.actions.format(DB_SCHEMA, ACTIONS))
    cursor.execute(Staging.storages.format(DB_SCHEMA, STORAGES))
    conn.commit()
    cursor.close()
    conn.close()
    print('All schemas and tables created in the database')


createAllTablesAndSchema()

# Error:
#     from ......Documents.Rhino_Storage.sql.create import Staging
# ImportError: attempted relative import with no known parent package

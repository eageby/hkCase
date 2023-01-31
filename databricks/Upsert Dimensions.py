# Databricks notebook source
# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# MAGIC sudo apt-get update
# MAGIC sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17

# COMMAND ----------

import pyodbc
server = "hk-analysis.database.windows.net"
username = dbutils.secrets.get(scope="key-vault", key="analysisSqlUser")
password = dbutils.secrets.get(scope="key-vault", key="analysisSqlPassword")
conn = pyodbc.connect( 'DRIVER=ODBC Driver 17 for SQL Server;SERVER=hk-analysis.database.windows.net;DATABASE=analysis;UID={user};PWD={password}'.format(user=username, password=password))

cursor=conn.cursor()
cursor.execute("""EXECUTE Merge_Customer
EXECUTE Merge_Store
EXECUTE Merge_Product
EXECUTE Merge_Voucher
EXECUTE Merge_Date""")
conn.commit()

# COMMAND ----------

conn.close()

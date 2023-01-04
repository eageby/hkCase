# Databricks notebook source
dbutils.fs.unmount("/mnt/hkdata")

# COMMAND ----------

dbutils.secrets.list(scope="key-vault")

# COMMAND ----------

clientId = dbutils.secrets.get(scope="key-vault", key="clientid")
clientSecret = dbutils.secrets.get(scope="key-vault", key="clientsecret")
tenantId = dbutils.secrets.get(scope="key-vault", key="tentantid")

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": clientId,
       "fs.azure.account.oauth2.client.secret": clientSecret,
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{}/oauth2/token".format(tenantId),
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

dbutils.fs.mount(
source = "abfss://hkdata@hkdatalake563456.dfs.core.windows.net/transactional",
mount_point = "/mnt/hkdata",
extra_configs = configs)



# COMMAND ----------

# MAGIC %fs ls /mnt/hkdata

# COMMAND ----------

stores = spark.read.format('csv').options(
    header='true', inferschema='true').load("/mnt/hkdata/dbostores.txt")

stores.printSchema()
stores.write.format("parquet").saveAsTable("Stores")

# COMMAND ----------

items = spark.read.format('csv').options(
    header='true', inferschema='true').load("/mnt/hkdata/dboorderitems.txt")

items.printSchema()
items.write.format("parquet").saveAsTable("OrderItems")

# COMMAND ----------

orders = spark.read.format('csv').options(
    header='true', inferschema='true').load("/mnt/hkdata/dboorders.txt")

orders.printSchema()
orders.write.format("parquet").saveAsTable("Orders")

# COMMAND ----------

payments = spark.read.format('csv').options(
    header='true', inferschema='true').load("/mnt/hkdata/dbopayments.txt")

payments.printSchema()
payments.write.format("parquet").saveAsTable("Payments")

-- Databricks notebook source
-- MAGIC %python
-- MAGIC orders = spark.read.format('csv').options(
-- MAGIC     header='true', inferschema='true').load("/mnt/hkdata/dbostores.txt")
-- MAGIC 
-- MAGIC orders.printSchema()
-- MAGIC orders.createOrReplaceTempView("Stores")

-- COMMAND ----------

CREATE OR REPLACE TABLE Store_Dimension (
  Store_Key BIGINT GENERATED ALWAYS AS IDENTITY,
  Store_Number BIGINT NOT NULL,
  Store_Area_Name VARCHAR(100) NOT NULL,
  Store_Placement VARCHAR(20) NOT NULL
)

-- COMMAND ----------

INSERT INTO Store_Dimension (Store_Number, Store_Area_Name, Store_Placement)
SELECT DISTINCT 
  StoreNr,
  INITCAP(Area),
  CASE WHEN InMall = 1 THEN 'In Mall' ELSE 'Outside Mall' END
 FROM Stores

-- COMMAND ----------

SELECT 
  *
FROM Store_Dimension

-- COMMAND ----------

CREATE TABLE Store_Dimension_OLAP
USING JDBC
OPTIONS (
url "jdbc:sqlserver://hk-analysis.database.windows.net;databaseName=analysis;",
dbtable "Store_Dimension",
user "${spark.sqlUser}",
password "${spark.sqlPassword}"
)
SELECT *
FROM Store_Dimension

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC stores = spark.read.table('Store_Dimension')
-- MAGIC stores.show()

-- COMMAND ----------



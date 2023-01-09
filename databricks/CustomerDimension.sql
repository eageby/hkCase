-- Databricks notebook source
-- MAGIC %python
-- MAGIC orders = (
-- MAGIC     spark.read.format("csv")
-- MAGIC     .options(header="true", inferschema="true")
-- MAGIC     .load(
-- MAGIC         "abfss://hkdata@hkdatalake563456.dfs.core.windows.net/transactional/dboorders.txt"
-- MAGIC     )
-- MAGIC )
-- MAGIC 
-- MAGIC # orders.printSchema()
-- MAGIC orders.createOrReplaceTempView("orders_bronze")

-- COMMAND ----------

SELECT *
from orders_bronze
where namefirst like 'Abdulruhman'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.format("jdbc").option(
-- MAGIC     "url",
-- MAGIC     "jdbc:sqlserver://hk-analysis.database.windows.net;databaseName=analysis;",
-- MAGIC ).option("dbtable", "Customer_Dimension").option(
-- MAGIC     "user", dbutils.secrets.get(scope="key-vault", key="analysisSqlUser")
-- MAGIC ).option(
-- MAGIC     "password", dbutils.secrets.get(scope="key-vault", key="analysisSqlPassword")
-- MAGIC ).load().createOrReplaceTempView(
-- MAGIC     "Customer_Dimension"
-- MAGIC )

-- COMMAND ----------

SELECT
  *
FROM
  Customer_Dimension

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW customer_silver (
  customer_first_name,
  customer_last_name,
  customer_email
) AS
SELECT DISTINCT 
  CASE
    WHEN namefirst IS NULL THEN 'Not Provided'
    ELSE INITCAP(namefirst)
  END,CASE
    WHEN namelast IS NULL THEN 'Not Provided'
    ELSE INITCAP(namelast)
  END,CASE
    WHEN email IS NULL THEN 'Not Provided'
    ELSE LOWER(email)
  END
from
  orders;

-- COMMAND ----------

select *
from customer_silver
Order by customer_first_name, customer_last_name, customer_email

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC customer_dim = spark.read.table("customer_silver")
-- MAGIC 
-- MAGIC (
-- MAGIC     customer_dim.write.format("jdbc")
-- MAGIC     .option(
-- MAGIC         "url",
-- MAGIC         "jdbc:sqlserver://hk-analysis.database.windows.net;databaseName=analysis;",
-- MAGIC     )
-- MAGIC     .option("dbtable", "Customer_Dimension")
-- MAGIC     .option("user", dbutils.secrets.get(scope="key-vault", key="analysisSqlUser"))
-- MAGIC     .option(
-- MAGIC         "password", dbutils.secrets.get(scope="key-vault", key="analysisSqlPassword")
-- MAGIC     )
-- MAGIC     .mode("append")
-- MAGIC     .save()
-- MAGIC )

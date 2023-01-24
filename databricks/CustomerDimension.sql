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
-- MAGIC orders.createOrReplaceTempView("orders_raw_data")

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

CREATE
OR REPLACE TEMPORARY VIEW customer_raw_data AS
SELECT
  namefirst,
  namelast,
  email,
  orderid,
  storenr,
  orderdate
FROM
  orders_raw_data

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW customer_formatted
AS SELECT
  INITCAP(namefirst) as namefirst,
  INITCAP(namelast) as namelast,
  LOWER(email) as email,
  orderdate,
  storeNr,
  CONCAT(orderid, '-', storeNr) as order_id
FROM 
  customer_raw_data

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW customer_no_duplicates
AS SELECT
  namefirst,
  namelast,
  MAX(email) as email,
  COUNT(DISTINCT order_id) as customer_number_of_orders,
  COUNT(DISTINCT storeNr) as customer_number_of_stores_visited
FROM 
  customer_formatted
WHERE namefirst IS NOT NULL AND namelast IS NOT NULL
GROUP BY
  namefirst, namelast

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW customer_silver (
  customer_first_name,
  customer_last_name,
  customer_email,
  customer_registered,
  customer_recurring,
  customer_number_of_orders,
  customer_number_of_orders_group,
  customer_number_of_stores_visited
) AS
SELECT
  CASE
    WHEN namefirst IS NULL THEN 'Not Provided'
    ELSE namefirst
  END,
  CASE
    WHEN namelast IS NULL THEN 'Not Provided'
    ELSE namelast
  END,
  CASE
    WHEN email IS NULL THEN 'Not Provided'
    ELSE email
  END,
  'Registered Customer',
  CASE
    WHEN customer_number_of_orders > 1 THEN 'Recurring Customer'
    ELSE 'Not Recurring Customer'
  END,
  customer_number_of_orders,
  CASE 
      WHEN customer_number_of_orders BETWEEN 1 AND 5 THEN '1-5 Orders'
      WHEN customer_number_of_orders BETWEEN 6 AND 10 THEN '6-10 Orders'
      WHEN customer_number_of_orders > 10 THEN 'More than 10 Orders'
      ELSE 'Not Applicable'
  END as customer_number_of_orders_group,
  customer_number_of_stores_visited
from
  customer_no_duplicates;

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW customer_insert AS
SELECT
  customer_first_name,
  customer_last_name,
  customer_email,
  customer_registered,
  customer_recurring,
  customer_number_of_orders,
  customer_number_of_orders_group,
  customer_number_of_stores_visited
FROM
  customer_silver
WHERE
  (customer_first_name, customer_last_name) NOT IN (
    SELECT
      customer_first_name,
      customer_last_name
    from
      Customer_Dimension
  )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC customer_dim = spark.read.table("customer_insert")
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

-- COMMAND ----------



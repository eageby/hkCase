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
  MAX(namefirst) as namefirst,
  MAX(namelast) as namelast,
  email,
  COUNT(DISTINCT order_id) as customer_number_of_orders
FROM 
  customer_formatted
  WHERE email is not null
GROUP BY
  email

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW customer_aggregated_orders AS
SELECT
  namefirst,
  namelast,
  email,
  C.customer_number_of_orders + COALESCE(D.customer_number_of_orders, 0) as customer_number_of_orders
FROM
  customer_no_duplicates C
  LEFT JOIN customer_dimension D on C.email = D.customer_email
  AND D.current_row_indicator = 'Current'

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
  customer_active
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
  email,
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
  'Active' as customer_active
from
  customer_aggregated_orders;

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
-- MAGIC     .option("dbtable", "Customer_Dimension_Staging")
-- MAGIC     .option("user", dbutils.secrets.get(scope="key-vault", key="analysisSqlUser"))
-- MAGIC     .option(
-- MAGIC         "password", dbutils.secrets.get(scope="key-vault", key="analysisSqlPassword")
-- MAGIC     )
-- MAGIC     .mode("overwrite")
-- MAGIC     .save()
-- MAGIC )

-- COMMAND ----------



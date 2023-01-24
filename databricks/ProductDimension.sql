-- Databricks notebook source
-- DBTITLE 1,Read Order and Order Items from Data Lake
-- MAGIC %python
-- MAGIC order_items = (
-- MAGIC     spark.read.format("csv")
-- MAGIC     .options(header="true", inferschema="true")
-- MAGIC     .load(
-- MAGIC         "abfss://hkdata@hkdatalake563456.dfs.core.windows.net/transactional/dboorderitems.txt"
-- MAGIC     )
-- MAGIC )
-- MAGIC 
-- MAGIC order_items.createOrReplaceTempView("order_items_raw_data")
-- MAGIC 
-- MAGIC orders = (
-- MAGIC     spark.read.format("csv")
-- MAGIC     .options(header="true", inferschema="true")
-- MAGIC     .load(
-- MAGIC         "abfss://hkdata@hkdatalake563456.dfs.core.windows.net/transactional/dboorders.txt"
-- MAGIC     )
-- MAGIC )
-- MAGIC 
-- MAGIC orders.createOrReplaceTempView("orders_raw_data")

-- COMMAND ----------

-- DBTITLE 1,Read Product Dimension from Data Warehouse
-- MAGIC %python
-- MAGIC product_dim_loader = spark.read.format("jdbc").option(
-- MAGIC     "url",
-- MAGIC     "jdbc:sqlserver://hk-analysis.database.windows.net;databaseName=analysis;",
-- MAGIC ).option("dbtable", "Product_Dimension").option(
-- MAGIC     "user", dbutils.secrets.get(scope="key-vault", key="analysisSqlUser")
-- MAGIC ).option(
-- MAGIC     "password", dbutils.secrets.get(scope="key-vault", key="analysisSqlPassword")
-- MAGIC )
-- MAGIC 
-- MAGIC product_dim_loader.load().createOrReplaceTempView(
-- MAGIC     "Product_Dimension"
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 1,Create Bronze Quality Product View
CREATE OR REPLACE TEMPORARY VIEW
 products_raw_data (Sku, Product_Name, order_date)
AS SELECT
  sku,
  productname,
  orderdate
FROM
  order_items_raw_data I JOIN orders_raw_data O on I.orderid = O.orderid and I.storeNr = O.storenr

-- COMMAND ----------

-- DBTITLE 1,Remove duplicates
CREATE
OR REPLACE TEMPORARY VIEW products_raw_no_duplicates AS
SELECT
  Sku,
  product_name,
  order_date
FROM
  (
    SELECT
      Sku,
      Product_name,
      order_date,
      ROW_NUMBER() OVER(
        PARTITION BY INITCAP(sku)
        ORDER BY
          order_date DESC
      ) as row_number
    FROM
     products_raw_data 
  )
WHERE
  row_number = 1

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW product_silver AS 
SELECT 
  CASE WHEN sku IS NULL THEN 'Not Applicable' ELSE sku END as sku,
  product_name
FROM
  products_raw_no_duplicates

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW product_insert 
AS SELECT 
  sku, product_name
FROM 
  product_silver
WHERE
  (sku, product_name) NOT IN (
  SELECT 
    sku,
    product_name
   FROM product_dimension
   )


-- COMMAND ----------

-- DBTITLE 1,Add New Entries to Product Dimension in Warehouse
-- MAGIC %python
-- MAGIC 
-- MAGIC product_dim = spark.read.table("product_insert")
-- MAGIC 
-- MAGIC (
-- MAGIC     product_dim.write.format("jdbc")
-- MAGIC     .option(
-- MAGIC         "url",
-- MAGIC         "jdbc:sqlserver://hk-analysis.database.windows.net;databaseName=analysis;",
-- MAGIC     )
-- MAGIC     .option("dbtable", "Product_Dimension")
-- MAGIC     .option("user", dbutils.secrets.get(scope="key-vault", key="analysisSqlUser"))
-- MAGIC     .option(
-- MAGIC         "password", dbutils.secrets.get(scope="key-vault", key="analysisSqlPassword")
-- MAGIC     )
-- MAGIC     .mode("append")
-- MAGIC     .save()
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 1,Load Updated Product Dimension from Data Warehouse into Update Staging Table
-- MAGIC %python 
-- MAGIC # product_dim_loader.load().write.mode("overwrite").option("mergeSchema","true").saveAsTable("Product_dimension_staging")

-- COMMAND ----------

-- DBTITLE 1,Perform Update on Staging Table
-- UPDATE 
--   Product_Dimension_Staging
-- SET
--   Product_name = "HELLO"
-- WHERE product_key = 32

-- COMMAND ----------

-- DBTITLE 1,Replace Data Warehouse Dimension with Updated Staging Table
-- MAGIC %python
-- MAGIC 
-- MAGIC # product_dim_staging = spark.read.table("product_dimension_staging")
-- MAGIC 
-- MAGIC # (
-- MAGIC #     product_dim_staging.write.format("jdbc")
-- MAGIC #     .option(
-- MAGIC #         "url",
-- MAGIC #         "jdbc:sqlserver://hk-analysis.database.windows.net;databaseName=analysis;",
-- MAGIC #     )
-- MAGIC #     .option("dbtable", "Product_Dimension")
-- MAGIC #     .option("user", dbutils.secrets.get(scope="key-vault", key="analysisSqlUser"))
-- MAGIC #     .option(
-- MAGIC #         "password", dbutils.secrets.get(scope="key-vault", key="analysisSqlPassword")
-- MAGIC #     )
-- MAGIC #     .mode("overwrite")
-- MAGIC #     .save()
-- MAGIC # )

-- COMMAND ----------

-- DBTITLE 1,Drop Staging Table
-- DROP TABLE IF EXISTS Product_Dimension_Staging 

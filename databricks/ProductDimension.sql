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
OR REPLACE TEMPORARY VIEW products_remove_duplicates AS
SELECT
  INITCAP(Sku) as sku,
  product_name
FROM
  (
    SELECT
      Sku,
      Product_name,
      order_date,
      ROW_NUMBER() OVER(
        PARTITION BY INITCAP(sku)
        ORDER BY
          CASE WHEN product_name is not null then 1 else 0 END DESC, order_date DESC
      ) as row_number
    FROM
     products_raw_data 
  )
WHERE
  row_number = 1
  and sku is not null

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW product_silver AS 
SELECT
  sku,
  CASE WHEN product_name IS NULL THEN 'Not Applicable' ELSE product_name END as product_name
FROM
  products_remove_duplicates

-- COMMAND ----------

-- DBTITLE 1,Add New Entries to Product Dimension in Warehouse
-- MAGIC %python
-- MAGIC 
-- MAGIC product_dim = spark.read.table("product_silver")
-- MAGIC 
-- MAGIC (
-- MAGIC     product_dim.write.format("jdbc")
-- MAGIC     .option(
-- MAGIC         "url",
-- MAGIC         "jdbc:sqlserver://hk-analysis.database.windows.net;databaseName=analysis;",
-- MAGIC     )
-- MAGIC     .option("dbtable", "Product_Dimension_Staging")
-- MAGIC     .option("user", dbutils.secrets.get(scope="key-vault", key="analysisSqlUser"))
-- MAGIC     .option(
-- MAGIC         "password", dbutils.secrets.get(scope="key-vault", key="analysisSqlPassword")
-- MAGIC     )
-- MAGIC     .mode("overwrite")
-- MAGIC     .save()
-- MAGIC )

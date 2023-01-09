-- Databricks notebook source
-- MAGIC %python
-- MAGIC 
-- MAGIC products = (
-- MAGIC     spark.read.format("csv")
-- MAGIC     .options(header="true", inferschema="true")
-- MAGIC     .load("/mnt/hkdata/dboorderitems.txt")
-- MAGIC )
-- MAGIC 
-- MAGIC products.printSchema()
-- MAGIC products.createOrReplaceTempView("OrderItems");

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW
 Product_Dimension (Product_Id, Sku, Product_Name)
AS SELECT
  DISTINCT productid,
  sku,
  productname
FROM
  OrderItems
WHERE
  ProductId IS NOT NULL

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC product_dim = spark.read.table("Product_Dimension")
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



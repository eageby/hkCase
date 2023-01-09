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

CREATE OR REPLACE TEMPORARY VIEW Voucher_Dimension (Voucher_Id, Voucher_Code, Voucher_Name)
AS SELECT 
   DISTINCT voucherid,
   CASE WHEN vouchercode IS NOT NULL THEN vouchercode ELSE 'Not Applicable' END,
   CASE WHEN vouchername IS NOT NULL THEN vouchername ELSE 'Not Applicable' END
FROM OrderItems
WHERE voucherid IS NOT NULL

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC voucher_dim = spark.read.table("Voucher_Dimension")
-- MAGIC 
-- MAGIC (
-- MAGIC     voucher_dim.write.format("jdbc")
-- MAGIC     .option(
-- MAGIC         "url", 
-- MAGIC         "jdbc:sqlserver://hk-analysis.database.windows.net;databaseName=analysis;",
-- MAGIC     )
-- MAGIC     .option("dbtable", "Voucher_Dimension")
-- MAGIC     .option("user", dbutils.secrets.get(scope="key-vault", key="analysisSqlUser"))
-- MAGIC     .option(
-- MAGIC         "password", dbutils.secrets.get(scope="key-vault", key="analysisSqlPassword")
-- MAGIC     )
-- MAGIC     .mode("append")
-- MAGIC     .save()
-- MAGIC )

-- COMMAND ----------



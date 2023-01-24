-- Databricks notebook source
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

-- MAGIC %python
-- MAGIC voucher_dim_loader = spark.read.format("jdbc").option(
-- MAGIC     "url",
-- MAGIC     "jdbc:sqlserver://hk-analysis.database.windows.net;databaseName=analysis;",
-- MAGIC ).option("dbtable", "Voucher_Dimension").option(
-- MAGIC     "user", dbutils.secrets.get(scope="key-vault", key="analysisSqlUser")
-- MAGIC ).option(
-- MAGIC     "password", dbutils.secrets.get(scope="key-vault", key="analysisSqlPassword")
-- MAGIC )
-- MAGIC 
-- MAGIC voucher_dim_loader.load().createOrReplaceTempView(
-- MAGIC     "Voucher_Dimension"
-- MAGIC )

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW vouchers_raw_data AS
SELECT
  vouchercode,
  voucherid,
  vouchername,
  moneydiscount,
  moneynetpriceperunit,
  orderdate
FROM
  order_items_raw_data I
  JOIN orders_raw_data O on I.orderid = O.orderid and I.storeNr = O.storeNr

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW vouchers_distinct AS
SELECT
  DISTINCT vouchercode,
  voucherid,
  vouchername
FROM
  vouchers_raw_data

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW vouchers_formatted AS
SELECT
  INITCAP(vouchercode) AS vouchercode,
  UPPER(voucherid) as voucherid,
  INITCAP(vouchername) AS vouchername
FROM
  vouchers_distinct

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW voucher_silver (Voucher_Id, Voucher_Code, Voucher_Name)
AS SELECT 
   CASE WHEN voucherid IS NOT NULL AND NOT voucherid = 0 THEN voucherid ELSE 'Not Applicable' END,
   CASE WHEN vouchercode IS NOT NULL  THEN vouchercode ELSE 'Not Applicable' END,
   CASE WHEN vouchername IS NOT NULL THEN vouchername ELSE 'Not Applicable' END
FROM vouchers_formatted

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW voucher_insert AS
SELECT
  voucher_id,
  voucher_code,
  voucher_name
FROM
  voucher_silver
WHERE
  (voucher_id, voucher_code, voucher_name) NOT IN (
    SELECT
      voucher_id,
      voucher_code,
      voucher_name
    FROM
      Voucher_Dimension
  )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC voucher_dim = spark.read.table("voucher_insert")
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

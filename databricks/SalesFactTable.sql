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
-- MAGIC spark.read.format("jdbc").option(
-- MAGIC     "url",
-- MAGIC     "jdbc:sqlserver://hk-analysis.database.windows.net;databaseName=analysis;",
-- MAGIC ).option("dbtable", "Product_Dimension").option(
-- MAGIC     "user", dbutils.secrets.get(scope="key-vault", key="analysisSqlUser")
-- MAGIC ).option(
-- MAGIC     "password", dbutils.secrets.get(scope="key-vault", key="analysisSqlPassword")
-- MAGIC ).load().createOrReplaceTempView("Product_Dimension")
-- MAGIC 
-- MAGIC spark.read.format("jdbc").option(
-- MAGIC     "url",
-- MAGIC     "jdbc:sqlserver://hk-analysis.database.windows.net;databaseName=analysis;",
-- MAGIC ).option("dbtable", "Store_Dimension").option(
-- MAGIC     "user", dbutils.secrets.get(scope="key-vault", key="analysisSqlUser")
-- MAGIC ).option(
-- MAGIC     "password", dbutils.secrets.get(scope="key-vault", key="analysisSqlPassword")
-- MAGIC ).load().createOrReplaceTempView("Store_Dimension")
-- MAGIC 
-- MAGIC spark.read.format("jdbc").option(
-- MAGIC     "url",
-- MAGIC     "jdbc:sqlserver://hk-analysis.database.windows.net;databaseName=analysis;",
-- MAGIC ).option("dbtable", "Voucher_Dimension").option(
-- MAGIC     "user", dbutils.secrets.get(scope="key-vault", key="analysisSqlUser")
-- MAGIC ).option(
-- MAGIC     "password", dbutils.secrets.get(scope="key-vault", key="analysisSqlPassword")
-- MAGIC ).load().createOrReplaceTempView("Voucher_Dimension")
-- MAGIC 
-- MAGIC spark.read.format("jdbc").option(
-- MAGIC     "url",
-- MAGIC     "jdbc:sqlserver://hk-analysis.database.windows.net;databaseName=analysis;",
-- MAGIC ).option("dbtable", "Date_Dimension").option(
-- MAGIC     "user", dbutils.secrets.get(scope="key-vault", key="analysisSqlUser")
-- MAGIC ).option(
-- MAGIC     "password", dbutils.secrets.get(scope="key-vault", key="analysisSqlPassword")
-- MAGIC ).load().createOrReplaceTempView("Date_Dimension")
-- MAGIC 
-- MAGIC spark.read.format("jdbc").option(
-- MAGIC     "url",
-- MAGIC     "jdbc:sqlserver://hk-analysis.database.windows.net;databaseName=analysis;",
-- MAGIC ).option("dbtable", "Customer_Dimension").option(
-- MAGIC     "user", dbutils.secrets.get(scope="key-vault", key="analysisSqlUser")
-- MAGIC ).option(
-- MAGIC     "password", dbutils.secrets.get(scope="key-vault", key="analysisSqlPassword")
-- MAGIC ).load().createOrReplaceTempView("Customer_Dimension")

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW sales_raw_data
AS SELECT   
  orderdate,
  I.orderid as orderid,
  sku,
  productname,
  I.storeNr as storeNr,
  namefirst,
  namelast,
  quantity,
  I.moneynetpriceperunit,
  I.moneyoriginalprice,
  I.moneydiscount,
  vouchercode,
  voucherid,
  vouchername
FROM 
    order_items_raw_data I JOIN orders_raw_data O on I.orderid = O.orderid and I.storeNr = O.storeNr

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW sales_conform_facts
AS SELECT
  orderdate,
  orderid as order_id,
  sku,
  productname,
  storeNr,
  namefirst,
  namelast,
  quantity as sales_quantity,
  moneynetpriceperunit as net_unit_price,
  moneyoriginalprice as unit_price,
  moneydiscount as discount_amount,
  voucherid,
  vouchername,
  vouchercode
FROM 
  sales_raw_data
  

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW sales_derived_facts
AS SELECT
  *,
  sales_quantity * discount_amount as extended_discount_amount,
  sales_quantity * unit_price as extended_sales_amount
FROM 
  sales_conform_facts
  

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW sales_concat_order_id AS
SELECT
  orderdate,
  CONCAT(order_id, '-', storeNr) as order_id,
  sku,
  productname,
  storeNr,
  namefirst,
  namelast,
  sales_quantity,
  net_unit_price,
  unit_price,
  discount_amount,
  voucherid,
  vouchername,
  vouchercode,
  extended_discount_amount,
  extended_sales_amount
FROM
  sales_derived_facts

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW sales_conform_date AS
SELECT
  *,
  DATE_FORMAT(TIMESTAMP_SECONDS(orderdate), 'yyyyMMdd') as date_key,
  DATE_FORMAT(TIMESTAMP_SECONDS(orderdate), 'HH:mm:ss') as time_of_day
FROM
  sales_concat_order_id;

-- COMMAND ----------

CREATE
  OR REPLACE TEMPORARY VIEW sales_conform_customer_fields AS
SELECT
  order_id,
  storeNr,
  sku,
    CASE
    WHEN namefirst IS NULL THEN 'Not Provided'
    ELSE INITCAP(namefirst)
  END as customer_first_name,CASE
    WHEN namelast IS NULL THEN 'Not Provided'
    ELSE INITCAP(namelast)
  END  as customer_last_name,
  date_key,
  time_of_day,
  sales_quantity,
  net_unit_price,
  unit_price,
  discount_amount,
  extended_discount_amount,
  extended_sales_amount,
  voucherid,
  vouchername,
  vouchercode
FROM
  sales_conform_date

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW sales_conform_voucher
AS SELECT
  order_id,
  storeNr,
  sku,
  customer_first_name,
  customer_last_name,
  date_key,
  time_of_day,
  sales_quantity,
  net_unit_price,
  unit_price,
  discount_amount,
  extended_discount_amount,
  extended_sales_amount,
   CASE WHEN voucherid IS NOT NULL AND NOT voucherid = 0 THEN  UPPER(voucherid) ELSE 'Not Applicable' END as voucher_id,
   CASE WHEN vouchercode IS NOT NULL  THEN INITCAP(vouchercode) ELSE 'Not Applicable' END  AS voucher_code,
   CASE WHEN vouchername IS NOT NULL THEN INITCAP(vouchername) ELSE 'Not Applicable' END AS voucher_name
FROM sales_conform_customer_fields

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW sales_product_dimension AS
SELECT
  order_id,
  storeNr,
  customer_first_name,
  customer_last_name,
  date_key,
  time_of_day,
  product_key,
  sales_quantity,
  net_unit_price,
  unit_price,
  discount_amount,
  extended_discount_amount,
  extended_sales_amount,
  voucher_id,
  voucher_code,
  voucher_name
FROM
  sales_conform_voucher s
  JOIN product_dimension p ON s.sku = p.sku

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW sales_store_dimension AS
SELECT
  order_id,
  customer_first_name,
  customer_last_name,
  date_key,
  time_of_day,
  product_key,
  store_key,
  sales_quantity,
  net_unit_price,
  unit_price,
  discount_amount,
  extended_discount_amount,
  extended_sales_amount,
    voucher_id,
  voucher_code,
  voucher_name
FROM
  sales_product_dimension s
  JOIN store_dimension p ON storeNr = store_number

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW sales_customer_dimension AS
SELECT
  date_key,
  time_of_day,
  product_key,
  store_key,
  customer_key,
  order_id,
  sales_quantity,
  net_unit_price,
  unit_price,
  discount_amount,
  extended_discount_amount,
  extended_sales_amount,
    voucher_id,
  voucher_code,
  voucher_name
FROM
  sales_store_dimension S
  JOIN customer_dimension C on S.customer_first_name = C.customer_first_name
  AND S.customer_last_name = C.customer_last_name

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW sales_voucher_dimension AS
SELECT
  date_key,
  time_of_day,
  product_key,
  store_key,
  customer_key,
  voucher_key,
  order_id,
  sales_quantity,
  net_unit_price,
  unit_price,
  discount_amount,
  extended_discount_amount,
  extended_sales_amount
FROM
  sales_customer_dimension S
  JOIN voucher_dimension V on S.voucher_code = V.voucher_code
  AND S.voucher_name = V.voucher_name
  AND S.voucher_id = V.voucher_id

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW sales_fact_silver AS
SELECT
  date_key,
  product_key,
  store_key,
  customer_key,
  voucher_key,
  order_id,
  time_of_day,
  sales_quantity,
  unit_price,
  discount_amount,
  net_unit_price,
  extended_discount_amount,
  extended_sales_amount
FROM
  sales_voucher_dimension

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC sales_fact = spark.read.table("sales_fact_silver")
-- MAGIC 
-- MAGIC (
-- MAGIC     sales_fact.write.format("jdbc")
-- MAGIC     .option(
-- MAGIC         "url",
-- MAGIC         "jdbc:sqlserver://hk-analysis.database.windows.net;databaseName=analysis;",
-- MAGIC     )
-- MAGIC     .option("dbtable", "Sales_Fact")
-- MAGIC     .option("user", dbutils.secrets.get(scope="key-vault", key="analysisSqlUser"))
-- MAGIC     .option(
-- MAGIC         "password", dbutils.secrets.get(scope="key-vault", key="analysisSqlPassword")
-- MAGIC     )
-- MAGIC     .mode("append")
-- MAGIC     .save()
-- MAGIC )

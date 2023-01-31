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
  email,
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
  email,
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

CREATE
OR REPLACE TEMPORARY VIEW sales_align_grain AS
SELECT
  orderdate,
  order_id,
  sku,
  productname,
  storeNr,
  email,
  sum(sales_quantity) as sales_quantity,
  SUM(NET_UNIT_PRICE) / NULLIF(sum(sales_quantity), 0) as net_unit_price,
  MAX(unit_price) as unit_price,
  sum(discount_amount) / NULLIF(sum(sales_quantity),0 ) as discount_amount,
  MAX(
    Case
      when vouchercode = 'Voyado' THEN 1
      else 0
    end
  ) as external_voucher_provider,
  MAX(
    CASE
      WHEN vouchercode like 'Voyado' THEN 0
      when vouchername is not null THEN 1
      when vouchercode is not null then 1
      when voucherid != 0 THEN 1
      else 0
    END
  ) as internal_promotion
FROM
  sales_conform_facts
GROUP BY
  orderdate,
  order_id,
  sku,
  productname,
  storeNr,
  email

-- COMMAND ----------

  CREATE OR REPLACE TEMPORARY VIEW sales_discount_percentage AS SELECT
  *, 
  100 * (
    1 - (NET_UNIT_PRICE / unit_price)
  ) as discount_percentage
  FROM sales_align_grain

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW sales_remove_null AS
SELECT
  orderdate,
  order_id,
  sku,
  productname,
  storeNr,
  email,
  sales_quantity,
  CASE
    WHEN net_unit_price IS NULL THEN 0
    ELSE net_unit_price
  END as net_unit_price,
  CASE
    WHEN unit_price IS NULL THEN 0
    ELSE unit_price
  END as unit_price,
  CASE
    WHEN discount_amount IS NULL THEN 0
    ELSE discount_amount
  END as discount_amount,
    CASE
    WHEN discount_percentage IS NULL THEN 0
    ELSE discount_percentage
  END as discount_percentage,
  external_voucher_provider,
  internal_promotion
FROM
  sales_discount_percentage

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW sales_derived_facts
AS SELECT
  *,
  sales_quantity * discount_amount as extended_discount_amount,
  sales_quantity * unit_price as extended_sales_amount,
  sales_quantity * unit_price -  sales_quantity * discount_amount as total_sales 
FROM 
  sales_remove_null
  

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW sales_concat_order_id AS
SELECT
  orderdate,
  CONCAT(order_id, '-', storeNr) as order_id,
  sku,
  productname,
  storeNr,
  email,
  sales_quantity,
  net_unit_price,
  unit_price,
  discount_amount,
  extended_discount_amount,
  extended_sales_amount,
  total_sales,
  discount_percentage,
  external_voucher_provider,
  internal_promotion
FROM
  sales_derived_facts

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW sales_conform_products AS
SELECT
  orderdate,
  order_id,
  INITCAP(sku)   as sku,
  productname,
  storeNr,
  email,
  sales_quantity,
  net_unit_price,
  unit_price,
  discount_amount,
  extended_discount_amount,
  extended_sales_amount,
  total_sales,
  discount_percentage,
  external_voucher_provider,
  internal_promotion
FROM
  sales_concat_order_id

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW sales_conform_date AS
SELECT
  *,
  DATE_FORMAT(TIMESTAMP_SECONDS(orderdate), 'yyyyMMdd') as date_key,
  DATE_FORMAT(TIMESTAMP_SECONDS(orderdate), 'HH:mm:ss') as time_of_day
FROM
  sales_conform_products;

-- COMMAND ----------

CREATE
  OR REPLACE TEMPORARY VIEW sales_conform_customer_fields AS
SELECT
  order_id,
  storeNr,
  sku,
  CASE WHEN email IS NULL THEN 'Not Provided' ELSE LOWER(email) END as customer_email,
  date_key,
  time_of_day,
  sales_quantity,
  net_unit_price,
  unit_price,
  discount_amount,
  extended_discount_amount,
  extended_sales_amount,
    total_sales,
    discount_percentage,
  external_voucher_provider,
  internal_promotion
FROM
  sales_conform_date

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW sales_conform_voucher AS
SELECT
  order_id,
  storeNr as store_number,
  sku,
  customer_email,
  date_key,
  time_of_day,
  sales_quantity,
  net_unit_price,
  unit_price,
  discount_amount,
  extended_discount_amount,
  extended_sales_amount,
    total_sales,
  CASE
    WHEN internal_promotion = 1 THEN "Internal Promotion"
    ELSE "Not Applicable"
  END as part_of_promotion,
  CASE
    WHEN external_voucher_provider = 1 THEN "Voyado"
    ELSE "Not Applicable"
  END as external_voucher_provider,
  CASE
    WHEN external_voucher_provider = 1
    or internal_promotion = 1 THEN "Yes"
    ELSE "No"
  END as voucher_used,
  CASE
    WHEN discount_percentage BETWEEN 1.0
    AND 10.0 THEN "1-10 %"
    WHEN discount_percentage BETWEEN 11.0
    AND 25.0 THEN "11-25 %"
    WHEN discount_percentage BETWEEN 26.0
    AND 50.0 THEN "26-50 %"
    WHEN discount_percentage BETWEEN 51.0
    AND 100.0 THEN "51-100 %"
    ELSE "Not Applicable"
  END as voucher_discount_range
FROM
  sales_conform_customer_fields

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW sales_fact AS
SELECT
  date_key ,
  sku ,
   store_number ,
  customer_email ,
  voucher_discount_range ,
  voucher_used ,
  part_of_promotion ,
  external_voucher_provider ,
  order_id ,
  time_of_day,
  sales_quantity ,
  unit_price ,
  discount_amount ,
  net_unit_price ,
  extended_discount_amount ,
  extended_sales_amount,
    total_sales

FROM
  sales_conform_voucher

-- COMMAND ----------

-- MAGIC  %python
-- MAGIC 
-- MAGIC sales_fact = spark.read.table("sales_fact")
-- MAGIC 
-- MAGIC (
-- MAGIC     sales_fact.write.format("jdbc")
-- MAGIC     .option(
-- MAGIC         "url",
-- MAGIC         "jdbc:sqlserver://hk-analysis.database.windows.net;databaseName=analysis;",
-- MAGIC     )
-- MAGIC     .option("dbtable", "Sales_Fact_Staging")
-- MAGIC     .option("user", dbutils.secrets.get(scope="key-vault", key="analysisSqlUser"))
-- MAGIC     .option(
-- MAGIC         "password", dbutils.secrets.get(scope="key-vault", key="analysisSqlPassword")
-- MAGIC     )
-- MAGIC     .mode("append")
-- MAGIC     .save()
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pyodbc
-- MAGIC 
-- MAGIC server = "hk-analysis.database.windows.net"
-- MAGIC 
-- MAGIC username = dbutils.secrets.get(scope="key-vault", key="analysisSqlUser")
-- MAGIC 
-- MAGIC password = dbutils.secrets.get(scope="key-vault", key="analysisSqlPassword")
-- MAGIC 
-- MAGIC conn = pyodbc.connect(
-- MAGIC     "DRIVER=ODBC Driver 17 for SQL Server;SERVER=hk-analysis.database.windows.net;DATABASE=analysis;UID={user};PWD={password}".format(
-- MAGIC         user=username, password=password
-- MAGIC     )
-- MAGIC )
-- MAGIC 
-- MAGIC 
-- MAGIC cursor = conn.cursor()
-- MAGIC 
-- MAGIC cursor.execute(
-- MAGIC     "EXECUTE Merge_sales"
-- MAGIC )
-- MAGIC 
-- MAGIC conn.commit()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC conn.close()

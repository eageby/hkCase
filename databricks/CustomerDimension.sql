-- Databricks notebook source
-- MAGIC %python
-- MAGIC orders = spark.read.format('csv').options(
-- MAGIC     header='true', inferschema='true').load("/mnt/hkdata/dboorders.txt")
-- MAGIC 
-- MAGIC orders.printSchema()
-- MAGIC orders.createOrReplaceTempView("Orders")

-- COMMAND ----------

CREATE OR REPLACE TABLE Customer_Dimension (
  Customer_Key BIGINT GENERATED ALWAYS AS IDENTITY KEY,
  Customer_First_Name VARCHAR(100) NOT NULL,
  Customer_Last_Name VARCHAR(100) NOT NULL,
  Customer_Email VARCHAR(100) NOT NULL
  );

-- COMMAND ----------

INSERT INTO Customer_Dimension (Customer_First_Name, Customer_Last_Name, Customer_Email)
select distinct
  CASE WHEN namefirst IS NULL THEN 'Not Applicable' ELSE INITCAP(namefirst) END
  ,CASE WHEN namelast IS NULL THEN 'Not Applicable' ELSE INITCAP(namelast) END
  ,CASE WHEN email IS NULL THEN 'Not Applicable' ELSE LOWER(email) END 
from orders
where namefirst is not null or namelast is not null;

-- COMMAND ----------

CREATE TABLE Customer_Dimension_OLAP
USING JDBC
OPTIONS (
url "jdbc:sqlserver://hk-analysis.database.windows.net;databaseName=analysis;",
dbtable "Customer_Dimension",
user "${spark.sqlUser}",
password "${spark.sqlPassword}"
)
SELECT *
FROM Customer_Dimension

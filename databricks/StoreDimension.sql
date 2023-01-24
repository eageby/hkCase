-- Databricks notebook source
-- DBTITLE 1,Read Transactional Data From Data Lake
-- MAGIC %python
-- MAGIC stores = (
-- MAGIC     spark.read.format("csv")
-- MAGIC     .options(header="true", inferschema="true")
-- MAGIC     .load(
-- MAGIC         "abfss://hkdata@hkdatalake563456.dfs.core.windows.net/transactional/dbostores.txt"
-- MAGIC     )
-- MAGIC )
-- MAGIC 
-- MAGIC stores.createOrReplaceTempView("stores_raw_data")

-- COMMAND ----------

-- DBTITLE 1,Read Store Dimension From Warehouse
-- MAGIC %python
-- MAGIC spark.read.format("jdbc").option(
-- MAGIC     "url",
-- MAGIC     "jdbc:sqlserver://hk-analysis.database.windows.net;databaseName=analysis;",
-- MAGIC ).option("dbtable", "Store_Dimension").option(
-- MAGIC     "user", dbutils.secrets.get(scope="key-vault", key="analysisSqlUser")
-- MAGIC ).option(
-- MAGIC     "password", dbutils.secrets.get(scope="key-vault", key="analysisSqlPassword")
-- MAGIC ).load().createOrReplaceTempView(
-- MAGIC     "Store_Dimension"
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 1,Remove Duplicates of Store Number
CREATE
OR REPLACE TEMPORARY VIEW stores_raw_no_duplicates  AS
SELECT
  StoreNr,
  Area,
  InMall
FROM
  (
    SELECT
      StoreNr,
      Area,
      InMall,
      ROW_NUMBER() OVER(
        PARTITION BY StoreNr
        Order by
          storenr DESC
      ) as row_number
    FROM
      stores_raw_data
  )
WHERE
  row_number = 1

-- COMMAND ----------

-- DBTITLE 1,Remove Area Names Suffix
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import split
-- MAGIC 
-- MAGIC df = spark.read.table("stores_raw_no_duplicates")
-- MAGIC split_area_col = split(df["Area"], "-", 2)
-- MAGIC df = df.withColumn("Area", split_area_col[0])
-- MAGIC df.createOrReplaceTempView("stores_trimmed_area_name")

-- COMMAND ----------

-- DBTITLE 1,Extract Main Area and Description from Area Name
-- MAGIC %python
-- MAGIC df = spark.read.table("stores_trimmed_area_name")
-- MAGIC 
-- MAGIC from pyspark.sql.functions import when, array_union, element_at, lit, size, col, array
-- MAGIC 
-- MAGIC df.withColumn("StoreName", df["Area"]).withColumn(
-- MAGIC     "split_area", split("Area", " ", 2)
-- MAGIC ).withColumn(
-- MAGIC     "split_area",
-- MAGIC     when(
-- MAGIC         size("split_area") == 1,
-- MAGIC         array_union(col("split_area"), array(lit("Not Applicable"))),
-- MAGIC     ).otherwise(col("split_area")),
-- MAGIC ).withColumn(
-- MAGIC     "Area", element_at("split_area", 1)
-- MAGIC ).withColumn(
-- MAGIC     "AreaDescription", element_at("split_area", 2)
-- MAGIC ).drop(
-- MAGIC     "split_area"
-- MAGIC ).createOrReplaceTempView(
-- MAGIC     "stores_area_description_split"
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 1,Indicate if Main Area is Shared with Other Stores
CREATE
OR REPLACE TEMPORARY VIEW stores_shared_area_added AS
SELECT
  s.StoreNr,
  s.StoreName,
  s.Area,
  s.AreaDescription,
  CASE
    WHEN c.AreaCount > 1 THEN "Shared Area"
    ELSE "Not Shared"
  END as SharedArea,
  s.InMall
FROM
  stores_area_description_split s
  JOIN (
    SELECT
      Area,
      COUNT(*) as AreaCount
    FROM
      Stores_area_description_split
    GROUP BY
      Area
  ) as c on s.Area = c.Area

-- COMMAND ----------

-- DBTITLE 1,Create Silver Quality View 
CREATE
OR REPLACE TEMPORARY VIEW store_silver (
  Store_Number,
  Store_Name,
  Store_Area_Name,
  Store_Area_Description,
  Store_Shared_area,
  Store_Placement
) AS
SELECT
  DISTINCT StoreNr,
  INITCAP(StoreName),
  INITCAP(Area),
  INITCAP(AreaDescription),
  SharedArea,
  CASE
    WHEN InMall = 1 THEN 'In Mall'
    ELSE 'Outside Mall'
  END
FROM
  stores_shared_area_added;

-- COMMAND ----------

-- DBTITLE 1,Create View to Append to Warehouse Dimension
CREATE
OR REPLACE TEMPORARY VIEW store_insert AS
SELECT
  Store_Number,
  Store_Name,
  Store_Area_Name,
  Store_Area_Description,
  Store_Shared_area,
  Store_Placement
FROM
  store_silver
WHERE
  (Store_number) NOT IN (
    SELECT
      Store_Number
    FROM
      Store_Dimension
  )

-- COMMAND ----------

-- DBTITLE 1,Append to Warehouse Dimension
-- MAGIC %python
-- MAGIC 
-- MAGIC store_dim = spark.read.table("store_insert")
-- MAGIC 
-- MAGIC (
-- MAGIC     store_dim.write.format("jdbc")
-- MAGIC     .option(
-- MAGIC         "url",
-- MAGIC         "jdbc:sqlserver://hk-analysis.database.windows.net;databaseName=analysis;",
-- MAGIC     )
-- MAGIC     .option("dbtable", "Store_Dimension")
-- MAGIC     .option("user", dbutils.secrets.get(scope="key-vault", key="analysisSqlUser"))
-- MAGIC     .option(
-- MAGIC         "password", dbutils.secrets.get(scope="key-vault", key="analysisSqlPassword")
-- MAGIC     )
-- MAGIC     .mode("append")
-- MAGIC     .save()
-- MAGIC )

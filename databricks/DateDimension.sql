-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC 
-- MAGIC dates = (
-- MAGIC     spark.read.format("csv")
-- MAGIC     .options(header="true", inferschema="true", delimiter=";")
-- MAGIC     .load(
-- MAGIC         "abfss://hkdata@hkdatalake563456.dfs.core.windows.net/transactional/dates.csv"
-- MAGIC     )
-- MAGIC )
-- MAGIC 
-- MAGIC dates = dates.select(
-- MAGIC     [col(c).alias(c.replace(" ", "_")) for c in dates.columns]
-- MAGIC )
-- MAGIC dates.printSchema()
-- MAGIC dates.createOrReplaceTempView("Dates");

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW Date_Dimension AS
SELECT
  date_key,
  CAST(DATEADD(day, full_date, '1900-01-01') as DATE) as full_date,
  day_of_week,
  day_num_in_month,
  day_num_overall,
  day_name,
  day_abbrev,
  weekday_flag,
  week_num_in_year,
  week_num_overall,
  CAST(
    DATEADD(day, week_begin_date, '1900-01-01') as DATE
  ) as week_begin_date,
  week_begin_date_key,
  month,
  month_num_overall,
  month_name,
  month_abbrev,
  quarter,
  year,
  yearmo,
  fiscal_month,
  fiscal_quarter,
  fiscal_year,
  last_day_in_month_flag,
  CAST(
    DATEADD(day, same_day_year_ago, '1900-01-01') as DATE
  ) as same_day_year_ago
FROM
  Dates;

-- COMMAND ----------

SELECT
  *
FROM
  DATE_DIMENSION

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC date_dim = spark.sql("SELECT * FROM Date_Dimension")
-- MAGIC 
-- MAGIC (
-- MAGIC     date_dim.write.format("jdbc")
-- MAGIC     .option(
-- MAGIC         "url",
-- MAGIC         "jdbc:sqlserver://hk-analysis.database.windows.net;databaseName=analysis;",
-- MAGIC     )
-- MAGIC     .option("dbtable", "Date_Dimension")
-- MAGIC     .option("user", dbutils.secrets.get(scope="key-vault", key="analysisSqlUser"))
-- MAGIC     .option(
-- MAGIC         "password", dbutils.secrets.get(scope="key-vault", key="analysisSqlPassword")
-- MAGIC     )
-- MAGIC     .mode("append")
-- MAGIC     .save()
-- MAGIC )

-- COMMAND ----------



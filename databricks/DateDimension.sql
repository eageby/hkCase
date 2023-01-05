-- Databricks notebook source
CREATE TABLE Date_Dimension
USING JDBC
OPTIONS (
url "jdbc:sqlserver://hk-analysis.database.windows.net;databaseName=analysis;",
dbtable "Date_Dimension",
user "${spark.sqlUser}",
password "${spark.sqlPassword}"
)
SELECT 
     date_key
    ,CAST(DATEADD(day,full_date,'1900-01-01') as DATE) as full_date
    ,day_of_week	
    ,day_num_in_month	
    ,day_num_overall	
    ,day_name	
    ,day_abbrev	
    ,weekday_flag	
    ,week_num_in_year	
    ,week_num_overall	
    ,CAST(DATEADD(day,week_begin_date,'1900-01-01') as DATE) as week_begin_date
    ,week_begin_date_key	
    ,month	
    ,month_num_overall	
    ,month_name	
    ,month_abbrev	
    ,quarter	
    ,year	
    ,yearmo	
    ,fiscal_month	
    ,fiscal_quarter	
    ,fiscal_year	
    ,last_day_in_month_flag	
    ,CAST(DATEADD(day,same_day_year_ago,'1900-01-01') as DATE) as same_day_year_ago
FROM Dates;

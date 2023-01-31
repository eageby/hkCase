CREATE OR ALTER PROCEDURE Merge_Date
AS
BEGIN
    -- MERGE Date_Dimension_Test AS Target
    MERGE Date_Dimension AS Target
    USING Date_Dimension_Staging AS Source
    ON Source.date_key = Target.date_key
    WHEN NOT MATCHED BY Target THEN 
        INSERT (
            date_key,
            full_date,
            day_of_week,
            day_num_in_month,
            day_num_overall,
            day_name,
            day_abbrev,
            weekday_flag,
            week_num_in_year,
            week_num_overall,
            week_begin_date,
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
            same_day_year_ago 
        ) 
        VALUES (
            Source.date_key,
            Source.full_date,
            Source.day_of_week,
            Source.day_num_in_month,
            Source.day_num_overall,
            Source.day_name,
            Source.day_abbrev,
            Source.weekday_flag,
            Source.week_num_in_year,
            Source.week_num_overall,
            Source.week_begin_date,
            Source.week_begin_date_key,
            Source.month,
            Source.month_num_overall,
            Source.month_name,
            Source.month_abbrev,
            Source.quarter,
            Source.year,
            Source.yearmo,
            Source.fiscal_month,
            Source.fiscal_quarter,
            Source.fiscal_year,
            Source.last_day_in_month_flag,
            Source.same_day_year_ago 
        );

END
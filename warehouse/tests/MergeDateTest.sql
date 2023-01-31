CREATE TABLE Date_Dimension_Test
(
  date_key INT NOT NULL,
  full_date DATE NOT NULL,
  day_of_week INT NOT NULL,
  day_num_in_month INT NOT NULL,
  day_num_overall INT NOT NULL,
  day_name VARCHAR(100) NOT NULL,
  day_abbrev VARCHAR(100) NOT NULL,
  weekday_flag VARCHAR(100) NOT NULL,
  week_num_in_year INT NOT NULL,
  week_num_overall INT NOT NULL,
  week_begin_date DATE NOT NULL,
  week_begin_date_key INT NOT NULL,
  month INT NOT NULL,
  month_num_overall INT NOT NULL,
  month_name VARCHAR(100) NOT NULL,
  month_abbrev VARCHAR(100) NOT NULL,
  quarter INT NOT NULL,
  year INT NOT NULL,
  yearmo INT NOT NULL,
  fiscal_month INT NOT NULL,
  fiscal_quarter INT NOT NULL,
  fiscal_year INT NOT NULL,
  last_day_in_month_flag VARCHAR(100) NOT NULL,
  same_day_year_ago DATE NOT NULL
)

INSERT INTO Date_Dimension_Test VALUES(20130101, '2013-1-1',2, 1, 1, 'Tuesday', 'Tue', 'Weekday', 1, 1, '2012-12-31', 20121231, 1, 1, 'January', 'Jan', 1, 2013, 201301, 7, 3, 2013, 'Not Month End', '2012-1-1');
insert into Date_Dimension_Test values(20130102, '2013-1-2',3, 2, 2, 'Wednesday', 'Wed', 'Weekday', 1, 1, '2012-12-31', 20121231, 1, 1, 'January', 'Jan', 1, 2013, 201301, 7, 3, 2013, 'Not Month End', '2012-1-2')

INSERT INTO Date_Dimension_Staging VALUES(20130101, '2013-1-1',2, 1, 1, 'Tuesday', 'Tue', 'Weekday', 1, 1, '2012-12-31', 20121231, 1, 1, 'January', 'Jan', 1, 2013, 201301, 7, 3, 2013, 'Not Month End', '2012-1-1');
insert into Date_Dimension_Staging values(20130102, '2013-1-2',3, 2, 2, 'Wednesday', 'Wed', 'Weekday', 1, 1, '2012-12-31', 20121231, 1, 1, 'January', 'Jan', 1, 2013, 201301, 7, 3, 2013, 'Not Month End', '2012-1-2')
insert into Date_Dimension_Staging values(20130103, '2013-1-3',4, 3, 3, 'UPDATE TEST', 'Thu', 'Weekday', 1, 1, '2012-12-31', 20121231, 1, 1, 'January', 'Jan', 1, 2013, 201301, 7, 3, 2013, 'Not Month End', '2012-1-3')


SELECT * FROM Date_Dimension_Test
SELECT * FROM Date_Dimension_Staging

EXECUTE Merge_date;

SELECT * FROM Date_Dimension_Test

DROP TABLE Date_Dimension_Test
DELETE FROM Date_Dimension_staging
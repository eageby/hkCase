DROP TABLE Product_Dimension;
CREATE TABLE Product_Dimension
(
  product_key INT IDENTITY NOT NULL,
  sku VARCHAR(100) NOT NULL,
  product_name VARCHAR(100) NOT NULL
);

DROP TABLE Product_Dimension_Staging;
CREATE TABLE Product_Dimension_Staging
(
  sku VARCHAR(100) NOT NULL,
  product_name VARCHAR(100) NOT NULL
);

SET IDENTITY_INSERT Product_Dimension ON
INSERT INTO Product_Dimension
  (product_key, sku, product_name)
VALUES
  (0, 'Not Applicable', 'Not Applicable')
SET IDENTITY_INSERT Product_Dimension OFF


DROP TABLE Customer_Dimension;
CREATE TABLE Customer_Dimension
(
  customer_key INT IDENTITY NOT NULL,
  customer_first_name VARCHAR(100) NOT NULL,
  customer_last_name VARCHAR(100) NOT NULL,
  customer_email VARCHAR(100) NOT NULL,
  customer_registered VARCHAR(100) NOT NULL,
  customer_recurring VARCHAR(100) NOT NULL,
  customer_number_of_orders INT NOT NULL,
  customer_number_of_orders_group VARCHAR(100) NOT NULL,
  customer_active VARCHAR(100) NOT NULL,
  row_effective_date DATE NOT NULL,
  row_expiration_date DATE NOT NULL,
  current_row_indicator VARCHAR(100) NOT NULL
);

DROP TABLE Customer_Dimension_Staging;
CREATE TABLE Customer_Dimension_Staging
(
  customer_first_name VARCHAR(100) NOT NULL,
  customer_last_name VARCHAR(100) NOT NULL,
  customer_email VARCHAR(100) NOT NULL,
  customer_registered VARCHAR(100) NOT NULL,
  customer_recurring VARCHAR(100) NOT NULL,
  customer_number_of_orders INT NOT NULL,
  customer_number_of_orders_group VARCHAR(100) NOT NULL,
  customer_active VARCHAR(100) NOT NULL
);

SET IDENTITY_INSERT Customer_Dimension ON
INSERT INTO Customer_Dimension
  (customer_key, customer_first_name, customer_last_name, customer_email, customer_registered, customer_recurring,customer_number_of_orders, customer_number_of_orders_group, customer_active, row_effective_date , row_expiration_date, current_row_indicator )
VALUES
  (0, 'Not Provided', 'Not Provided', 'Not Provided', 'Unregistered Customer', 'Not Applicable', 0, 'Not Applicable' , 'Not Applicable', CAST('1753-01-01' AS DATE), CAST('9999-12-31' AS DATE), 'Current')
SET IDENTITY_INSERT Customer_Dimension OFF

DROP TABLE Date_Dimension
CREATE TABLE Date_Dimension
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
DROP TABLE Date_Dimension_Staging
CREATE TABLE Date_Dimension_Staging
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

DROP TABLE Store_Dimension
CREATE TABLE Store_Dimension
(
  store_key INT IDENTITY NOT NULL,
  store_number INT NOT NULL,
  store_name VARCHAR(100) NOT NULL,
  store_area_name VARCHAR(100) NOT NULL,
  store_area_description VARCHAR(100) NOT NULL,
  store_shared_area VARCHAR(100) NOT NULL,
  store_placement VARCHAR(100) NOT NULL,
  store_in_operation VARCHAR(100) NOT NULL
)

DROP TABLE Store_Dimension_Staging
CREATE TABLE Store_Dimension_Staging
(
  store_number INT NOT NULL,
  store_name VARCHAR(100) NOT NULL,
  store_area_name VARCHAR(100) NOT NULL,
  store_area_description VARCHAR(100) NOT NULL,
  store_shared_area VARCHAR(100) NOT NULL,
  store_placement VARCHAR(100) NOT NULL
)

DROP TABLE Sales_Fact_Staging;
CREATE TABLE Sales_Fact_Staging
(
  date_key INT NOT NULL,
  sku VARCHAR(100),
  store_number VARCHAR(100) NOT NULL,
  customer_email VARCHAR(100) NOT NULL,
  voucher_discount_range VARCHAR(100) NOT NULL,
  voucher_used VARCHAR(100) NOT NULL,
  part_of_promotion VARCHAR(100) NOT NULL,
  external_voucher_provider VARCHAR(100) NOT NULL,
  order_id VARCHAR(100) NOT NULL,
  time_of_day VARCHAR(100) NOT NULL,
  sales_quantity INT NOT NULL,
  unit_price DECIMAL NOT NULL,
  discount_amount DECIMAL NOT NULL,
  net_unit_price DECIMAL NOT NULL,
  extended_discount_amount DECIMAL NOT NULL,
  extended_sales_amount DECIMAL NOT NULL,
  total_sales DECIMAL NOT NULL
)

DROP TABLE Sales_Fact;
CREATE TABLE Sales_Fact
(
  date_key INT NOT NULL,
  product_key INT NOT NULL,
  store_key INT NOT NULL,
  customer_key INT NOT NULL,
  voucher_key INT NOT NULL,
  order_id VARCHAR(100) NOT NULL,
  time_of_day VARCHAR(100) NOT NULL,
  sales_quantity INT NOT NULL,
  unit_price DECIMAL NOT NULL,
  discount_amount DECIMAL NOT NULL,
  net_unit_price DECIMAL NOT NULL,
  extended_discount_amount DECIMAL NOT NULL,
  extended_sales_amount DECIMAL NOT NULL,
  total_sales DECIMAL NOT NULL
)

DROP TABLE Voucher_Dimension;
CREATE TABLE Voucher_Dimension
(
  voucher_key INT IDENTITY NOT NULL,
  voucher_used VARCHAR(100) NOT NULL,
  voucher_discount_range VARCHAR(100) NOT NULL,
  external_voucher_provider VARCHAR(100) NOT NULL,
  part_of_promotion VARCHAR(100) NOT NULL
);

DROP TABLE Voucher_Dimension_Staging;
CREATE TABLE Voucher_Dimension_Staging
(
  voucher_used VARCHAR(100) NOT NULL,
  voucher_discount_range VARCHAR(100) NOT NULL,
  external_voucher_provider VARCHAR(100) NOT NULL,
  part_of_promotion VARCHAR(100) NOT NULL
);


SET IDENTITY_INSERT Voucher_Dimension ON
INSERT INTO Voucher_Dimension
  (voucher_key, voucher_used, voucher_discount_range, external_voucher_provider, part_of_promotion)
VALUES
  (0, 'No', 'Not Applicable', 'Not Applicable', 'Not Applicable') 
SET IDENTITY_INSERT Voucher_Dimension OFF
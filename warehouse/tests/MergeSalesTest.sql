CREATE TABLE Sales_Fact_Test
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
  extended_sales_amount DECIMAL NOT NULL
)

INSERT INTO Sales_Fact_TEST
  (date_key,product_key,store_key,customer_key,voucher_key,order_id,time_of_day,sales_quantity,unit_price,discount_amount,net_unit_price,extended_discount_amount,extended_sales_amount)
VALUES
  ( 20180712, 50562, 1, 0, 0, '10000-101', CAST('13:59:30' as TIME), 1, 1, 0, 1, 0, 1)

INSERT INTO Sales_Fact_TEST
  (date_key,product_key,store_key,customer_key,voucher_key,order_id,time_of_day,sales_quantity,unit_price,discount_amount,net_unit_price,extended_discount_amount,extended_sales_amount)
VALUES
  (20180316,6541,5,0,0,'10000-108',CAST( '10:43:50' as TIME),1,140,0,140,0,140)


SELECT * FROM Sales_Fact_TEST

INSERT INTO Sales_Fact_Staging
  (date_key,product_key,store_key,customer_key,voucher_key,order_id,time_of_day,sales_quantity,unit_price,discount_amount,net_unit_price,extended_discount_amount,extended_sales_amount)
VALUES
  (20180316,6541,5,0,0,'10000-108',CAST( '10:43:50' as TIME),1,140,0,140,0,140)
INSERT INTO Sales_Fact_Staging
  (date_key,product_key,store_key,customer_key,voucher_key,order_id,time_of_day,sales_quantity,unit_price,discount_amount,net_unit_price,extended_discount_amount,extended_sales_amount)
VALUES
  (20180316,6541,5,0,0,'10000-108',CAST( '10:43:50' as TIME),1,140,0,140,0,140)
INSERT INTO Sales_Fact_Staging
  (date_key,product_key,store_key,customer_key,voucher_key,order_id,time_of_day,sales_quantity,unit_price,discount_amount,net_unit_price,extended_discount_amount,extended_sales_amount)
VALUES
  (20180921,35454,22,0,0,'10000-214',CAST('08:30:35' as TIME),1,204,0,204,0,204)


EXECUTE Merge_Sales



SELECT * FROM Sales_Fact_TEST

DROP TABLE Sales_Fact_Test
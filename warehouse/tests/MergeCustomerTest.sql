CREATE TABLE Customer_Dimension_TEST
(
  customer_key INT IDENTITY NOT NULL,
  customer_first_name VARCHAR(100) NOT NULL,
  customer_last_name VARCHAR(100) NOT NULL,
  customer_email VARCHAR(100) NOT NULL,
  customer_registered VARCHAR(100) NOT NULL,
  customer_recurring VARCHAR(100) NOT NULL,
  customer_number_of_orders INT,
  customer_number_of_orders_group VARCHAR(100) NOT NULL,
  row_effective_date DATE NOT NULL,
  row_expiration_date DATE NOT NULL,
  current_row_indicator VARCHAR(100) NOT NULL
);

DECLARE @Yesterday DATE = DATEADD(dd,-1,GETDATE())
DECLARE @Today DATE = GETDATE()
DECLARE @Future DATE = CAST('9999-12-31' AS DATE)

INSERT INTO   Customer_Dimension_Test
    (customer_first_name, customer_last_name, customer_email, customer_registered, customer_recurring,customer_number_of_orders, customer_number_of_orders_group,  row_effective_date , row_expiration_date, current_row_indicator )
VALUES
    ('Emma', 'Stenström', '-emma-ida-louise@hotmail.com', 'Registered Customer', 'Not Recurring Customer', 1, '1-5 Orders', CAST('1/1/2000' as DATE), CAST('12/31/9999' AS DATE), 'Current');
INSERT INTO   Customer_Dimension_Test
    (customer_first_name, customer_last_name, customer_email, customer_registered, customer_recurring,customer_number_of_orders, customer_number_of_orders_group,  row_effective_date , row_expiration_date, current_row_indicator )
VALUES
    ('TO BE', 'DELETED', 'test@tesst.com', 'Registered Customer', 'Not Recurring Customer', 1, '1-5 Orders', CAST('1/1/2000' as DATE), CAST('12/31/9999' AS DATE), 'Current');


INSERT INTO   Customer_Dimension_Staging
    (customer_first_name, customer_last_name, customer_email, customer_registered, customer_recurring,customer_number_of_orders, customer_number_of_orders_group)
VALUES
    ('Emma', 'Stenström', '-emma-ida-louise@hotmail.com', 'Registered Customer', 'Recurring Customer', 6, '6-10 Orders');

INSERT INTO   Customer_Dimension_Staging
    (customer_first_name, customer_last_name, customer_email, customer_registered, customer_recurring,customer_number_of_orders, customer_number_of_orders_group)
VALUES
    ('Test', 'Test', 'test@test.com', 'Registered Customer', 'Not Recurring Customer', 1, '1-5 Orders');

SELECT *
FROM Customer_Dimension_Staging
SELECT *
FROM Customer_Dimension_Test

EXECUTE Merge_Customer

SELECT *
FROM Customer_Dimension_Staging
SELECT *
FROM Customer_Dimension_Test

DROP TABLE Customer_Dimension_Test
DELETE FROM Customer_Dimension_Staging
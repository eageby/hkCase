-- SCD 
-- Customer Recurring - TYPE 2 

CREATE OR ALTER PROCEDURE Merge_customer
AS
BEGIN

    DECLARE @Yesterday DATE = DATEADD(dd,-1,GETDATE())
    DECLARE @Today DATE = GETDATE()
    DECLARE @MaxDate DATE = CAST('9999-12-31' AS DATE)

    INSERT INTO Customer_Dimension
    -- INSERT INTO Customer_Dimension_Test
    SELECT
        customer_first_name, customer_last_name, customer_email, customer_registered, customer_recurring, customer_number_of_orders, customer_number_of_orders_group, customer_active, row_effective_date , row_expiration_date, current_row_indicator
    FROM (
    MERGE Customer_Dimension AS Target
    -- MERGE Customer_Dimension_Test AS Target
    USING Customer_Dimension_Staging AS Staging
    ON Staging.customer_email = Target.customer_email
    WHEN NOT MATCHED BY Target THEN 
        INSERT (customer_first_name, customer_last_name, customer_email, customer_registered, customer_recurring,customer_number_of_orders, customer_number_of_orders_group,  customer_active, row_effective_date , row_expiration_date, current_row_indicator )
        VALUES (Staging.customer_first_name, Staging.customer_last_name, Staging.customer_email, Staging.customer_registered, Staging.customer_recurring, Staging.customer_number_of_orders,Staging.customer_number_of_orders_group, Staging.customer_active, @Today, @MaxDate, 'Current')

    WHEN MATCHED AND current_row_indicator = 'Current' THEN UPDATE SET 
        current_row_indicator = 'Expired',
        row_expiration_date = @Today

    WHEN NOT MATCHED BY SOURCE THEN UPDATE SET
        customer_active = 'Inactive'

    OUTPUT 
        $Action as ActionType,
        Staging.customer_first_name, 
        Staging.customer_last_name, 
        Staging.customer_email, 
        Staging.customer_registered, 
        Staging.customer_recurring, 
        Staging.customer_number_of_orders,
        Staging.customer_number_of_orders_group,
        Staging.customer_active,
        @Today as row_effective_date,
        @MaxDate as row_expiration_date, 
        'Current' as current_row_indicator
    ) m
    WHERE m.ActionType = 'UPDATE' AND customer_active = 'Active'

    DELETE FROM Customer_Dimension_Staging;
END
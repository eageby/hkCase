CREATE OR ALTER PROCEDURE Merge_Sales
AS
BEGIN
    -- MERGE Sales_Fact_Test AS Target
    MERGE Sales_Fact AS Target
    USING 
    (
        SELECT
        date_key,
        COALESCE(product_key, 0) AS product_key,
        store_key,
        COALESCE(customer_key, 0) AS customer_key,
        voucher_key,
        order_id,
        time_of_day,
        sales_quantity,
        unit_price,
        discount_amount,
        net_unit_price,
        extended_discount_amount,
       extended_sales_amount,
       total_sales
    FROM
        Sales_Fact_Staging S
        LEFT JOIN product_dimension P ON S.sku = P.sku
        LEFT JOIN store_dimension SD ON S.store_number = SD.store_number
        LEFT JOIN customer_dimension C on S.customer_email = C.customer_email AND C.current_row_indicator = 'Current'
        LEFT JOIN voucher_dimension V on S.voucher_discount_range = V.voucher_discount_range
            AND S.voucher_used = V.voucher_used
            AND S.part_of_promotion = V.part_of_promotion
            AND S.external_voucher_provider = V.external_voucher_provider
    )
    AS Source ON Source.order_id = Target.order_id AND Source.date_key = Target.date_key
    WHEN NOT MATCHED BY Target THEN 
        INSERT (date_key,product_key,store_key,customer_key,voucher_key,order_id,time_of_day,sales_quantity,unit_price,discount_amount,net_unit_price,extended_discount_amount,extended_sales_amount, total_sales) 
        VALUES (Source.date_key, Source.product_key, Source.store_key, Source.customer_key, Source.voucher_key, Source.order_id, Source.time_of_day, Source.sales_quantity, Source.unit_price, Source.discount_amount, Source.net_unit_price, Source.extended_discount_amount, Source.extended_sales_amount, Source.total_sales)
    
        WHEN NOT MATCHED BY SOURCE THEN 
            DELETE
;

DELETE FROM Sales_Fact_Staging;

END
-- SCD 
-- Product Name - TYPE 1 

CREATE OR ALTER PROCEDURE Merge_Product AS 
BEGIN
-- MERGE Product_Dimension_Test AS Target
MERGE Product_Dimension AS Target
USING Product_Dimension_Staging AS Source
ON Source.sku = Target.sku
WHEN NOT MATCHED BY Target THEN 
    INSERT (sku,  product_name)
    VALUES (Source.sku, Source.product_name)

    WHEN MATCHED THEN UPDATE SET 
        Target.product_name = Source.product_name

    WHEN NOT MATCHED BY SOURCE THEN 
        DELETE;

DELETE FROM Product_Dimension_Staging;
END
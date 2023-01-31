-- SCD 
-- Store Shared Area - TYPE 1 

CREATE OR ALTER PROCEDURE Merge_Store
AS
BEGIN
    -- MERGE Store_Dimension_Test AS Target
    MERGE Store_Dimension AS Target
    USING Store_Dimension_Staging AS Source
    ON Source.store_number = Target.store_number
    WHEN NOT MATCHED BY Target THEN 
        INSERT ( store_number, store_name , store_area_name , store_area_description , store_shared_area , store_placement, store_in_operation ) 
        VALUES( Source.store_number, Source.store_name , Source.store_area_name , Source.store_area_description , Source.store_shared_area , Source.store_placement,  'In Operation')
    
        WHEN MATCHED THEN UPDATE SET
            Target.store_shared_area = Source.store_shared_area

        WHEN NOT MATCHED BY SOURCE THEN UPDATE SET
            Target.store_in_operation = 'Out Of Operation' 
    ;

    DELETE FROM Store_Dimension_Staging;

END
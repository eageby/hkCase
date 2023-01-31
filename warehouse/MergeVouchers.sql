CREATE OR ALTER PROCEDURE Merge_Voucher
AS
BEGIN
    MERGE Voucher_Dimension AS Target
USING Voucher_Dimension_Staging AS Source
ON Source.voucher_used = Target.voucher_used
        AND Source.voucher_discount_range = Target.voucher_discount_range
        AND Source.external_voucher_provider = Target.external_voucher_provider
        AND Source.part_of_promotion = Target.part_of_promotion

WHEN NOT MATCHED BY Target THEN 
    INSERT ( voucher_used, voucher_discount_range, external_voucher_provider, part_of_promotion)
    VALUES ( Source.voucher_used, Source.voucher_discount_range, Source.external_voucher_provider, Source.part_of_promotion);

    DELETE FROM Voucher_Dimension_Staging;
END
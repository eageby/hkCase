CREATE TABLE Product_Dimension_Test
(
  product_key INT IDENTITY NOT NULL,
  sku VARCHAR(100) NOT NULL,
  product_name VARCHAR(100) NOT NULL
);

INSERT INTO Product_Dimension_Test (sku, product_name) VALUES ('12345', 'Piller')
INSERT INTO Product_Dimension_Test (sku, product_name) VALUES ('678', 'To be Deleted')

INSERT INTO Product_Dimension_Staging (sku, product_name) VALUES ('12345', 'Piller 20st')
INSERT INTO Product_Dimension_Staging (sku, product_name) VALUES ('543321', 'Ansiktskräm')

SELECT * FROM Product_Dimension_Staging
SELECT * FROM Product_Dimension_Test

EXECUTE Merge_Product

SELECT * FROM Product_Dimension_Staging
SELECT * FROM Product_Dimension_Test

DROP TABLE  Product_Dimension_Test
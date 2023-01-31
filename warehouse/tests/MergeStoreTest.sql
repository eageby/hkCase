CREATE TABLE Store_Dimension_Test
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

INSERT INTO Store_Dimension_Test (store_number, store_name, store_area_name, store_area_description, store_shared_area, store_placement, store_in_operation) VALUES (123, 'Stockholm Centralstation', 'Stockholm','Centralstation','Not Shared Area', 'In Mall', 'In Operation')
INSERT INTO Store_Dimension_Test (store_number, store_name, store_area_name, store_area_description, store_shared_area, store_placement, store_in_operation) VALUES (567, 'Göteborg Nordstan', 'Göteborg','Nordstan','Not Shared Area', 'In Mall', 'In Operation')

INSERT INTO Store_Dimension_Staging (store_number, store_name, store_area_name, store_area_description, store_shared_area, store_placement) VALUES (123, 'Stockholm Centralstation', 'Stockholm','Centralstation','Shared Area', 'In Mall')
INSERT INTO Store_Dimension_Staging (store_number, store_name, store_area_name, store_area_description, store_shared_area, store_placement) VALUES (456, 'Stockholm Mariatorget', 'Stockholm','Mariatorget','Shared Area', 'Outside Mall')

SELECT * FROM Store_Dimension_Staging
SELECT * FROM Store_Dimension_Test

EXECUTE Merge_Store

SELECT * FROM Store_Dimension_Test

DROP TABLE  Store_Dimension_Test
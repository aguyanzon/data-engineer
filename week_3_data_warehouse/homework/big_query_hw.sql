-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-375812.fhv_table.ny_taxis`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_dtc-de-375812/fhv/fhv_tripdata_2019-*.csv.gz']
);

-- Check yellow trip data
SELECT COUNT(*) FROM `fhv_table.ny_taxis`;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE fhv_table.ny_taxis_non_partitoned AS
SELECT * FROM fhv_table.ny_taxis;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE fhv_table.ny_taxis_partitoned
PARTITION BY
  DATE(pickup_datetime) AS
SELECT * FROM fhv_table.ny_taxis;
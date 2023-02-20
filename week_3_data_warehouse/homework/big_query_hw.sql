-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-375812.ny_taxi.fhv_external_data`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_data_lake_dtc-de-375812/fhv/fhv_tripdata_2019-*.parquet']
);

-- Check yellow trip data
SELECT COUNT(*) FROM `ny_taxi.fhv_external_data`;

SELECT COUNT(DISTINCT(affiliated_base_number)) FROM `ny_taxi.fhv_external_data`;
SELECT COUNT(DISTINCT(affiliated_base_number)) FROM `ny_taxi.fhv_tripdata`;

SELECT COUNT(*) FROM `ny_taxi.fhv_external_data` AS t
WHERE t.DOlocationID IS NULL AND t.PUlocationID IS NULL;

CREATE OR REPLACE TABLE ny_taxi.fhv_tripdata_cast (
  `dispatching_base_num` STRING,
  `pickup_datetime` DATETIME,
  `dropOff_datetime` DATETIME,
  `PUlocationID` FLOAT64,
  `DOlocationID` FLOAT64,
  `SR_Flag` FLOAT64,
  `Affiliated_base_number` STRING 
) AS
SELECT dispatching_base_num, CAST(pickup_datetime AS DATETIME), CAST(dropOff_datetime AS DATETIME), PUlocationID, DOlocationID, SR_Flag, Affiliated_base_number FROM ny_taxi.fhv_external_data;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE ny_taxi.fhv_non_partitoned_tripdata AS
SELECT * FROM ny_taxi.fhv_tripdata_cast;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE ny_taxi.fhv_partitoned_tripdata
PARTITION BY
  DATE(pickup_datetime) AS
SELECT * FROM ny_taxi.fhv_tripdata_cast;

SELECT DISTINCT(Affiliated_base_number) FROM `ny_taxi.fhv_non_partitoned_tripdata`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

SELECT DISTINCT(Affiliated_base_number) FROM `ny_taxi.fhv_partitoned_tripdata`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

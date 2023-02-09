-- Query public avaible table
SELECT station_id, name FROM 
bigquery-public-data.new_york_citibike.citibike_stations 
LIMIT 1000;

-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-375812.trips_data_all.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_dtc-de-375812/data/yellow/yellow_tripdata_2019-*.csv', 'gs://dtc_data_lake_dtc-de-375812/data/yellow/yellow_tripdata_2020-*.csv']
);

-- Check yellow trip data
SELECT * FROM `trips_data_all.external_yellow_tripdata` LIMIT 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE trips_data_all.yellow_tripdata_non_partitoned AS
SELECT * FROM trips_data_all.external_yellow_tripdata;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE trips_data_all.yellow_tripdata_partitoned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM trips_data_all.external_yellow_tripdata;

-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `trips_data_all.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitoned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE trips_data_all.yellow_tripdata_partitoned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM trips_data_all.external_yellow_tripdata;

-- Query scans 1.1 GB
SELECT count(*) as trips
FROM trips_data_all.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-01-01' AND '2020-01-31'
  AND VendorID=1;

-- Query scans 864.5 MB
SELECT count(*) as trips
FROM trips_data_all.yellow_tripdata_partitoned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-01-01' AND '2020-01-31'
  AND VendorID=1;
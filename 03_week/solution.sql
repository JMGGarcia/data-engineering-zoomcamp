CREATE OR REPLACE EXTERNAL TABLE `ID.trips_data_all.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_ID/fhv-data/fhv_tripdata_2019-*.csv.gz']
);

SELECT * FROM ID.trips_data_all.external_fhv_tripdata limit 10;

CREATE OR REPLACE TABLE ID.trips_data_all.fhv_tripdata_non_partitoned AS
SELECT * FROM ID.trips_data_all.external_fhv_tripdata;

SELECT * FROM ID.trips_data_all.fhv_tripdata_non_partitoned limit 10;

-- Question 1

SELECT COUNT(*) FROM ID.trips_data_all.fhv_tripdata_non_partitoned;

-- Question 2
-- Just need to highlight the query in Google query to obtain the answer

SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM ID.trips_data_all.external_fhv_tripdata;
SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM ID.trips_data_all.fhv_tripdata_non_partitoned;

-- Question 3

SELECT COUNT(*) FROM ID.trips_data_all.fhv_tripdata_non_partitoned WHERE PUlocationID IS NULL AND DOlocationID IS NULL;

-- Question 4
-- Watch https://www.youtube.com/watch?v=-CqXf7vhhDs
-- Implies we wouldn't be able to partition over string column

-- Question 5 

CREATE OR REPLACE TABLE ID.trips_data_all.fhv_tripdata_partitoned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS
SELECT * FROM ID.trips_data_all.external_fhv_tripdata;

SELECT DISTINCT(Affiliated_base_number) 
FROM ID.trips_data_all.fhv_tripdata_non_partitoned 
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

SELECT DISTINCT(Affiliated_base_number) 
FROM ID.trips_data_all.fhv_tripdata_partitoned_clustered 
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

-- Question 6 
-- External tables aren't really formated in BigQuery

-- Question 7 
-- Consider that under 1Gb of data, it might not be worth it
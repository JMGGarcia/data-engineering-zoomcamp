# Solution for week 1 assignment 

Solutions for the [week 1 assignment](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2023/week_1_docker_sql/homework.md). 

To setup, I just used the provided code locally to fill up the data base. 
To load the green taxi data to the database, the only change needed was to change the pickup/dropoff variables
from `tpep` to `lpep`. 

### Question 1

```
docker build --list
```


### Question 2

```
docker run -it python3.9 /bin/bash
pip list
```


### Question 3

```
SELECT count(*)
FROM public.green_taxi_data 
	WHERE lpep_pickup_datetime >= '2019-01-15' 
	AND  lpep_dropoff_datetime < '2019-01-16';
```

### Question 4

```
SELECT lpep_pickup_datetime, trip_distance
FROM public.green_taxi_data 
	ORDER BY trip_distance DESC limit 1;
```

### Question 5

```
SELECT passenger_count, count(*)
FROM public.green_taxi_data 
	WHERE lpep_pickup_datetime >= '2019-01-01' 
	AND  lpep_pickup_datetime < '2019-01-02'
	GROUP BY passenger_count
	ORDER BY passenger_count;
```

### Question 6

```
SELECT doz."Zone"
FROM green_taxi_data t, zones puz, zones doz
	WHERE t."PULocationID" = puz."LocationID" 
	AND t."DOLocationID" = doz."LocationID"
	AND puz."Zone" LIKE 'Astoria'
	ORDER BY tip_amount desc
	LIMIT 1;
```


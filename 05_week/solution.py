import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types


spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

!wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-06.csv.gz

!wc -l fhvhv_tripdata_2021-06.csv.gz

!gzip -d fhvhv_tripdata_2021-06.csv.gz

df = spark.read \
    .option("header", "true") \
    .csv('fhvhv_tripdata_2021-06.csv')

df.schema

schema = types.StructType([
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropoff_datetime', types.TimestampType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('SR_Flag', types.StringType(), True),
    types.StructField('Affiliated_base_number', types.StringType(), True)
])

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('fhvhv_tripdata_2021-06.csv')

df = df.repartition(12)

df.write.parquet('fhvhv/2021/06/')

df = spark.read.parquet('fhvhv/2021/06/')
df.createOrReplaceTempView('fhvhv')

spark.sql("""
SELECT
    COUNT(*)
FROM
    fhvhv
WHERE
    pickup_datetime >= '2021-06-15 00:00' AND pickup_datetime < '2021-06-16 00:00' 
""").show()

spark.sql("""
SELECT
    pickup_datetime, dropoff_datetime, (dropoff_datetime - pickup_datetime) as diff
FROM
    fhvhv
ORDER BY diff DESC LIMIT 10
""").show()

df_zones = spark.read.parquet('zones/')

df_result = df.join(df_zones, df.PULocationID == df_zones.LocationID)

df_result.createOrReplaceTempView('fhvhv_zones')

df_result.schema

spark.sql("""
SELECT
    COUNT(*) as zone_count, Zone
FROM
    fhvhv_zones
GROUP BY Zone ORDER BY zone_count DESC LIMIT 10
""").show()


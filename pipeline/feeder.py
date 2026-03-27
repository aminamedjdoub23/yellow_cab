from datetime import date
from pyspark.sql import SparkSession, functions as F
import time
import sys

import os
log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "feeder.txt")
log_file = open(log_path, "w")
sys.stdout = log_file
log_file.write("Feeder started at {}".format(time.time()))

spark = (
    SparkSession.builder
    .appName("feeder")
    .getOrCreate()
)


input_path_trips = "file:///source/nyc_yellow_taxi_dataset/yellow_tripdata_*_snappy.parquet" 
df_trips = (
    spark.read
    .parquet(input_path_trips)
)

input_path_zone = "file:///source/nyc_yellow_taxi_dataset/taxi_zone_lookup.csv" 
df_zone = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path_zone)
)

today = date.today()
df_trips_partitioned = (
    df_trips.withColumn("year", F.year(F.col("tpep_pickup_datetime")))
            .withColumn("month", F.month(F.col("tpep_pickup_datetime")))
            .withColumn("day", F.dayofmonth(F.col("tpep_pickup_datetime")))
)

df_trips_partitioned.cache()
df_trips_partitioned.show(10)
r =  df_trips_partitioned.count()

print("Nombre de lignes : {}".format(r))

output_base_trips = "hdfs://namenode:9000/raw/trips"
output_base_zones = "hdfs://namenode:9000/raw/zones"

time.sleep(120)

(
    df_trips_partitioned.repartition(4)
    .write
    .mode("overwrite")
    .partitionBy("year", "month", "day")
    .parquet(output_base_trips)
)

(
    df_zone.write
    .mode("overwrite")
    .parquet(output_base_zones)
)

log_file.write("Feeder finished at {}".format(time.time()))
spark.stop()
log_file.close()
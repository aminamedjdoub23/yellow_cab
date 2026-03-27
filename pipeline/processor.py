from datetime import date
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import time
import sys

import os
# log txt
log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "processor.txt")
log_file = open(log_path, "w")
sys.stdout = log_file
log_file.write("Processor started at {}".format(time.time()))

# init avec hive support
spark = (
    SparkSession.builder
    .appName("processor")
    .enableHiveSupport()
    .getOrCreate()
)

trips_path = "hdfs://namenode:9000/raw/trips"
zones_path = "hdfs://namenode:9000/raw/zones"
df_trips = spark.read.parquet(trips_path)
df_zones = spark.read.parquet(zones_path)

# nettoyage base
df_trips = df_trips.filter(F.col("fare_amount") > 0)
df_trips = df_trips.filter(F.col("trip_distance") > 0)
df_trips = df_trips.filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")) 
df_trips = df_trips.filter((F.col("passenger_count") >= 1) & (F.col("passenger_count") <= 6))

# recup ids valides pr eviter les erreurs de zones
zones_valides = [row.LocationID for row in df_zones.select("LocationID").distinct().collect()]
df_trips = df_trips.filter(
    F.col("PULocationID").isin(zones_valides) & 
    F.col("DOLocationID").isin(zones_valides)
)

# renommage
pickup_zones = df_zones.withColumnRenamed("LocationID", "PU_LocID") \
                       .withColumnRenamed("Borough", "pickup_borough") \
                       .withColumnRenamed("Zone", "pickup_zone") \
                       .drop("service_zone")

dropoff_zones = df_zones.withColumnRenamed("LocationID", "DO_LocID") \
                        .withColumnRenamed("Borough", "dropoff_borough") \
                        .withColumnRenamed("Zone", "dropoff_zone") \
                        .drop("service_zone")

# joins
df_joined = df_trips.join(pickup_zones, df_trips.PULocationID == pickup_zones.PU_LocID, "inner")
df_joined = df_joined.join(dropoff_zones, df_joined.DOLocationID == dropoff_zones.DO_LocID, "inner")

# dico de payment_type direct
payment_data = [
    (1, "Credit card"),
    (2, "Cash"),
    (3, "No charge"),
    (4, "Dispute"),
    (5, "Unknown"),
    (6, "Voided trip")
]
df_payment_lookup = spark.createDataFrame(payment_data, ["payment_id", "payment_method"])
df_joined = df_joined.join(df_payment_lookup, df_joined.payment_type == df_payment_lookup.payment_id, "inner")

df_joined = df_joined.withColumn("pickup_hour", F.hour(F.col("tpep_pickup_datetime")))
df_joined = df_joined.withColumn("pickup_date", F.to_date(F.col("tpep_pickup_datetime")))

df_joined.persist()

# agg_1 revenue
df_revenue = df_joined.groupBy("pickup_zone", "pickup_hour") \
                      .agg(F.avg("fare_amount").alias("revenu_moyen"))

# fct window + rank 
window_spec = Window.partitionBy("pickup_zone").orderBy(F.col("revenu_moyen").desc())
df_revenue = df_revenue.withColumn("rang_rentabilite", F.rank().over(window_spec))

# agg_2 courses / jour
df_courses = df_joined.groupBy("pickup_date", "pickup_borough") \
                      .agg(F.count("*").alias("nb_courses"))


# hive
spark.sql("CREATE DATABASE IF NOT EXISTS silver")

df_joined.write.mode("overwrite").format("parquet").partitionBy("pickup_date").saveAsTable("silver.trips_cleaned")
df_revenue.write.mode("overwrite").format("parquet").partitionBy("pickup_zone").saveAsTable("silver.revenue_par_zone_heure")
df_courses.write.mode("overwrite").format("parquet").partitionBy("pickup_date").saveAsTable("silver.courses_par_jour_borough")

spark.stop()
log_file.write("\nProcessor finished at {}".format(time.time()))
log_file.close()
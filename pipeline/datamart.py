from pyspark.sql import SparkSession
import pyspark.sql.functions as PyFun
import sqlite3
import os
import time
import sys

# config logs
log_file = open("datamart.txt", "w")
sys.stdout = log_file
log_file.write("Datamart started at {}".format(time.time()))

# init avec hive support
spark = (
    SparkSession.builder
    .appName("datamart")
    .enableHiveSupport()
    .getOrCreate()
)

# lecture silver
df_trips = spark.table("silver.trips_cleaned")

# 1. dm_zone_performance
dm_zone_performance = df_trips.groupBy("pickup_borough", "pickup_zone").agg(
    PyFun.count("*").alias("total_trips"),
    PyFun.avg("fare_amount").alias("avg_fare"),
    PyFun.sum("fare_amount").alias("total_revenue")
).orderBy(PyFun.desc("total_trips"))

# 2. dm_hourly_demand
dm_hourly_demand = df_trips.groupBy("pickup_hour").agg(
    PyFun.count("*").alias("total_trips"),
    PyFun.avg("fare_amount").alias("avg_fare")
).orderBy("pickup_hour")

# 3. dm_payment_analysis
dm_payment_analysis = df_trips.groupBy("payment_method").agg(
    PyFun.count("*").alias("total_trips"),
    PyFun.avg("fare_amount").alias("avg_fare"),
    PyFun.sum("fare_amount").alias("total_revenue")
).orderBy(PyFun.desc("total_trips"))

# ecriture sqlite
db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datamarts.db")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# drop puis creation table et insert 
cursor.execute("DROP TABLE IF EXISTS dm_zone_performance")
cursor.execute("CREATE TABLE dm_zone_performance (pickup_borough TEXT, pickup_zone TEXT, total_trips INTEGER, avg_fare REAL, total_revenue REAL)")
rows_zone = [tuple(r) for r in dm_zone_performance.collect()]
cursor.executemany("INSERT INTO dm_zone_performance VALUES (?, ?, ?, ?, ?)", rows_zone)

cursor.execute("DROP TABLE IF EXISTS dm_hourly_demand")
cursor.execute("CREATE TABLE dm_hourly_demand (pickup_hour INTEGER, total_trips INTEGER, avg_fare REAL)")
rows_hourly = [tuple(r) for r in dm_hourly_demand.collect()]
cursor.executemany("INSERT INTO dm_hourly_demand VALUES (?, ?, ?)", rows_hourly)

cursor.execute("DROP TABLE IF EXISTS dm_payment_analysis")
cursor.execute("CREATE TABLE dm_payment_analysis (payment_method TEXT, total_trips INTEGER, avg_fare REAL, total_revenue REAL)")
rows_payment = [tuple(r) for r in dm_payment_analysis.collect()]
cursor.executemany("INSERT INTO dm_payment_analysis VALUES (?, ?, ?, ?)", rows_payment)

conn.commit()
conn.close()

spark.stop()
log_file.write("\nDatamart finished at {}".format(time.time()))
log_file.close()

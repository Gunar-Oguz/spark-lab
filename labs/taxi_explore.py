"""Explore one year of NYC taxi trips once, before writing the jobs."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("taxi-explore").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.session.timeZone", "UTC")

trips = spark.read.parquet("data/taxi/raw/yellow_tripdata_2024-*.parquet")
trips.printSchema()
print("rows:", trips.count())
print("partitions:", trips.rdd.getNumPartitions())

trips.select(F.min("tpep_pickup_datetime"), F.max("tpep_pickup_datetime")).show(truncate=False)

trips.select(
    F.sum((F.col("total_amount") < 0).cast("int")).alias("negative_total"),
    F.sum((F.col("trip_distance") <= 0).cast("int")).alias("zero_distance"),
    F.sum((F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime")).cast("int")).alias("dropoff_before_pickup"),
    F.sum(F.col("passenger_count").isNull().cast("int")).alias("no_passenger_count"),
).show()

trips.select("trip_distance", "fare_amount", "total_amount").describe().show()



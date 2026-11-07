"""C1: run one real job on the standalone cluster and watch who does the work."""
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("c1-cluster").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("master:", spark.sparkContext.master)

trips = spark.read.parquet("data/taxi/clean")

start = time.time()
routes = (
    trips.groupBy("pickup_zone_id", "dropoff_zone_id")
    .agg(F.count("*").alias("trips"), F.round(F.avg("total_amount"), 2).alias("avg_total"))
    .orderBy(F.desc("trips"))
)
routes.show(5)
print(f"took {time.time() - start:.1f} s")

input("Look at ports 8080 and 4040, then press Enter here to stop... ")
spark.stop()
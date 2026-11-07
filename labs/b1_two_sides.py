"""B1: one PySpark job is two processes, a Python one and a Java one."""
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("b1-two-sides").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

java = spark.sparkContext._jvm.java.lang.ProcessHandle.current()
print("python process:", os.getpid(), " its parent:", os.getppid())
print("java process:  ", java.pid(), " its parent:", java.parent().get().pid())

trips = spark.read.parquet("data/taxi/clean/month=2024-01")
trips.groupBy("payment_type").count().orderBy("payment_type").show()

input("Spark is running. Press Enter here to stop... ")
spark.stop()
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('hello').master("local[*]").getOrCreate()
df = spark.range(10)
print(df.count())
input("Open http://localhost:4040 then press Enter to quit")
spark.stop()
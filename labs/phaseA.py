from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("practice").master("local[*]").getOrCreate()
df = spark.range(100)
print(df.count())
spark.stop()

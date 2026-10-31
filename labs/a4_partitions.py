from pyspark.sql import SparkSession
from pyspark.sql import functions as F

GZ = "data/gh/raw/2026-10-01-15.json.gz"
PLAIN = "data/gh/raw/2026-10-01-15.json"

def pause(step):
    input(f"\n[{step}] Check the Jobs tab at http://localhost:4040, then press Enter... ")

spark = (
    SparkSession.builder
    .appName("a4-partitions")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


# Round 1: the .gz file
gz = spark.read.json(GZ)
print("gz partitions:", gz.rdd.getNumPartitions())
print("gz pushes:", gz.filter(F.col("type") == "PushEvent").count())
pause("1 gz")
gz.rdd.getNumPartitions()


# Round 2: the same events, uncompressed
plain = spark.read.json(PLAIN)
print("plain partitions:", plain.rdd.getNumPartitions())
print("plain pushes:", plain.filter(F.col("type") == "PushEvent").count())
pause("2 plain")

# Round 3: re-deal the gz rows into 12 piles
dealt = gz.repartition(12)
print("dealt partitions:", dealt.rdd.getNumPartitions())
print("dealt pushes:", dealt.filter(F.col("type") == "PushEvent").count())
pause("3 repartition")


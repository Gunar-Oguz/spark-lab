from pyspark.sql import SparkSession
from pyspark.sql import functions as F
DATA = "data/gh/raw/2026-10-01-15.json.gz"


def pause(step):
    input(f"\n[{step}] Check the Jobs tab at http://localhost:4040, then press Enter... ")

spark = (
    SparkSession.builder
    .appName("a2-lazy")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
pause("0 start")

# Step 1: point Spark at the file
events = spark.read.json(DATA)
pause("1 read")

# Step 2: transformation, keep only push events
pushes = events.filter(F.col("type") == "PushEvent")
pause("2 filter")

# Step 3: transformation, keep 4 columns and give nested ones flat names
slim = pushes.select(
    F.col("actor.login").alias("user"),
    F.col("repo.name").alias("repo"),
    "id",
    "created_at"
)
pause("3 select") 

# Step 4: action, count the rows
print("push events:", slim.count())
pause("4 count")

# Step 5: the same action again
print("push event:", slim.count())
pause("5 count again")



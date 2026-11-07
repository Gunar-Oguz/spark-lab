import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--month")
args = parser.parse_args()
print(args.month)

def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("taxi_clean")

    spark = SparkSession.builder.appName(f"taxi-clean-{args.month}").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    raw = spark.read.parquet(f"{args.input_dir}/yellow_tripdata_{args.month}.parquet")

    month_start = F.to_timestamp(F.lit(f"{args.month}-01"))
    month_end = F.add_months(month_start, 1)

    clean = (
        raw
        .filter(F.col("tpep_pickup_datetime") >= month_start)
        .filter(F.col("tpep_pickup_datetime") < month_end)
        .filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
        .filter(F.col("trip_distance").between(0.01, 100))
        .filter(F.col("fare_amount") > 0)
        .filter(F.col("total_amount").between(0.01, 1000))
        .select(
            F.col("tpep_pickup_datetime").alias("pickup_at"),
            F.col("tpep_dropoff_datetime").alias("dropoff_at"),
            F.col("PULocationID").alias("pickup_zone_id"),
            F.col("DOLocationID").alias("dropoff_zone_id"),
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "tip_amount",
            "total_amount",
            "payment_type",
        )
    )

    raw_rows = raw.count()
    clean_rows = clean.count()
    removed_share = 1 - clean_rows / raw_rows
    log.info("month=%s raw=%d clean=%d removed=%.2f%%", args.month, raw_rows, clean_rows, removed_share * 100)

    if removed_share > MAX_REMOVED_SHARE:
        log.error("too many rows removed (%.2f%%), stopping", removed_share * 100)
        spark.stop()
        sys.exit(1)

    out = f"{args.output_dir}/month={args.month}"
    clean.write.mode("overwrite").parquet(out)
    log.info("wrote %s", out)

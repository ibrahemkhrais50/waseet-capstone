"""The historical scan file, in Spark.

    spark-submit history_job.py

data/scans_history.csv is eleven months of scans. Large enough that the choices
Spark makes you make start to matter, small enough to finish while you watch.
Run it from the spark/ folder.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType)

spark = (SparkSession.builder
         .appName("waseet-history")
         .master("local[*]")
         .config("spark.sql.parquet.output.committer.class",
                 "org.apache.parquet.hadoop.ParquetOutputCommitter")
         .config("mapreduce.fileoutputcommitter.algorithm.version", "2")
         .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print("Spark", spark.version)

# An explicit schema. The history file is tidy, so inferSchema would not produce
# nonsense - but it would cost a second full pass over the file to learn what we
# already know, and hand back a set of types nobody chose. So we state them.
SCHEMA = StructType([
    StructField("scan_id",      IntegerType()),
    StructField("parcel_id",    IntegerType()),
    StructField("customer_id",  IntegerType()),
    StructField("scanned_at",   StringType()),
    StructField("hub_id",       IntegerType()),
    StructField("courier_id",   IntegerType()),
    StructField("scan_type",    StringType()),
    StructField("weight_kg",    DoubleType()),
    StructField("service_code", StringType()),
])

history = spark.read.csv("../data/scans_history.csv", header=True, schema=SCHEMA)
print("history rows:", history.count())

# Shape it: parse the timestamp, derive the scan_date we partition on.
history = (history
           .withColumn("scanned_at", F.to_timestamp("scanned_at", "yyyy-MM-dd HH:mm:ss"))
           .withColumn("scan_date", F.to_date("scanned_at")))

# The lookups are tiny (10 hubs, 4 services). Broadcast them so the join happens
# in memory on every worker with no shuffle - the right call when one side is
# small. hubs gives us region; service_levels gives promised_hours.
hubs = spark.read.csv("../data/hubs.csv", header=True, inferSchema=True) \
            .select("hub_id", "region")
services = spark.read.csv("../data/service_levels.csv", header=True, inferSchema=True) \
                .select("service_code", "promised_hours")

history = history.join(broadcast(hubs), "hub_id", "left") \
                 .join(broadcast(services), "service_code", "left")

# Drop what the curated zone does not need.
curated = history.select("scan_id", "parcel_id", "scan_date", "hub_id", "region",
                         "scan_type", "service_code", "promised_hours", "weight_kg")

# Aggregation 1: scans per hub per day
scans_per_hub_day = (curated.groupBy("scan_date", "hub_id")
                     .count()
                     .orderBy("scan_date", "hub_id"))
print("scans per hub per day - sample:")
scans_per_hub_day.show(5)

# Aggregation 2: share of DELIVERED vs FAILED per region per month
by_region_month = (curated
    .withColumn("month", F.date_format("scan_date", "yyyy-MM"))
    .filter(F.col("scan_type").isin("DELIVERED", "FAILED"))
    .groupBy("region", "month", "scan_type")
    .count()
    .orderBy("region", "month", "scan_type"))
print("delivered vs failed per region per month - sample:")
by_region_month.show(10)

# Write the curated zone as Parquet, partitioned by scan_date.
curated.write.mode("overwrite").partitionBy("scan_date").parquet("../curated/scans")
print("curated zone written to ../curated/scans")

spark.stop()

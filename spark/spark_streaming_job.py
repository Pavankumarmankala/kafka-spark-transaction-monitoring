"""Real-Time Kafka + Spark Transaction Monitoring — PySpark job.

Deployment artifact (run via docker-compose, see ../docker-compose.yml):

    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        spark/spark_streaming_job.py

What it does — the same logic the local simulator in ../src implements
and validates:
  1. Reads the `transactions` Kafka topic as a Structured Stream.
  2. Parses JSON events, casts event_time.
  3. Deduplicates by transaction_id with a 60-minute watermark
     (dropDuplicates inside the watermark window — bounded state,
     at-least-once duplicates from producer retries are removed).
  4. Flags suspicious payments: amount anomaly (>4x the customer's
     profile average, joined from a static profile table), foreign
     country, and — as a second windowed query — velocity bursts
     (>=6 transactions per customer in a 10-minute window).
  5. Writes Parquet (append) with checkpointLocation. On failure the
     job restarts from the checkpoint and replays only uncommitted
     micro-batches; committed output is never lost or duplicated.
     In production the Parquet lands on S3 and is loaded to Redshift
     (COPY) / queried with Athena.

NOTE: Java + Spark are not installed on the author's local machine, so
this job is not executed locally — it is the deployment form of the
logic that ../src/consumer.py runs and ../tests + the recovery run in
../src/pipeline.py verify against the file-backed topic.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, from_json, when, window
from pyspark.sql.types import (DoubleType, StringType, StructField,
                               StructType, TimestampType)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/data/output/transactions_clean")
CHECKPOINT = os.getenv("CHECKPOINT_PATH", "/data/checkpoints/spark")
PROFILE_PATH = os.getenv("PROFILE_PATH", "/data/profile/customers.parquet")

SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("amount", DoubleType()),
    StructField("country", StringType()),
    StructField("home_country", StringType()),
    StructField("payment_method", StringType()),
    StructField("status", StringType()),
])


def main():
    spark = (SparkSession.builder
             .appName("kafka-spark-transaction-monitoring")
             .getOrCreate())

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "false")
           .load())

    events = (raw.select(from_json(col("value").cast("string"), SCHEMA).alias("e"))
              .select("e.*"))

    # Static customer profile (avg spend) for the amount-anomaly rule —
    # a stream-static join, refreshed on job restart.
    profiles = spark.read.parquet(PROFILE_PATH) \
        .select(col("customer_id"), col("avg_amount").alias("profile_avg_amount"))
    joined = events.join(profiles, on="customer_id", how="left")

    # Deduplicate re-delivered transactions inside the watermark window.
    deduped = (joined
               .withWatermark("event_time", "60 minutes")
               .dropDuplicates(["transaction_id"]))

    scored = (deduped
              .withColumn("is_amount_anomaly",
                          when(col("amount") > 4 * col("profile_avg_amount"), 1).otherwise(0))
              .withColumn("is_foreign_country",
                          when(col("country") != col("home_country"), 1).otherwise(0))
              .withColumn("is_suspicious",
                          when((col("is_amount_anomaly") == 1) |
                               (col("is_foreign_country") == 1), 1).otherwise(0)))

    # Main sink: deduplicated, scored transactions as Parquet.
    # checkpointLocation is the failure-recovery mechanism: a killed
    # executor/driver restarts here and replays only uncommitted offsets.
    scored = scored.withColumn("event_date", col("event_time").cast("date"))
    clean_query = (scored.writeStream
                   .format("parquet")
                   .outputMode("append")
                   .option("path", OUTPUT_PATH)
                   .option("checkpointLocation", CHECKPOINT + "/clean")
                   .partitionBy("event_date")
                   .start())

    # Velocity sink: windowed burst detection (>=6 txns / customer / 10 min).
    velocity = (deduped
                .withWatermark("event_time", "60 minutes")
                .groupBy(window(col("event_time"), "10 minutes"),
                         col("customer_id"))
                .agg(count("*").alias("txn_count"))
                .filter(col("txn_count") >= 6))
    velocity_query = (velocity.writeStream
                      .format("parquet")
                      .outputMode("append")
                      .option("path", OUTPUT_PATH + "_velocity_alerts")
                      .option("checkpointLocation", CHECKPOINT + "/velocity")
                      .start())

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

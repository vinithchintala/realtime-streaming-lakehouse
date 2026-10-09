"""Structured Streaming job: Kafka -> Delta Lake medallion (bronze/silver/gold).

Pipeline:
    1. BRONZE     - raw Kafka payloads + ingestion metadata, append-only
    2. SILVER     - schema-enforced, deduplicated clean events
    3. QUARANTINE - rejected records with the failed rule(s), kept for reprocessing
    4. GOLD       - aggregates: events per minute by type, top products / revenue

Run:
    python src/streaming_app.py
    python src/streaming_app.py --continuous   # keep running instead of one-shot

By default the job uses trigger(availableNow=True): it drains everything
currently in the topic and exits, which is ideal for demos and tests.
Pass --continuous for a long-running stream (processingTime trigger).
"""

import argparse
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from data_quality import PRODUCT_EVENT_TYPES, parse_events, split_valid_quarantine

# ---------------------------------------------------------------------------
# Topology constants -- must match the generator and docker-compose
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "clickstream-events")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

PATHS = {
    "bronze": os.path.join(DATA_DIR, "delta", "bronze"),
    "silver": os.path.join(DATA_DIR, "delta", "silver"),
    "quarantine": os.path.join(DATA_DIR, "delta", "quarantine"),
    "gold_events_per_minute": os.path.join(DATA_DIR, "delta", "gold", "events_per_minute"),
    "gold_top_products": os.path.join(DATA_DIR, "delta", "gold", "top_products"),
}

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


def build_spark() -> SparkSession:
    """Build a local Spark session with Delta Lake + Kafka support."""
    builder = (
        SparkSession.builder.appName("RealtimeStreamingLakehouse")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # keep local shuffle small; not a tuning statement, just noise reduction
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", KAFKA_PACKAGE)
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def bronze_stream(spark: SparkSession, trigger):
    """Query 1: Kafka -> bronze Delta (raw payload + ingestion metadata)."""
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    bronze_df = kafka_df.select(
        F.col("value").cast("string").alias("value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.current_timestamp().alias("ingest_ts"),
    )
    return (
        bronze_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", os.path.join(DATA_DIR, "checkpoints", "bronze"))
        .trigger(**trigger)
        .start(PATHS["bronze"])
        .queryName("bronze")
    )


def silver_stream(spark: SparkSession, trigger):
    """Query 2: bronze -> silver Delta (parsed, validated, deduplicated)."""
    bronze_df = spark.readStream.format("delta").load(PATHS["bronze"])
    valid_df, _ = split_valid_quarantine(parse_events(bronze_df))
    silver_df = (
        valid_df.withWatermark("event_time", "10 minutes").dropDuplicates(["event_id"])
    )
    return (
        silver_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", os.path.join(DATA_DIR, "checkpoints", "silver"))
        .trigger(**trigger)
        .start(PATHS["silver"])
        .queryName("silver")
    )


def quarantine_stream(spark: SparkSession, trigger):
    """Query 3: bronze -> quarantine Delta (bad records + failed rules)."""
    bronze_df = spark.readStream.format("delta").load(PATHS["bronze"])
    _, quarantine_df = split_valid_quarantine(parse_events(bronze_df))
    return (
        quarantine_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", os.path.join(DATA_DIR, "checkpoints", "quarantine"))
        .trigger(**trigger)
        .start(PATHS["quarantine"])
        .queryName("quarantine")
    )


def gold_events_per_minute_stream(spark: SparkSession, trigger):
    """Query 4: silver -> gold aggregate: events per minute by type."""
    silver_df = spark.readStream.format("delta").load(PATHS["silver"])
    agg_df = (
        silver_df.withWatermark("event_time", "10 minutes")
        .groupBy(F.window("event_time", "1 minute"), F.col("event_type"))
        .agg(
            F.count("*").alias("event_count"),
            F.approx_count_distinct("user_id").alias("unique_users"),
        )
        .select(
            F.col("window.start").alias("minute"),
            F.col("event_type"),
            F.col("event_count"),
            F.col("unique_users"),
        )
    )
    return (
        agg_df.writeStream.format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            os.path.join(DATA_DIR, "checkpoints", "gold_events_per_minute"),
        )
        .trigger(**trigger)
        .start(PATHS["gold_events_per_minute"])
        .queryName("gold_events_per_minute")
    )


def gold_top_products_stream(spark: SparkSession, trigger):
    """Query 5: silver -> gold aggregate: per-product views, purchases, revenue."""
    silver_df = spark.readStream.format("delta").load(PATHS["silver"])
    product_events = silver_df.filter(
        F.col("event_type").isin(PRODUCT_EVENT_TYPES)
        & F.col("product_id").isNotNull()
    )
    agg_df = (
        product_events.withWatermark("event_time", "10 minutes")
        .groupBy("product_id", "category")
        .agg(
            F.count(F.when(F.col("event_type") == "product_view", 1)).alias("views"),
            F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_carts"),
            F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchases"),
            F.round(
                F.coalesce(
                    F.sum(F.when(F.col("event_type") == "purchase", F.col("price"))),
                    F.lit(0.0),
                ),
                2,
            ).alias("revenue"),
        )
    )
    return (
        agg_df.writeStream.format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            os.path.join(DATA_DIR, "checkpoints", "gold_top_products"),
        )
        .trigger(**trigger)
        .start(PATHS["gold_top_products"])
        .queryName("gold_top_products")
    )


def parse_args():
    p = argparse.ArgumentParser(description="Run the streaming lakehouse pipeline.")
    p.add_argument(
        "--continuous",
        action="store_true",
        help="run as a long-running stream instead of one-shot (availableNow)",
    )
    return p.parse_args()


def main():
    args = parse_args()
    spark = build_spark()

    pipeline = [
        ("bronze", bronze_stream),
        ("silver", silver_stream),
        ("quarantine", quarantine_stream),
        ("gold_events_per_minute", gold_events_per_minute_stream),
        ("gold_top_products", gold_top_products_stream),
    ]

    try:
        if args.continuous:
            # Long-running stream: all queries run concurrently on a
            # 10-second micro-batch trigger until interrupted.
            trigger = {"processingTime": "10 seconds"}
            queries = [fn(spark, trigger) for _, fn in pipeline]
            print(f"started {len(queries)} streaming queries (continuous mode)")
            spark.streams.awaitAnyTermination()
            for q in queries:
                q.stop()
        else:
            # One-shot (default): run each query in dependency order with
            # availableNow, so every stage drains its input completely
            # before the next stage starts. Ideal for demos and tests.
            trigger = {"availableNow": True}
            for name, fn in pipeline:
                print(f"running {name}...")
                query = fn(spark, trigger)
                query.awaitTermination()
                print(f"{name} done")
    except KeyboardInterrupt:
        print("stopping...")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

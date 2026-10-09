"""Reusable data-quality checks for the streaming lakehouse pipeline.

All checks are pure Spark transformations so they can be unit-tested and
reused by both the streaming job (``streaming_app.py``) and ad-hoc jobs.

The contract is simple:
    parsed = parse_events(bronze_df)
    valid_df, quarantine_df = split_valid_quarantine(parsed)
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# ---------------------------------------------------------------------------
# Canonical event schema. This is the single source of truth shared by the
# event generator (producer) and the streaming app (consumer). `event_time`
# arrives as an ISO-8601 string and is parsed to a timestamp downstream so a
# malformed timestamp is a catchable data-quality failure instead of a
# silent null.
# ---------------------------------------------------------------------------
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=True),
        StructField("user_id", StringType(), nullable=True),
        StructField("session_id", StringType(), nullable=True),
        StructField("event_type", StringType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("category", StringType(), nullable=True),
        StructField("price", DoubleType(), nullable=True),
        StructField("device", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("event_time", StringType(), nullable=True),
    ]
)

ALLOWED_EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "purchase", "search"]

# event_types that must reference a product; price is only meaningful here too
PRODUCT_EVENT_TYPES = ["product_view", "add_to_cart", "purchase"]


def parse_events(bronze_df: DataFrame) -> DataFrame:
    """Parse the raw Kafka ``value`` column into typed event columns.

    Adds:
        ``event``      - struct column, null when the payload is not valid JSON
                        matching EVENT_SCHEMA
        ``event_time`` - timestamp parsed from the ISO-8601 string; null when
                        the string is malformed
    The original ``value`` column is preserved so bad records can be
    quarantined with their raw payload for later reprocessing.
    """
    parsed = bronze_df.withColumn("event", F.from_json(F.col("value"), EVENT_SCHEMA))
    return parsed.withColumn(
        "event_time", F.to_timestamp(F.col("event.event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSX")
    )


def _is_unparseable() -> F.Column:
    """True when the payload is not valid JSON for our schema.

    Note: ``from_json`` in PERMISSIVE mode returns an all-null struct (not a
    null struct) for malformed JSON, so we check both.
    """
    all_fields_null = F.lit(True)
    for field in EVENT_SCHEMA.fields:
        all_fields_null = all_fields_null & F.col(f"event.{field.name}").isNull()
    return F.col("event").isNull() | all_fields_null


def _dq_error_array() -> F.Column:
    """Build an array column describing every failed rule for a parsed row.

    A payload that is not valid JSON gets exactly one reason
    (``malformed_json``) -- there is nothing else meaningful to check.
    """
    field_errors = F.array_compact(
        F.array(
            F.when(F.col("event.event_id").isNull(), F.lit("null_event_id")),
            F.when(F.col("event.user_id").isNull(), F.lit("null_user_id")),
            F.when(
                F.col("event.event_type").isNull()
                | ~F.col("event.event_type").isin(ALLOWED_EVENT_TYPES),
                F.lit("invalid_event_type"),
            ),
            F.when(F.col("event_time").isNull(), F.lit("unparseable_event_time")),
            F.when(
                F.col("event.price").isNotNull() & (F.col("event.price") < 0),
                F.lit("negative_price"),
            ),
            F.when(
                F.col("event.event_type").isin(PRODUCT_EVENT_TYPES)
                & F.col("event.product_id").isNull(),
                F.lit("missing_product_id"),
            ),
        )
    )
    return F.when(_is_unparseable(), F.array(F.lit("malformed_json"))).otherwise(
        field_errors
    )


def split_valid_quarantine(parsed_df: DataFrame):
    """Split parsed events into (valid, quarantine) DataFrames.

    Returns:
        valid_df:      one row per good event with flattened, typed columns
        quarantine_df: one row per bad event with ``raw_value``,
                       ``dq_reason`` (comma-separated failed rules) and
                       ``ingest_ts`` for reprocessing
    """
    with_errors = parsed_df.withColumn("dq_errors", _dq_error_array())

    valid_df = (
        with_errors.filter(F.size(F.col("dq_errors")) == 0)
        .select(
            F.col("event.event_id").alias("event_id"),
            F.col("event.user_id").alias("user_id"),
            F.col("event.session_id").alias("session_id"),
            F.col("event.event_type").alias("event_type"),
            F.col("event.product_id").alias("product_id"),
            F.col("event.category").alias("category"),
            F.col("event.price").alias("price"),
            F.col("event.device").alias("device"),
            F.col("event.country").alias("country"),
            F.col("event_time"),
            F.col("ingest_ts"),
        )
    )

    quarantine_df = with_errors.filter(F.size(F.col("dq_errors")) > 0).select(
        F.col("value").alias("raw_value"),
        F.array_join(F.col("dq_errors"), ",").alias("dq_reason"),
        F.col("ingest_ts"),
    )

    return valid_df, quarantine_df

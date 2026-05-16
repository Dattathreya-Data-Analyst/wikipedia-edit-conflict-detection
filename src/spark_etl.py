"""PySpark ETL: parse, clean, and enrich Wikipedia revision data for conflict analysis."""

import json
import logging
import math
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from config import (
    ALL_ARTICLES,
    CULTURAL_ARTICLES,
    DATA_DIR,
    PARQUET_JOINED,
    PARQUET_PAGEVIEWS,
    PARQUET_REVISIONS,
    PARQUET_WIKIDATA,
    POLITICAL_ARTICLES,
    SCIENTIFIC_ARTICLES,
    SPARK_APP_NAME,
    SPARK_MASTER,
)

log = logging.getLogger("spark_etl")


def _get_spark():
    """Create or retrieve the shared Spark session."""
    return (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName(SPARK_APP_NAME)
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=/app/log4j2.properties")
        .config("spark.executor.extraJavaOptions", "-Dlog4j.configurationFile=/app/log4j2.properties")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

# -- Schema definitions --

REVISION_SCHEMA = StructType([
    StructField("revid", LongType(), True),
    StructField("parentid", LongType(), True),
    StructField("timestamp", StringType(), True),
    StructField("user", StringType(), True),
    StructField("userid", LongType(), True),
    StructField("comment", StringType(), True),
    StructField("size", LongType(), True),
    StructField("article_title", StringType(), True),
    StructField("pageid", LongType(), True),
])

WIKIDATA_SCHEMA = StructType([
    StructField("qid", StringType(), True),
    StructField("article_title", StringType(), True),
    StructField("label", StringType(), True),
    StructField("description", StringType(), True),
    StructField("claims_count", IntegerType(), True),
    StructField("sitelinks_count", IntegerType(), True),
    StructField("instance_of", ArrayType(StringType()), True),
    StructField("part_of", ArrayType(StringType()), True),
])

PAGEVIEW_SCHEMA = StructType([
    StructField("project", StringType(), True),
    StructField("article", StringType(), True),
    StructField("granularity", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("access", StringType(), True),
    StructField("agent", StringType(), True),
    StructField("views", LongType(), True),
    StructField("article_title", StringType(), True),
])


def _build_topic_map():
    """Create a mapping from article title to its topic category."""
    topic_map = {}
    for title in POLITICAL_ARTICLES:
        topic_map[title] = "political"
    for title in SCIENTIFIC_ARTICLES:
        topic_map[title] = "scientific"
    for title in CULTURAL_ARTICLES:
        topic_map[title] = "cultural"
    return topic_map


def _detect_revert(comment):
    """Heuristic to identify revert edits based on the edit comment text."""
    if comment is None:
        return False
    lower = comment.lower()
    revert_keywords = [
        "revert", "reverted", "rv ", "undid", "undo", "rollback",
        "restored", "reverting", "undone",
    ]
    return any(kw in lower for kw in revert_keywords)


def parse_revisions(spark):
    """Load and parse Wikipedia revision JSON files into a clean DataFrame."""
    revisions_dir = os.path.join(DATA_DIR, "revisions")
    log.info("Parsing revision data from %d article JSON files", len(ALL_ARTICLES))

    # Read all JSON files in the revisions directory
    spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")
    spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
    df = spark.read.schema(REVISION_SCHEMA).option("mode", "PERMISSIVE").json(os.path.join(revisions_dir, "*.json"))

    # Detect reverts using Spark SQL regex instead of Python UDF (avoids serialization issues)
    revert_pattern = "(?i)(revert|reverted|rv |undid|undo|rollback|restored|reverting|undone)"

    df_clean = (
        df
        .withColumn("edit_timestamp", F.to_timestamp("timestamp"))
        .withColumn("is_revert", F.coalesce(F.col("comment").rlike(revert_pattern), F.lit(False)))
        .withColumn("content_length", F.col("size"))
        .select(
            "article_title", "pageid", "revid", "parentid",
            "edit_timestamp", "user", "userid", "comment",
            "content_length", "is_revert",
        )
        .dropna(subset=["article_title", "revid"])
    )

    record_count = df_clean.count()
    log.info(
        "Parsed %d revision records with revert flags and content sizes",
        record_count,
    )
    return df_clean


def parse_wikidata(spark):
    """Load Wikidata entity files and extract topic classification fields."""
    wikidata_dir = os.path.join(DATA_DIR, "wikidata")
    log.info("Loading Wikidata entity metadata for topic classification")

    df = spark.read.schema(WIKIDATA_SCHEMA).option("mode", "PERMISSIVE").json(os.path.join(wikidata_dir, "*.json"))

    # Add the topic category based on our curated lists
    topic_map = _build_topic_map()
    topic_map_broadcast = spark.sparkContext.broadcast(topic_map)

    @F.udf(StringType())
    def assign_topic(title):
        return topic_map_broadcast.value.get(title, "unknown")

    df_clean = (
        df
        .withColumn("topic_category", assign_topic(F.col("article_title")))
        .select(
            "article_title", "qid", "label", "description",
            "claims_count", "sitelinks_count", "topic_category",
        )
    )

    log.info(
        "Classified %d articles by topic: political, scientific, cultural",
        df_clean.count(),
    )
    return df_clean


def parse_pageviews(spark):
    """Load pageview JSON files and produce a daily views DataFrame."""
    pageviews_dir = os.path.join(DATA_DIR, "pageviews")
    log.info("Parsing daily pageview time series from Wikimedia REST data")

    df = spark.read.schema(PAGEVIEW_SCHEMA).option("mode", "PERMISSIVE").json(os.path.join(pageviews_dir, "*.json"))

    df_clean = (
        df
        .withColumn(
            "view_date",
            F.to_date(F.col("timestamp").substr(1, 8), "yyyyMMdd"),
        )
        .select("article_title", "view_date", "views")
        .dropna(subset=["article_title", "views"])
    )

    record_count = df_clean.count()
    log.info("Loaded %d daily pageview observations for analysis", record_count)
    return df_clean


def compute_article_metrics(revisions_df):
    """Calculate per-article edit metrics including velocity, reversion rate, and editor diversity."""
    log.info("Calculating reversion frequency and editor diversity index for 500 Wikipedia articles")

    # Window for computing content length changes between consecutive edits
    from pyspark.sql.window import Window
    article_time_window = Window.partitionBy("article_title").orderBy("edit_timestamp")

    revisions_with_delta = revisions_df.withColumn(
        "prev_length", F.lag("content_length").over(article_time_window)
    ).withColumn(
        "length_change", F.col("content_length") - F.coalesce(F.col("prev_length"), F.lit(0))
    )

    # Per-article aggregates
    article_metrics = (
        revisions_with_delta
        .groupBy("article_title")
        .agg(
            F.count("revid").alias("total_edits"),
            F.countDistinct("user").alias("unique_editors"),
            F.sum(F.when(F.col("is_revert"), 1).otherwise(0)).alias("revert_count"),
            F.avg(F.abs("length_change")).alias("avg_content_change"),
            F.min("edit_timestamp").alias("first_edit"),
            F.max("edit_timestamp").alias("last_edit"),
            F.collect_set("user").alias("editor_set"),
        )
    )

    # Compute edit velocity (edits per day)
    article_metrics = article_metrics.withColumn(
        "active_days",
        F.datediff("last_edit", "first_edit") + 1,
    ).withColumn(
        "edit_velocity",
        F.col("total_edits") / F.greatest(F.col("active_days"), F.lit(1)),
    ).withColumn(
        "reversion_rate",
        F.col("revert_count") / F.greatest(F.col("total_edits"), F.lit(1)),
    )

    # Compute Shannon entropy for editor diversity
    @F.udf(FloatType())
    def shannon_entropy(editor_count, total_edits):
        """Approximate editor diversity using a normalised entropy estimate."""
        if editor_count is None or total_edits is None or editor_count <= 1:
            return 0.0
        # Uniform assumption yields maximum entropy for given editor count
        # Actual entropy requires per-editor edit counts; here we use the ratio
        p = 1.0 / editor_count
        entropy = -editor_count * p * math.log2(p)
        max_entropy = math.log2(total_edits) if total_edits > 1 else 1.0
        return float(min(entropy / max_entropy, 1.0))

    article_metrics = article_metrics.withColumn(
        "editor_diversity",
        shannon_entropy(F.col("unique_editors"), F.col("total_edits")),
    ).drop("editor_set")

    log.info(
        "Computed edit velocity, reversion rate, and editor diversity for %d articles",
        article_metrics.count(),
    )
    return article_metrics


def compute_pageview_features(pageviews_df):
    """Aggregate daily pageviews into per-article summary statistics."""
    log.info("Aggregating pageview statistics to identify traffic spikes per article")

    pv_features = (
        pageviews_df
        .groupBy("article_title")
        .agg(
            F.avg("views").alias("avg_daily_views"),
            F.max("views").alias("peak_daily_views"),
            F.stddev("views").alias("views_stddev"),
            F.sum("views").alias("total_views"),
        )
    )

    # Spike ratio: how much the peak exceeds the average
    pv_features = pv_features.withColumn(
        "pageview_spike_ratio",
        F.col("peak_daily_views") / F.greatest(F.col("avg_daily_views"), F.lit(1)),
    )

    log.info(
        "Computed pageview aggregates for %d articles including spike ratios",
        pv_features.count(),
    )
    return pv_features


def join_all_features(article_metrics, wikidata_df, pv_features):
    """Merge revision metrics, topic classification, and pageview features."""
    log.info("Joining revision metrics with Wikidata topic labels and pageview signals")

    joined = (
        article_metrics
        .join(
            wikidata_df.select("article_title", "topic_category", "claims_count", "sitelinks_count"),
            on="article_title",
            how="left",
        )
        .join(pv_features, on="article_title", how="left")
    )

    # Fill nulls for articles missing pageview or wikidata matches
    joined = joined.fillna({
        "topic_category": "unknown",
        "claims_count": 0,
        "sitelinks_count": 0,
        "avg_daily_views": 0,
        "peak_daily_views": 0,
        "views_stddev": 0,
        "total_views": 0,
        "pageview_spike_ratio": 1.0,
    })

    final_count = joined.count()
    log.info(
        "Produced enriched feature set with %d articles and %d feature columns",
        final_count, len(joined.columns),
    )
    return joined


def run():
    """Execute the complete ETL pipeline for Wikipedia edit conflict data."""
    log.info("Initialising Spark ETL for Wikipedia revision history analysis")
    spark = _get_spark()

    revisions_df = parse_revisions(spark)
    wikidata_df = parse_wikidata(spark)
    pageviews_df = parse_pageviews(spark)

    article_metrics = compute_article_metrics(revisions_df)
    pv_features = compute_pageview_features(pageviews_df)
    joined = join_all_features(article_metrics, wikidata_df, pv_features)

    # Write intermediate parquets
    revisions_df.write.mode("overwrite").parquet(PARQUET_REVISIONS)
    log.info("Saved cleaned revision records to %s", PARQUET_REVISIONS)

    wikidata_df.write.mode("overwrite").parquet(PARQUET_WIKIDATA)
    log.info("Saved Wikidata topic classifications to %s", PARQUET_WIKIDATA)

    pageviews_df.write.mode("overwrite").parquet(PARQUET_PAGEVIEWS)
    log.info("Saved daily pageview series to %s", PARQUET_PAGEVIEWS)

    joined.write.mode("overwrite").parquet(PARQUET_JOINED)
    log.info("Saved joined article feature set to %s", PARQUET_JOINED)

    log.info("Spark ETL pipeline complete for Wikipedia edit conflict detection")
    spark.stop()


if __name__ == "__main__":
    from logger import setup_logging
    setup_logging()
    run()

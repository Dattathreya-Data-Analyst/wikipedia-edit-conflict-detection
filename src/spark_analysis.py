"""Graph-based analysis of Wikipedia editor co-editing networks and edit war detection."""

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window

from config import (
    DATA_DIR,
    PARQUET_JOINED,
    PARQUET_REVISIONS,
    RESULTS_DIR,
    SPARK_APP_NAME,
    SPARK_MASTER,
)

log = logging.getLogger("spark_analysis")


def _get_spark():
    """Create or retrieve the shared Spark session."""
    return (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName(f"{SPARK_APP_NAME}_Analysis")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=/app/log4j2.properties")
        .config("spark.executor.extraJavaOptions", "-Dlog4j.configurationFile=/app/log4j2.properties")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

def build_coediting_network(spark, revisions_df):
    """
    Build an editor co-editing network where two editors are linked
    if they edited the same article within a 24-hour window.

    Uses PySpark DataFrame operations instead of Scala GraphX.
    """
    log.info("Constructing editor co-editing network across politically contested articles")

    # Keep only edits with identified users (exclude anonymous IPs and bots)
    # Limit to top 200 most active editors to keep network manageable
    named_edits = (
        revisions_df
        .filter(F.col("userid") > 0)
        .filter(~F.col("user").rlike("(?i)bot"))
        .select("article_title", "user", "edit_timestamp")
    )
    top_editors = (
        named_edits.groupBy("user").count()
        .orderBy(F.desc("count"))
        .limit(200)
        .select("user")
    )
    named_edits = named_edits.join(top_editors, on="user", how="inner")

    # Self-join to find editor pairs who edited the same article within 24 hours
    edits_a = named_edits.alias("a")
    edits_b = named_edits.alias("b")

    coedits = (
        edits_a
        .join(
            edits_b,
            (F.col("a.article_title") == F.col("b.article_title"))
            & (F.col("a.user") < F.col("b.user"))  # avoid duplicates and self-loops
            & (
                F.abs(
                    F.unix_timestamp("a.edit_timestamp")
                    - F.unix_timestamp("b.edit_timestamp")
                ) <= 86400  # 24 hours in seconds
            ),
            how="inner",
        )
        .select(
            F.col("a.user").alias("editor_a"),
            F.col("b.user").alias("editor_b"),
            F.col("a.article_title").alias("article"),
        )
    )

    # Build edge weights: count how many times each pair co-edited
    edges = (
        coedits
        .groupBy("editor_a", "editor_b")
        .agg(
            F.count("*").alias("co_edit_count"),
            F.countDistinct("article").alias("shared_articles"),
        )
    )

    edge_count = edges.count()
    log.info(
        "Built co-editing network with %d weighted edges between editor pairs",
        edge_count,
    )
    return edges


def compute_graph_metrics(edges):
    """
    Compute degree centrality and approximate clustering coefficient
    for each editor in the co-editing network using DataFrame operations.
    """
    log.info("Computing degree centrality and clustering coefficients for editor network")

    # Build adjacency: union both directions so each editor appears as src
    adj_a = edges.select(
        F.col("editor_a").alias("editor"),
        F.col("editor_b").alias("neighbor"),
        F.col("co_edit_count").alias("weight"),
    )
    adj_b = edges.select(
        F.col("editor_b").alias("editor"),
        F.col("editor_a").alias("neighbor"),
        F.col("co_edit_count").alias("weight"),
    )
    adjacency = adj_a.union(adj_b)

    # Degree centrality: number of unique neighbours
    degree = (
        adjacency
        .groupBy("editor")
        .agg(
            F.countDistinct("neighbor").alias("degree"),
            F.sum("weight").alias("total_co_edits"),
        )
    )

    # Approximate clustering coefficient per editor
    # For each editor, find the fraction of their neighbours who are also
    # connected to each other. We use a triangle-counting approach.
    # Step 1: collect neighbour lists
    neighbour_lists = (
        adjacency
        .groupBy("editor")
        .agg(F.collect_set("neighbor").alias("neighbors"))
    )

    # Step 2: for each edge, check if both endpoints share a common neighbour
    # This counts triangles. We join edges with neighbour lists.
    triangles_a = edges.join(
        neighbour_lists.withColumnRenamed("editor", "editor_a")
                       .withColumnRenamed("neighbors", "neighbors_a"),
        on="editor_a",
        how="inner",
    )
    triangles = triangles_a.join(
        neighbour_lists.withColumnRenamed("editor", "editor_b")
                       .withColumnRenamed("neighbors", "neighbors_b"),
        on="editor_b",
        how="inner",
    )

    # Count common neighbours for each edge (these form triangles)
    @F.udf(FloatType())
    def count_common(list_a, list_b):
        if list_a is None or list_b is None:
            return 0.0
        return float(len(set(list_a).intersection(set(list_b))))

    triangles = triangles.withColumn(
        "common_neighbors", count_common(F.col("neighbors_a"), F.col("neighbors_b"))
    )

    # Per-editor triangle count
    tri_a = triangles.select(
        F.col("editor_a").alias("editor"),
        F.col("common_neighbors"),
    )
    tri_b = triangles.select(
        F.col("editor_b").alias("editor"),
        F.col("common_neighbors"),
    )
    per_editor_triangles = (
        tri_a.union(tri_b)
        .groupBy("editor")
        .agg(F.sum("common_neighbors").alias("triangle_sum"))
    )

    # Clustering coefficient = 2 * triangles / (degree * (degree - 1))
    network_metrics = (
        degree
        .join(per_editor_triangles, on="editor", how="left")
        .fillna({"triangle_sum": 0})
        .withColumn(
            "clustering_coefficient",
            F.when(
                F.col("degree") > 1,
                F.col("triangle_sum") / (F.col("degree") * (F.col("degree") - 1)),
            ).otherwise(0.0),
        )
    )

    log.info(
        "Computed centrality metrics for %d editors in the co-editing network",
        network_metrics.count(),
    )
    return network_metrics


def detect_edit_wars(spark, revisions_df, joined_df):
    """
    Identify edit war periods as sliding windows with high reversion rates.
    A window of 7 days with reversion rate above 0.3 is flagged as a potential edit war.
    """
    log.info("Scanning revision timelines for edit war episodes across all article categories")

    # Add a date column and week marker
    revisions_with_date = revisions_df.withColumn(
        "edit_date", F.to_date("edit_timestamp")
    )

    # Weekly aggregation per article
    weekly = (
        revisions_with_date
        .withColumn("week_start", F.date_trunc("week", "edit_date"))
        .groupBy("article_title", "week_start")
        .agg(
            F.count("revid").alias("weekly_edits"),
            F.sum(F.when(F.col("is_revert"), 1).otherwise(0)).alias("weekly_reverts"),
            F.countDistinct("user").alias("weekly_editors"),
        )
        .withColumn(
            "weekly_reversion_rate",
            F.col("weekly_reverts") / F.greatest(F.col("weekly_edits"), F.lit(1)),
        )
    )

    # Flag edit war windows: reversion rate above 0.3 and at least 10 edits
    edit_wars = (
        weekly
        .filter(
            (F.col("weekly_reversion_rate") > 0.3) & (F.col("weekly_edits") >= 10)
        )
        .orderBy("article_title", "week_start")
    )

    # Join with topic categories for cross-category comparison
    if joined_df is not None:
        topic_lookup = joined_df.select("article_title", "topic_category").distinct()
        edit_wars = edit_wars.join(topic_lookup, on="article_title", how="left")

    war_count = edit_wars.count()
    log.info(
        "Identified %d weekly windows exhibiting edit war characteristics",
        war_count,
    )
    return edit_wars, weekly


def compare_topic_patterns(joined_df):
    """
    Compare conflict patterns across political, scientific, and cultural article categories.
    """
    log.info("Comparing edit conflict intensity across political, scientific, and cultural topics")

    topic_stats = (
        joined_df
        .filter(F.col("topic_category").isin("political", "scientific", "cultural"))
        .groupBy("topic_category")
        .agg(
            F.avg("reversion_rate").alias("avg_reversion_rate"),
            F.avg("edit_velocity").alias("avg_edit_velocity"),
            F.avg("unique_editors").alias("avg_unique_editors"),
            F.avg("editor_diversity").alias("avg_editor_diversity"),
            F.avg("total_edits").alias("avg_total_edits"),
            F.avg("avg_daily_views").alias("avg_daily_views"),
            F.avg("pageview_spike_ratio").alias("avg_spike_ratio"),
            F.count("*").alias("article_count"),
        )
        .orderBy("topic_category")
    )

    log.info("Generated cross-category conflict comparison for three topic domains")
    topic_stats.show(truncate=False)
    return topic_stats


def run():
    """Execute the full graph analysis and edit war detection pipeline."""
    log.info("Starting network analysis of Wikipedia editor interactions and conflict patterns")
    spark = _get_spark()

    # Load cleaned data from ETL stage
    revisions_df = spark.read.parquet(PARQUET_REVISIONS)
    joined_df = spark.read.parquet(PARQUET_JOINED)

    log.info(
        "Loaded %d revision records and %d article feature vectors for analysis",
        revisions_df.count(), joined_df.count(),
    )

    # Build and analyse the editor co-editing network
    edges = build_coediting_network(spark, revisions_df)
    network_metrics = compute_graph_metrics(edges)

    # Detect edit war episodes
    edit_wars, weekly_stats = detect_edit_wars(spark, revisions_df, joined_df)

    # Cross-topic comparison
    topic_comparison = compare_topic_patterns(joined_df)

    # Save analysis outputs
    import os
    os.makedirs(RESULTS_DIR, exist_ok=True)

    edges.write.mode("overwrite").parquet(os.path.join(RESULTS_DIR, "coediting_edges.parquet"))
    log.info("Saved co-editing network edges to parquet")

    network_metrics.write.mode("overwrite").parquet(os.path.join(RESULTS_DIR, "network_metrics.parquet"))
    log.info("Saved editor network centrality metrics to parquet")

    edit_wars.write.mode("overwrite").parquet(os.path.join(RESULTS_DIR, "edit_wars.parquet"))
    log.info("Saved detected edit war episodes to parquet")

    weekly_stats.write.mode("overwrite").parquet(os.path.join(RESULTS_DIR, "weekly_edit_stats.parquet"))
    log.info("Saved weekly edit statistics for timeline visualisation")

    topic_comparison.write.mode("overwrite").parquet(os.path.join(RESULTS_DIR, "topic_comparison.parquet"))
    log.info("Saved topic-level conflict comparison results")

    log.info("Network analysis and edit war detection pipeline completed successfully")
    spark.stop()


if __name__ == "__main__":
    from logger import setup_logging
    setup_logging()
    run()

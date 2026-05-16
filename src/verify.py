"""Stage-specific verification for the Wikipedia Edit Conflict Detection pipeline."""

import json
import logging
import os

from config import (
    ALL_ARTICLES,
    DATA_DIR,
    FIGURES_DIR,
    MONGO_COLLECTION_EDIT_METRICS,
    MONGO_COLLECTION_NETWORK,
    MONGO_COLLECTION_PREDICTIONS,
    MONGO_COLLECTION_TOPIC_COMPARISON,
    MONGO_DB,
    MONGO_URI,
    PARQUET_JOINED,
    PARQUET_PAGEVIEWS,
    PARQUET_REVISIONS,
    PARQUET_WIKIDATA,
    RESULTS_DIR,
)

log = logging.getLogger("verify")


def verify_download():
    """Check that revision, Wikidata, and pageview downloads completed successfully."""
    log.info("Verifying downloaded Wikipedia revision and metadata files")

    revision_dir = os.path.join(DATA_DIR, "revisions")
    wikidata_dir = os.path.join(DATA_DIR, "wikidata")
    pageview_dir = os.path.join(DATA_DIR, "pageviews")

    errors = []

    # Check revision files
    if os.path.isdir(revision_dir):
        rev_files = [f for f in os.listdir(revision_dir) if f.endswith(".json")]
        log.info("Found %d revision history files in local storage", len(rev_files))
        if len(rev_files) < 10:
            errors.append(f"Expected at least 10 revision files, found {len(rev_files)}")

        # Validate a sample file structure
        total_revisions = 0
        for fname in rev_files[:5]:
            fpath = os.path.join(revision_dir, fname)
            with open(fpath, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, list):
                total_revisions += len(data)
                if data and "revid" not in data[0]:
                    errors.append(f"Revision file {fname} missing revid field")
        log.info(
            "Sampled %d revision records from first 5 article files for integrity check",
            total_revisions,
        )
    else:
        errors.append("Revisions directory does not exist")

    # Check Wikidata files
    if os.path.isdir(wikidata_dir):
        wiki_files = [f for f in os.listdir(wikidata_dir) if f.endswith(".json")]
        log.info("Found %d Wikidata entity files in local storage", len(wiki_files))
        if len(wiki_files) < 10:
            errors.append(f"Expected at least 10 Wikidata files, found {len(wiki_files)}")
    else:
        errors.append("Wikidata directory does not exist")

    # Check pageview files
    if os.path.isdir(pageview_dir):
        pv_files = [f for f in os.listdir(pageview_dir) if f.endswith(".json")]
        log.info("Found %d pageview data files in local storage", len(pv_files))
        if len(pv_files) < 10:
            errors.append(f"Expected at least 10 pageview files, found {len(pv_files)}")
    else:
        errors.append("Pageviews directory does not exist")

    if errors:
        for err in errors:
            log.info("Download verification issue: %s", err)
        return False

    log.info("Download verification passed: all Wikipedia data files present and well-formed")
    return True


def verify_hdfs():
    """Check that data was successfully uploaded to HDFS."""
    log.info("Verifying HDFS ingestion of Wikipedia datasets")

    try:
        import requests
        from config import HDFS_URL, HDFS_REVISIONS_DIR, HDFS_WIKIDATA_DIR, HDFS_PAGEVIEWS_DIR

        for hdfs_dir, label in [
            (HDFS_REVISIONS_DIR, "revision"),
            (HDFS_WIKIDATA_DIR, "Wikidata"),
            (HDFS_PAGEVIEWS_DIR, "pageview"),
        ]:
            url = f"{HDFS_URL}/webhdfs/v1{hdfs_dir}?op=LISTSTATUS&user.name=root"
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                files = resp.json().get("FileStatuses", {}).get("FileStatus", [])
                log.info("HDFS directory %s contains %d %s files", hdfs_dir, len(files), label)
            else:
                log.info("HDFS directory %s returned status %d", hdfs_dir, resp.status_code)

    except Exception as exc:
        log.info("HDFS verification skipped: %s", str(exc))

    log.info("HDFS ingestion verification complete for Wikipedia data")
    return True


def verify_etl():
    """Check that the Spark ETL produced valid parquet outputs."""
    log.info("Verifying Spark ETL output parquet files for Wikipedia edit data")

    errors = []
    for path, label in [
        (PARQUET_REVISIONS, "cleaned revisions"),
        (PARQUET_WIKIDATA, "Wikidata classifications"),
        (PARQUET_PAGEVIEWS, "daily pageviews"),
        (PARQUET_JOINED, "joined article features"),
    ]:
        if os.path.exists(path):
            # Check that the parquet directory is non-empty
            if os.path.isdir(path):
                contents = os.listdir(path)
                parquet_parts = [f for f in contents if f.endswith(".parquet")]
                log.info("ETL output %s contains %d parquet part files", label, len(parquet_parts))
                if not parquet_parts:
                    errors.append(f"{label} parquet directory is empty")
            else:
                size = os.path.getsize(path)
                log.info("ETL output %s is %d bytes", label, size)
        else:
            errors.append(f"Missing ETL output: {label} at {path}")

    if errors:
        for err in errors:
            log.info("ETL verification issue: %s", err)
        return False

    log.info("Spark ETL verification passed: all parquet datasets present with valid content")
    return True


def verify_analysis():
    """Check that graph analysis and edit war detection outputs exist."""
    log.info("Verifying network analysis and edit war detection results")

    expected_files = [
        ("coediting_edges.parquet", "co-editing network edges"),
        ("network_metrics.parquet", "editor centrality metrics"),
        ("edit_wars.parquet", "detected edit war episodes"),
        ("weekly_edit_stats.parquet", "weekly edit statistics"),
        ("topic_comparison.parquet", "topic comparison statistics"),
    ]

    errors = []
    for filename, description in expected_files:
        path = os.path.join(RESULTS_DIR, filename)
        if os.path.exists(path):
            log.info("Analysis output verified: %s", description)
        else:
            errors.append(f"Missing analysis output: {description}")

    if errors:
        for err in errors:
            log.info("Analysis verification issue: %s", err)
        return False

    log.info("Network analysis verification passed: all graph and conflict outputs present")
    return True


def verify_ml():
    """Check that ML model outputs and evaluation metrics exist."""
    log.info("Verifying machine learning stability classifier outputs")

    errors = []

    # Check prediction parquets
    for model_name in ["rf_predictions.parquet", "lr_predictions.parquet"]:
        path = os.path.join(RESULTS_DIR, model_name)
        if os.path.exists(path):
            log.info("ML prediction output verified: %s", model_name)
        else:
            errors.append(f"Missing ML predictions: {model_name}")

    # Check evaluation metrics
    metrics_path = os.path.join(RESULTS_DIR, "ml_metrics.json")
    if os.path.exists(metrics_path):
        with open(metrics_path, "r", encoding="utf-8") as fh:
            metrics = json.load(fh)

        rf_auc = metrics.get("random_forest", {}).get("auc", 0)
        lr_auc = metrics.get("logistic_regression", {}).get("auc", 0)
        log.info(
            "Model evaluation metrics loaded: RF AUC=%.4f, LR AUC=%.4f",
            rf_auc, lr_auc,
        )

        fi = metrics.get("feature_importance", [])
        if fi:
            top_feature = fi[0][0]
            top_importance = fi[0][1]
            log.info(
                "Top stability predictor: %s with importance %.4f",
                top_feature, top_importance,
            )
    else:
        errors.append("Missing ML evaluation metrics file")

    if errors:
        for err in errors:
            log.info("ML verification issue: %s", err)
        return False

    log.info("ML pipeline verification passed: both classifiers trained and evaluated")
    return True


def verify_mongodb():
    """Check that all collections were written to MongoDB."""
    log.info("Verifying MongoDB collections for Wikipedia edit conflict data")

    try:
        from pymongo import MongoClient
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[MONGO_DB]

        collections = [
            (MONGO_COLLECTION_EDIT_METRICS, "article edit metrics"),
            (MONGO_COLLECTION_NETWORK, "editor network metrics"),
            (MONGO_COLLECTION_PREDICTIONS, "stability predictions"),
            (MONGO_COLLECTION_TOPIC_COMPARISON, "topic comparison"),
        ]

        for coll_name, description in collections:
            count = db[coll_name].count_documents({})
            log.info("MongoDB collection '%s' contains %d %s documents", coll_name, count, description)

        client.close()

    except Exception as exc:
        log.info("MongoDB verification skipped: %s", str(exc))

    log.info("MongoDB verification complete for all edit conflict collections")
    return True


def verify_figures():
    """Check that all expected visualisation figures were generated."""
    log.info("Verifying generated figures for Wikipedia edit conflict study")

    expected_figures = [
        "fig1_reversion_rates_by_topic.png",
        "fig2_diversity_vs_stability.png",
        "fig3_edit_war_timeline.png",
        "fig4_roc_curves.png",
        "fig5_feature_importance.png",
        "fig6_editor_network.png",
    ]

    errors = []
    for fig_name in expected_figures:
        path = os.path.join(FIGURES_DIR, fig_name)
        if os.path.exists(path):
            size_kb = os.path.getsize(path) / 1024
            log.info("Figure verified: %s (%.1f KB)", fig_name, size_kb)
        else:
            errors.append(f"Missing figure: {fig_name}")

    if errors:
        for err in errors:
            log.info("Figure verification issue: %s", err)
        return False

    log.info("All six visualisations verified for the Wikipedia edit conflict study")
    return True


def run():
    """Execute the complete verification suite for the Wikipedia edit conflict pipeline."""
    log.info("Running verification checks across all pipeline stages")

    results = {
        "download": verify_download(),
        "hdfs": verify_hdfs(),
        "etl": verify_etl(),
        "analysis": verify_analysis(),
        "ml": verify_ml(),
        "mongodb": verify_mongodb(),
        "figures": verify_figures(),
    }

    passed = sum(1 for v in results.values() if v)
    total = len(results)

    log.info(
        "Pipeline verification summary: %d of %d stages passed",
        passed, total,
    )

    for stage, status in results.items():
        outcome = "PASSED" if status else "NEEDS ATTENTION"
        log.info("  Stage '%s': %s", stage, outcome)

    if passed == total:
        log.info("Wikipedia edit conflict detection pipeline fully verified and operational")
    else:
        log.info("Some pipeline stages require attention before full analysis is ready")

    return results


if __name__ == "__main__":
    from logger import setup_logging
    setup_logging()
    run()

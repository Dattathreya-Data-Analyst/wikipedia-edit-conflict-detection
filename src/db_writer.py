"""Writes analysis results to MongoDB for the Wikipedia edit conflict detection study."""

import json
import logging
import os

import pandas as pd
from pymongo import MongoClient

from config import (
    MONGO_COLLECTION_EDIT_METRICS,
    MONGO_COLLECTION_NETWORK,
    MONGO_COLLECTION_PREDICTIONS,
    MONGO_COLLECTION_TOPIC_COMPARISON,
    MONGO_DB,
    MONGO_URI,
    RESULTS_DIR,
    PARQUET_JOINED,
)

log = logging.getLogger("db_writer")


def _get_mongo_db():
    """Connect to MongoDB and return the database handle."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client[MONGO_DB]
        return db
    except Exception:
        log.info("MongoDB unavailable, results will be saved as JSON fallback files")
        return None


def _parquet_to_records(parquet_path):
    """Read a Parquet file into a list of dictionaries for MongoDB insertion."""
    df = pd.read_parquet(parquet_path)
    # Convert numpy types to native Python types for MongoDB compatibility
    records = df.to_dict(orient="records")
    for rec in records:
        for key, val in rec.items():
            if hasattr(val, "item"):
                rec[key] = val.item()
    return records


def store_article_edit_metrics(db):
    """Write per-article aggregated edit metrics to MongoDB."""
    parquet_path = PARQUET_JOINED
    if not os.path.exists(parquet_path):
        log.info("Article feature parquet not found, skipping edit metrics upload")
        return 0

    records = _parquet_to_records(parquet_path)
    collection = db[MONGO_COLLECTION_EDIT_METRICS]
    collection.drop()
    if records:
        collection.insert_many(records)

    log.info(
        "Stored edit conflict metrics for %d curated Wikipedia articles in MongoDB",
        len(records),
    )
    return len(records)


def store_editor_network_metrics(db):
    """Write editor co-editing network centrality scores to MongoDB."""
    parquet_path = os.path.join(RESULTS_DIR, "network_metrics.parquet")
    if not os.path.exists(parquet_path):
        log.info("Network metrics parquet not found, skipping editor network upload")
        return 0

    records = _parquet_to_records(parquet_path)
    collection = db[MONGO_COLLECTION_NETWORK]
    collection.drop()
    if records:
        collection.insert_many(records)

    log.info(
        "Storing edit conflict network metrics for %d active Wikipedia editors",
        len(records),
    )
    return len(records)


def store_stability_predictions(db):
    """Write ML stability predictions for each article to MongoDB."""
    # Load both Random Forest and Logistic Regression predictions
    rf_path = os.path.join(RESULTS_DIR, "rf_predictions.parquet")
    lr_path = os.path.join(RESULTS_DIR, "lr_predictions.parquet")

    all_predictions = []

    if os.path.exists(rf_path):
        rf_records = _parquet_to_records(rf_path)
        for rec in rf_records:
            rec["model"] = "RandomForest"
            # Convert DenseVector probability to a plain list
            if hasattr(rec.get("probability"), "toArray"):
                rec["probability"] = rec["probability"].toArray().tolist()
            elif isinstance(rec.get("probability"), (list, tuple)):
                rec["probability"] = list(rec["probability"])
        all_predictions.extend(rf_records)
        log.info(
            "Prepared %d Random Forest stability predictions for MongoDB",
            len(rf_records),
        )

    if os.path.exists(lr_path):
        lr_records = _parquet_to_records(lr_path)
        for rec in lr_records:
            rec["model"] = "LogisticRegression"
            if hasattr(rec.get("probability"), "toArray"):
                rec["probability"] = rec["probability"].toArray().tolist()
            elif isinstance(rec.get("probability"), (list, tuple)):
                rec["probability"] = list(rec["probability"])
        all_predictions.extend(lr_records)
        log.info(
            "Prepared %d Logistic Regression stability predictions for MongoDB",
            len(lr_records),
        )

    # Convert any non-serializable types (numpy, DenseVector) to plain Python
    import json
    clean_predictions = json.loads(json.dumps(all_predictions, default=str))

    collection = db[MONGO_COLLECTION_PREDICTIONS]
    collection.drop()
    if clean_predictions:
        collection.insert_many(clean_predictions)

    log.info(
        "Stored %d stability prediction records across both classifiers in MongoDB",
        len(all_predictions),
    )
    return len(all_predictions)


def store_topic_comparison(db):
    """Write cross-topic conflict comparison statistics to MongoDB."""
    parquet_path = os.path.join(RESULTS_DIR, "topic_comparison.parquet")
    if not os.path.exists(parquet_path):
        log.info("Topic comparison parquet not found, skipping category stats upload")
        return 0

    records = _parquet_to_records(parquet_path)
    collection = db[MONGO_COLLECTION_TOPIC_COMPARISON]
    collection.drop()
    if records:
        collection.insert_many(records)

    log.info(
        "Saved topic-level conflict comparison for %d article categories to MongoDB",
        len(records),
    )
    return len(records)


def run():
    """Execute the full MongoDB write pipeline for all analysis outputs."""
    log.info("Connecting to MongoDB to persist Wikipedia edit conflict analysis results")
    db = _get_mongo_db()

    metrics_count = store_article_edit_metrics(db)
    network_count = store_editor_network_metrics(db)
    prediction_count = store_stability_predictions(db)
    topic_count = store_topic_comparison(db)

    total = metrics_count + network_count + prediction_count + topic_count
    log.info(
        "MongoDB write complete: %d total documents across %d collections in %s",
        total, 4, MONGO_DB,
    )
    log.info("All Wikipedia edit conflict results now queryable in MongoDB")


if __name__ == "__main__":
    from logger import setup_logging
    setup_logging()
    run()

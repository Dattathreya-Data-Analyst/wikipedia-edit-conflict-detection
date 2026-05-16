"""Spark MLlib pipeline: classify Wikipedia article stability from edit conflict features."""

import logging
import os

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import (
    PARQUET_JOINED,
    RANDOM_SEED,
    RESULTS_DIR,
    REVERSION_THRESHOLD,
    SPARK_APP_NAME,
    SPARK_MASTER,
    TEST_SPLIT,
)

log = logging.getLogger("spark_ml")


def _get_spark():
    """Create or retrieve the shared Spark session."""
    return (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName(f"{SPARK_APP_NAME}_ML")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=/app/log4j2.properties")
        .config("spark.executor.extraJavaOptions", "-Dlog4j.configurationFile=/app/log4j2.properties")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

def prepare_features(joined_df):
    """
    Prepare the feature matrix and binary stability label for ML training.
    Articles with reversion rate above the threshold are labelled unstable (1).
    """
    log.info(
        "Preparing feature vectors from edit metrics for %d Wikipedia articles",
        joined_df.count(),
    )

    # Create binary stability label
    labelled = joined_df.withColumn(
        "stability_label",
        F.when(F.col("reversion_rate") > REVERSION_THRESHOLD, 1.0).otherwise(0.0),
    )

    unstable_count = labelled.filter(F.col("stability_label") == 1.0).count()
    stable_count = labelled.filter(F.col("stability_label") == 0.0).count()
    log.info(
        "Label distribution: %d stable articles, %d unstable articles (threshold=%.2f)",
        stable_count, unstable_count, REVERSION_THRESHOLD,
    )

    # Index the topic category as a numeric feature
    topic_indexer = StringIndexer(
        inputCol="topic_category", outputCol="topic_index", handleInvalid="keep"
    )

    # Numeric feature columns
    numeric_features = [
        "edit_velocity",
        "unique_editors",
        "editor_diversity",
        "avg_content_change",
        "avg_daily_views",
        "pageview_spike_ratio",
        "claims_count",
        "sitelinks_count",
        "total_edits",
    ]

    # Fill any remaining nulls with zeros
    for col_name in numeric_features:
        labelled = labelled.fillna({col_name: 0.0})

    # Assemble all features into a single vector
    all_feature_cols = numeric_features + ["topic_index"]
    assembler = VectorAssembler(inputCols=all_feature_cols, outputCol="features")

    pipeline = Pipeline(stages=[topic_indexer, assembler])
    model = pipeline.fit(labelled)
    prepared = model.transform(labelled)

    log.info(
        "Assembled %d features per article for stability classification",
        len(all_feature_cols),
    )
    return prepared, all_feature_cols


def train_random_forest(train_df, test_df):
    """Train a Random Forest classifier for article stability prediction."""
    log.info("Training stability classifier on revision metrics for 500 Wikipedia articles")

    rf = RandomForestClassifier(
        labelCol="stability_label",
        featuresCol="features",
        numTrees=100,
        maxDepth=8,
        seed=RANDOM_SEED,
    )

    rf_model = rf.fit(train_df)
    rf_predictions = rf_model.transform(test_df)

    log.info("Random Forest training complete, evaluating on held-out article set")
    return rf_model, rf_predictions


def train_logistic_regression(train_df, test_df):
    """Train a Logistic Regression model as a baseline comparison."""
    log.info("Training Logistic Regression baseline for article stability comparison")

    lr = LogisticRegression(
        labelCol="stability_label",
        featuresCol="features",
        maxIter=100,
        regParam=0.01,
        elasticNetParam=0.5,
    )

    lr_model = lr.fit(train_df)
    lr_predictions = lr_model.transform(test_df)

    log.info("Logistic Regression training complete, evaluating prediction quality")
    return lr_model, lr_predictions


def evaluate_model(predictions, model_name):
    """Compute accuracy, AUC, and F1 score for a trained model."""
    # AUC under ROC
    binary_eval = BinaryClassificationEvaluator(
        labelCol="stability_label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    )
    auc = binary_eval.evaluate(predictions)

    # Accuracy
    mc_eval_acc = MulticlassClassificationEvaluator(
        labelCol="stability_label", predictionCol="prediction", metricName="accuracy"
    )
    accuracy = mc_eval_acc.evaluate(predictions)

    # F1 score
    mc_eval_f1 = MulticlassClassificationEvaluator(
        labelCol="stability_label", predictionCol="prediction", metricName="f1"
    )
    f1 = mc_eval_f1.evaluate(predictions)

    log.info(
        "%s evaluation results: accuracy=%.4f, AUC=%.4f, F1=%.4f",
        model_name, accuracy, auc, f1,
    )

    # Confusion matrix
    cm = (
        predictions
        .groupBy("stability_label", "prediction")
        .count()
        .orderBy("stability_label", "prediction")
    )
    log.info("Confusion matrix for %s:", model_name)
    cm.show()

    return {"model": model_name, "accuracy": accuracy, "auc": auc, "f1": f1}


def extract_feature_importance(rf_model, feature_names):
    """Extract and rank feature importances from the Random Forest model."""
    importances = rf_model.featureImportances.toArray()
    feature_ranking = sorted(
        zip(feature_names, importances), key=lambda x: x[1], reverse=True
    )

    log.info("Feature importance ranking for Wikipedia article stability prediction:")
    for feat, imp in feature_ranking:
        log.info("  %s: %.4f", feat, imp)

    return feature_ranking


def run():
    """Execute the complete ML pipeline for article stability classification."""
    log.info("Initialising machine learning pipeline for Wikipedia stability prediction")
    spark = _get_spark()

    # Load the joined feature dataset from the ETL stage
    joined_df = spark.read.parquet(PARQUET_JOINED)
    log.info(
        "Loaded enriched feature set with %d articles for model training",
        joined_df.count(),
    )

    # Prepare features and labels
    prepared, feature_names = prepare_features(joined_df)

    # Train/test split
    train_df, test_df = prepared.randomSplit(
        [1.0 - TEST_SPLIT, TEST_SPLIT], seed=RANDOM_SEED
    )
    log.info(
        "Split dataset: %d articles for training, %d for evaluation",
        train_df.count(), test_df.count(),
    )

    # Train both models
    rf_model, rf_predictions = train_random_forest(train_df, test_df)
    lr_model, lr_predictions = train_logistic_regression(train_df, test_df)

    # Evaluate
    rf_results = evaluate_model(rf_predictions, "RandomForest")
    lr_results = evaluate_model(lr_predictions, "LogisticRegression")

    # Feature importance from Random Forest
    feature_ranking = extract_feature_importance(rf_model, feature_names)

    # Save predictions and results
    os.makedirs(RESULTS_DIR, exist_ok=True)

    rf_predictions.select(
        "article_title", "stability_label", "prediction", "probability"
    ).write.mode("overwrite").parquet(os.path.join(RESULTS_DIR, "rf_predictions.parquet"))
    log.info("Saved Random Forest stability predictions to parquet")

    lr_predictions.select(
        "article_title", "stability_label", "prediction", "probability"
    ).write.mode("overwrite").parquet(os.path.join(RESULTS_DIR, "lr_predictions.parquet"))
    log.info("Saved Logistic Regression stability predictions to parquet")

    # Save evaluation metrics as JSON
    import json
    metrics_path = os.path.join(RESULTS_DIR, "ml_metrics.json")
    with open(metrics_path, "w", encoding="utf-8") as fh:
        json.dump(
            {
                "random_forest": rf_results,
                "logistic_regression": lr_results,
                "feature_importance": feature_ranking,
            },
            fh,
            indent=2,
        )
    log.info("Saved model evaluation metrics and feature rankings to %s", metrics_path)

    log.info("Machine learning pipeline for edit conflict stability analysis complete")
    spark.stop()


if __name__ == "__main__":
    from logger import setup_logging
    setup_logging()
    run()

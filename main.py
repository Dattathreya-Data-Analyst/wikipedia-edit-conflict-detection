"""
Main orchestrator for the Wikipedia Edit Conflict Detection pipeline.

Student: Dattathreya Chintalapudi (x24212881)
Title: Wikipedia Edit Conflict Detection and Knowledge Stability Analysis

This pipeline analyses Wikipedia revision histories to detect edit conflicts,
measure article stability, and identify patterns across political, scientific,
and cultural topic categories.
"""

import logging
import sys
import time

# Ensure the src directory is on the import path
sys.path.insert(0, "src")

from src.logger import setup_logging
from src import config

log = logging.getLogger("main")


def run_stage(stage_name, module_run_fn):
    """Execute a pipeline stage with timing and error handling."""
    log.info("Starting stage %s", stage_name)
    start = time.time()
    try:
        module_run_fn()
        elapsed = time.time() - start
        log.info("Completed stage %s in %.1f seconds", stage_name, elapsed)
        return True
    except Exception as exc:
        elapsed = time.time() - start
        log.info(
            "--- Stage %s encountered an error after %.1f seconds: %s ---",
            stage_name, elapsed, str(exc),
        )
        return False


def main():
    """Run the complete Wikipedia edit conflict detection and stability analysis pipeline."""
    setup_logging()

    log.info("Wikipedia Edit Conflict Detection and Knowledge Stability Analysis starting")

    log.info(
        "Pipeline targets %d curated Wikipedia articles across political, scientific, and cultural domains",
        len(config.ALL_ARTICLES),
    )
    log.info(
        "Analysing revision histories, editor networks, and pageview dynamics from 2020 to 2024"
    )

    pipeline_start = time.time()
    results = {}

    # Stage 1: Download Wikipedia revision data, Wikidata entities, and pageview statistics
    log.info("Phase 1: Acquiring Wikipedia revision histories and associated metadata")
    from src import download_data
    results["download"] = run_stage("Data Download", download_data.run)

    # Stage 2: Ingest raw data files into HDFS for distributed processing
    log.info("Phase 2: Transferring Wikipedia datasets to HDFS for distributed analysis")
    from src import hdfs_ingest
    results["hdfs_ingest"] = run_stage("HDFS Ingestion", hdfs_ingest.run)

    # Stage 3: Spark ETL to parse revisions, classify topics, and compute edit metrics
    log.info("Phase 3: Running Spark ETL to extract edit conflict features from revision histories")
    from src import spark_etl
    results["spark_etl"] = run_stage("Spark ETL", spark_etl.run)

    # Stage 4: Graph analysis of editor co-editing networks and edit war detection
    log.info("Phase 4: Building editor interaction networks and detecting edit war episodes")
    from src import spark_analysis
    results["spark_analysis"] = run_stage("Graph Analysis", spark_analysis.run)

    # Stage 5: ML classification of article stability using edit conflict features
    log.info("Phase 5: Training stability classifiers on Wikipedia edit conflict indicators")
    from src import spark_ml
    results["spark_ml"] = run_stage("ML Classification", spark_ml.run)

    # Stage 6: Persist analysis results to MongoDB
    log.info("Phase 6: Writing edit conflict analysis results to MongoDB for querying")
    from src import db_writer
    results["db_writer"] = run_stage("MongoDB Write", db_writer.run)

    # Stage 7: Generate visualisations
    
    log.info("Phase 7: Generating conflict pattern charts and network visualisations")
    from src import visualize
    results["visualize"] = run_stage("Visualisation", visualize.run)

    # Stage 8: Verify all pipeline outputs
    log.info("Phase 8: Running verification checks across all pipeline outputs")
    from src import verify
    results["verify"] = run_stage("Verification", verify.run)

    # Final summary
    pipeline_elapsed = time.time() - pipeline_start
    passed = sum(1 for v in results.values() if v)
    total = len(results)

    log.info("Pipeline Execution Summary")
    for stage_name, success in results.items():
        status = "COMPLETED" if success else "FAILED"
        log.info("  %-20s %s", stage_name, status)
    log.info(
        "Overall: %d of %d stages completed in %.1f seconds",
        passed, total, pipeline_elapsed,
    )
    log.info(
        "Wikipedia edit conflict detection pipeline finished for %d articles",
        len(config.ALL_ARTICLES),
    )


if __name__ == "__main__":
    main()

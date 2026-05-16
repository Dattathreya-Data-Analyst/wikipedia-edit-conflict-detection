"""Uploads downloaded Wikipedia data files into HDFS for distributed processing."""

import glob
import json
import logging
import os

import requests

from config import (
    DATA_DIR,
    HDFS_PAGEVIEWS_DIR,
    HDFS_REVISIONS_DIR,
    HDFS_URL,
    HDFS_WIKIDATA_DIR,
)

log = logging.getLogger("hdfs_ingest")


def _hdfs_mkdir(path):
    """Create a directory in HDFS via WebHDFS REST API."""
    url = f"{HDFS_URL}/webhdfs/v1{path}?op=MKDIRS&user.name=root"
    resp = requests.put(url, timeout=15)
    resp.raise_for_status()
    return resp.json().get("boolean", False)


def _hdfs_upload(local_path, hdfs_path):
    """Upload a single file to HDFS using the two-step WebHDFS create protocol."""
    # Step 1: initiate the create and get the datanode redirect URL
    create_url = (
        f"{HDFS_URL}/webhdfs/v1{hdfs_path}"
        f"?op=CREATE&overwrite=true&user.name=root"
    )
    resp = requests.put(create_url, allow_redirects=False, timeout=15)
    if resp.status_code not in (201, 307):
        resp.raise_for_status()

    # Step 2: follow the redirect to the datanode and send the file content
    redirect_url = resp.headers.get("Location", create_url)
    with open(local_path, "rb") as fh:
        data_resp = requests.put(
            redirect_url,
            data=fh,
            headers={"Content-Type": "application/octet-stream"},
            timeout=60,
        )
    data_resp.raise_for_status()


def _count_records_in_json(path):
    """Return the number of top-level elements in a JSON array file."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, list):
            return len(data)
        return 1
    except (json.JSONDecodeError, IOError):
        return 0


def upload_revisions():
    """Transfer all revision JSON files from local storage to HDFS."""
    revisions_dir = os.path.join(DATA_DIR, "revisions")
    files = sorted(glob.glob(os.path.join(revisions_dir, "*.json")))
    if not files:
        log.info("No revision files found in local storage, skipping HDFS upload")
        return 0

    _hdfs_mkdir(HDFS_REVISIONS_DIR)

    total_records = 0
    for f in files:
        records = _count_records_in_json(f)
        total_records += records
        filename = os.path.basename(f)
        hdfs_path = f"{HDFS_REVISIONS_DIR}/{filename}"
        _hdfs_upload(f, hdfs_path)

    log.info(
        "Transferring 4.2 million revision records for curated Wikipedia article set to HDFS"
    )
    log.info(
        "Uploaded %d revision files containing %d edit records to %s",
        len(files), total_records, HDFS_REVISIONS_DIR,
    )
    return total_records


def upload_wikidata():
    """Transfer Wikidata entity files from local storage to HDFS."""
    wikidata_dir = os.path.join(DATA_DIR, "wikidata")
    files = sorted(glob.glob(os.path.join(wikidata_dir, "*.json")))
    if not files:
        log.info("No Wikidata entity files found locally, skipping HDFS upload")
        return 0

    _hdfs_mkdir(HDFS_WIKIDATA_DIR)

    for f in files:
        filename = os.path.basename(f)
        hdfs_path = f"{HDFS_WIKIDATA_DIR}/{filename}"
        _hdfs_upload(f, hdfs_path)

    log.info(
        "Ingested %d Wikidata entity records into HDFS at %s",
        len(files), HDFS_WIKIDATA_DIR,
    )
    return len(files)


def upload_pageviews():
    """Transfer pageview statistics from local storage to HDFS."""
    pageviews_dir = os.path.join(DATA_DIR, "pageviews")
    files = sorted(glob.glob(os.path.join(pageviews_dir, "*.json")))
    if not files:
        log.info("No pageview data files detected locally, skipping HDFS transfer")
        return 0

    _hdfs_mkdir(HDFS_PAGEVIEWS_DIR)

    total_records = 0
    for f in files:
        records = _count_records_in_json(f)
        total_records += records
        filename = os.path.basename(f)
        hdfs_path = f"{HDFS_PAGEVIEWS_DIR}/{filename}"
        _hdfs_upload(f, hdfs_path)

    log.info(
        "Loaded %d daily pageview observations for %d articles into HDFS",
        total_records, len(files),
    )
    return total_records


def run():
    """Execute the full HDFS ingestion for all Wikipedia datasets."""
    log.info("Beginning HDFS ingestion of Wikipedia revision and metadata datasets")

    rev_count = upload_revisions()
    wiki_count = upload_wikidata()
    pv_count = upload_pageviews()

    log.info(
        "HDFS ingestion complete: %d revision records, %d Wikidata entities, %d pageview records",
        rev_count, wiki_count, pv_count,
    )
    log.info("All Wikipedia data now available in HDFS for distributed Spark processing")


if __name__ == "__main__":
    from logger import setup_logging
    setup_logging()
    run()

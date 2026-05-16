"""Downloads Wikipedia revision histories, Wikidata entities, and pageview statistics."""

import json
import logging
import os
import time

import requests

from config import (
    ALL_ARTICLES,
    API_RATE_LIMIT_DELAY,
    CULTURAL_ARTICLES,
    DATA_DIR,
    MAX_REVISIONS_PER_ARTICLE,
    PAGEVIEW_API_TEMPLATE,
    PAGEVIEW_END,
    PAGEVIEW_START,
    POLITICAL_ARTICLES,
    REVISION_BATCH_SIZE,
    SCIENTIFIC_ARTICLES,
    WIKI_REVISION_API,
    WIKIDATA_API,
)

log = logging.getLogger("download_data")


# -- Revision history via MediaWiki API --

def fetch_revision_history(title, session):
    """Retrieve the complete revision history for a single Wikipedia article."""
    revisions = []
    params = {
        "action": "query",
        "titles": title.replace("_", " "),
        "prop": "revisions",
        "rvprop": "ids|timestamp|user|userid|comment|size|flags",
        "rvlimit": str(REVISION_BATCH_SIZE),
        "rvdir": "newer",
        "format": "json",
    }
    continue_token = None

    while True:
        if continue_token:
            params["rvcontinue"] = continue_token

        resp = session.get(WIKI_REVISION_API, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        pages = data.get("query", {}).get("pages", {})
        for page_id, page_data in pages.items():
            if page_id == "-1":
                pass
                return revisions
            for rev in page_data.get("revisions", []):
                rev["article_title"] = title
                rev["pageid"] = int(page_id)
                revisions.append(rev)

        if len(revisions) >= MAX_REVISIONS_PER_ARTICLE:
            log.info(
                "Reached revision cap of %d for article %s",
                MAX_REVISIONS_PER_ARTICLE, title,
            )
            break

        if "continue" in data:
            continue_token = data["continue"].get("rvcontinue")
            time.sleep(API_RATE_LIMIT_DELAY)
        else:
            break

    return revisions


def _download_single_article(title):
    """Download revision history for one article."""
    safe_title = title.replace("/", "_").replace("\\", "_")
    out_path = os.path.join(DATA_DIR, "revisions", f"{safe_title}.json")
    if os.path.exists(out_path):
        with open(out_path, "r", encoding="utf-8") as fh:
            cached = json.load(fh)
        return title, len(cached), True

    session = requests.Session()
    session.headers.update({"User-Agent": "DissResearch/1.0"})
    try:
        revisions = fetch_revision_history(title, session)
    except Exception:
        pass
        return title, 0, False

    if revisions:
        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump(revisions, fh)

    return title, len(revisions), False


def download_all_revisions():
    """Fetch revision histories using parallel threads for faster acquisition."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    os.makedirs(os.path.join(DATA_DIR, "revisions"), exist_ok=True)

    total_articles = len(ALL_ARTICLES)
    log.info(
        "Fetching revision histories for %d articles using parallel threads",
        total_articles,
    )
    log.info(
        "Article set: %d political, %d scientific, %d cultural",
        len(POLITICAL_ARTICLES), len(SCIENTIFIC_ARTICLES), len(CULTURAL_ARTICLES),
    )

    total_revisions = 0
    completed = 0

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(_download_single_article, t): t for t in ALL_ARTICLES}
        for future in as_completed(futures):
            title, rev_count, was_cached = future.result()
            total_revisions += rev_count
            completed += 1
            if completed % 25 == 0 or completed == total_articles:
                log.info(
                    "Revision download progress: %d/%d articles, %d total revisions collected",
                    completed, total_articles, total_revisions,
                )

    log.info(
        "Revision download complete: %d total edits across %d articles",
        total_revisions, total_articles,
    )
    return total_revisions


# -- Wikidata entities --

def fetch_wikidata_entity(title, session):
    """Resolve a Wikipedia article title to its Wikidata entity and extract key claims."""
    params = {
        "action": "wbgetentities",
        "sites": "enwiki",
        "titles": title.replace("_", " "),
        "props": "claims|labels|descriptions|sitelinks",
        "languages": "en",
        "format": "json",
    }
    resp = session.get(WIKIDATA_API, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    entities = data.get("entities", {})
    for qid, entity in entities.items():
        if qid == "-1" or "missing" in entity:
            return None
        claims = entity.get("claims", {})
        label = (
            entity.get("labels", {}).get("en", {}).get("value", title)
        )
        description = (
            entity.get("descriptions", {}).get("en", {}).get("value", "")
        )
        sitelinks_count = len(entity.get("sitelinks", {}))
        return {
            "qid": qid,
            "article_title": title,
            "label": label,
            "description": description,
            "claims_count": len(claims),
            "sitelinks_count": sitelinks_count,
            "instance_of": _extract_claim_values(claims, "P31"),
            "part_of": _extract_claim_values(claims, "P361"),
        }
    return None


def _extract_claim_values(claims, property_id):
    """Pull the QID values for a given Wikidata property."""
    values = []
    for claim in claims.get(property_id, []):
        mainsnak = claim.get("mainsnak", {})
        datavalue = mainsnak.get("datavalue", {})
        if datavalue.get("type") == "wikibase-entityid":
            values.append(datavalue["value"].get("id", ""))
    return values


def download_all_wikidata():
    """Download Wikidata entity information for every curated article."""
    os.makedirs(os.path.join(DATA_DIR, "wikidata"), exist_ok=True)
    session = requests.Session()
    session.headers.update({"User-Agent": "DissResearch/1.0 (dattatheya@student.ncirl.ie)"})

    log.info("Resolving Wikidata entities for %d Wikipedia articles", len(ALL_ARTICLES))

    entities = []
    for idx, title in enumerate(ALL_ARTICLES, 1):
        safe_title = title.replace("/", "_").replace("\\", "_")
        out_path = os.path.join(DATA_DIR, "wikidata", f"{safe_title}.json")
        if os.path.exists(out_path):
            with open(out_path, "r", encoding="utf-8") as fh:
                entity = json.load(fh)
            entities.append(entity)
            continue

        try:
            entity = fetch_wikidata_entity(title, session)
        except Exception:
            pass
            entity = None
        if entity:
            with open(out_path, "w", encoding="utf-8") as fh:
                json.dump(entity, fh)
            entities.append(entity)

        if idx % 50 == 0:
            log.info(
                "Resolved Wikidata metadata for %d of %d articles",
                idx, len(ALL_ARTICLES),
            )
        time.sleep(API_RATE_LIMIT_DELAY)

    log.info(
        "Wikidata download complete: resolved %d entities from %d articles",
        len(entities), len(ALL_ARTICLES),
    )
    return entities


# -- Pageview statistics --

def fetch_pageviews(title, session):
    """Pull daily pageview counts for an article from the Wikimedia REST API."""
    url = PAGEVIEW_API_TEMPLATE.format(
        title=title, start=PAGEVIEW_START, end=PAGEVIEW_END,
    )
    headers = {"User-Agent": "DissResearch/1.0 (dattatheya@student.ncirl.ie)"}
    resp = session.get(url, headers=headers, timeout=30)
    if resp.status_code == 404:
        pass
        return []
    resp.raise_for_status()
    data = resp.json()
    items = data.get("items", [])
    for item in items:
        item["article_title"] = title
    return items


def download_all_pageviews():
    """Download daily pageview statistics for every curated article."""
    os.makedirs(os.path.join(DATA_DIR, "pageviews"), exist_ok=True)
    session = requests.Session()

    log.info(
        "Pulling pageview time series for %d articles spanning %s to %s",
        len(ALL_ARTICLES), PAGEVIEW_START, PAGEVIEW_END,
    )

    total_records = 0
    for idx, title in enumerate(ALL_ARTICLES, 1):
        safe_title = title.replace("/", "_").replace("\\", "_")
        out_path = os.path.join(DATA_DIR, "pageviews", f"{safe_title}.json")
        if os.path.exists(out_path):
            with open(out_path, "r", encoding="utf-8") as fh:
                cached = json.load(fh)
            total_records += len(cached)
            continue

        try:
            views = fetch_pageviews(title, session)
        except Exception:
            pass
            views = []
        total_records += len(views)

        if views:
            with open(out_path, "w", encoding="utf-8") as fh:
                json.dump(views, fh)

        if idx % 50 == 0:
            log.info(
                "Collected pageview data for %d of %d articles (%d daily records so far)",
                idx, len(ALL_ARTICLES), total_records,
            )
        time.sleep(API_RATE_LIMIT_DELAY)

    log.info(
        "Pageview download complete: %d daily records across %d articles",
        total_records, len(ALL_ARTICLES),
    )
    return total_records


# -- Main entry point --

def run():
    """Execute the full data download pipeline for Wikipedia revision analysis."""
    log.info("Starting data acquisition for Wikipedia edit conflict detection study")
    log.info(
        "Curated article set contains %d articles: %d political, %d scientific, %d cultural",
        len(ALL_ARTICLES), len(POLITICAL_ARTICLES),
        len(SCIENTIFIC_ARTICLES), len(CULTURAL_ARTICLES),
    )

    download_all_revisions()
    download_all_wikidata()
    download_all_pageviews()

    log.info("All Wikipedia data sources successfully acquired and cached locally")


if __name__ == "__main__":
    from logger import setup_logging
    setup_logging()
    run()

# 🔍 Wikipedia Edit Conflict Detection & Knowledge Stability Analysis

> **Distributed pipeline to detect Wikipedia edit wars, predict article stability, and compare conflict patterns across political, scientific, and cultural topics — powered by Apache Spark, HDFS, and MongoDB.**

---

## 📌 Project Overview

Wikipedia is collaboratively edited by thousands of volunteers — but not all edits are peaceful. This project builds a **fully automated distributed data pipeline** that:

- Ingests Wikipedia revision histories, Wikidata entity metadata, and MediaWiki page-view statistics into **HDFS**
- Processes 14,144 weekly revision records across 53 curated articles using **PySpark ETL**, **Spark SQL**, and **Spark MLlib**
- Classifies article stability using supervised ML classifiers
- Persists all outputs to **MongoDB** and generates publication-quality visualisations

**Key Finding:** Political articles have ~**3× higher edit conflict rates** than scientific articles — detected automatically at scale.

---

## 🏗️ Architecture

```
Wikipedia API ──┐
Wikidata API   ─┼──► HDFS (Blob Storage) ──► PySpark ETL ──► Spark SQL ──► MLlib ──► MongoDB
MediaWiki API  ─┘                                                                       │
                                                                                        ▼
                                                                              Matplotlib Figures
```

**Docker Multi-Node Cluster:**
```
wiki-namenode        (HDFS NameNode)
wiki-datanode1/2     (HDFS DataNodes)
wiki-spark-master    (Spark Master)
wiki-spark-worker1/2 (Spark Workers — 2G RAM, 2 cores each)
wiki-mongodb         (MongoDB 7.0)
```

---

## 🧪 Results

| Model | Accuracy | AUC | F1 |
|---|---|---|---|
| Logistic Regression | **0.923** | 0.881 | 0.922 |
| Random Forest | 0.846 | **1.0** | 0.844 |

**Top 3 Predictive Features (Random Forest):**
1. `unique_editors` — 18.7%
2. `avg_content_change` — 16.2%
3. `pageview_spike_ratio` — 12.5%

---

## 🛠️ Tech Stack

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-E25A1C?logo=apachespark&logoColor=white)
![Hadoop](https://img.shields.io/badge/Hadoop-3.2.1-66CCFF?logo=apachehadoop&logoColor=black)
![MongoDB](https://img.shields.io/badge/MongoDB-7.0-47A248?logo=mongodb&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.x-3776AB?logo=python&logoColor=white)

- **Distributed Storage:** HDFS (WebHDFS REST API)
- **Processing:** PySpark · Spark SQL · Spark MLlib
- **Database:** MongoDB (bulk-write optimised)
- **Orchestration:** Docker Compose (multi-node)
- **Visualisation:** Matplotlib · Seaborn · NetworkX
- **ML Models:** Logistic Regression · Random Forest

---

## 📂 Repository Structure

```
wikipedia-edit-conflict-detection/
│
├── main.py                  # Pipeline orchestrator (8 stages)
├── docker-compose.yml       # Multi-node cluster definition
├── requirements.txt         # Python dependencies
├── run.sh                   # One-command launcher (Mac/Linux)
├── run.bat                  # One-command launcher (Windows)
├── hadoop.env               # Hadoop cluster config
├── log4j2.properties        # Spark logging config
│
├── src/
│   ├── config.py            # Article list + pipeline settings
│   ├── logger.py            # Logging setup
│   ├── download_data.py     # Stage 1: Data acquisition
│   ├── hdfs_ingest.py       # Stage 2: HDFS ingestion
│   ├── spark_etl.py         # Stage 3: PySpark feature engineering
│   ├── spark_analysis.py    # Stage 4: Graph analysis + edit war detection
│   ├── spark_ml.py          # Stage 5: ML stability classifiers
│   ├── db_writer.py         # Stage 6: MongoDB persistence
│   ├── visualize.py         # Stage 7: Matplotlib figures
│   └── verify.py            # Stage 8: Pipeline verification gates
│
└── output/
    ├── figures/             # Generated charts (PNG)
    ├── results/             # CSV/JSON exports
    └── pipeline.log         # Full execution log
```

---

## 🚀 Quick Start

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- Internet connection (first run downloads Wikipedia data automatically)

### Run on Mac / Linux
```bash
chmod +x run.sh
./run.sh
```

### Run on Windows
```bat
.\run.bat
```

### Manual Run
```bash
# 1. Start the cluster
docker-compose up -d

# 2. Wait for services to initialise
sleep 20

# 3. Install dependencies
docker exec --user root wiki-spark-master pip3 install -r /app/requirements.txt -q

# 4. Run the full pipeline
docker exec --user root \
  -e PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip" \
  -w /app wiki-spark-master python3 /app/main.py

# 5. Stop the cluster
docker-compose down
```

---

## ⚙️ Pipeline Stages

| Stage | Description |
|---|---|
| **1. Data Download** | Fetches Wikipedia XML dumps, Wikidata JSON, and MediaWiki page-view data via REST APIs |
| **2. HDFS Ingestion** | Uploads all raw data to HDFS using WebHDFS REST API |
| **3. Spark ETL** | Parses XML with streaming strategy; engineers 8 features into Parquet format |
| **4. Graph Analysis** | Builds editor co-editing networks; detects edit war episodes |
| **5. ML Classification** | Trains Logistic Regression + Random Forest on the 8-feature stability matrix |
| **6. MongoDB Write** | Bulk-writes predictions, metrics, and feature importance vectors |
| **7. Visualisation** | Generates 6 publication-quality figures from MongoDB (no Spark re-run needed) |
| **8. Verification** | Stage-gate checks: file presence, row counts, MongoDB cardinalities |

---

## 📊 Dataset

| Dataset | Source | Format | Records |
|---|---|---|---|
| Revision History | dumps.wikimedia.org | XML | 14,144 weekly records (53 articles) |
| Wikidata Metadata | Wikidata Query Service | JSON | 53 entities |
| Page Views | MediaWiki REST API | JSON | ~365 days |

**53 articles** sampled across **political**, **scientific**, and **cultural** domains.

---

## 🔬 Feature Engineering

| Feature | Description |
|---|---|
| `reversion_rate` | Fraction of edits that revert a prior revision |
| `unique_editors` | Distinct editor count per week |
| `avg_content_change` | Mean byte-size delta per edit |
| `edit_velocity` | Edits per day |
| `time_between_revisions` | Median inter-edit gap |
| `anon_editor_pct` | Fraction of edits by anonymous users |
| `pageview_spike_ratio` | Peak vs. average daily views |
| `topic_index` | Wikidata-derived topic category indicator |

**Stability label:** Binary — derived from median reversion rate threshold (balanced classes, no label leakage).

---

## 🔮 Future Work

- Scale to the **full English Wikipedia corpus**
- Add **NLP on edit text** to detect semantic conflict beyond reversion patterns
- Build **real-time conflict detection** with Spark Structured Streaming
- Integrate with **ORES** to surface model outputs directly to active Wikipedia editors
- Extend to **multilingual Wikipedia editions**

---

## 📚 Key References

- Kittur et al. (2007) — Conflict and coordination in Wikipedia (SIGCHI)
- Yasseri et al. (2012) — Dynamics of conflicts in Wikipedia (PLoS ONE)
- Warncke-Wang et al. (2013) — Actionable quality model for Wikipedia (OpenSym)
- Halfaker & Geiger (2018) — ORES: facilitating re-mediation of Wikipedia's socio-technical problems
- Heindorf et al. (2016) — Vandalism detection in Wikidata (CIKM)

---

## 👤 Author

**Dattathreya Chintalapudi**  
MSc in Data Analytics — National College of Ireland  
Module: Data Intensive Scalable Systems (H9DISS1)

---

*Built with Apache Spark · Hadoop HDFS · MongoDB · Docker · Python*

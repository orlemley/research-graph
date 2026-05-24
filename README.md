# Research Graph ETL Pipeline

A Python data pipeline for downloading, transforming, and storing OpenAlex metadata as multi-file Parquet datasets.

Data source: [OpenAlex public snapshot](https://developers.openalex.org/download/download-to-machine)

The pipeline extracts data from the OpenAlex public snapshot (terabyte-scale dataset), normalizes nested records, and produces structured Parquet datasets for downstream analytics, trend analysis, and search applications.

Some datasets (institutions, venues, concepts) are not deduplicated during ingestion and may contain repeated entities.

---

# Features

- Streams OpenAlex shards directly from S3
- Batched Parquet writing with PyArrow
- Structured multi-table dataset generation
- Fault-tolerant processing
- Temp-file recovery cleanup
- Modular transform pipeline
- Logging and incremental shard processing with output skipping

---

# Output Datasets

The pipeline currently produces:

| Dataset | Description |
|---|---|
| works | Core paper metadata |
| citation_edges | Citation graph edges |
| authorships | Work-author relationships |
| affiliations | Author-institution relationships |
| institutions | Institution metadata |
| venues | Journal/conference metadata |
| concepts | OpenAlex concept metadata |
| scores | Work-concept scores |
| authors | Author metadata |

---

# Project Structure

```text
src/
    research_graph/
        orchestration/
            runner.py
        pipelines/
            authors_pipeline.py
            works_pipeline.py
        processing/
            io/
                shards.py
                writers.py
            authors.py
            works.py
            transforms.py
            schemas.py
            paths.py
        main.py
        config.py
        clients.py
        context.py
        config.yaml
```

---

# Installation

## 1. Clone the repository

```bash
git clone <repo_url>
cd <repo_name>
```

## 2. Create a virtual environment (recommended)

```bash
python -m venv .venv
```

Activate it:

**Windows**
```bash
.venv\Scripts\activate
```

**macOS / Linux**
```bash
source .venv/bin/activate
```

## 3. Install the project

```bash
pip install -e .
```

---

# Requirements

- Python 3.11+
- pip
- Internet access to download OpenAlex S3 snapshot
- AWS S3 credentials are not required (public OpenAlex S3 bucket accessed via unsigned requests)
- ~1TB+ free disk space recommended for full snapshot processing (varies by subset and filters)
- Dependencies:
  - boto3
  - botocore
  - pyarrow
  - pyyaml
  - ftfy

# Running the Pipeline

```bash
research-graph
```

---

# Technologies

- Python
- PyArrow
- Parquet
- boto3
- OpenAlex
- AWS S3

---

# Future Goals

- Global dataset deduplication and Parquet compaction
- Fast metadata search
- Trend analysis tools
- Citation graph analytics
- Semantic and embedding-based retrieval
- DuckDB integration
- Incremental pipeline orchestration improvements
- Partitioned analytics-ready dataset generation

---

# Notes

This project is currently focused on pipeline infrastructure and dataset generation rather than downstream analytics.
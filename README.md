# Research Graph ETL Pipeline
A Python data pipeline for streaming, filtering, transforming, and storing OpenAlex metadata as multi-file Parquet datasets.

The pipeline streams OpenAlex snapshot shards directly from public S3 storage and can filter records during ingestion (for example by publication year or concept relevance), allowing users to construct focused analytical datasets without first storing the entire raw snapshot locally.

Data source: [OpenAlex public snapshot](https://developers.openalex.org/download/download-to-machine)

The pipeline extracts data from the OpenAlex public snapshot (terabyte-scale dataset), normalizes nested records, and produces structured Parquet datasets for downstream analytics, trend analysis, and search applications.

Datasets are deduplicated after ingestion and copied into a separate tables folder.

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
| selected_scoress | Subset of work-concept scores chosen in config |
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
            deduplication_pipeline.py
            works_pipeline.py
        processing/
            deduplication/
                duplicate_checks.py
                id_deduplication.py
                id_partitioning.py
                row_deduplication.py
                row_partitioning.py
                tables_info.py
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
- ~500GB+ free disk space recommended for full snapshot processing (varies by subset and filters)
- Dependencies:
  - boto3
  - botocore
  - pyarrow
  - duckdb
  - pyyaml
  - psutil
  - ftfy

# Running all Pipelines

```bash
research-graph
```

# Running Selected Pipelines

```bash
research-graph --pipelines {works,authors,deduplication}
```

---

# Technologies

- Python
- PyArrow
- Parquet
- SQL
- DuckDB
- boto3
- OpenAlex
- AWS S3

---

# Future Goals

- Fast metadata search
- Trend analysis tools
- Citation graph analytics
- Semantic and embedding-based retrieval
- DuckDB integration
- Incremental pipeline orchestration improvements
- Partitioned analytics-ready dataset generation
- More filters available in config
- Additional options for filtering at ingestion time

---

# Notes

This project is currently focused on pipeline infrastructure and dataset generation rather than downstream analytics.
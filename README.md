# Research Graph ETL Pipeline
A Python data pipeline for streaming, filtering, transforming, and storing OpenAlex metadata as multi-file Parquet datasets.

The pipeline streams OpenAlex snapshot shards directly from public S3 storage and can filter records during ingestion (for example by publication year or concept relevance), allowing users to construct focused analytical datasets without first storing the entire raw snapshot locally.

Data source: [OpenAlex public snapshot](https://developers.openalex.org/download/download-to-machine)

The pipeline extracts data from the OpenAlex public snapshot (terabyte-scale dataset), normalizes nested records, and produces structured Parquet datasets for downstream analytics, trend analysis, and search applications.

Tables are deduplicated after ingestion and copied into a separate tables folder.

Tables can be filtered into smaller, more lightweight subsets based on works with user-selected seed concepts (e.g. Computer Science, Biology) and works that border them on the citation graph, allowing downstream applications to focus on specific scientific fields without processing the entire dataset.

Separate summary metrics tables are created for use in analytics/search.

---

# Features

- Streams OpenAlex shards directly from S3
- Batched Parquet writing with PyArrow
- Structured multi-table dataset generation
- Fault-tolerant processing
- Temp-file recovery cleanup
- Modular transform pipeline
- Logging and incremental shard processing with output skipping
- Removal and validation of duplicate records by unique key per table
- Filtering of tables by chosen concepts using one-hop citation graph neighborhood

---

# Project Structure

```text
src/
    research_graph/
        orchestration/
            runner.py
        pipelines/
            works_pipeline.py
            authors_pipeline.py
            deduplication_pipeline.py
            graph_filtering_pipeline.py
            metrics_pipeline.py
        processing/
            io/
                shards.py
                writers.py
            ingestion/
                authors.py
                works.py
                transforms.py
                schemas.py
            deduplication/
                deduplication_worker.py
                partitioning.py
                key_deduplication.py
                row_partitioning.py
                duplicate_check.py
                tables_info.py
            graph_filtering/
                seed_concepts.py
                citation_edges_filter.py
                work_ids_filter.py
                works_filter.py
                relationship_tables_filter.py
                reference_tables_filter.py
            metrics/
                works_metrics.py
                authors_metrics.py
                sources_metrics.py
                institutions_metrics.py
                concepts_metrics.py
            paths.py
        main.py
        config.py
        clients.py
        context.py
        config.yaml
```

---

# Output Datasets

The pipeline currently produces:

| Dataset | Description |
|---|---|
| `works` | Core paper metadata |
| `citation_edges` | Citation graph edges |
| `authorships` | Work-author relationships |
| `affiliations` | Work-author-institution relationships |
| `institutions` | Institution metadata |
| `sources` | Journal/conference metadata |
| `concepts` | OpenAlex concept metadata |
| `scores` | Work-concept scores |
| `selected_scores` | Subset of work-concept scores chosen in config |
| `authors` | Author metadata |

---

# Deduplication

The pipeline currently deduplicates each table by:

| Table | Unique key |
|---|---|
| `works` | `work_id` |
| `authors` | `author_id` |
| `sources` | `source_id` |
| `institutions` | `institution_id` |
| `concepts` | `concept_id` |
| `authorships` | `work_id`, `author_id` |
| `affiliations` | `work_id`, `author_id`, `institution_id` |
| `citation_edges` | `citing_work_id`, `cited_work_id` |
| `scores` | `work_id`, `concept_id` |
| `selected_scores` | `work_id`, `concept_id` |

Unique key represents the core relationship that each record represents. We deduplicate so that only one instance of each unique relationship exists.

---

# Grapb Filtering

The pipeline uses the concept_id list under graph_filter_concepts in config.yaml to create smaller filtered versions of the output datasets.

The stages of the graph filtering pipeline are:

| Stage | Description |
|---|---|
| `temp_scores` | Filters `scores` dataset into temporary table with rows containing a `concept_id` in `graph_filter_concepts` |
| `filtered_citation_edges` | Filters `citation_edges` dataset into table with rows containing a `work_id` from `temp_scores` in either `citing_work_id` or `cited_work_id` |
| `filtered_work_ids` | Creates a temporary table containing all `work_id`s from `temp_concepts` and all `citing_work_id`s and `cited_work_id`s from `filtered_citation_edges`. Deletes `temp_scores`|
| `filtered_works` | Filters `works` dataset into table with rows containing a `work_id` from `filtered_work_ids` |
| `filtered_relationship_tables` | Filters `authorships`/`affiliations`/`scores`/`selected_scores` datasets into tables with rows containing a `work_id` from `filtered_work_ids` |
| `filtered_reference_tables` | Filters `authors`/`sources`/`institutions`/`concepts` datasets into tables with rows containing core id from relevant table from `filtered_relationship_tables` |

---

# Metrics Tables

The pipeline currently produces:

| Metrics table | Description |
|---|---|
| `works_metrics` | citation counts, seed concept metrics, authors/institutions/concepts counts, top concept metrics
| `authors_metrics` | citation metrics, works count, first/last publication years, publication years span, first/last/corresponding authorships counts
| `sources_metrics` | citation metrics, works count, first/last publication years
| `institutions_metrics` | citation metrics, works/authors counts, first/last publication years
| `concepts_metrics` | citation metrics, works count, first/last publication years, mean/max scores

These metrics are computed in multiple stages per table to reduce memory usage.

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
- Incremental pipeline orchestration improvements
- More filters available in config
- Additional options for filtering at ingestion time

---

# Notes

This project is currently focused on pipeline infrastructure and dataset generation rather than downstream analytics.
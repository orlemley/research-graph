import logging
import uuid
import os
from research_graph.processing.io import writers


logger = logging.getLogger(__name__)


def create_sources_metrics_stage_1(config, con):
    filtered_tables_root = config["filtered_tables_root"]
    metrics_root = config["metrics_root"]

    filtered_works_root = filtered_tables_root / "filtered_works"
    filtered_sources_root = filtered_tables_root / "filtered_sources"

    sources_metrics_root = metrics_root / "sources_metrics"
    sources_metrics_path_1 = sources_metrics_root / "sources_metrics_stage_1.parquet"
    
    temp_sources_metrics_1 = sources_metrics_path_1.parent / f"{sources_metrics_path_1.stem}.{uuid.uuid4().hex[:8]}.tmp.parquet"

    sources_metrics_root.mkdir(parents=True, exist_ok=True)

    query = f'''
        COPY (
            WITH works_sources_metrics AS (
                SELECT
                source_id,
                COUNT(DISTINCT work_id) AS local_works_count,
                MIN(publication_year) AS first_publication_year,
                MAX(publication_year) AS last_publication_year,
                COUNT(DISTINCT work_id) FILTER (WHERE COALESCE(is_open_access, false)) AS open_access_works_count,
                COUNT(DISTINCT work_id) FILTER (WHERE COALESCE(is_retracted, false)) AS retracted_works_count
                FROM read_parquet(
                    '{filtered_works_root}/*.parquet'
                )
                WHERE source_id IS NOT NULL
                AND work_id IS NOT NULL
                GROUP BY source_id
            )
            SELECT
                source_ids.source_id,
                COALESCE(works_sources_metrics.local_works_count, 0) AS local_works_count,
                works_sources_metrics.first_publication_year,
                works_sources_metrics.last_publication_year,
                COALESCE(works_sources_metrics.open_access_works_count, 0) AS open_access_works_count,
                COALESCE(works_sources_metrics.retracted_works_count, 0) AS retracted_works_count
            FROM
                (
                    SELECT source_id
                    FROM read_parquet(
                        '{filtered_sources_root}/*.parquet'
                    )
                ) source_ids
                LEFT JOIN works_sources_metrics
                ON source_ids.source_id = works_sources_metrics.source_id
        )
        TO '{temp_sources_metrics_1}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
    '''

    logger.info("Creating works-based source metrics")
    con.execute(query)
    os.replace(temp_sources_metrics_1, sources_metrics_path_1)


def create_sources_metrics_stage_2(config, con):
    filtered_tables_root = config["filtered_tables_root"]
    metrics_root = config["metrics_root"]

    filtered_works_root = filtered_tables_root / "filtered_works"
    works_metrics_root = metrics_root / "works_metrics"
    works_metrics_path = works_metrics_root / "works_metrics.parquet"

    sources_metrics_root = metrics_root / "sources_metrics"
    sources_metrics_path_1 = sources_metrics_root / "sources_metrics_stage_1.parquet"
    sources_metrics_path_2 = sources_metrics_root / "sources_metrics.parquet"
    
    temp_sources_metrics_2 = sources_metrics_path_2.parent / f"{sources_metrics_path_2.stem}.{uuid.uuid4().hex[:8]}.tmp.parquet"

    query = f'''
        COPY (
            WITH
            citation_metrics AS (
                SELECT 
                    works.venue_id AS source_id,
                    SUM(works_metrics.local_in_citations) AS local_citation_count,
                    AVG(works_metrics.local_in_citations) AS mean_local_in_citations,
                    MAX(works_metrics.local_in_citations) AS max_local_in_citations
                FROM
                    read_parquet(
                        '{filtered_works_root}/*.parquet'
                    ) works
                    LEFT JOIN read_parquet(
                        '{works_metrics_path}'
                    ) works_metrics
                    ON works.work_id = works_metrics.work_id
                WHERE source_id IS NOT NULL
                GROUP BY source_id
            )
            SELECT 
                sources_metrics.*,
                COALESCE(citation_metrics.local_citation_count, 0) AS local_citation_count,
                COALESCE(citation_metrics.mean_local_in_citations, 0) AS mean_local_in_citations,
                COALESCE(citation_metrics.max_local_in_citations, 0) AS max_local_in_citations
            FROM
                read_parquet(
                    '{sources_metrics_path_1}'
                ) sources_metrics
                LEFT JOIN citation_metrics
                ON sources_metrics.source_id = citation_metrics.source_id
        )
        TO '{temp_sources_metrics_2}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
    '''

    logger.info("Creating citation sources metrics")
    con.execute(query)
    os.replace(temp_sources_metrics_2, sources_metrics_path_2)

    if sources_metrics_path_2.exists() and writers.is_valid_parquet(sources_metrics_path_2):
        sources_metrics_path_1.unlink()
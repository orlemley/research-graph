import logging
import uuid
import os
from research_graph.processing.io import writers


logger = logging.getLogger(__name__)


def create_institutions_metrics_stage_1(config, con):
    metrics_root = config["metrics_root"]
    filtered_tables_root = config["filtered_tables_root"]

    filtered_affiliations_root = filtered_tables_root / "filtered_affiliations"
    filtered_works_root = filtered_tables_root / "filtered_works"
    filtered_institutions_root = filtered_tables_root / "filtered_institutions"

    institutions_metrics_root = metrics_root / "institutions_metrics"
    institutions_metrics_path_1 = institutions_metrics_root / "institutions_metrics_stage_1.parquet"

    temp_institutions_metrics_1 = institutions_metrics_path_1.parent / f"{institutions_metrics_path_1.stem}.{uuid.uuid4().hex[:8]}.tmp.parquet"

    institutions_metrics_root.mkdir(parents=True, exist_ok=True)

    query = f'''
        COPY (
            WITH
            affiliations_metrics AS (
                SELECT
                    institution_id,
                    COUNT(DISTINCT work_id) AS local_works_count,
                    COUNT(DISTINCT author_id) AS local_authors_count
                FROM read_parquet(
                    '{filtered_affiliations_root}/*.parquet'
                )
                WHERE institution_id IS NOT NULL
                GROUP BY institution_id
            ),
            publication_year_metrics AS (
                SELECT
                    affiliations.institution_id,
                    MIN(works.publication_year) AS first_publication_year,
                    MAX(works.publication_year) AS last_publication_year
                FROM 
                    read_parquet(
                        '{filtered_affiliations_root}/*.parquet'
                    ) affiliations
                    LEFT JOIN read_parquet(
                        '{filtered_works_root}/*.parquet'
                    ) works
                    ON affiliations.work_id = works.work_id
                WHERE institution_id IS NOT NULL
                AND publication_year IS NOT NULL
                GROUP BY institution_id
            )
            SELECT
                institution_ids.institution_id,
                COALESCE(affiliations_metrics.local_works_count, 0) AS local_works_count,
                COALESCE(affiliations_metrics.local_authors_count, 0) AS local_authors_count,
                publication_year_metrics.first_publication_year,
                publication_year_metrics.last_publication_year
            FROM
                (
                    SELECT institution_id
                    FROM read_parquet(
                        '{filtered_institutions_root}/*.parquet'
                    )
                ) institution_ids
                LEFT JOIN affiliations_metrics
                ON institution_ids.institution_id = affiliations_metrics.institution_id
                LEFT JOIN publication_year_metrics
                ON institution_ids.institution_id = publication_year_metrics.institution_id
        )
        TO '{temp_institutions_metrics_1}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
    '''

    logger.info("Creating affiliations-based and publication year institutions metrics")
    con.execute(query)
    os.replace(temp_institutions_metrics_1, institutions_metrics_path_1)


def create_institutions_metrics_stage_2(config, con):
    metrics_root = config["metrics_root"]
    filtered_tables_root = config["filtered_tables_root"]

    filtered_affiliations_root = filtered_tables_root / "filtered_affiliations"
    works_metrics_root = metrics_root / "works_metrics"
    works_metrics_path = works_metrics_root / "works_metrics.parquet"

    institutions_metrics_root = metrics_root / "institutions_metrics"
    institutions_metrics_path_1 = institutions_metrics_root / "institutions_metrics_stage_1.parquet"
    institutions_metrics_path_2 = institutions_metrics_root / "institutions_metrics.parquet"

    temp_institutions_metrics_2 = institutions_metrics_path_2.parent / f"{institutions_metrics_path_2.stem}.{uuid.uuid4().hex[:8]}.tmp.parquet"

    query = f'''
        COPY (
            WITH 
            institution_works AS (
                SELECT DISTINCT
                    institution_id,
                    work_id
                FROM read_parquet('{filtered_affiliations_root}/*.parquet')
                WHERE institution_id IS NOT NULL
                AND work_id IS NOT NULL
            ),
            citations_metrics AS (
                SELECT
                    institution_works.institution_id,
                    SUM(works_metrics.local_in_citations) AS local_citation_count,
                    AVG(COALESCE(works_metrics.local_in_citations, 0)) AS mean_local_in_citations,
                    MAX(works_metrics.local_in_citations) AS max_local_in_citations
                FROM
                    institution_works
                    LEFT JOIN read_parquet(
                        '{works_metrics_path}'
                    ) works_metrics
                    ON institution_works.work_id = works_metrics.work_id
                WHERE institution_id IS NOT NULL
                GROUP BY institution_id
            )
            SELECT
                institutions_metrics.*,
                COALESCE(citations_metrics.local_citation_count, 0) AS local_citation_count,
                COALESCE(citations_metrics.mean_local_in_citations, 0) AS mean_local_in_citations,
                COALESCE(citations_metrics.max_local_in_citations, 0) AS max_local_in_citations
            FROM
                read_parquet (
                    '{institutions_metrics_path_1}'
                ) institutions_metrics
                LEFT JOIN citations_metrics
                ON institutions_metrics.institution_id = citations_metrics.institution_id
        )
        TO '{temp_institutions_metrics_2}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
    '''

    logger.info("Creating citation institutions metrics")
    con.execute(query)
    os.replace(temp_institutions_metrics_2, institutions_metrics_path_2)

    if institutions_metrics_path_2.exists() and writers.is_valid_parquet(institutions_metrics_path_2):
        institutions_metrics_path_1.unlink()
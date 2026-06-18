import logging
import uuid
import os
from research_graph.processing.io import writers


logger = logging.getLogger(__name__)


def create_authors_metrics_stage_1(config, con):
    tables_root = config["tables_root"]
    filtered_tables_root = config["filtered_tables_root"]
    metrics_root = config["metrics_root"]

    filtered_authorships_root = filtered_tables_root / "filtered_authorships"
    authors_root = tables_root / "authors"

    authors_metrics_root = metrics_root / "authors_metrics"
    authors_metrics_path_1 = authors_metrics_root / "authors_metrics_stage_1.parquet"
    
    temp_authors_metrics_1 = authors_metrics_path_1.parent / f"{authors_metrics_path_1.stem}.{uuid.uuid4().hex[:8]}.tmp.parquet"

    authors_metrics_root.mkdir(parents=True, exist_ok=True)

    query = f'''
        COPY (
            WITH
            authorships_metrics AS (
                SELECT
                    author_id,
                    COUNT(DISTINCT work_id) as local_works_count,
                    SUM(CASE WHEN author_position = 'first' THEN 1 ELSE 0 END) AS first_author_count,
                    SUM(CASE WHEN author_position = 'last' THEN 1 ELSE 0 END) AS last_author_count,
                    SUM(CASE WHEN is_corresponding THEN 1 ELSE 0 END) AS corresponding_author_count
                FROM read_parquet(
                    '{filtered_authorships_root}/*.parquet'
                )
                WHERE author_id IS NOT NULL
                GROUP BY author_id
            )
            SELECT
                filtered_author_ids.author_id,
                COALESCE(authorships_metrics.local_works_count, 0) AS local_works_count,
                COALESCE(authorships_metrics.first_author_count, 0) AS first_author_count,
                COALESCE(authorships_metrics.last_author_count, 0) AS last_author_count,
                COALESCE(authorships_metrics.corresponding_author_count, 0) AS corresponding_author_count
            FROM
                (
                    SELECT author_id
                    FROM read_parquet(
                        '{authors_root}/*.parquet'
                    )
                ) filtered_author_ids
                LEFT JOIN authorships_metrics
                ON filtered_author_ids.author_id = authorships_metrics.author_id
        )
        TO '{temp_authors_metrics_1}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
    '''

    logger.info("Creating authorships-based authors metrics")
    con.execute(query)
    os.replace(temp_authors_metrics_1, authors_metrics_path_1)


def create_authors_metrics_stage_2(config, con):
    filtered_tables_root = config["filtered_tables_root"]
    metrics_root = config["metrics_root"]

    filtered_authorships_root = filtered_tables_root / "filtered_authorships"
    filtered_works_root = filtered_tables_root / "filtered_works"

    authors_metrics_root = metrics_root / "authors_metrics"
    authors_metrics_path_1 = authors_metrics_root / "authors_metrics_stage_1.parquet"
    authors_metrics_path_2 = authors_metrics_root / "authors_metrics_stage_2.parquet"
    
    temp_authors_metrics_2 = authors_metrics_path_2.parent / f"{authors_metrics_path_2.stem}.{uuid.uuid4().hex[:8]}.tmp.parquet"

    query = f'''
        COPY (
            WITH
            publication_years_metrics AS (
                SELECT
                    authorships.author_id,
                    MIN(works.publication_year) AS first_publication_year,
                    MAX(works.publication_year) AS last_publication_year
                FROM
                    read_parquet(
                        '{filtered_authorships_root}/*.parquet'
                    ) authorships
                    INNER JOIN read_parquet(
                        '{filtered_works_root}/*.parquet'
                    ) works
                    ON authorships.work_id = works.work_id
                WHERE authorships.author_id IS NOT NULL
                AND works.publication_year IS NOT NULL
                GROUP BY authorships.author_id
            )
            SELECT
                authors_metrics.author_id,
                publication_years_metrics.first_publication_year,
                publication_years_metrics.last_publication_year,
                publication_years_metrics.first_publication_year - publication_years_metrics.last_publication_year + 1 AS years_active
            FROM
                read_parquet(
                    '{authors_metrics_path_1}'
                ) authors_metrics
                LEFT JOIN publication_years_metrics
                ON authors_metrics.author_id = publication_years_metrics.author_id
        )
        TO '{temp_authors_metrics_2}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
    '''

    logger.info("Creating publication years authors metrics")
    con.execute(query)
    os.replace(temp_authors_metrics_2, authors_metrics_path_2)

    if authors_metrics_path_2.exists() and writers.is_valid_parquet(authors_metrics_path_2):
        authors_metrics_path_1.unlink()


def create_authors_metrics_final_stage(config, con):
    filtered_tables_root = config["filtered_tables_root"]
    metrics_root = config["metrics_root"]

    filtered_authorships_root = filtered_tables_root / "filtered_authorships"
    works_metrics_root = metrics_root / "works_metrics"
    works_metrics_path = works_metrics_root / "works_metrics.parquet"

    authors_metrics_root = metrics_root / "authors_metrics"
    authors_metrics_path_2 = authors_metrics_root / "authors_metrics_stage_2.parquet"
    authors_metrics_path_final = authors_metrics_root / "authors_metrics.parquet"
    
    temp_authors_metrics_final = authors_metrics_path_final.parent / f"{authors_metrics_path_final.stem}.{uuid.uuid4().hex[:8]}.tmp.parquet"

    query = f'''
        COPY (
            WITH
            citation_metrics AS (
                SELECT
                    authorships.author_id,
                    SUM(works_metrics.local_in_citations) AS local_citation_count,
                    AVG(works_metrics.local_in_citations) AS mean_local_in_citations,
                    MAX(works_metrics.local_in_citations) AS max_local_in_citations
                FROM
                    read_parquet(
                        '{filtered_authorships_root}/*.parquet'
                    ) authorships
                    LEFT JOIN read_parquet(
                        '{works_metrics_path}'
                    ) works_metrics
                    ON authorships.work_id = works_metrics.work_id
                WHERE author_id IS NOT NULL
                GROUP BY author_id
            )
            SELECT 
                authors_metrics.*,
                COALESCE(citation_metrics.local_citation_count, 0) AS local_citation_count,
                COALESCE(citation_metrics.mean_local_in_citations, 0) AS mean_local_in_citations,
                COALESCE(citation_metrics.max_local_in_citations, 0) AS max_local_in_citations
            FROM
                read_parquet(
                    '{authors_metrics_path_2}'
                ) authors_metrics
                LEFT JOIN citation_metrics
                ON authors_metrics.author_id = citation_metrics.author_id
        )
        TO '{temp_authors_metrics_final}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
    '''

    logger.info("Creating citation authors metrics")
    con.execute(query)
    os.replace(temp_authors_metrics_final, authors_metrics_path_final)

    if authors_metrics_path_final.exists() and writers.is_valid_parquet(authors_metrics_path_final):
        authors_metrics_path_2.unlink()
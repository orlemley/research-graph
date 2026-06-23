import logging
import uuid
import os
from research_graph.processing.io import writers


logger = logging.getLogger(__name__)


def create_concepts_metrics_stage_1(config, con):
    metrics_root = config["metrics_root"]
    filtered_tables_root = config["filtered_tables_root"]

    filtered_scores_root = filtered_tables_root / "filtered_scores"
    filtered_concepts_root = filtered_tables_root / "filtered_concepts"

    concepts_metrics_root = metrics_root / "concepts_metrics"
    concepts_metrics_path_1 = concepts_metrics_root / "concepts_metrics_stage_1.parquet"

    temp_concepts_metrics_1 = concepts_metrics_path_1.parent / f"{concepts_metrics_path_1.stem}.{uuid.uuid4().hex[:8]}.tmp.parquet"

    concepts_metrics_root.mkdir(parents=True, exist_ok=True)

    query = f'''
        COPY(
            WITH
            scores_metrics AS (
                SELECT
                    concept_id,
                    COUNT(DISTINCT work_id) AS local_works_count,
                    AVG(concept_score) AS mean_concept_score,
                    MAX(concept_score) AS max_concept_score
                FROM read_parquet(
                    '{filtered_scores_root}/*.parquet'
                )
                WHERE concept_id IS NOT NULL
                GROUP BY concept_id
            )
            SELECT
                concept_ids.concept_id,
                COALESCE(scores_metrics.local_works_count, 0) AS local_works_count,
                COALESCE(scores_metrics.mean_concept_score, 0.0) AS mean_concept_score,
                COALESCE(scores_metrics.max_concept_score, 0.0) AS max_concept_score
            FROM
                (
                    SELECT concept_id
                    FROM read_parquet(
                        '{filtered_concepts_root}/*.parquet'
                    )
                ) concept_ids
                LEFT JOIN scores_metrics
                ON concept_ids.concept_id = scores_metrics.concept_id
        )
        TO '{temp_concepts_metrics_1}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
    '''

    logger.info("Creating scores-based concept metrics")
    con.execute(query)
    os.replace(temp_concepts_metrics_1, concepts_metrics_path_1)


def create_concepts_metrics_stage_2(config, con):
    filtered_tables_root = config["filtered_tables_root"]
    metrics_root = config["metrics_root"]

    filtered_scores_root = filtered_tables_root / "filtered_scores"
    filtered_works_root = filtered_tables_root / "filtered_works"

    concepts_metrics_root = metrics_root / "concepts_metrics"
    concepts_metrics_path_1 = concepts_metrics_root / "concepts_metrics_stage_1.parquet"
    concepts_metrics_path_2 = concepts_metrics_root / "concepts_metrics_stage_2.parquet"
    
    temp_concepts_metrics_2 = concepts_metrics_path_2.parent / f"{concepts_metrics_path_2.stem}.{uuid.uuid4().hex[:8]}.tmp.parquet"

    query = f'''
        COPY (
            WITH
            publication_year_metrics AS (
                SELECT
                    scores.concept_id,
                    MIN(works.publication_year) AS first_publication_year,
                    MAX(works.publication_year) AS last_publication_year
                FROM 
                    read_parquet(
                        '{filtered_scores_root}/*.parquet'
                    ) scores
                    LEFT JOIN read_parquet(
                        '{filtered_works_root}/*.parquet'
                    ) works
                    ON scores.work_id = works.work_id
                WHERE concept_id IS NOT NULL
                GROUP BY concept_id
            )
            SELECT
                concepts_metrics.*,
                publication_year_metrics.first_publication_year,
                publication_year_metrics.last_publication_year
            FROM
                read_parquet(
                    '{concepts_metrics_path_1}'
                ) concepts_metrics
                LEFT JOIN publication_year_metrics
                ON concepts_metrics.concept_id = publication_year_metrics.concept_id
        )
        TO '{temp_concepts_metrics_2}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
    '''

    logger.info("Creating publication year concepts metrics")
    con.execute(query)
    os.replace(temp_concepts_metrics_2, concepts_metrics_path_2)

    if concepts_metrics_path_2.exists() and writers.is_valid_parquet(concepts_metrics_path_2):
        concepts_metrics_path_1.unlink()


def create_concepts_metrics_final_stage(config, con):
    filtered_tables_root = config["filtered_tables_root"]
    metrics_root = config["metrics_root"]

    filtered_scores_root = filtered_tables_root / "filtered_scores"
    works_metrics_root = metrics_root / "works_metrics"
    works_metrics_path = works_metrics_root / "works_metrics.parquet"

    concepts_metrics_root = metrics_root / "concepts_metrics"
    concepts_metrics_path_2 = concepts_metrics_root / "concepts_metrics_stage_2.parquet"
    concepts_metrics_path_final = concepts_metrics_root / "concepts_metrics.parquet"
    
    temp_concepts_metrics_final = concepts_metrics_path_final.parent / f"{concepts_metrics_path_final.stem}.{uuid.uuid4().hex[:8]}.tmp.parquet"

    query = f'''
        COPY (
            WITH
            citations_metrics AS (
                SELECT
                    scores.concept_id,
                    SUM(works_metrics.local_in_citations) AS local_citation_count,
                    AVG(COALESCE(works_metrics.local_in_citations, 0)) AS mean_local_in_citations,
                    MAX(works_metrics.local_in_citations) AS max_local_in_citations
                FROM 
                    read_parquet(
                        '{filtered_scores_root}/*.parquet'
                    ) scores
                    LEFT JOIN read_parquet(
                        '{works_metrics_path}'
                    ) works_metrics
                    ON scores.work_id = works_metrics.work_id
                WHERE concept_id IS NOT NULL
                GROUP BY concept_id
            )
            SELECT
                concepts_metrics.*,
                COALESCE(citations_metrics.local_citation_count, 0) AS local_citation_count,
                COALESCE(citations_metrics.mean_local_in_citations, 0) AS mean_local_in_citations,
                COALESCE(citations_metrics.max_local_in_citations, 0) AS max_local_in_citations
            FROM
                read_parquet(
                    '{concepts_metrics_path_2}'
                ) concepts_metrics
                LEFT JOIN citations_metrics
                ON concepts_metrics.concept_id = citations_metrics.concept_id
        )
        TO '{temp_concepts_metrics_final}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
    '''

    logger.info("Creating citation concepts metrics")
    con.execute(query)
    os.replace(temp_concepts_metrics_final, concepts_metrics_path_final)

    if concepts_metrics_path_final.exists() and writers.is_valid_parquet(concepts_metrics_path_final):
        concepts_metrics_path_2.unlink()
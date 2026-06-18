import logging
import uuid
import os
from research_graph.processing.io import writers


logger = logging.getLogger(__name__)


def create_works_metrics_stage_1(concepts, config, con):
    filtered_tables_root = config["filtered_tables_root"]
    metrics_root = config["metrics_root"]

    filtered_works_root = filtered_tables_root / "filtered_works"
    filtered_scores_root = filtered_tables_root / "filtered_scores"
    filtered_citation_edges_root = filtered_tables_root / "filtered_citation_edges"

    works_metrics_root = metrics_root / "works_metrics"
    works_metrics_path_1 = works_metrics_root / "works_metrics_stage_1.parquet"

    temp_works_metrics_1 = works_metrics_path_1.parent / f"{works_metrics_path_1.stem}.{uuid.uuid4().hex[:8]}.tmp.parquet"

    works_metrics_root.mkdir(parents=True, exist_ok=True)

    query = f"""
        COPY (
            WITH
            in_counts AS (
                SELECT
                    cited_work_id AS work_id,
                    COUNT(*) AS local_in_citations
                FROM read_parquet(
                    '{filtered_citation_edges_root}/*.parquet'
                )
                GROUP BY cited_work_id
            ),
            out_counts AS (
                SELECT
                    citing_work_id AS work_id,
                    COUNT(*) AS local_out_citations
                FROM read_parquet(
                    '{filtered_citation_edges_root}/*.parquet'
                )
                GROUP BY citing_work_id
            ),
            concept_scores_metrics AS (
                SELECT
                    work_id,
                    MAX(concept_score) AS seed_concept_max_score,
                    SUM(concept_score) AS seed_concept_sum_score,
                    COUNT(DISTINCT concept_id) AS seed_concept_count
                FROM read_parquet(
                    '{filtered_scores_root}/*.parquet'
                )
                WHERE concept_id IN (
                    SELECT *
                    FROM UNNEST(?)
                )
                GROUP BY work_id
            )
            SELECT
                filtered_work_ids.work_id,
                COALESCE(in_counts.local_in_citations, 0) AS local_in_citations,
                COALESCE(out_counts.local_out_citations, 0) AS local_out_citations,
                COALESCE(concept_scores_metrics.seed_concept_max_score, 0.0) AS seed_concept_max_score,
                COALESCE(concept_scores_metrics.seed_concept_sum_score, 0.0) AS seed_concept_sum_score,
                COALESCE(concept_scores_metrics.seed_concept_count, 0) AS seed_concept_count
            FROM
                (
                    SELECT work_id
                    FROM read_parquet('{filtered_works_root}/*.parquet')
                ) filtered_work_ids
                LEFT JOIN in_counts
                ON filtered_work_ids.work_id = in_counts.work_id
                LEFT JOIN out_counts
                ON filtered_work_ids.work_id = out_counts.work_id
                LEFT JOIN concept_scores_metrics
                ON filtered_work_ids.work_id = concept_scores_metrics.work_id
        )
        TO '{temp_works_metrics_1}'
        (
            FORMAT PARQUET, 
            COMPRESSION ZSTD
        )
    """

    logger.info("Creating citations and local concepts works metrics")
    con.execute(query, [concepts])
    os.replace(temp_works_metrics_1, works_metrics_path_1)


def create_works_metrics_stage_2(config, con):
    filtered_tables_root = config["filtered_tables_root"]
    metrics_root = config["metrics_root"]

    filtered_scores_root = filtered_tables_root / "filtered_scores"
    filtered_authorships_root = filtered_tables_root / "filtered_authorships"
    filtered_affiliations_root = filtered_tables_root / "filtered_affiliations"

    works_metrics_root = metrics_root / "works_metrics"
    works_metrics_path_1 = works_metrics_root / "works_metrics_stage_1.parquet"
    works_metrics_path_2 = works_metrics_root / "works_metrics_stage_2.parquet"

    temp_works_metrics_2 = works_metrics_path_2.parent / f"{works_metrics_path_2.stem}.{uuid.uuid4().hex[:8]}.tmp.parquet"

    query = f"""
        COPY (
            WITH
            author_counts AS (
                SELECT
                    work_id,
                    COUNT(DISTINCT author_id) as authors_count
                FROM read_parquet(
                    '{filtered_authorships_root}/*.parquet'
                )
                GROUP BY work_id
            ),
            institution_counts AS (
                SELECT
                    work_id,
                    COUNT(DISTINCT institution_id) as institutions_count
                FROM read_parquet(
                    '{filtered_affiliations_root}/*.parquet'
                )
                GROUP BY work_id
            ),
            concept_counts AS (
                SELECT
                    work_id,
                    COUNT(DISTINCT concept_id) as concepts_count
                FROM read_parquet(
                    '{filtered_scores_root}/*.parquet'
                )
                GROUP BY work_id
            )
            SELECT
                works_metrics.*,
                COALESCE(author_counts.authors_count, 0) AS authors_count,
                COALESCE(institution_counts.institutions_count, 0) AS institutions_count,
                COALESCE(concept_counts.concepts_count, 0) AS concepts_count
            FROM
                read_parquet(
                    '{works_metrics_path_1}'
                ) works_metrics
                LEFT JOIN author_counts
                ON works_metrics.work_id = author_counts.work_id
                LEFT JOIN institution_counts
                ON works_metrics.work_id = institution_counts.work_id
                LEFT JOIN concept_counts
                ON works_metrics.work_id = concept_counts.work_id
        )
        TO '{temp_works_metrics_2}'
        (
            FORMAT PARQUET, 
            COMPRESSION ZSTD
        )
    """

    logger.info("Creating author/institution/concept count works metrics")
    con.execute(query)
    os.replace(temp_works_metrics_2, works_metrics_path_2)

    if works_metrics_path_2.exists() and writers.is_valid_parquet(works_metrics_path_2):
        works_metrics_path_1.unlink()
    

def create_works_metrics_final_stage(config, con):
    tables_root = config["tables_root"]
    filtered_tables_root = config["filtered_tables_root"]
    metrics_root = config["metrics_root"]

    concepts_table_root = tables_root / "concepts"
    filtered_scores_root = filtered_tables_root / "filtered_scores"

    works_metrics_root = metrics_root / "works_metrics"
    works_metrics_path_2 = works_metrics_root / "works_metrics_stage_2.parquet"
    works_metrics_path_final = works_metrics_root / "works_metrics.parquet"

    temp_works_metrics_final = works_metrics_path_final.parent / f"{works_metrics_path_final.stem}.{uuid.uuid4().hex[:8]}.tmp.parquet"

    query = f"""
        COPY (
            WITH
            top_concept_metrics AS (
                SELECT
                    scores.work_id,
                    concepts.concept_name AS top_concept_name,
                    scores.concept_id AS top_concept_id,
                    scores.concept_score AS top_concept_score
                FROM 
                    read_parquet(
                        '{filtered_scores_root}/*.parquet'
                    ) scores
                    LEFT JOIN read_parquet(
                        '{concepts_table_root}/*.parquet'
                    ) concepts
                    ON scores.concept_id = concepts.concept_id
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY work_id
                    ORDER BY 
                        scores.concept_score DESC,
                        scores.concept_id,
                        COALESCE(concepts.concept_name, '')
                ) = 1
            )
            SELECT
                works_metrics.*,
                top_concept_metrics.top_concept_name,
                top_concept_metrics.top_concept_id,
                COALESCE(top_concept_metrics.top_concept_score, 0.0) AS top_concept_score
            FROM
                read_parquet(
                    '{works_metrics_path_2}'
                ) works_metrics
                LEFT JOIN top_concept_metrics
                ON works_metrics.work_id = top_concept_metrics.work_id
        )
        TO '{temp_works_metrics_final}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
    """

    logger.info("Creating top concepts works metrics")
    con.execute(query)
    os.replace(temp_works_metrics_final, works_metrics_path_final)

    if works_metrics_path_final.exists() and writers.is_valid_parquet(works_metrics_path_final):
        works_metrics_path_2.unlink()
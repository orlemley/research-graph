import logging
from research_graph.processing import writers


logger = logging.getLogger(__name__)


def filter_work_ids(buckets_count, config, con):
    output_root = config['filtered_tables_root']

    work_ids_output_root = output_root / "filtered_work_ids"
    temp_scores_root = output_root / "temp_scores"
    filtered_citation_edges_root = output_root / "filtered_citation_edges"

    writers.check_required_parquet_files(temp_scores_root, "temp_scores")
    writers.check_required_parquet_files(filtered_citation_edges_root, "filtered_citation_edges")

    work_ids_output_root.mkdir(parents=True, exist_ok=True)

    writers.reset_folder(work_ids_output_root)

    query = f'''
        COPY (
            SELECT 
                DISTINCT work_id, 
                abs(hash(work_id)) % {buckets_count} AS bucket
            FROM (
                SELECT work_id 
                FROM read_parquet(
                    '{temp_scores_root}/*.parquet'
                )
                UNION ALL
                SELECT cited_work_id AS work_id
                FROM read_parquet(
                    '{filtered_citation_edges_root}/*.parquet'
                )
                UNION ALL
                SELECT citing_work_id AS work_id
                FROM read_parquet(
                    '{filtered_citation_edges_root}/*.parquet'
                )
            )
            WHERE work_id IS NOT NULL
        )
        TO '{work_ids_output_root}'
        (
            FORMAT PARQUET,
            PARTITION_BY (bucket),
            COMPRESSION ZSTD
        )
    '''

    logger.info(f"Executing work ids filtering query")         
    con.execute(query)
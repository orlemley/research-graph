import logging
import uuid
import os


logger = logging.getLogger(__name__)


def filter_citation_edges(file_number, config, con):
    tables_root = config["tables_root"]
    filtered_tables_root = config["filtered_tables_root"]
    filtered_scores_root = filtered_tables_root / "temp_scores"

    citation_edges_table_root = tables_root / "citation_edges"
    filtered_citation_edges_root = filtered_tables_root / "filtered_citation_edges"

    filtered_citation_edges_root.mkdir(parents=True, exist_ok=True)

    input_path = citation_edges_table_root / f"citation_edges_{file_number}.parquet"
    output_path = filtered_citation_edges_root / f"filtered_citation_edges_{file_number}.parquet"

    temp_output = output_path.parent / f"{output_path.stem}.{uuid.uuid4().hex[:8]}.tmp.parquet"

    query = f'''
        COPY (
            SELECT *
            FROM read_parquet(
                '{input_path}'
            )
            WHERE citing_work_id IN (
                SELECT work_id
                FROM read_parquet(
                '{filtered_scores_root}/*.parquet'
                )
            )
            UNION
            SELECT *
            FROM read_parquet(
                '{input_path}'
            )
            WHERE cited_work_id IN (
                SELECT work_id
                FROM read_parquet(
                '{filtered_scores_root}/*.parquet'
                )
            )
        )
        TO '{temp_output}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
    '''

    logger.info(f"Filtering citation edges bucket {file_number}")
    con.execute(query)

    os.replace(temp_output, output_path)
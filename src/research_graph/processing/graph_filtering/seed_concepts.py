import logging
import uuid
import os


logger = logging.getLogger(__name__)


def create_temp_scores(concepts, file_number, config, con):
    tables_root = config["tables_root"]
    filtered_tables_root = config["filtered_tables_root"]

    scores_table_root = tables_root / "scores"
    temp_scores_root = filtered_tables_root / "temp_scores"

    temp_scores_root.mkdir(parents=True, exist_ok=True)

    input_path = scores_table_root / f"scores_{file_number}.parquet"
    output_path = temp_scores_root / f"temp_scores_{file_number}.parquet"

    temp_output = output_path.parent / f"{output_path.stem}.{uuid.uuid4().hex[:8]}.tmp.parquet"

    query = f'''
        COPY (
            SELECT *
            FROM read_parquet(
                '{input_path}'
            )
            WHERE concept_id IN (
            SELECT *
            FROM UNNEST(?)
            )
        )
        TO '{temp_output}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
    '''

    logger.info(f"Creating seed concepts from bucket {file_number}")
    con.execute(query, [concepts])

    os.replace(temp_output, output_path)
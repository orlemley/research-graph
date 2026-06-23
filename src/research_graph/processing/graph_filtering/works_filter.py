import logging
import uuid
import os
from research_graph.processing.io import writers


logger = logging.getLogger(__name__)


def filter_works_table(file_number, config, con):
    input_root = config['tables_root']
    output_root = config['filtered_tables_root']

    works_input_root = input_root / "works"
    works_output_root = output_root / "filtered_works"
    filtered_work_ids_input_root = output_root / "filtered_work_ids"

    writers.check_required_parquet_files(filtered_work_ids_input_root, "filtered_work_ids")

    works_output_root.mkdir(parents=True, exist_ok=True)

    output_path = works_output_root / f"filtered_works_{file_number}.parquet"
    works_input_path = works_input_root / f"works_{file_number}.parquet"
    temp_output = output_path.parent / f"{output_path.stem}.{uuid.uuid4().hex[:8]}.tmp.parquet"

    query = f'''
        COPY (
            SELECT works.*
            FROM 
                read_parquet(
                    '{works_input_path}'
                ) AS works
                INNER JOIN read_parquet(
                    '{filtered_work_ids_input_root}/**/*.parquet'
                ) AS work_ids
                ON works.work_id = work_ids.work_id
        )
        TO '{temp_output}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
    '''

    logger.info(f"Filtering works bucket {file_number}")
    con.execute(query)

    os.replace(temp_output, output_path)
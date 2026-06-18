import logging
import uuid
import os
from research_graph.processing import writers


logger = logging.getLogger(__name__)


def filter_relationship_table(name, file_number, config, con):
    tables_root = config["tables_root"]
    filtered_tables_root = config["filtered_tables_root"]

    sub_tables_root = tables_root / name
    sub_filtered_tables_root = filtered_tables_root / f"filtered_{name}"
    filtered_work_ids_input_root = filtered_tables_root / "filtered_work_ids"

    writers.check_required_parquet_files(filtered_work_ids_input_root, "filtered_work_ids")

    sub_filtered_tables_root.mkdir(parents=True, exist_ok=True)

    input_path = sub_tables_root / f"{name}_{file_number}.parquet"
    output_path = sub_filtered_tables_root / f"filtered_{name}_{file_number}.parquet"

    temp_output = output_path.parent / f"{output_path.stem}.{uuid.uuid4().hex[:8]}.tmp.parquet"

    query = f'''
        COPY (
            SELECT rt.*
            FROM
                read_parquet(
                    '{input_path}'
                ) AS rt
                INNER JOIN read_parquet(
                    '{filtered_work_ids_input_root}/**/*.parquet'
                ) AS work_ids
                ON rt.work_id = work_ids.work_id
        )
        TO '{temp_output}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
    '''

    logger.info(f"Filtering {name} bucket {file_number}")
    con.execute(query)

    os.replace(temp_output, output_path)
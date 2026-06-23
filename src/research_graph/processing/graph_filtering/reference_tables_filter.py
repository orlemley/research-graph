import logging
import uuid
import os
from research_graph.processing.io import writers


logger = logging.getLogger(__name__)


def filter_reference_table(file_number, name, values, config, con):
    input_root = config['tables_root']
    output_root = config['filtered_tables_root']

    id_column = values['id_column']
    base_table = values['base_table']

    table_input_root = input_root / name
    table_output_root = output_root / f"filtered_{name}"
    filtered_base_table_input_root = output_root / f"filtered_{base_table}"

    writers.check_required_parquet_files(filtered_base_table_input_root, f"filtered_{base_table}")

    table_output_root.mkdir(parents=True, exist_ok=True)

    output_path = table_output_root / f"filtered_{name}_{file_number}.parquet"
    table_input_path = table_input_root / f"{name}_{file_number}.parquet"
    temp_output = output_path.parent / f"{output_path.stem}.{uuid.uuid4().hex[:8]}.tmp.parquet"

    query = f'''
    COPY (
        SELECT rt.*
        FROM read_parquet(
            '{table_input_path}'
        ) AS rt
        WHERE EXISTS (
            SELECT 1
            FROM read_parquet(
                '{filtered_base_table_input_root}/*.parquet'
            ) AS bt
            WHERE bt.{id_column} = rt.{id_column}
        )
    )
    TO '{temp_output}'
    (
        FORMAT PARQUET,
        COMPRESSION ZSTD
    )
'''

    logger.info(f"Filtering {name} bucket {file_number}")
    con.execute(query)

    os.replace(temp_output,output_path)
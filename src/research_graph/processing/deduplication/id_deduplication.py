import logging
import time
import uuid
import os


logger = logging.getLogger(__name__)


def deduplicate_by_id(bucket, name, values, config, con):
    temp_buckets_root = config["temp_buckets_root"]
    sub_buckets_root = temp_buckets_root / name

    if not (sub_buckets_root / f"bucket={bucket}").exists():
        logger.error(f"{name} missing bucket={bucket}")
        raise RuntimeError(f"{name} missing bucket={bucket}")

    tables_root = config["tables_root"]
    output_root = tables_root / name

    output_root.mkdir(parents=True, exist_ok=True)

    id_column = values['id']
    sorting_column = values['sortby']

    output_name = output_root / f"{name}_{bucket}.parquet"
    temp_output = output_name.parent / f"{output_name.stem}.{uuid.uuid4().hex[:8]}.tmp.parquet"

    query = (f"""
        COPY (
            SELECT * EXCLUDE (bucket)
            FROM read_parquet(
                '{sub_buckets_root}/bucket={bucket}/*.parquet'
            )
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY {id_column}
                ORDER BY 
                    filename DESC,
                    {sorting_column} DESC NULLS LAST
            ) = 1
        )
        TO '{temp_output}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
    """)

    start_time = time.perf_counter()

    logger.info(f"Deduplicating {name} bucket {bucket}")
    con.execute(query)

    row_count = con.execute(f"""
    SELECT COUNT(*)
    FROM read_parquet('{temp_output}')
    """).fetchone()[0]

    if row_count == 0:
        logger.error(f"Deduplicated {name}_{bucket} output is empty")
        raise ValueError(f"Deduplicated {name}_{bucket} output is empty")
    
    elapsed = time.perf_counter() - start_time

    rows_per_second = row_count / elapsed if elapsed > 0 else 0

    duplicate_count = con.execute(f"""
        SELECT COUNT(*)
        FROM (
            SELECT
                {id_column},
                COUNT(*) AS c
            FROM read_parquet(
                '{temp_output}'
            )
            GROUP BY {id_column}
            HAVING c > 1
        )
    """).fetchone()[0]

    if duplicate_count > 0:
        logger.error(f"Found {duplicate_count:,} duplicate {id_column}s in {name}_{bucket}")
        raise ValueError(f"Found {duplicate_count:,} duplicate {id_column}s in {name}_{bucket}")
    else:
        logger.info(f"Found {duplicate_count:,} duplicate {id_column}s in {name}_{bucket}")

    os.replace(temp_output, output_name)
    logger.info(f"Wrote {row_count:,} deduplicated {name} rows to {name}_{bucket}")

    return rows_per_second
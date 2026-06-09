import logging
import shutil
import uuid
import os


logger = logging.getLogger(__name__)


def deduplicate_by_row(bucket, name, config, con):
    temp_buckets_root = config["temp_buckets_root"]
    sub_buckets_root = temp_buckets_root / name

    if not (sub_buckets_root / f"bucket={bucket}").exists():
        logger.error(f"{name} missing bucket={bucket}")
        raise

    tables_root = config["tables_root"]
    output_root = tables_root / name

    output_root.mkdir(parents=True, exist_ok=True)

    output_name = output_root / f"{name}_{bucket}.parquet"
    temp_output = output_name.parent / f"{output_name.stem}.{uuid.uuid4().hex[:8]}.tmp.parquet"

    query = (f"""
        COPY (
            SELECT DISTINCT *
            FROM read_parquet(
                '{sub_buckets_root}/bucket={bucket}/*.parquet'
            )
        )
        TO '{temp_output}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
    """)

    logger.info(f"Deduplicating {name} bucket {bucket}")
    con.execute(query)

    row_count = con.execute(f"""
    SELECT COUNT(*)
    FROM read_parquet('{temp_output}')
    """).fetchone()[0]

    if row_count == 0:
        logger.error(f"Deduplicated {name}_{bucket} output is empty")
        raise ValueError(f"Deduplicated {name}_{bucket} output is empty")

    duplicate_count = con.execute(f"""
        SELECT COUNT(*)
        FROM (
            SELECT *
            FROM read_parquet('{temp_output}')
            GROUP BY *
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    if duplicate_count > 0:
        logger.error(f"Found {duplicate_count:,} duplicate rows in {name}_{bucket}")
        raise ValueError(f"Found {duplicate_count:,} duplicate rows in {name}_{bucket}")
    else:
        logger.info(f"Found {duplicate_count:,} duplicate rows in {name}_{bucket}")

    os.replace(temp_output, output_name)
    logger.info(f"Wrote {row_count:,} deduplicated {name} rows to {name}_{bucket}")
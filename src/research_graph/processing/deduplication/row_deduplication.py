import logging
import time
import uuid
import os


logger = logging.getLogger(__name__)


def deduplicate_by_row(bucket, name, values, config, con):
    temp_buckets_root = config["temp_buckets_root"]
    sub_buckets_root = temp_buckets_root / name

    if not (sub_buckets_root / f"bucket={bucket}").exists():
        logger.error(f"{name} missing bucket={bucket}")
        raise RuntimeError(f"{name} missing bucket={bucket}")

    tables_root = config["tables_root"]
    output_root = tables_root / name

    output_root.mkdir(parents=True, exist_ok=True)

    deduplication_key_sql = values["deduplication_key_sql"]

    columns = [col.strip() for col in deduplication_key_sql.split(",")]
    key_not_null_sql = " AND ".join(f"{column} IS NOT NULL" for column in columns)
    key_has_null_sql = " OR ".join(f"{column} IS NULL" for column in columns)

    output_name = output_root / f"{name}_{bucket}.parquet"
    temp_output = output_name.parent / f"{output_name.stem}.{uuid.uuid4().hex[:8]}.tmp.parquet"

    query = (f"""
        COPY (
            SELECT 
                {deduplication_key_sql},
                MAX(snapshot_date) AS snapshot_date
            FROM read_parquet(
                '{sub_buckets_root}/bucket={bucket}/*.parquet'
            )
            WHERE {key_not_null_sql}
            GROUP BY {deduplication_key_sql}
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

    null_rows = con.execute(f"""
        SELECT COUNT(*)
        FROM read_parquet('{temp_output}')
        WHERE {key_has_null_sql}
    """).fetchone()[0]

    if null_rows > 0:
        logger.error(f"Deduplicated {name}_{bucket} output contains NULL keys")
        raise ValueError(f"Deduplicated {name}_{bucket} output contains NULL keys")

    duplicate_count = con.execute(f"""
        SELECT COUNT(*)
        FROM (
            SELECT {deduplication_key_sql}
            FROM read_parquet('{temp_output}')
            GROUP BY {deduplication_key_sql}
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    if duplicate_count > 0:
        logger.error(f"Found {duplicate_count:,} duplicates in {name}_{bucket}")
        raise ValueError(f"Found {duplicate_count:,} duplicates in {name}_{bucket}")
    else:
        logger.info(f"Found {duplicate_count:,} duplicates in {name}_{bucket}")

    os.replace(temp_output, output_name)
    logger.info(f"Wrote {row_count:,} deduplicated {name} rows to {name}_{bucket}")

    return rows_per_second
import logging
import json
from research_graph.processing.io import writers


logger = logging.getLogger(__name__)


def partition_by_key(name, values, config, con):
    shards_root = config["shards_root"]
    temp_buckets_root = config["temp_buckets_root"]

    sub_shards_root = shards_root / name
    sub_buckets_root = temp_buckets_root / name

    sub_buckets_root.mkdir(parents=True, exist_ok=True)

    deduplication_key_sql = values['deduplication_key_sql']
    buckets_count = values['buckets_count']

    input_paths_sql = writers.get_sql_input_paths(name, sub_shards_root)
    if not input_paths_sql:
        logger.warning(f"No input shards found for {name} table, proceeding to next table...")
        return False
    
    snapshot_date_regex = r"([0-9]{4}-[0-9]{2}-[0-9]{2})"

    writers.reset_folder(sub_buckets_root)
    
    query = f"""
        COPY (
            SELECT
                * EXCLUDE (filename),
                regexp_extract(filename, '{snapshot_date_regex}', 1) AS snapshot_date,
                abs(hash({deduplication_key_sql})) % {buckets_count} AS bucket
            FROM read_parquet(
                [{input_paths_sql}],
                filename=true
            )
        )
        TO '{sub_buckets_root}'
        (
            FORMAT PARQUET,
            PARTITION_BY (bucket),
            COMPRESSION ZSTD
        )
    """

    logger.info(f"Running {name} partitioning query")
    con.execute(query)
    
    done_file = sub_buckets_root / "done.json"
    with done_file.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "deduplication_key_sql": values['deduplication_key_sql'],
                "order_by_sql": values['order_by_sql'],
                "buckets_count": values['buckets_count'],
            },
            f,
            indent=2
        )
        
    return True
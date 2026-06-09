import logging
import json
from research_graph.processing import writers


logger = logging.getLogger(__name__)


def partition_by_row(name, buckets_count, config, con):
    shards_root = config["shards_root"]
    temp_buckets_root = config["temp_buckets_root"]

    sub_shards_root = shards_root / name
    sub_buckets_root = temp_buckets_root / name

    sub_buckets_root.mkdir(parents=True, exist_ok=True)

    input_paths_sql = writers.get_sql_input_paths(name, sub_shards_root)
    if not input_paths_sql:
        logger.warning(f"No input shards found for {name} table, proceeding to next table...")
        return False

    writers.reset_folder(sub_buckets_root)
    
    query = f"""
        COPY (
            SELECT
                *,
                abs(hash(*columns(*))) % {buckets_count} AS bucket
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
    with done_file.open("w") as f:
        json.dump({"buckets_count": buckets_count}, f, indent=2)
        
    return True
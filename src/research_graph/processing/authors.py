import logging
from research_graph.processing import schemas
from research_graph.processing import paths
from research_graph.processing import transforms
from research_graph.processing import shards
from research_graph.processing import writers
import time


logger = logging.getLogger(__name__)


def process_authors_shard(shard_key, context):
    config = context.config

    shard_name = shards.get_shard_name(shard_key)
    output_path = paths.authors_output_path(shard_name, config["shards_root"])
    
    authors_writer = writers.BatchedParquetWriter(output_path,schemas.AUTHORS_SCHEMA, compression=config["parquet_compression"])
    
    start_time = time.perf_counter()

    shard_authors_processed = 0
    shard_authors_failed = 0

    logger.info(f"{shard_name} started")

    for author in shards.open_shard(shard_key, context):
        try:
            authors_writer.write(transforms.get_authors_row(author))
        except Exception as e:
            logger.warning(f"Failed author {author.get('id')}: {e}")
            shard_authors_failed += 1
            continue

        shard_authors_processed += 1

    authors_writer.close()

    elapsed = time.perf_counter() - start_time

    authors_per_second = shard_authors_processed / elapsed if elapsed > 0 else 0

    logger.info(f"{shard_name} finished")

    return {
        "success": True,
        "authors_processed": shard_authors_processed,
        "authors_failed": shard_authors_failed,
        "time_to_complete": elapsed,
        "authors_per_second": authors_per_second
    }
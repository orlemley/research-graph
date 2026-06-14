import logging
import time
import uuid
import json
from research_graph.processing import schemas
from research_graph.processing import paths
from research_graph.processing import transforms
from research_graph.processing import shards
from research_graph.processing import writers


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
            logger.exception(f"Failed author {author.get('id')}: {e}")
            shard_authors_failed += 1
            failure_path = config["shards_root"] / f"{shard_name}.authors_failures.jsonl"
            try:
                writers.write_failure_record(
                    failure_path, 
                    {
                        "shard_key": shard_key,
                        "shard_name": shard_name,
                        "stage": "transform",
                        "work_id": author.get("id", None),
                        "error_type": type(e).__name__,
                        "error": str(e),
                    }
                )
            except Exception:
                logger.exception(f"Failed to write author failure record")
                raise RuntimeError
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
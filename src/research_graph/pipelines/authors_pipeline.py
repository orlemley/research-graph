import logging
import time
from botocore.exceptions import ResponseStreamingError, BotoCoreError
from research_graph.processing import paths
from research_graph.processing.io import shards
from research_graph.processing.io import writers
from research_graph.processing.ingestion import authors


logger = logging.getLogger(__name__)


def run(context):  
    logger.info("Starting authors pipeline")

    config = context.config

    s3 = context.s3

    paginator = s3.get_paginator('list_objects_v2')

    pages = paginator.paginate(Bucket=config["openalex_bucket"], Prefix='data/authors/')

    authors_count = 0
    for page in pages:
        if 'Contents' not in page:
            continue

        for obj in page['Contents']:
            shard_key = obj['Key']
            shard_name = shards.get_shard_name(shard_key)
            output_path = paths.authors_output_path(shard_name, config["shards_root"])
            if (output_path.exists()) and writers.is_valid_parquet(output_path):
                logger.info(f"Skipping {shard_key}, already fully processed.")
                authors_count += 1
                logger.info(f"{authors_count} total authors shards processed")
            else:
                if output_path.exists():
                    logger.warning(f"{shard_name} already partially processed or corrupt, deleting shard outputs before reprocessing.")
                    writers.cleanup_shard_outputs({"authors": output_path})

                processed = None
                delay = 1
                for attempt in range(config["max_connection_retries"]):
                    try:
                        processed = authors.process_authors_shard(shard_key, context)
                        break
                    except (ResponseStreamingError, BotoCoreError) as e:
                        if attempt == config["max_connection_retries"] - 1:
                            logger.exception(f"Exhausted retries for {shard_key}")
                            raise e 
                        logger.warning(f"Retrying shard {shard_key} in {delay}s. Error: {e}")
                        time.sleep(delay)
                        delay *= 2

                if processed["success"]:
                    authors_count += 1
                    logger.info(f"{authors_count} total authors shards processed")
                
                logger.info(f"{shard_name}: {processed['authors_processed']} authors successfully processed")
                if processed["authors_failed"] > 0:
                    logger.warning(f"{shard_name}: {processed['authors_failed']} authors failed to process")
                else:
                    logger.info(f"{shard_name}: {processed['authors_failed']} authors failed to process")

                logger.info(f"completed in {processed['time_to_complete']:.2f}s")
                logger.info(f"({processed['authors_per_second']:.2f} authors/sec)")
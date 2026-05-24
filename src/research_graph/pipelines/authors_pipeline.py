import logging
from research_graph.processing import paths
from research_graph.processing import shards
from research_graph.processing import writers
from research_graph.processing import authors


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
            output_path = paths.authors_output_path(shard_name, config["data_root"])
            if (output_path.exists()) and writers.is_valid_parquet(output_path):
                logger.info(f"Skipping {shard_key}, already fully processed.")
                authors_count += 1
                logger.info(f"{authors_count} total authors shards processed")
            else:
                if output_path.exists():
                    logger.warning(f"{shard_name} already partially processed or corrupt, deleting shard outputs before reprocessing.")
                    writers.cleanup_shard_outputs({"authors": output_path})

                processed = None
                try:
                    processed = authors.process_authors_shard(shard_key, context)
                except Exception:
                    logger.exception(f"Failed shard {shard_key}")
                    continue

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
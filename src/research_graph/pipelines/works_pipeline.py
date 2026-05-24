import logging
from research_graph.processing import paths
from research_graph.processing import shards
from research_graph.processing import writers
from research_graph.processing import works


logger = logging.getLogger(__name__)


def run(context):
    logger.info("Starting works pipeline")

    config = context.config

    s3 = context.s3

    paginator = s3.get_paginator('list_objects_v2')

    pages = paginator.paginate(Bucket=config["openalex_bucket"], Prefix='data/works/')

    works_count = 0
    for page in pages:
        if 'Contents' not in page:
            continue
        
        for obj in page['Contents']:
            shard_key = obj['Key']
            shard_name = shards.get_shard_name(shard_key)
            output_paths = paths.works_output_paths(shard_name, config["data_root"])
            paths_to_check = output_paths.copy()
            if not config['selected_concepts']["enabled"]:
                paths_to_check.pop("selected_concepts")
            path_valid = []
            for path in paths_to_check.values():
                path_valid.append(path.exists() and writers.is_valid_parquet(path))
            unexpected_outputs_exist = False
            for name, path in output_paths.items():
                if name not in paths_to_check and path.exists():
                    unexpected_outputs_exist = True
            if all(path_valid) and not unexpected_outputs_exist:
                logger.info(f"Skipping {shard_key}, already fully processed.")
                works_count += 1
                logger.info(f"{works_count} works shards processed")
            else:
                output_path_exists = []
                for output_path in output_paths.values():
                    output_path_exists.append(output_path.exists())
                if any(output_path_exists):
                    logger.warning(f"{shard_name} already partially processed or corrupt, deleting shard outputs before reprocessing.")
                    writers.cleanup_shard_outputs(output_paths)

                processed = None
                try:
                    processed = works.process_works_shard(shard_key, context)
                except Exception:
                    logger.exception(f"Failed shard {shard_key}")
                    continue

                if processed["success"]:
                    works_count += 1
                    logger.info(f"{works_count} works shards processed")

                logger.info(f"{shard_name}: {processed['papers_processed']} papers successfully processed")
                logger.info(f"{shard_name}: {processed['papers_filtered']} papers filtered out due to publication year not in range")
                if processed["papers_failed"] > 0:
                    logger.warning(f"{shard_name}: {processed['papers_failed']} papers failed to process")
                else:
                    logger.info(f"{shard_name}: {processed['papers_failed']} papers failed to process")

                logger.info(f"completed in {processed['time_to_complete']:.2f}s")
                logger.info(f"({processed['papers_per_second']:.2f} papers/sec)")
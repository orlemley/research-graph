import gzip
import json
from pathlib import Path
import logging
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)


def get_shard_name(shard_key):
    shard_path = Path(shard_key)
    if "works" in shard_path.parts:
        parts_after = shard_path.parts[shard_path.parts.index("works") + 1 :]
    elif "authors" in shard_path.parts:
        parts_after = shard_path.parts[shard_path.parts.index("authors") + 1 :]
    else:
        logger.exception(f"Unknown shard path: {shard_key}")
        raise ValueError(f"Unknown shard path: {shard_key}")
    shard_name = "_".join(parts_after).replace(".gz","")
    return shard_name


def open_shard(shard_key, context):
    config = context.config
    s3 = context.s3

    try:
        response = s3.get_object(Bucket=config["openalex_bucket"], Key=shard_key)
    except ClientError as e:
        logger.exception(f"Error downloading {shard_key}: {e}")
        raise
    except Exception as e:
        logger.exception(f"Unexpected error while downloading {shard_key}: {e}")
        raise
    
    if shard_key.endswith('.gz'):
        with gzip.GzipFile(fileobj=response['Body']) as f:
            logger.info(f"File {shard_key} is gzipped. Processing lines...")
            for line in f:
                try:
                    paper = json.loads(line)
                    #print(f"Yielding paper {paper.get('id', 'Unknown ID')}")
                    yield paper
                except json.JSONDecodeError as e:
                    logger.info(f"Skipping malformed JSON in {shard_key}: {e}")
    elif shard_key.endswith('.json'):
        body = response['Body'].read().decode('utf-8')
        logger.info(f"File {shard_key} is a JSON file. Processing lines...")
        for line in body.splitlines():
            if line.strip():
                try:
                    paper = json.loads(line)
                    #print(f"Yielding paper {paper.get('id', 'Unknown ID')}")
                    yield paper
                except json.JSONDecodeError as e:
                    logger.info(f"Skipping malformed JSON in {shard_key}: {e}")
    else:
        logger.warning(f"Skipping unknown file type: {shard_key}")
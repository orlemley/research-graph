import logging
import json
import shutil
import duckdb
from research_graph.processing import id_deduplication
from research_graph.processing import id_partitioning
from research_graph.processing import row_deduplication
from research_graph.processing import row_partitioning
from research_graph.processing import duplicate_checks
from research_graph.processing import tables_info


logger = logging.getLogger(__name__)


def run(context):
    config = context.config

    tables_root = config["tables_root"]
    temp_buckets_root = config["temp_buckets_root"]
    
    tables_root.mkdir(parents=True, exist_ok=True)
    temp_buckets_root.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        for name, values in tables_info.REFERENCE_TABLES.items():
            id_column = values['id']
            buckets_count = values['buckets_count']
            output_root = tables_root / name
            sub_buckets_root = temp_buckets_root / name

            expected = {
                "id_column": id_column,
                "sorting_column": values["sortby"],
                "buckets_count": buckets_count
            }
            if(sub_buckets_root / "done.json").exists():
                done_file = sub_buckets_root / "done.json"
                with done_file.open() as f:
                    actual = json.load(f)
            else:
                actual = None

            shards_exist = True
            if not ((sub_buckets_root / "done.json").exists() and (actual == expected)) and not (output_root / ".done").exists():
                shards_exist = id_partitioning.partition_by_id(name, values, config, con)
                logger.info(f"Completed {name} partitioning")

            if(sub_buckets_root / "done.json").exists():
                done_file = sub_buckets_root / "done.json"
                with done_file.open() as f:
                    actual = json.load(f)
            else:
                actual = None

            if (sub_buckets_root / "done.json").exists() and (actual == expected):
                for bucket in range(buckets_count):
                    if not (output_root / f"{name}_{bucket}.parquet").exists():
                        id_deduplication.deduplicate_by_id(bucket, name, values, config, con)

                logger.info(f"Checking for duplicate {id_column}s across all {name} outputs")
                duplicate_count = duplicate_checks.check_duplicate_ids(output_root, id_column, con)
                if duplicate_count > 0:
                    logger.error(f"Found {duplicate_count:,} duplicate {id_column}s in {name} output")
                    raise ValueError(f"Found {duplicate_count:,} duplicate {id_column}s in {name} output")
                else:
                    logger.info(f"Found {duplicate_count:,} duplicate {id_column}s in {name} output")

                (output_root / ".done").touch(exist_ok=True)
                logger.info(f"Completed all deduplication for {name} table")
                logger.info(f"Deleting {name} temporary buckets folder")
                shutil.rmtree(sub_buckets_root)
            elif not shards_exist:
                logger.warning(f"Skipping {name} table deduplication, no shards exist")
                continue
            elif (output_root / ".done").exists():
                pass
            else:
                logger.error(f"Partitioning not complete for {name} table")
                raise
            
        for name, buckets_count in tables_info.RELATIONSHIP_TABLES.items():
            output_root = tables_root / name
            sub_buckets_root = temp_buckets_root / name

            expected = {"buckets_count": buckets_count}
            if(sub_buckets_root / "done.json").exists():
                done_file = sub_buckets_root / "done.json"
                with done_file.open() as f:
                    actual = json.load(f)
            else:
                actual = None

            shards_exist = True
            if not ((sub_buckets_root / "done.json").exists() and (actual == expected)) and not (output_root / ".done").exists():
                shards_exist = row_partitioning.partition_by_row(name, buckets_count, config, con)
                logger.info(f"Completed {name} partitioning")

            if(sub_buckets_root / "done.json").exists():
                done_file = sub_buckets_root / "done.json"
                with done_file.open() as f:
                    actual = json.load(f)
            else:
                actual = None

            if (sub_buckets_root / "done.json").exists() and (actual == expected):
                for bucket in range(buckets_count):
                    if not (output_root / f"{name}_{bucket}.parquet").exists():
                        row_deduplication.deduplicate_by_row(bucket, name, config, con)

                logger.info(f"Checking for duplicate rows across all {name} outputs")
                duplicate_count = duplicate_checks.check_duplicate_rows(output_root, con)
                if duplicate_count > 0:
                    logger.error(f"Found {duplicate_count:,} duplicate rows in {name} output")
                    raise ValueError(f"Found {duplicate_count:,} duplicate rows in {name} output")
                else:
                    logger.info(f"Found {duplicate_count:,} duplicate rows in {name} output")

                (output_root / ".done").touch(exist_ok=True)
                logger.info(f"Completed all deduplication for {name} table")
                logger.info(f"Deleting {name} temporary buckets folder")
                shutil.rmtree(sub_buckets_root)
            elif not shards_exist:
                logger.warning(f"Skipping {name} table deduplication, no shards exist")
                continue
            elif (output_root / ".done").exists():
                pass
            else:
                logger.error(f"Partitioning not complete for {name} table")
                raise

    finally:
        con.close()